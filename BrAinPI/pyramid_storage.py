"""Shared pyramid ledger and a process-wide barrier for cache maintenance.

Only completed generated artifacts are accounted for. Temporary generation
space is deliberately outside this soft quota. All application requests hold
a worker lease until their response iterable is closed; cached readers keep
that lease alive between requests. Cleanup requires an exclusive lease.
"""

from contextlib import contextmanager
import inspect
import logging
import math
import os
from pathlib import Path
import re
import shutil
import sqlite3
import threading
import time

from filelock import FileLock, Timeout

try:
    import fcntl
except ImportError:  # Quotas must fail explicitly on unsupported platforms.
    fcntl = None


log = logging.getLogger(__name__)


def tree_size(path):
    """Logical file bytes, without following symlinks (also used on demand)."""
    path = Path(path)
    if path.is_symlink():
        return 0
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    total = 0

    def fail(error):
        raise error

    for directory, dirs, files in os.walk(path, followlinks=False, onerror=fail):
        dirs[:] = [name for name in dirs if not Path(directory, name).is_symlink()]
        for name in files:
            item = Path(directory, name)
            try:
                if not item.is_symlink():
                    total += item.stat().st_size
            except FileNotFoundError:
                pass  # Concurrent cache eviction / manual deletion.
    return total


def close_resources(owner):
    """Close known reader/store ownership edges, deduplicating aliases.

    Do not traverse arbitrary metadata or close the shared disk cache.
    Exceptions propagate: a failed close must retain the worker's lease.
    """
    seen = set()

    def close(resource):
        if resource is None or id(resource) in seen:
            return
        seen.add(id(resource))
        store = getattr(resource, "store", None)
        if store is not resource:
            close(store)
        callback = getattr(resource, "close", None)
        if callable(callback):
            result = callback()
            if inspect.isawaitable(result):
                from zarr.core.sync import sync
                sync(result)

    for resource in getattr(owner, "arrays", {}).values():
        close(resource)
    for name in ("active_reader", "pyramid_reader", "source_reader", "image", "hf"):
        close(getattr(owner, name, None))
    source = getattr(owner, "source_reader", None)
    uncache = getattr(source, "uncache", None)
    if callable(uncache):
        uncache()
    for holder in getattr(source, "file_map", {}).values():
        close(holder.fileobj)
    if hasattr(owner, "arrays"):
        owner.arrays.clear()
    for name in ("active_reader", "pyramid_reader", "source_reader", "image", "hf"):
        if hasattr(owner, name):
            setattr(owner, name, None)


class PyramidStorage:
    def __init__(self, stores, state_dir, limit_bytes, cleanup_fraction=0.7,
                 poll_seconds=2, drain_timeout=120, retry_seconds=60):
        if fcntl is None:
            raise RuntimeError("Pyramid quota coordination requires POSIX file locks")
        self.stores = [(Path(root).expanduser().absolute(), suffix)
                       for root, suffix in stores]
        self.state_dir = Path(state_dir).expanduser().absolute()
        self.limit_bytes = int(limit_bytes)
        self.cleanup_fraction = float(cleanup_fraction)
        self.poll_seconds = float(poll_seconds)
        self.drain_timeout = float(drain_timeout)
        self.retry_seconds = float(retry_seconds)
        if self.limit_bytes < 0 or not 0 < self.cleanup_fraction < 1:
            raise ValueError("Invalid pyramid quota or cleanup_fraction")
        if min(self.poll_seconds, self.drain_timeout, self.retry_seconds) <= 0:
            raise ValueError("Pyramid maintenance intervals must be positive")
        self.database = self.state_dir / "ledger.sqlite3"
        self.marker = self.state_dir / "cleanup_requested"
        self.lease_path = self.state_dir / "readers.lock"
        self._ready = False
        self._bootstrapped = False

    @classmethod
    def from_settings(cls, settings):
        if not hasattr(settings, "has_section"):
            return None
        shared_root = settings.get("pyramid_store", "path", fallback=None)
        if shared_root is not None:
            if not shared_root.strip():
                raise ValueError("[pyramid_store] path must not be empty")
            shared_root = Path(shared_root.strip()).expanduser().absolute()
        stores = []
        for section, child, suffix in (("tif_loader", "tif", ".ome.tif"),
                                       ("nifti_loader", "nifti", ".nii.zarr"),
                                       ("jp2_loader", "jp2", ".ome.tif")):
            root = shared_root / child if shared_root is not None else settings.get(
                section, "pyramids_images_store", fallback=None
            )
            if root:
                stores.append((root, settings.get(section, "extension_type", fallback=suffix)))
        if not stores:
            return None
        if shared_root is not None:
            state_dir = shared_root / ".brainpi-storage"
        else:
            # Legacy configs continue using their original directories/ledger.
            default_state = Path(stores[0][0]).expanduser().absolute().parent / ".brainpi-storage"
            state_dir = settings.get("pyramid_storage", "state_dir", fallback=str(default_state))

        def option(name, default):
            section = "pyramid_store" if settings.has_option("pyramid_store", name) else "pyramid_storage"
            return settings.getfloat(section, name, fallback=default)

        return cls(
            stores,
            state_dir,
            option("max_size_gb", 50) * 1024**3,
            option("cleanup_fraction", 0.7),
            option("poll_seconds", 2),
            option("drain_timeout_seconds", 120),
            option("retry_seconds", 60),
        )

    def prepare(self):
        if self._ready:
            return
        for root, _suffix in self.stores:
            root.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        with FileLock(str(self.state_dir / "schema.lock")):
            with self.connect() as db:
                db.executescript("""
                    CREATE TABLE IF NOT EXISTS artifacts (
                        path TEXT PRIMARY KEY, size INTEGER NOT NULL,
                        created REAL NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS metadata (
                        key TEXT PRIMARY KEY, value TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS totals (
                        id INTEGER PRIMARY KEY CHECK(id=1), bytes INTEGER NOT NULL,
                        count INTEGER NOT NULL
                    );
                    INSERT OR IGNORE INTO totals
                        SELECT 1, COALESCE(SUM(size),0), COUNT(*) FROM artifacts;
                    CREATE TRIGGER IF NOT EXISTS artifact_insert AFTER INSERT ON artifacts
                    BEGIN
                        UPDATE totals SET bytes=bytes+NEW.size, count=count+1 WHERE id=1;
                    END;
                    CREATE TRIGGER IF NOT EXISTS artifact_update AFTER UPDATE ON artifacts
                    BEGIN
                        UPDATE totals SET bytes=bytes+NEW.size-OLD.size WHERE id=1;
                    END;
                    CREATE TRIGGER IF NOT EXISTS artifact_delete AFTER DELETE ON artifacts
                    BEGIN
                        UPDATE totals SET bytes=bytes-OLD.size, count=count-1 WHERE id=1;
                    END;
                """)
        self._ready = True

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.database, timeout=30)
        try:
            with db:
                yield db
        finally:
            db.close()

    def valid_artifact(self, path):
        """Recognize only our hash/hash/generated-name layout, never symlinks."""
        path = Path(path).absolute()
        for root, suffix in self.stores:
            try:
                parts = path.relative_to(root).parts
            except ValueError:
                continue
            if len(parts) != 3:
                continue
            match = re.fullmatch(r"([0-9a-f]{64})" + re.escape(suffix), parts[2])
            if match and parts[:2] == (match[1][:2], match[1][2:4]):
                return not any(p.is_symlink() for p in (path, *path.parents))
        return False

    def bootstrap(self):
        """One adoption scan per ledger, not one scan per worker or request."""
        if self._bootstrapped:
            return
        self.prepare()
        with FileLock(str(self.state_dir / "bootstrap.lock")):
            with self.connect() as db:
                if db.execute("SELECT 1 FROM metadata WHERE key='initialized'").fetchone():
                    self._bootstrapped = True
                    return
            rows = []
            for root, suffix in self.stores:
                for path in root.glob(f"*/*/*{suffix}"):
                    if self.valid_artifact(path):
                        try:
                            rows.append((str(path), tree_size(path), path.stat().st_mtime))
                        except FileNotFoundError:
                            continue
            with self.connect() as db:
                db.executemany("INSERT OR IGNORE INTO artifacts VALUES (?,?,?)", rows)
                db.execute("INSERT INTO metadata VALUES ('initialized','1')")
            self.request_if_over_limit()
            self._bootstrapped = True

    def snapshot(self):
        self.prepare()
        with self.connect() as db:
            used, count = db.execute("SELECT bytes,count FROM totals WHERE id=1").fetchone()
            initialized = bool(db.execute(
                "SELECT 1 FROM metadata WHERE key='initialized'"
            ).fetchone())
            last = db.execute("SELECT value FROM metadata WHERE key='last_cleanup'").fetchone()
        return {"used_bytes": used, "artifact_count": count,
                "limit_bytes": self.limit_bytes, "cleanup_fraction": self.cleanup_fraction,
                "initialized": initialized, "cleanup_pending": self.marker.exists(),
                "last_cleanup": last[0] if last else None,
                "measurement": "ledger"}

    def request_if_over_limit(self):
        if self.limit_bytes and self.snapshot()["used_bytes"] >= self.limit_bytes:
            self.marker.touch(exist_ok=True)

    def publish(self, temporary, destination):
        """Commit accounting before atomic publication; crashes overcount safely."""
        self.prepare()
        destination = str(Path(destination).absolute())
        if not self.valid_artifact(destination):
            raise ValueError("Refusing to register a path outside generated pyramid storage")
        size = tree_size(temporary)
        with self.connect() as db:
            db.execute("""INSERT INTO artifacts VALUES (?,?,?)
                ON CONFLICT(path) DO UPDATE SET size=excluded.size, created=excluded.created
                """, (destination, size, time.time()))
        try:
            os.replace(temporary, destination)
        finally:
            self.request_if_over_limit()

    @contextmanager
    def subprocess_lease(self):
        """Let a generation child retain protection even if its worker dies."""
        self.prepare()
        with self.lease_path.open("a+b") as lease:
            fcntl.flock(lease, fcntl.LOCK_SH)
            yield lease.fileno()

    def _delete_oldest(self):
        """Caller owns the exclusive global lease. Keep failed deletes charged."""
        with self.connect() as db:
            rows = db.execute("SELECT path,size FROM artifacts ORDER BY created,path").fetchall()
        # Correct manual deletions before choosing the actual cleanup target.
        existing = []
        for path, size in rows:
            if not self.valid_artifact(path):
                log.error("Ignoring unsafe pyramid ledger path: %s", path)
                continue
            if not os.path.lexists(path):
                with self.connect() as db:
                    db.execute("DELETE FROM artifacts WHERE path=?", (path,))
            else:
                existing.append((path, size))
        total = self.snapshot()["used_bytes"]
        if not self.limit_bytes or total < self.limit_bytes:
            return
        target = math.ceil(total * self.cleanup_fraction)
        removed = 0
        for path, size in existing:
            if removed >= target:
                break
            try:
                if Path(path).is_dir():
                    shutil.rmtree(path)
                else:
                    os.unlink(path)
            except OSError:
                log.exception("Could not delete pyramid %s", path)
                continue
            with self.connect() as db:
                db.execute("DELETE FROM artifacts WHERE path=?", (path,))
            removed += size
        log.info("Pyramid cleanup: removed %s of %s target bytes", removed, target)
        with self.connect() as db:
            db.execute("INSERT OR REPLACE INTO metadata VALUES ('last_cleanup',?)",
                       (str(time.time()),))

    def maintain(self, worker):
        """Elect one cleaner. Other workers only drain their local readers."""
        self.bootstrap()
        worker.drain_if_requested()
        try:
            with FileLock(str(self.state_dir / "maintenance.lock"), timeout=0):
                with self.connect() as db:
                    row = db.execute("SELECT value FROM metadata WHERE key='retry_after'").fetchone()
                # A marker left by a dead cleaner must be recovered immediately.
                if not self.marker.exists() and row and float(row[0]) > time.time():
                    return
                if not self.marker.exists():
                    self.request_if_over_limit()
                if not self.marker.exists():
                    return
                exclusive = self.lease_path.open("a+b")
                acquired = False
                try:
                    deadline = time.monotonic() + self.drain_timeout
                    while True:
                        worker.drain_if_requested()
                        try:
                            fcntl.flock(exclusive, fcntl.LOCK_EX | fcntl.LOCK_NB)
                            acquired = True
                            break
                        except BlockingIOError:
                            if time.monotonic() >= deadline:
                                raise TimeoutError("Workers did not release pyramid readers in time")
                            worker.stopped.wait(min(self.poll_seconds, 0.5))
                    self._delete_oldest()
                finally:
                    # Even a failed cleanup must not leave requests paused forever.
                    try:
                        with self.connect() as db:
                            db.execute("INSERT OR REPLACE INTO metadata VALUES ('retry_after',?)",
                                       (str(time.time() + self.retry_seconds),))
                    finally:
                        try:
                            self.marker.unlink(missing_ok=True)
                        finally:
                            if acquired:
                                fcntl.flock(exclusive, fcntl.LOCK_UN)
                            exclusive.close()
        except Timeout:
            pass  # A different worker is the maintenance coordinator.


class StorageWorker:
    """Per-process cache lease; initialized lazily after any Gunicorn fork."""
    def __init__(self, storage, close_objects):
        self.storage = storage
        self.close_objects = close_objects
        self.guard = threading.RLock()
        self.active = 0
        self.lease = None
        self.stopped = threading.Event()

    def start(self):
        self.storage.prepare()
        thread = threading.Thread(target=self._watch, name="pyramid-maintenance", daemon=True)
        thread.start()

    def _watch(self):
        while not self.stopped.is_set():
            try:
                self.storage.maintain(self)
            except Exception:
                log.exception("Pyramid maintenance failed; readers remain protected")
            self.stopped.wait(self.storage.poll_seconds)

    def enter(self):
        with self.guard:
            if self.storage.marker.exists():
                return False
            if self.lease is None:
                lease = self.storage.lease_path.open("a+b")
                try:
                    fcntl.flock(lease, fcntl.LOCK_SH | fcntl.LOCK_NB)
                except BlockingIOError:
                    lease.close()
                    return False
                self.lease = lease
            # Close the check/acquire race with the coordinator.
            if self.storage.marker.exists():
                self.drain_if_requested()
                return False
            self.active += 1
            return True

    def leave(self):
        with self.guard:
            self.active -= 1
            self.drain_if_requested()

    def drain_if_requested(self):
        with self.guard:
            if self.active or not self.storage.marker.exists() or self.lease is None:
                return
            self.close_objects()  # Failure retains the shared lock, preventing deletion.
            fcntl.flock(self.lease, fcntl.LOCK_UN)
            self.lease.close()
            self.lease = None


class StorageMiddleware:
    """Keep streaming responses protected until their iterable is closed."""
    def __init__(self, app, config):
        self.app = app
        self.config = config
        self.pid = None
        self.worker = None
        self.guard = threading.Lock()
        self.maintenance_template = None

    def maintenance_response(self, environ):
        """Render browser navigation without entering Flask or opening datasets."""
        from werkzeug.wrappers import Request, Response
        request = Request(environ)
        headers = {"Retry-After": "2", "Cache-Control": "no-store", "Vary": "Accept"}
        # Wildcard/image/JSON clients keep the plain 503 response expected by
        # data consumers. Only an explicit HTML preference receives a page.
        accepts_html = "text/html" in request.headers.get("Accept", "").lower()
        preferred = request.accept_mimetypes.best_match(
            ["text/html", "application/json", "application/octet-stream"]
        )
        if accepts_html and preferred == "text/html":
            with self.guard:
                if self.maintenance_template is None:
                    from jinja2 import Environment, FileSystemLoader, select_autoescape
                    environment = Environment(
                        loader=FileSystemLoader(Path(__file__).parent / "templates"),
                        autoescape=select_autoescape(["html"]),
                    )
                    self.maintenance_template = environment.get_template("storage_maintenance.html")
            body = self.maintenance_template.render(
                static_prefix=request.script_root + "/static",
                retry_label="Try again" if request.method in ("GET", "HEAD") else "Return to page",
            )
            return Response(body, status=503, mimetype="text/html", headers=headers)
        return Response("Pyramid storage maintenance in progress. Please retry shortly.\n",
                        status=503, mimetype="text/plain", headers=headers)

    def __call__(self, environ, start_response):
        from werkzeug.wsgi import ClosingIterator
        with self.guard:
            if self.pid != os.getpid():
                self.worker = StorageWorker(self.config.pyramid_storage,
                                            self.config.close_datasets)
                self.worker.start()
                self.pid = os.getpid()
        path = environ.get("PATH_INFO", "")
        if path == "/healthz" or path.startswith("/static/") or path == "/browser/storage":
            return self.app(environ, start_response)
        if not self.worker.enter():
            response = self.maintenance_response(environ)
            return response(environ, start_response)
        worker = self.worker
        finished = False
        finish_guard = threading.Lock()

        def finish():
            nonlocal finished
            with finish_guard:
                if not finished:
                    finished = True
                    worker.leave()

        try:
            iterable = self.app(environ, start_response)
        except BaseException:
            finish()
            raise
        return ClosingIterator(iterable, finish)
