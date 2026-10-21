"""On-demand storage reporting restricted to authenticated ``[all]`` users."""

import logging
import os
from pathlib import Path
import time

from filelock import FileLock, Timeout
from flask import abort, jsonify, request
from flask_login import current_user

from pyramid_storage import tree_size


log = logging.getLogger(__name__)


def can_view_storage():
    if not current_user.is_authenticated:
        return False
    from config_tools import get_config
    groups = get_config("groups.ini", allow_no_value=True)
    return groups.has_section("all") and str(current_user.id).casefold() in {
        name.casefold() for name in groups.options("all")
    }


def setup_storage_status(app, config):
    @app.get("/browser/storage")
    def browser_storage_status():
        # Authenticate/authorize before touching either storage tree or ledger.
        if not can_view_storage():
            abort(403)
        kind = request.args.get("kind", "all")
        if kind not in ("cache", "pyramid", "all"):
            abort(400, description="Select cache, pyramid or all storage")
        storage = config.pyramid_storage
        if storage is None:
            return jsonify(error="Pyramid storage is not configured"), 503
        storage.prepare()
        try:
            # Prevent simultaneous expensive scans from different browser tabs.
            with FileLock(str(storage.state_dir / "status.lock"), timeout=0):
                def cache_usage():
                    root = config.settings.get(
                        "disk_cache", "location_win" if os.name == "nt" else "location_unix",
                        fallback=None,
                    )
                    return {
                        "used_bytes": tree_size(Path(root).expanduser()) if root else 0,
                        "limit_bytes": config.settings.getfloat(
                            "disk_cache", "cacheSizeGB", fallback=0) * 1024**3,
                        "configured": bool(root),
                    }

                def pyramid_usage():
                    result = storage.snapshot()
                    result["ledger_bytes"] = result["used_bytes"]
                    roots = sorted({root for root, _suffix in storage.stores}, key=lambda p: len(p.parts))
                    unique_roots = []
                    for root in roots:
                        if not any(root.is_relative_to(parent) for parent in unique_roots):
                            unique_roots.append(root)
                    result["used_bytes"] = sum(tree_size(root) for root in unique_roots)
                    result["measurement"] = "files"
                    return result

                if kind == "all":
                    result = {"cache": cache_usage(), "pyramid": pyramid_usage()}
                elif kind == "cache":
                    result = cache_usage()
                else:
                    result = pyramid_usage()
                result["checked_at"] = time.time()
                response = jsonify(result)
                response.headers["Cache-Control"] = "no-store, private"
                return response
        except Timeout:
            return jsonify(error="Another storage check is running. Please retry shortly."), 429
        except OSError:
            log.exception("Storage measurement failed")
            return jsonify(error="Storage could not be measured. Please retry later."), 503

    return app
