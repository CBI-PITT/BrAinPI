# Deployment and troubleshooting

[Back to installation and startup](../README.md#installation)

## Docker services and persistent data

The [Dockerfile](../Dockerfile) builds a Python 3.12 Linux image with the native
image libraries. [Compose](../compose.yaml) starts two services from that image:

| Service | Process | Container port |
|---|---|---|
| `brainpi` | Gunicorn, 8 `gthread` workers with 2 threads each | `5001` |
| `neuroglancer` | One standalone Neuroglancer frontend | `9999` |

Build `brainpi` before starting a fresh deployment so the shared image is
available to both services. Gunicorn workers never start the frontend themselves.

| Host setting or volume | Container destination | Access |
|---|---|---|
| `BRAINPI_DATA_DIR` | `/data` | Read-only source datasets |
| `docker/settings.ini` | `/etc/brainpi/settings.ini` | Read-only configuration |
| `docker/groups.ini` | `/etc/brainpi/groups.ini` | Read-only group membership |
| `brainpi-cache` volume | `/var/cache/brainpi` | Writable cache |
| `brainpi-pyramids` volume | `/var/lib/brainpi/pyramids` | Writable pyramids and state |

Host paths must exist and be readable by the container user (UID/GID `10001` by
default). On Docker Desktop, share the source directory with Docker. INI dataset
aliases use paths under `/data`, not the host's original path. Compose prefixes
volume names with the project name unless configured otherwise.

## Applying changes

Keep existing `.env`, `settings.ini`, and `groups.ini` files during updates;
copying templates again replaces deployment-specific settings.

| Change | Apply with |
|---|---|
| INI settings | `docker compose restart brainpi neuroglancer` |
| Group membership | Read on subsequent browser/permission requests |
| `.env`, ports, mounts, UID/GID, or resource limits | Recreate the affected containers; UID/GID changes also require rebuilding the image |
| Application code, Dockerfile, or dependencies | Build the image, then recreate containers |

Recreate after environment or Compose changes:

```bash
docker compose up -d --force-recreate
```

Rebuild and recreate after code or dependency changes:

```bash
docker compose build brainpi
docker compose up -d --force-recreate
```

For logs and readiness:

```bash
docker compose logs -f --tail=100 brainpi neuroglancer
curl http://localhost:5001/healthz
curl http://localhost:9999/v/base/
```

`/healthz` remains available during automatic pyramid cleanup. Readiness URLs
must use the host ports selected for your deployment.

## Ports and public URLs

Published ports and advertised URLs are separate settings. The containers keep
listening on `5001` and `9999`; changing a host port does not rewrite links.

| `.env` variable | Purpose | Default |
|---|---|---|
| `BRAINPI_PORT` | Host port mapped to BrAinPI's `5001` | `5001` |
| `BRAINPI_PUBLIC_URL` | Browser-visible BrAinPI URL; overrides `[app] url` | `http://localhost:5001/` |
| `BRAINPI_NG_PORT` | Host port mapped to the viewer's `9999` | `9999` |
| `BRAINPI_NG_PUBLIC_URL` | Browser-visible viewer URL; overrides `[neuroglancer] url` | `http://localhost:9999/v/base/` |

For example, publish the services on host ports `8000` and `10999`:

```dotenv
BRAINPI_PORT=8000
BRAINPI_PUBLIC_URL=http://localhost:8000/
BRAINPI_NG_PORT=10999
BRAINPI_NG_PUBLIC_URL=http://localhost:10999/v/base/
```

For remote access, replace `localhost` with the server's reachable hostname.
Behind a TLS reverse proxy, use the external HTTPS URLs even though the
containers listen internally over HTTP. The user's browser connects to both
URLs; Compose-only names such as `http://brainpi:5001/` are unsuitable.

Keep `[neuroglancer] local_ip = 0.0.0.0` and `local_port = 9999` in Docker.
For native startup, bind the standalone viewer to `0.0.0.0` if it must accept
remote connections. Use plain URLs in INI files, not Markdown link syntax.

### Hosted Neuroglancer

To use Google's hosted viewer, set:

```dotenv
BRAINPI_NG_PUBLIC_URL=https://neuroglancer-demo.appspot.com/
```

After building the shared image, start only BrAinPI; stop a previously running
local frontend if needed:

```bash
docker compose stop neuroglancer
docker compose up -d brainpi
```

The viewer URL does not select Compose services. A bare `docker compose up -d`
starts both services, even when links point to the hosted viewer.

## Authentication

Configure FreeIPA or AD-LDAP in the deployment's `[auth]` section. Without either
backend, the application starts and anonymous browsing works, but login is
unavailable. Keep `bypass_auth = False` outside testing. Secure session cookies
require HTTPS in normal browser use.

For FreeIPA:

```ini
[auth]
ipa_auth = True
ipa_server = ipa.example.org
ipa_use_tls = True
ipa_ca_file = /etc/ipa/ca.crt
```

IPA is tried first. Success does not require AD-LDAP settings. Rejected
credentials or connection failures fall back to AD-LDAP when `domain_server`,
`domain_port`, and `domain_name` are configured; otherwise login is rejected.
IPA is disabled by default.

The IPA base DN is derived from the hostname (`ipa.example.org` becomes
`dc=example,dc=org`). TLS uses LDAPS on `636`; non-TLS uses LDAP on `389`.
`ipa_ca_file` enables certificate validation with that CA bundle. The current
implementation skips certificate validation when the CA path is empty.

Configure browser memberships in `groups.ini`; keep an `[all]` section even if
empty. Members of `[all]` can browse all protected roots and manually query
storage usage. Other groups match first-level directories when
`restrict_paths_to_matched_username = True`. These browser restrictions do not
secure the separate viewer-facing data routes; enforce deployment access
control before exposing protected datasets.

### Docker IPA certificate mount

Place the deployment CA certificate at `docker/ca.crt`, or set
`BRAINPI_IPA_CA_FILE` in `.env` to another existing host file. After building the
shared image, enable the optional overlay:

```bash
docker compose -f compose.yaml -f compose.ipa.yaml up -d
```

The certificate is mounted read-only at `/etc/ipa/ca.crt`. A missing host file
fails rather than creating an empty directory. Certificates are ignored by Git
and must be supplied for each deployment.

Use the same file selection for subsequent commands, for example:

```bash
docker compose -f compose.yaml -f compose.ipa.yaml logs -f --tail=100 brainpi
docker compose -f compose.yaml -f compose.ipa.yaml up -d --force-recreate
```

## Environment overrides

See [.env.example](../.env.example) for host-side Compose options. Compose
supplies the following application environment variables:

| Variable | Effect |
|---|---|
| `BRAINPI_SETTINGS`, `BRAINPI_GROUPS` | Select the INI files read by the application |
| `BRAINPI_SECRET_KEY` | Overrides the Flask session/signed-URL secret |
| `BRAINPI_PUBLIC_URL`, `BRAINPI_NG_PUBLIC_URL` | Override the public URLs |
| `BRAINPI_CACHE_DIR` | Overrides the Unix disk-cache path |
| `BRAINPI_PYRAMIDS_DIR` | Overrides the unified pyramid root, including state |
| `BRAINPI_PYRAMID_LIMIT_GB` | Overrides the combined pyramid quota when non-empty |
| `BRAINPI_LOG_FILE` | Optional file-log destination; Compose uses an empty value for stdout logging |

Native startup also supports `BRAINPI_CONFIG_DIR` for the configuration directory.
Environment variables take precedence over the corresponding INI values.
Compose fixes cache and pyramid locations to their persistent volume mounts;
changing only the INI path does not override those locations.

`BRAINPI_PORT`, `BRAINPI_NG_PORT`, `BRAINPI_SETTINGS_FILE`,
`BRAINPI_GROUPS_FILE`, `BRAINPI_NOFILE`, and UID/GID variables are interpreted
by Compose, not as application INI overrides. Native startup does not load the
repository `.env` file automatically; export application variables in its shell.

## Troubleshooting

### Empty browser or unavailable login

Templates intentionally expose no dataset aliases. Add aliases under
`[dir_anon]` or `[dir_auth]`; in Docker, use container paths under `/data`.
Authenticated aliases require a working login backend and group configuration.

### Open-file limits and cache errors

Each worker may hold many dataset files, sockets, and cache database handles.
Compose sets soft and hard `nofile` limits to `500000` per process. Override
`BRAINPI_NOFILE` in `.env` if needed, recreate the container, then check:

```bash
docker compose exec brainpi sh -c 'ulimit -Sn; ulimit -Hn'
```

Changing the host shell's limit or merely restarting a container does not apply
new Compose resource settings. If Docker rejects the value, check the daemon,
host, or Docker Desktop VM limits. When using the IPA overlay, include its file
selection in these commands as well.

For a native Linux deployment, inspect a running worker and its descriptor use:

```bash
pgrep -af gunicorn
# Replace 12345 with the worker PID.
prlimit --pid 12345 --nofile
ls /proc/12345/fd | wc -l
```

Raise the shell limit before starting Gunicorn in that shell, within the hard
limit permitted by the host, or use `LimitNOFILE=500000` in a systemd unit:

```bash
ulimit -Sn
ulimit -Hn
ulimit -n 500000
```

Investigate steadily growing usage rather than only increasing the limit.
Descriptor exhaustion normally reports `[Errno 24] Too many open files`.
DiskCache may suppress an earlier directory-creation error before failing to
write a `.val` file, so a subsequent `[Errno 2]` may hide the original cause.
Also check disk space, inodes (`df -h`, `df -i`), permissions, mounts, and
external cleanup jobs. Do not remove an active cache directory.

### Worker exits during JP2 conversion

JP2 generation stages the decoded image in memory. It can be much larger than
the compressed source, and the source-size limit does not bound decoded memory.
Repeated worker `SIGKILL` events warrant checking container memory and reducing
worker/conversion concurrency. The default image uses 8 workers and 2 threads.

### Build notes

The image builds `v3d-py-helper` with signed plain `char` for ARM64. It includes
HDF5 build libraries for `h5py` source builds and OpenJPEG in the runtime image.
Configuration, source datasets, cache, and pyramids are supplied outside the
image. See the [Dockerfile](../Dockerfile) for pinned build/runtime choices.
