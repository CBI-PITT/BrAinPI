# syntax=docker/dockerfile:1
FROM python:3.12-slim-bookworm AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    SETUPTOOLS_SCM_PRETEND_VERSION_FOR_BRAINPI=0.0.0+docker

RUN apt-get update \
    && apt-get install --yes --no-install-recommends build-essential git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY pyproject.toml setup.py setup.cfg README.md LICENCE ./
COPY BrAinPI ./BrAinPI
RUN python -m pip install --upgrade pip wheel setuptools setuptools-scm \
    && python -m pip wheel --wheel-dir /wheels .


FROM python:3.12-slim-bookworm AS runtime

ARG BRAINPI_UID=10001
ARG BRAINPI_GID=10001

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app/BrAinPI \
    BRAINPI_SETTINGS=/etc/brainpi/settings.ini \
    BRAINPI_GROUPS=/etc/brainpi/groups.ini \
    OMP_NUM_THREADS=1 \
    OPENBLAS_NUM_THREADS=1 \
    MKL_NUM_THREADS=1 \
    NUMEXPR_NUM_THREADS=1

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates libgomp1 libopenjp2-7 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid "${BRAINPI_GID}" brainpi \
    && useradd --uid "${BRAINPI_UID}" --gid "${BRAINPI_GID}" --create-home brainpi \
    && mkdir -p /app /data /etc/brainpi /var/cache/brainpi /var/lib/brainpi/pyramids \
    && chown -R brainpi:brainpi /app /var/cache/brainpi /var/lib/brainpi

COPY --from=builder /wheels /wheels
RUN python -m pip install --no-index --no-deps /wheels/*.whl \
    && rm -rf /wheels

WORKDIR /app/BrAinPI
COPY --chown=brainpi:brainpi BrAinPI /app/BrAinPI

USER brainpi
EXPOSE 5001

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:5001/healthz', timeout=3)"]

CMD ["gunicorn", "--worker-class", "gthread", "--workers", "12", "--threads", "4", "--timeout", "1800", "--graceful-timeout", "60", "--bind", "0.0.0.0:5001", "wsgi:app"]
