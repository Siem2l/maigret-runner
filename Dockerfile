# maigret-runner container image.
#
# Maigret has a large transitive dep tree (chardet, aiohttp, mock, sphinx
# for some reason, etc.) and a few of those wheels require build tools.
# We do a two-stage build so the final image only ships the venv +
# our source code -- no toolchain footprint at runtime.
FROM python:3.12-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    UV_LINK_MODE=copy \
    PYTHONDONTWRITEBYTECODE=1

# uv handles dep resolution + venv creation in one shot. Bookworm's
# default `pip install` works fine but uv is ~10x faster and gives a
# repeatable lockfile next time we rebuild.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        curl \
        git \
    && rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    cp /root/.local/bin/uv /usr/local/bin/uv

WORKDIR /app
# hatchling reads README.md + LICENSE during package metadata
# resolution (pyproject.toml's `readme = "README.md"` and the license
# table); copy all three so `uv pip install .` doesn't ENOENT.
COPY pyproject.toml README.md LICENSE ./
COPY src ./src

# Resolve deps + install into a venv under /opt/venv. We let uv build
# from pyproject.toml directly so the package + its deps land in one go.
RUN uv venv /opt/venv && \
    uv pip install --python /opt/venv/bin/python .

# ── Runtime stage ───────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Non-root user; data dir owned by the same uid so the bind-mount on
# /data from the host gets the right perms.
RUN useradd --create-home --uid 1000 maigret \
    && mkdir -p /data \
    && chown -R maigret:maigret /data

COPY --from=builder /opt/venv /opt/venv
COPY --chown=maigret:maigret templates /app/templates
COPY --chown=maigret:maigret src /app/src

WORKDIR /app
USER maigret
EXPOSE 8080

CMD ["uvicorn", "runner.main:app", "--host", "0.0.0.0", "--port", "8080", "--app-dir", "src"]
