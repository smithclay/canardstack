# syntax=docker/dockerfile:1.7

FROM rust:1.92-slim-bookworm AS chef

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        cmake \
        libssl-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/* \
    && cargo install cargo-chef --locked

WORKDIR /app

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY benches ./benches
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder

ARG TARGETPLATFORM

COPY --from=planner /app/recipe.json recipe.json

# Compile dependencies (including the bundled DuckDB C++ amalgamation). This is
# the heavy, cacheable layer: it rebuilds only when recipe.json changes, and the
# compiled output stays in the image layer so the type=gha layer cache reuses it
# across CI runs. A --mount=type=cache target dir would NOT persist in GitHub
# Actions, which is why dependencies were recompiled cold on every release build.
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git \
    cargo chef cook --release --locked --recipe-path recipe.json

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY benches ./benches

RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git \
    cargo build --release --locked \
    && cp /app/target/release/canardstack /app/canardstack

RUN mkdir -p /opt/duckdb/extensions \
    && CANARDSTACK_DUCKDB_EXTENSION_DIR=/opt/duckdb/extensions \
       /app/canardstack install-ducklake-extension

FROM debian:bookworm-slim AS runtime

ARG CANARDSTACK_UID=10001
ARG CANARDSTACK_GID=10001

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libgcc-s1 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid "${CANARDSTACK_GID}" canardstack \
    && useradd \
        --uid "${CANARDSTACK_UID}" \
        --gid "${CANARDSTACK_GID}" \
        --home-dir /var/lib/canardstack \
        --create-home \
        --shell /usr/sbin/nologin \
        canardstack \
    && mkdir -p /usr/local/lib/duckdb/extensions /var/lib/canardstack/storage \
    && chown -R canardstack:canardstack /var/lib/canardstack /usr/local/lib/duckdb

COPY --from=builder /app/canardstack /usr/local/bin/canardstack
COPY --from=builder --chown=canardstack:canardstack /opt/duckdb/extensions /usr/local/lib/duckdb/extensions

ENV HOME=/var/lib/canardstack \
    CANARDSTACK_BIND=0.0.0.0:4318 \
    CANARDSTACK_DATA_DIR=/var/lib/canardstack \
    CANARDSTACK_DUCKDB_EXTENSION_DIR=/usr/local/lib/duckdb/extensions

USER 10001:10001
EXPOSE 4318

HEALTHCHECK --interval=5s --timeout=5s --start-period=20s --retries=12 \
    CMD ["canardstack", "healthcheck", "http://127.0.0.1:4318/healthz"]

CMD ["canardstack", "serve"]
