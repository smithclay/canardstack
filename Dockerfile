FROM rust:1.92-slim-bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        cmake \
        libssl-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY benches ./benches

RUN cargo build --release --locked

RUN mkdir -p /opt/duckdb/extensions \
    && CANARDSTACK_DUCKDB_EXTENSION_DIR=/opt/duckdb/extensions \
       ./target/release/canardstack install-ducklake-extension

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

COPY --from=builder /app/target/release/canardstack /usr/local/bin/canardstack
COPY --from=builder /opt/duckdb/extensions /usr/local/lib/duckdb/extensions

ENV HOME=/var/lib/canardstack \
    CANARDSTACK_BIND=0.0.0.0:4318 \
    CANARDSTACK_DATA_DIR=/var/lib/canardstack \
    CANARDSTACK_DUCKDB_PATH=/var/lib/canardstack/canardstack.duckdb \
    CANARDSTACK_STORAGE_DIR=/var/lib/canardstack/storage \
    CANARDSTACK_DUCKDB_EXTENSION_DIR=/usr/local/lib/duckdb/extensions

USER 10001:10001
EXPOSE 4318

HEALTHCHECK --interval=5s --timeout=5s --start-period=20s --retries=12 \
    CMD ["canardstack", "healthcheck", "http://127.0.0.1:4318/healthz"]

CMD ["canardstack", "serve"]
