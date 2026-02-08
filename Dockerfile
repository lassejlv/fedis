FROM rust:1.93.0-bookworm AS builder
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
RUN mkdir src && printf 'fn main() {}' > src/main.rs
RUN cargo build --release
RUN rm -rf src

COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

ARG FEDIS_HOST
ARG FEDIS_PORT
ARG FEDIS_LISTEN
ARG FEDIS_USERNAME
ARG FEDIS_PASSWORD
ARG FEDIS_USERS
ARG FEDIS_USER_COMMANDS
ARG FEDIS_USER_ENABLED
ARG FEDIS_DATA_PATH
ARG FEDIS_AOF_PATH
ARG FEDIS_AOF_FSYNC
ARG FEDIS_SNAPSHOT_PATH
ARG FEDIS_SNAPSHOT_INTERVAL_SEC
ARG FEDIS_METRICS_ADDR
ARG FEDIS_CONFIG
ARG FEDIS_URL
ARG FEDIS_LOG
ARG FEDIS_NON_REDIS_MODE
ARG FEDIS_DEBUG_RESPONSE_ID

COPY --from=builder /app/target/release/fedis /usr/local/bin/fedis
EXPOSE 6379
CMD ["fedis"]
