FROM rust:1-bookworm AS builder

WORKDIR /build
COPY . .
RUN cargo build --release --bin ingest-batch

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/ingest-batch /usr/local/bin/ingest-batch

ENTRYPOINT ["ingest-batch"]
