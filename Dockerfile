FROM rust:latest AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake clang pkg-config && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/sieve /usr/local/bin/sieve

WORKDIR /app

ENTRYPOINT ["sieve"]
