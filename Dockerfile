FROM rust:1-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
COPY config ./config
COPY src ./src
RUN cargo build --release -p dmq-cli

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/dmq-broker /usr/local/bin/dmq-broker
COPY --from=builder /app/target/release/custom-dmq /usr/local/bin/custom-dmq
COPY config/cluster.docker.toml /etc/dmq/cluster.toml
ENTRYPOINT ["dmq-broker"]
