FROM rust:1.90 AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN cargo build --release --locked -p sapflux --bin sapflux

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app

COPY --from=builder /app/target/release/sapflux /app/sapflux

RUN useradd -m -u 10001 sapflux && chown -R sapflux:sapflux /app
USER sapflux

CMD ["/app/sapflux", "serve", "--addr", "0.0.0.0:8080"]
