FROM rust:1.77 AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY frontend ./frontend
RUN cargo build --release --bin sapflux

FROM debian:trixie-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/sapflux /app/sapflux

EXPOSE 8080

ENTRYPOINT ["/app/sapflux"]
