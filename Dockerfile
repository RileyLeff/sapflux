FROM rust:1.90 AS builder

WORKDIR /app

# Copy workspace manifests only so dependency layers stay cacheable.
COPY Cargo.toml Cargo.lock ./
COPY crates/sapflux/Cargo.toml crates/sapflux/
COPY crates/sapflux-admin/Cargo.toml crates/sapflux-admin/
COPY crates/sapflux-core/Cargo.toml crates/sapflux-core/
COPY crates/sapflux-parser/Cargo.toml crates/sapflux-parser/

# Pre-fetch dependencies; create minimal stubs so path deps resolve without sources.
RUN mkdir -p crates/sapflux/src \
        crates/sapflux-admin/src \
        crates/sapflux-core/src \
        crates/sapflux-parser/src \
    && touch crates/sapflux/src/main.rs \
        crates/sapflux-admin/src/main.rs \
        crates/sapflux-core/src/lib.rs \
        crates/sapflux-parser/src/lib.rs \
    && cargo fetch --locked

# Bring in the full source tree for the actual build.
COPY crates ./crates

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked -p sapflux --bin sapflux \
    && mkdir -p /app/bin \
    && cp target/release/sapflux /app/bin/sapflux

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app

COPY --from=builder /app/bin/sapflux /app/sapflux

RUN useradd -m -u 10001 sapflux && chown -R sapflux:sapflux /app
USER sapflux

CMD ["/app/sapflux", "serve", "--addr", "0.0.0.0:8080"]
