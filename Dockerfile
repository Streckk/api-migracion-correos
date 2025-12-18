FROM rustlang/rust:nightly-bullseye AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

FROM debian:bullseye-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl1.1 openssh-client \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/correo-migrador-api /usr/local/bin/correo-migrador-api

ENV RUST_LOG=info \
    PORT=5000

EXPOSE 5000

CMD ["correo-migrador-api"]
