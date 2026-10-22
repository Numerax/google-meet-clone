# Estágio 1: Build com a versão mais recente do Rust oficial (suporte a edition 2024)
FROM rust:slim as builder

WORKDIR /app

COPY rust-server/Cargo.toml rust-server/Cargo.lock ./
COPY rust-server/src ./src

RUN cargo build --release

# Estágio 2: Imagem final ultra leve de produção (~20MB)
FROM debian:bookworm-slim

WORKDIR /app

COPY --from=builder /app/target/release/google-meet-clone-rust /app/server
COPY public /app/public

ENV PORT=3000
EXPOSE 3000

CMD ["/app/server"]
