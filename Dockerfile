# Estágio 1: Build com a versão mais recente do Rust oficial (suporte a edition 2024)
FROM rust:slim as builder

WORKDIR /app

COPY rust-server/Cargo.toml rust-server/Cargo.lock ./
COPY rust-server/src ./src

RUN cargo build --release

# Estágio 2: Imagem final de produção com FFmpeg para transmissão YouTube Live (RTMP)
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/google-meet-clone-rust /app/server
COPY public /app/public

ENV PORT=3000
EXPOSE 3000

CMD ["/app/server"]
