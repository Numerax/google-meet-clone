# Estágio 1: Build com Rust oficial
FROM rust:1.80-slim as builder

WORKDIR /app

# Copia dependências e código
COPY Cargo.toml ./
COPY src ./src

# Compilação em modo release otimizado
RUN cargo build --release

# Estágio 2: Imagem final ultra enxuta (~20MB)
FROM debian:bookworm-slim

WORKDIR /app

# Copia o binário compilado
COPY --from=builder /app/target/release/google-meet-clone-rust /app/server
COPY ../public /app/public

ENV PORT=3000
EXPOSE 3000

CMD ["/app/server"]
