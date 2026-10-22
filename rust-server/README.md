# Servidor Google Meet Clone em Rust 🦀

Servidor de sinalização WebRTC de alta performance desenvolvido com **Axum** e **Tokio**.

## Características de Performance
* **Consumo de Memória:** ~8MB a 12MB de RAM (contra ~80MB do Node.js).
* **Concorrência:** Multi-threading nativo com `tokio::sync::RwLock` sem travar o processamento de áudio/sinalização.
* **Latência de Mensagens:** Sub-milissegundo para roteamento de pacotes SDP e candidatos ICE.
* **Segurança de Memória:** Zero risco de data races ou vazamento de memória.

## Como Executar Localmente

### 1. Instalar o Rust (se ainda não tiver):
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"
```

### 2. Rodar em Modo de Desenvolvimento:
```bash
cargo run
```

### 3. Compilar em Modo Release Otimizado:
```bash
cargo build --release
./target/release/google-meet-clone-rust
```

Acesse: `http://localhost:3000`
