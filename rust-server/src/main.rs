use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    net::SocketAddr,
    path::Path,
    process::Stdio,
    sync::Arc,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, Command},
    sync::{mpsc, RwLock},
};
use tower_http::services::ServeDir;
use tracing::{info, warn};

type Tx = mpsc::UnboundedSender<Message>;

struct Peer {
    name: String,
    tx: Tx,
}

// Salas em memória: Map<RoomId, Map<PeerId, Peer>>
type Rooms = Arc<RwLock<HashMap<String, HashMap<String, Peer>>>>;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let rooms: Rooms = Arc::new(RwLock::new(HashMap::new()));

    // Localiza a pasta public (tanto se executado de dentro de rust-server quanto da raiz)
    let public_dir = if Path::new("./public").exists() {
        "./public"
    } else if Path::new("../public").exists() {
        "../public"
    } else {
        "./"
    };

    let static_service = ServeDir::new(public_dir);

    let app = Router::new()
        .route("/signaling", get(ws_handler))
        .route("/rtmp-stream", get(rtmp_ws_handler))
        .fallback_service(static_service)
        .with_state(rooms);

    let port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "3000".to_string())
        .parse()
        .unwrap_or(3000);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("=======================================================");
    info!("🦀 Servidor Google Meet em RUST rodando na porta {}", port);
    info!("Acesse no navegador: http://localhost:{}", port);
    info!("=======================================================");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("Falha ao abrir porta TCP");

    axum::serve(listener, app)
        .await
        .expect("Erro ao rodar servidor Axum");
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(rooms): State<Rooms>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, rooms))
}

async fn rtmp_ws_handler(ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(handle_rtmp_socket)
}

async fn handle_socket(socket: WebSocket, rooms: Rooms) {
    let (mut ws_sender, mut ws_receiver) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();

    // Gera um Peer ID único (ex: peer_x8f2a1)
    let peer_id = format!("peer_{:x}", rand_u64());
    let mut current_room_id: Option<String> = None;
    let mut user_name = "Participante".to_string();

    // Tarefa para descarregar mensagens do canal no socket do cliente
    let send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if ws_sender.send(msg).await.is_err() {
                break;
            }
        }
    });

    // Loop de recepção de frames WebSocket
    while let Some(Ok(msg)) = ws_receiver.next().await {
        match msg {
            Message::Text(text) => {
                if let Ok(val) = serde_json::from_str::<Value>(&text) {
                    let msg_type = val.get("type").and_then(|v| v.as_str()).unwrap_or("");

                    // 1. Entrar na sala
                    if msg_type == "join" {
                        let room_raw = val.get("roomId").and_then(|v| v.as_str()).unwrap_or("geral");
                        let room_clean = room_raw.to_lowercase().trim().replace(' ', "-");
                        user_name = val.get("name").and_then(|v| v.as_str()).unwrap_or("Convidado").trim().to_string();
                        current_room_id = Some(room_clean.clone());

                        let mut rooms_guard = rooms.write().await;
                        let room = rooms_guard.entry(room_clean.clone()).or_default();

                        // Lista participantes já existentes
                        let mut existing_peers = Vec::new();
                        for (id, p) in room.iter() {
                            existing_peers.push(json!({
                                "peerId": id,
                                "name": p.name
                            }));
                        }

                        // Envia confirmação de entrada para o novo participante
                        let room_joined_msg = json!({
                            "type": "room_joined",
                            "peerId": peer_id,
                            "roomId": room_clean,
                            "peers": existing_peers
                        });
                        let _ = tx.send(Message::Text(room_joined_msg.to_string()));

                        // Notifica os outros sobre o novo participante
                        let peer_joined_msg = json!({
                            "type": "peer_joined",
                            "peerId": peer_id,
                            "name": user_name
                        });
                        for peer in room.values() {
                            let _ = peer.tx.send(Message::Text(peer_joined_msg.to_string()));
                        }

                        // Registra o peer na sala
                        room.insert(peer_id.clone(), Peer {
                            name: user_name.clone(),
                            tx: tx.clone(),
                        });

                        info!("[Meet Rust] {} ({}) entrou na sala: {}. Total: {}", user_name, peer_id, room_clean, room.len());
                    }

                    // 2. Encaminhar sinalização WebRTC (SDP / ICE) diretamente para o par alvo
                    else if msg_type == "signal" {
                        if let (Some(room_id), Some(target_id)) = (&current_room_id, val.get("targetPeerId").and_then(|v| v.as_str())) {
                            let rooms_guard = rooms.read().await;
                            if let Some(room) = rooms_guard.get(room_id) {
                                if let Some(target_peer) = room.get(target_id) {
                                    let forward_msg = json!({
                                        "type": "signal",
                                        "senderPeerId": peer_id,
                                        "name": user_name,
                                        "signalData": val.get("signalData")
                                    });
                                    let _ = target_peer.tx.send(Message::Text(forward_msg.to_string()));
                                }
                            }
                        }
                    }

                    // 3. Chat de Mensagens
                    else if msg_type == "chat" {
                        if let Some(room_id) = &current_room_id {
                            let rooms_guard = rooms.read().await;
                            if let Some(room) = rooms_guard.get(room_id) {
                                let chat_msg = json!({
                                    "type": "chat",
                                    "senderPeerId": peer_id,
                                    "senderName": user_name,
                                    "text": val.get("text").and_then(|v| v.as_str()).unwrap_or(""),
                                    "time": current_time_str()
                                });
                                for peer in room.values() {
                                    let _ = peer.tx.send(Message::Text(chat_msg.to_string()));
                                }
                            }
                        }
                    }

                    // 4. Reações, Mão Levantada, Apresentação, Gravação, Live e Estado de Mídia (Broadcast para a sala)
                    else if msg_type == "reaction"
                        || msg_type == "raise_hand"
                        || msg_type == "media_state"
                        || msg_type == "presentation_state"
                        || msg_type == "recording_state"
                        || msg_type == "live_state"
                    {
                        if let Some(room_id) = &current_room_id {
                            let rooms_guard = rooms.read().await;
                            if let Some(room) = rooms_guard.get(room_id) {
                                let broadcast_msg = json!({
                                    "type": msg_type,
                                    "senderPeerId": peer_id,
                                    "senderName": user_name,
                                    "data": val.get("data")
                                });
                                for peer in room.values() {
                                    let _ = peer.tx.send(Message::Text(broadcast_msg.to_string()));
                                }
                            }
                        }
                    }

                    // 5. Ping Keepalive
                    else if msg_type == "ping" {
                        let _ = tx.send(Message::Text(json!({ "type": "pong" }).to_string()));
                    }
                }
            }
            Message::Ping(payload) => {
                let _ = tx.send(Message::Pong(payload));
            }
            Message::Close(_) => {
                break;
            }
            _ => {}
        }
    }

    // Limpeza ao desconectar
    send_task.abort();

    if let Some(room_id) = current_room_id {
        let mut rooms_guard = rooms.write().await;
        if let Some(room) = rooms_guard.get_mut(&room_id) {
            room.remove(&peer_id);
            info!("[Meet Rust] {} ({}) saiu da sala: {}. Restantes: {}", user_name, peer_id, room_id, room.len());

            let peer_left_msg = json!({
                "type": "peer_left",
                "peerId": peer_id,
                "name": user_name
            });
            for peer in room.values() {
                let _ = peer.tx.send(Message::Text(peer_left_msg.to_string()));
            }

            if room.is_empty() {
                rooms_guard.remove(&room_id);
            }
        }
    }
}

// =========================================================================
// PONTE RTMP PARA YOUTUBE LIVE (WebSocket Binary Stream -> FFmpeg -> RTMP)
// =========================================================================
async fn handle_rtmp_socket(socket: WebSocket) {
    let (mut ws_sender, mut ws_receiver) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();

    let send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if ws_sender.send(msg).await.is_err() {
                break;
            }
        }
    });

    let mut ffmpeg_child: Option<Child> = None;
    let mut ffmpeg_stdin: Option<ChildStdin> = None;

    while let Some(Ok(msg)) = ws_receiver.next().await {
        match msg {
            Message::Text(text) => {
                if let Ok(val) = serde_json::from_str::<Value>(&text) {
                    let msg_type = val.get("type").and_then(|v| v.as_str()).unwrap_or("");

                    if msg_type == "start_rtmp" {
                        // Encerra instância anterior caso exista
                        drop(ffmpeg_stdin.take());
                        if let Some(mut old_child) = ffmpeg_child.take() {
                            let _ = old_child.kill().await;
                        }

                        let rtmp_base = val
                            .get("rtmpUrl")
                            .and_then(|v| v.as_str())
                            .unwrap_or("rtmp://a.rtmp.youtube.com/live2")
                            .trim()
                            .trim_end_matches('/');

                        let stream_key = val
                            .get("streamKey")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .trim();

                        let quality = val
                            .get("quality")
                            .and_then(|v| v.as_str())
                            .unwrap_or("720p");

                        if stream_key.is_empty() || (!rtmp_base.starts_with("rtmp://") && !rtmp_base.starts_with("rtmps://")) {
                            let _ = tx.send(Message::Text(
                                json!({
                                    "type": "rtmp_error",
                                    "message": "URL RTMP ou Chave de Transmissão (Stream Key) inválida."
                                })
                                .to_string(),
                            ));
                            continue;
                        }

                        let full_rtmp_url = format!("{}/{}", rtmp_base, stream_key);
                        let (v_bitrate, v_maxrate, v_bufsize) = if quality == "1080p" {
                            ("4500k", "4800k", "9000k")
                        } else {
                            ("2800k", "3200k", "6000k")
                        };

                        info!("[YouTube Live] Iniciando FFmpeg RTMP ({}) para {}", quality, rtmp_base);

                        let spawn_res = Command::new("ffmpeg")
                            .args([
                                "-loglevel", "warning",
                                "-fflags", "+genpts",
                                "-i", "pipe:0",
                                "-c:v", "libx264",
                                "-preset", "veryfast",
                                "-tune", "zerolatency",
                                "-pix_fmt", "yuv420p",
                                "-r", "30",
                                "-g", "60",
                                "-b:v", v_bitrate,
                                "-maxrate", v_maxrate,
                                "-bufsize", v_bufsize,
                                "-c:a", "aac",
                                "-b:a", "128k",
                                "-ar", "44100",
                                "-ac", "2",
                                "-f", "flv",
                                &full_rtmp_url,
                            ])
                            .stdin(Stdio::piped())
                            .stdout(Stdio::null())
                            .stderr(Stdio::piped())
                            .kill_on_drop(true)
                            .spawn();

                        match spawn_res {
                            Ok(mut child) => {
                                ffmpeg_stdin = child.stdin.take();
                                if let Some(stderr) = child.stderr.take() {
                                    let tx_err = tx.clone();
                                    tokio::spawn(async move {
                                        let mut reader = BufReader::new(stderr).lines();
                                        while let Ok(Some(line)) = reader.next_line().await {
                                            warn!("[FFmpeg RTMP]: {}", line);
                                            if line.contains("Connection refused")
                                                || line.contains("Input/output error")
                                                || line.contains("Server error")
                                                || line.contains("Failed to update header")
                                            {
                                                let _ = tx_err.send(Message::Text(
                                                    json!({
                                                        "type": "rtmp_log",
                                                        "message": line
                                                    })
                                                    .to_string(),
                                                ));
                                            }
                                        }
                                    });
                                }
                                ffmpeg_child = Some(child);

                                let _ = tx.send(Message::Text(
                                    json!({
                                        "type": "rtmp_started",
                                        "message": "Ponte FFmpeg RTMP iniciada com sucesso!"
                                    })
                                    .to_string(),
                                ));
                            }
                            Err(e) => {
                                warn!("[YouTube Live] Falha ao iniciar FFmpeg: {}", e);
                                let _ = tx.send(Message::Text(
                                    json!({
                                        "type": "rtmp_error",
                                        "message": format!("FFmpeg não encontrado ou falhou no servidor ({}). No Render com Docker ele já vem instalado automaticamente.", e)
                                    })
                                    .to_string(),
                                ));
                            }
                        }
                    } else if msg_type == "stop_rtmp" {
                        info!("[YouTube Live] Encerrando transmissão RTMP a pedido do usuário.");
                        break;
                    } else if msg_type == "ping" {
                        let _ = tx.send(Message::Text(json!({ "type": "pong" }).to_string()));
                    }
                }
            }
            Message::Binary(bytes) => {
                if let Some(stdin) = ffmpeg_stdin.as_mut() {
                    if let Err(e) = stdin.write_all(&bytes).await {
                        warn!("[YouTube Live] Pipe FFmpeg fechado durante escrita: {}", e);
                        let _ = tx.send(Message::Text(
                            json!({
                                "type": "rtmp_error",
                                "message": "A conexão RTMP com o servidor do YouTube foi interrompida (verifique sua Chave de Transmissão)."
                            })
                            .to_string(),
                        ));
                        break;
                    }
                }
            }
            Message::Ping(payload) => {
                let _ = tx.send(Message::Pong(payload));
            }
            Message::Close(_) => {
                break;
            }
            _ => {}
        }
    }

    // Limpeza segura do processo FFmpeg ao fechar o socket
    drop(ffmpeg_stdin.take());
    if let Some(mut child) = ffmpeg_child.take() {
        let _ = child.kill().await;
        let _ = child.wait().await;
        info!("[YouTube Live] Processo FFmpeg finalizado com segurança.");
    }
    send_task.abort();
}

// Gerador simples de identificador sem dependência pesada de random
fn rand_u64() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let ptr = &nanos as *const u128 as usize as u64;
    (nanos as u64) ^ ptr ^ 0x517cc1b727220a95
}

// Formata hora no padrão HH:MM
fn current_time_str() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let total_secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let secs_of_day = (total_secs % 86400) as i64;
    // Ajuste fuso aproximado BRT (-3h)
    let brt_secs = (secs_of_day - 3 * 3600).rem_euclid(86400);
    let hours = brt_secs / 3600;
    let minutes = (brt_secs % 3600) / 60;
    format!("{:02}:{:02}", hours, minutes)
}
