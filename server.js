const http = require('http');
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 3000;
const PUBLIC_DIR = path.join(__dirname, 'public');

// Servidor de arquivos estáticos
const server = http.createServer((req, res) => {
  let reqPath = req.url.split('?')[0];
  if (reqPath === '/') reqPath = '/index.html';
  
  const filePath = path.join(PUBLIC_DIR, reqPath);
  
  if (!fs.existsSync(filePath) || fs.statSync(filePath).isDirectory()) {
    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Página não encontrada');
    return;
  }

  const ext = path.extname(filePath).toLowerCase();
  const mimeTypes = {
    '.html': 'text/html; charset=utf-8',
    '.js': 'application/javascript; charset=utf-8',
    '.css': 'text/css; charset=utf-8',
    '.json': 'application/json',
    '.svg': 'image/svg+xml'
  };

  const contentType = mimeTypes[ext] || 'application/octet-stream';
  fs.readFile(filePath, (err, content) => {
    if (err) {
      res.writeHead(500);
      res.end('Erro interno');
    } else {
      res.writeHead(200, { 'Content-Type': contentType });
      res.end(content);
    }
  });
});

// WebSocket Servers (/signaling e /rtmp-stream)
const wss = new WebSocketServer({ noServer: true });
const rtmpWss = new WebSocketServer({ noServer: true });

server.on('upgrade', (request, socket, head) => {
  const pathname = (request.url || '').split('?')[0];
  if (pathname === '/signaling') {
    wss.handleUpgrade(request, socket, head, ws => {
      wss.emit('connection', ws, request);
    });
  } else if (pathname === '/rtmp-stream') {
    rtmpWss.handleUpgrade(request, socket, head, ws => {
      rtmpWss.emit('connection', ws, request);
    });
  } else {
    socket.destroy();
  }
});

// Salas de Reunião em Memória: Map<roomId, Map<peerId, { ws, name }>>
const rooms = new Map();

function sendWs(wsClient, data) {
  if (wsClient && wsClient.readyState === WebSocket.OPEN) {
    try {
      wsClient.send(typeof data === 'string' ? data : JSON.stringify(data));
    } catch (e) {
      console.warn('Erro ao enviar frame WS:', e.message);
    }
  }
}

wss.on('connection', (wsClient, req) => {
  const peerId = 'peer_' + Math.random().toString(36).substring(2, 9);
  let currentRoomId = null;
  let userName = 'Participante';

  // Heartbeat do socket para compatibilidade com Render / Envoy
  wsClient.isAlive = true;
  wsClient.on('pong', () => {
    wsClient.isAlive = true;
  });

  wsClient.on('message', rawMsg => {
    try {
      const msg = JSON.parse(rawMsg.toString());

      // 1. Entrar na Sala
      if (msg.type === 'join') {
        currentRoomId = (msg.roomId || 'geral').toLowerCase().trim().replace(/\s+/g, '-');
        userName = (msg.name || 'Convidado').trim();

        if (!rooms.has(currentRoomId)) {
          rooms.set(currentRoomId, new Map());
        }

        const room = rooms.get(currentRoomId);

        // Informa os membros existentes para o novo participante
        const existingPeers = [];
        for (const [id, peer] of room.entries()) {
          existingPeers.push({ peerId: id, name: peer.name });
        }

        sendWs(wsClient, {
          type: 'room_joined',
          peerId: peerId,
          roomId: currentRoomId,
          peers: existingPeers
        });

        // Notifica quem já estava na sala sobre o novo participante
        for (const [id, peer] of room.entries()) {
          sendWs(peer.ws, {
            type: 'peer_joined',
            peerId: peerId,
            name: userName
          });
        }

        // Adiciona à sala
        room.set(peerId, { ws: wsClient, name: userName });
        console.log(`[Meet] ${userName} (${peerId}) entrou na sala: ${currentRoomId}. Total na sala: ${room.size}`);
      }

      // 2. Troca de Sinalização WebRTC (SDP Offer / Answer e ICE Candidates)
      else if (msg.type === 'signal') {
        if (currentRoomId && rooms.has(currentRoomId)) {
          const targetPeer = rooms.get(currentRoomId).get(msg.targetPeerId);
          if (targetPeer) {
            sendWs(targetPeer.ws, {
              type: 'signal',
              senderPeerId: peerId,
              name: userName,
              signalData: msg.signalData
            });
          }
        }
      }

      // 3. Chat
      else if (msg.type === 'chat') {
        if (currentRoomId && rooms.has(currentRoomId)) {
          const room = rooms.get(currentRoomId);
          const chatMsg = {
            type: 'chat',
            senderPeerId: peerId,
            senderName: userName,
            text: msg.text,
            time: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
          };
          for (const peer of room.values()) {
            sendWs(peer.ws, chatMsg);
          }
        }
      }

      // 4. Reações, Mão Levantada, Estado de Mídia (Mic/Cam), Apresentação, Gravação e Live
      else if (
        msg.type === 'reaction' ||
        msg.type === 'raise_hand' ||
        msg.type === 'media_state' ||
        msg.type === 'presentation_state' ||
        msg.type === 'recording_state' ||
        msg.type === 'live_state'
      ) {
        if (currentRoomId && rooms.has(currentRoomId)) {
          const room = rooms.get(currentRoomId);
          for (const peer of room.values()) {
            sendWs(peer.ws, {
              type: msg.type,
              senderPeerId: peerId,
              senderName: userName,
              data: msg.data
            });
          }
        }
      }

      // 5. Ping do cliente para manter conexão viva no mobile
      else if (msg.type === 'ping') {
        sendWs(wsClient, { type: 'pong' });
      }

    } catch (err) {
      console.error('Erro ao processar mensagem WS:', err);
    }
  });

  wsClient.on('close', () => {
    if (currentRoomId && rooms.has(currentRoomId)) {
      const room = rooms.get(currentRoomId);
      room.delete(peerId);
      console.log(`[Meet] ${userName} (${peerId}) saiu da sala: ${currentRoomId}. Restantes: ${room.size}`);

      for (const peer of room.values()) {
        sendWs(peer.ws, {
          type: 'peer_left',
          peerId: peerId,
          name: userName
        });
      }

      if (room.size === 0) {
        rooms.delete(currentRoomId);
      }
    }
  });

  wsClient.on('error', err => {
    console.warn(`[WS Error ${peerId}]:`, err.message);
  });
});

// Ponte RTMP para YouTube Live no servidor Node.js
rtmpWss.on('connection', wsClient => {
  let ffmpegProc = null;

  const stopFfmpeg = () => {
    if (ffmpegProc) {
      try {
        if (ffmpegProc.stdin) ffmpegProc.stdin.end();
        ffmpegProc.kill('SIGINT');
      } catch (e) {}
      ffmpegProc = null;
    }
  };

  wsClient.on('message', (data, isBinary) => {
    if (!isBinary) {
      try {
        const msg = JSON.parse(data.toString());
        if (msg.type === 'start_rtmp') {
          stopFfmpeg();
          const rtmpBase = (msg.rtmpUrl || 'rtmp://a.rtmp.youtube.com/live2').trim().replace(/\/+$/, '');
          const streamKey = (msg.streamKey || '').trim();
          const quality = msg.quality || '720p';

          if (!streamKey || (!rtmpBase.startsWith('rtmp://') && !rtmpBase.startsWith('rtmps://'))) {
            sendWs(wsClient, {
              type: 'rtmp_error',
              message: 'URL RTMP ou Chave de Transmissão (Stream Key) inválida.'
            });
            return;
          }

          const fullRtmpUrl = `${rtmpBase}/${streamKey}`;
          const [vBitrate, vMaxrate, vBufsize] = quality === '1080p'
            ? ['4500k', '4800k', '9000k']
            : ['2800k', '3200k', '6000k'];

          try {
            ffmpegProc = spawn('ffmpeg', [
              '-loglevel', 'warning',
              '-fflags', '+genpts',
              '-i', 'pipe:0',
              '-c:v', 'libx264',
              '-preset', 'veryfast',
              '-tune', 'zerolatency',
              '-pix_fmt', 'yuv420p',
              '-r', '30',
              '-g', '60',
              '-b:v', vBitrate,
              '-maxrate', vMaxrate,
              '-bufsize', vBufsize,
              '-c:a', 'aac',
              '-b:a', '128k',
              '-ar', '44100',
              '-ac', '2',
              '-f', 'flv',
              fullRtmpUrl
            ]);

            ffmpegProc.on('error', err => {
              sendWs(wsClient, {
                type: 'rtmp_error',
                message: `FFmpeg não encontrado ou falhou no servidor (${err.message}).`
              });
            });

            if (ffmpegProc.stdin) {
              ffmpegProc.stdin.on('error', () => {});
            }

            if (ffmpegProc.stderr) {
              ffmpegProc.stderr.on('data', chunk => {
                const line = chunk.toString();
                if (line.includes('Connection refused') || line.includes('Input/output error') || line.includes('Server error')) {
                  sendWs(wsClient, { type: 'rtmp_log', message: line.trim() });
                }
              });
            }

            sendWs(wsClient, {
              type: 'rtmp_started',
              message: 'Ponte FFmpeg RTMP iniciada com sucesso!'
            });
          } catch (err) {
            sendWs(wsClient, {
              type: 'rtmp_error',
              message: `Erro ao iniciar FFmpeg: ${err.message}`
            });
          }
        } else if (msg.type === 'stop_rtmp') {
          stopFfmpeg();
          wsClient.close();
        } else if (msg.type === 'ping') {
          sendWs(wsClient, { type: 'pong' });
        }
      } catch (e) {}
    } else if (ffmpegProc && ffmpegProc.stdin && !ffmpegProc.stdin.destroyed) {
      ffmpegProc.stdin.write(data);
    }
  });

  wsClient.on('close', stopFfmpeg);
  wsClient.on('error', stopFfmpeg);
});

// Intervalo de Ping para manter conexões ativas através do proxy do Render
const pingInterval = setInterval(() => {
  wss.clients.forEach(wsClient => {
    if (wsClient.isAlive === false) {
      return wsClient.terminate();
    }
    wsClient.isAlive = false;
    wsClient.ping();
  });
}, 25000);

wss.on('close', () => {
  clearInterval(pingInterval);
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`=======================================================`);
  console.log(`Google Meet Clone Server rodando na porta ${PORT}`);
  console.log(`Acesse no navegador: http://localhost:${PORT}`);
  console.log(`=======================================================`);
});
