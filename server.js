const http = require('http');
const fs = require('fs');
const path = require('path');
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

// WebSocket Server oficial com suporte a Heartbeat/Ping-Pong automático
const wss = new WebSocketServer({ server, path: '/signaling' });

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
