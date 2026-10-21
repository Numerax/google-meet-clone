const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

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

// Parser nativo RFC 6455 para WebSockets (zero dependências npm)
function parseWsFrame(buffer) {
  if (buffer.length < 2) return null;
  const firstByte = buffer[0];
  const secondByte = buffer[1];
  const opcode = firstByte & 0x0f;
  const isMasked = (secondByte & 0x80) !== 0;
  let payloadLength = secondByte & 0x7f;
  let offset = 2;

  if (payloadLength === 126) {
    if (buffer.length < 4) return null;
    payloadLength = buffer.readUInt16BE(2);
    offset = 4;
  } else if (payloadLength === 127) {
    if (buffer.length < 10) return null;
    payloadLength = Number(buffer.readBigUInt64BE(2));
    offset = 10;
  }

  if (isMasked) {
    if (buffer.length < offset + 4 + payloadLength) return null;
    const mask = buffer.slice(offset, offset + 4);
    offset += 4;
    const payload = Buffer.allocUnsafe(payloadLength);
    for (let i = 0; i < payloadLength; i++) {
      payload[i] = buffer[offset + i] ^ mask[i % 4];
    }
    return { opcode, payload, totalLength: offset + payloadLength };
  } else {
    if (buffer.length < offset + payloadLength) return null;
    const payload = buffer.slice(offset, offset + payloadLength);
    return { opcode, payload, totalLength: offset + payloadLength };
  }
}

function sendWs(socket, data) {
  try {
    const payload = Buffer.from(typeof data === 'string' ? data : JSON.stringify(data), 'utf8');
    const len = payload.length;
    let header;

    if (len < 126) {
      header = Buffer.from([0x81, len]);
    } else if (len <= 65535) {
      header = Buffer.alloc(4);
      header[0] = 0x81;
      header[1] = 126;
      header.writeUInt16BE(len, 2);
    } else {
      header = Buffer.alloc(10);
      header[0] = 0x81;
      header[1] = 127;
      header.writeBigUInt64BE(BigInt(len), 2);
    }
    socket.write(Buffer.concat([header, payload]));
  } catch (e) {
    // Socket fechado
  }
}

// Salas de Reunião em Memória (Signaling WebRTC)
// rooms: { [roomId]: Map<peerId, { socket, name }> }
const rooms = new Map();

server.on('upgrade', (req, socket, head) => {
  if (req.url.startsWith('/signaling')) {
    const key = req.headers['sec-websocket-key'];
    const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');

    socket.write(
      'HTTP/1.1 101 Switching Protocols\r\n' +
      'Upgrade: websocket\r\n' +
      'Connection: Upgrade\r\n' +
      `Sec-WebSocket-Accept: ${accept}\r\n\r\n`
    );

    const peerId = 'peer_' + Math.random().toString(36).substring(2, 9);
    let currentRoomId = null;
    let userName = 'Participante';
    let accumulatedBuffer = Buffer.alloc(0);

    socket.on('data', chunk => {
      accumulatedBuffer = Buffer.concat([accumulatedBuffer, chunk]);

      while (accumulatedBuffer.length > 0) {
        const frame = parseWsFrame(accumulatedBuffer);
        if (!frame) break;
        accumulatedBuffer = accumulatedBuffer.slice(frame.totalLength);

        if (frame.opcode === 1) { // Text frame (JSON)
          try {
            const msg = JSON.parse(frame.payload.toString('utf8'));

            // 1. Entrar na Sala
            if (msg.type === 'join') {
              currentRoomId = msg.roomId || 'geral';
              userName = msg.name || 'Convidado';

              if (!rooms.has(currentRoomId)) {
                rooms.set(currentRoomId, new Map());
              }

              const room = rooms.get(currentRoomId);

              // Informa o novo usuário sobre os membros que já estão na sala
              const existingPeers = [];
              for (const [id, peer] of room.entries()) {
                existingPeers.push({ peerId: id, name: peer.name });
              }

              sendWs(socket, {
                type: 'room_joined',
                peerId: peerId,
                roomId: currentRoomId,
                peers: existingPeers
              });

              // Notifica quem já estava na sala sobre o novo participante
              for (const [id, peer] of room.entries()) {
                sendWs(peer.socket, {
                  type: 'peer_joined',
                  peerId: peerId,
                  name: userName
                });
              }

              // Adiciona à sala
              room.set(peerId, { socket, name: userName });
              console.log(`[Meet] ${userName} (${peerId}) entrou na sala: ${currentRoomId}`);
            }

            // 2. Troca de Sinalização WebRTC (SDP Offer / Answer e ICE Candidates)
            else if (msg.type === 'signal') {
              if (currentRoomId && rooms.has(currentRoomId)) {
                const targetPeer = rooms.get(currentRoomId).get(msg.targetPeerId);
                if (targetPeer) {
                  sendWs(targetPeer.socket, {
                    type: 'signal',
                    senderPeerId: peerId,
                    name: userName,
                    signalData: msg.signalData
                  });
                }
              }
            }

            // 3. Chat de Mensagens
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
                  sendWs(peer.socket, chatMsg);
                }
              }
            }

            // 4. Reações (Emojis) e Levantar a Mão
            else if (msg.type === 'reaction' || msg.type === 'raise_hand') {
              if (currentRoomId && rooms.has(currentRoomId)) {
                const room = rooms.get(currentRoomId);
                for (const peer of room.values()) {
                  sendWs(peer.socket, {
                    type: msg.type,
                    senderPeerId: peerId,
                    senderName: userName,
                    data: msg.data
                  });
                }
              }
            }

          } catch (e) {
            console.error('Erro ao processar mensagem WS:', e);
          }
        } else if (frame.opcode === 8) { // Close
          socket.end();
        }
      }
    });

    socket.on('close', () => {
      if (currentRoomId && rooms.has(currentRoomId)) {
        const room = rooms.get(currentRoomId);
        room.delete(peerId);
        console.log(`[Meet] ${userName} (${peerId}) saiu da sala: ${currentRoomId}`);

        // Notifica os outros membros que o participante saiu
        for (const peer of room.values()) {
          sendWs(peer.socket, {
            type: 'peer_left',
            peerId: peerId
          });
        }

        if (room.size === 0) {
          rooms.delete(currentRoomId);
        }
      }
    });
  } else {
    socket.destroy();
  }
});

server.listen(PORT, () => {
  console.log(`=======================================================`);
  console.log(`Google Meet Clone Server rodando na porta ${PORT}`);
  console.log(`Acesse no navegador: http://localhost:${PORT}`);
  console.log(`=======================================================`);
});
