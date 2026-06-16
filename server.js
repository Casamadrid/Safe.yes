const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;

// Stockage temporaire des salles en mémoire (jamais persisté)
const rooms = new Map();

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('SafeYes Server OK');
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  let currentRoom = null;
  let currentRole = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {

      // Personne A crée une salle avec un code à 4 chiffres
      case 'create': {
        const code = msg.code;
        if (!code || rooms.has(code)) {
          ws.send(JSON.stringify({ type: 'error', reason: 'code_taken' }));
          return;
        }
        rooms.set(code, { a: ws, b: null, created: Date.now() });
        currentRoom = code;
        currentRole = 'a';
        ws.send(JSON.stringify({ type: 'created', code }));

        // Expire la salle après 10 minutes si personne B ne rejoint pas
        setTimeout(() => {
          if (rooms.has(code) && !rooms.get(code).b) {
            rooms.delete(code);
          }
        }, 10 * 60 * 1000);
        break;
      }

      // Personne B rejoint avec le code
      case 'join': {
        const code = msg.code;
        const room = rooms.get(code);
        if (!room) {
          ws.send(JSON.stringify({ type: 'error', reason: 'room_not_found' }));
          return;
        }
        if (room.b) {
          ws.send(JSON.stringify({ type: 'error', reason: 'room_full' }));
          return;
        }
        room.b = ws;
        currentRoom = code;
        currentRole = 'b';
        ws.send(JSON.stringify({ type: 'joined', code }));
        // Notifier A que B a rejoint
        if (room.a && room.a.readyState === 1) {
          room.a.send(JSON.stringify({ type: 'peer_joined' }));
        }
        break;
      }

      // Relayer un message à l'autre téléphone
      case 'signal': {
        const room = rooms.get(currentRoom);
        if (!room) return;
        const peer = currentRole === 'a' ? room.b : room.a;
        if (peer && peer.readyState === 1) {
          peer.send(JSON.stringify({ type: 'signal', data: msg.data }));
        }
        break;
      }

      // Mot d'arrêt déclenché — alerter l'autre téléphone immédiatement
      case 'stop': {
        const room = rooms.get(currentRoom);
        if (!room) return;
        const peer = currentRole === 'a' ? room.b : room.a;
        if (peer && peer.readyState === 1) {
          peer.send(JSON.stringify({ type: 'stop_alert', word: msg.word || '' }));
        }
        // Supprimer la salle
        rooms.delete(currentRoom);
        break;
      }
    }
  });

  ws.on('close', () => {
    if (!currentRoom) return;
    const room = rooms.get(currentRoom);
    if (!room) return;
    // Notifier l'autre que la connexion est perdue
    const peer = currentRole === 'a' ? room.b : room.a;
    if (peer && peer.readyState === 1) {
      peer.send(JSON.stringify({ type: 'peer_disconnected' }));
    }
    rooms.delete(currentRoom);
  });
});

server.listen(PORT, () => {
  console.log(`SafeYes server running on port ${PORT}`);
});
