const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const crypto = require('crypto');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: '/ws', maxPayload: 32 * 1024 });
const PORT = process.env.PORT || 10000;
const rooms = new Map();
const rankedQueue = [];
const rankedProfiles = new Map(); // memoria del proceso: para persistencia real se conectará PostgreSQL
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

app.use(express.static(path.join(__dirname, 'public')));
app.get('/healthz', (_req, res) => res.json({ ok: true, service: 'Famous Smash', rooms: rooms.size, rankedQueue: rankedQueue.length }));
function rankName(rp){return rp>=1400?'LEYENDA':rp>=1000?'MAESTRO':rp>=700?'DIAMANTE':rp>=450?'PLATINO':rp>=250?'ORO':rp>=100?'PLATA':'BRONCE'}
app.get('/api/leaderboard', (_req,res)=>{const players=[...rankedProfiles.values()].sort((a,b)=>b.rp-a.rp||b.wins-a.wins).slice(0,10).map(p=>({alias:p.alias,rp:p.rp,rank:rankName(p.rp),wins:p.wins}));res.json({players});});

function makeCode() {
  let code;
  do {
    code = Array.from({ length: 4 }, () => CODE_CHARS[crypto.randomInt(CODE_CHARS.length)]).join('');
  } while (rooms.has(code));
  return code;
}
function send(ws, data) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data));
}
function broadcast(room, data, exceptId = null) {
  for (const p of room.players.values()) {
    if (p.id !== exceptId) send(p.ws, data);
  }
}
function publicPlayers(room) {
  return [...room.players.values()].map(p => ({ id: p.id, alias: p.alias, hero: p.hero, team: p.team }));
}
function roomUpdate(room) {
  const payload = { type: 'roomUpdate', code: room.code, mode: room.mode, hostId: room.hostId, started: room.started, players: publicPlayers(room) };
  broadcast(room, payload);
}
function cleanString(value, max, fallback) {
  if (typeof value !== 'string') return fallback;
  const v = value.trim().replace(/[<>\u0000-\u001f]/g, '').slice(0, max);
  return v || fallback;
}
function removeFromRoom(ws) {
  const code = ws.roomCode;
  if (!code) return;
  const room = rooms.get(code);
  ws.roomCode = null;
  if (!room) return;
  const id = ws.playerId;
  const wasHost = room.hostId === id;
  room.players.delete(id);
  if (room.players.size === 0) {
    rooms.delete(code);
    return;
  }
  if (wasHost) room.hostId = room.players.keys().next().value;
  broadcast(room, { type: 'playerLeft', id, hostId: room.hostId, message: 'Un jugador salió de la sala.' });
  roomUpdate(room);
}
function joinRoom(ws, room, profile) {
  if (room.started) return send(ws, { type: 'error', message: 'La partida ya comenzó. Crea otra sala.' });
  if (room.players.size >= room.mode * 2) return send(ws, { type: 'error', message: 'La sala está llena.' });
  const counts = [0, 0];
  for (const p of room.players.values()) counts[p.team]++;
  const team = counts[0] <= counts[1] ? 0 : 1;
  const id = crypto.randomUUID();
  const player = { id, alias: profile.alias, hero: profile.hero, team, ws };
  room.players.set(id, player);
  ws.playerId = id;
  ws.roomCode = room.code;
  send(ws, { type: 'roomState', selfId: id, code: room.code, mode: room.mode, hostId: room.hostId, started: room.started, players: publicPlayers(room) });
}


function profileFor(msg){
 const profileId=cleanString(msg.profileId,80,'');
 if(!profileId) return null;
 let p=rankedProfiles.get(profileId);
 if(!p){p={profileId,alias:cleanString(msg.alias,14,'Jugador'),rp:Math.max(0,Math.min(5000,Number(msg.rp)||0)),wins:0,losses:0};rankedProfiles.set(profileId,p)}
 p.alias=cleanString(msg.alias,14,p.alias); return p;
}
function removeQueue(ws){const i=rankedQueue.findIndex(x=>x.ws===ws);if(i>=0)rankedQueue.splice(i,1)}
function tryMatch(){
 while(rankedQueue.length>=2){
  rankedQueue.sort((a,b)=>a.rp-b.rp);
  let bestA=-1,bestB=-1,bestGap=Infinity;
  for(let i=0;i<rankedQueue.length;i++)for(let j=i+1;j<rankedQueue.length;j++){
   const gap=Math.abs(rankedQueue[i].rp-rankedQueue[j].rp);
   if(gap<bestGap){bestGap=gap;bestA=i;bestB=j;}
  }
  if(bestA<0) return;
  const b=rankedQueue.splice(bestB,1)[0],a=rankedQueue.splice(bestA,1)[0];
  if(a.ws.readyState!==WebSocket.OPEN||b.ws.readyState!==WebSocket.OPEN)continue;
  const room={code:makeCode(),mode:1,hostId:null,players:new Map(),started:true,ranked:true,results:new Set()};rooms.set(room.code,room);
  for(const [idx,item] of [a,b].entries()){
   const id=crypto.randomUUID(),player={id,alias:item.profile.alias,hero:item.hero,team:idx,ws:item.ws,profileId:item.profile.profileId};
   room.players.set(id,player);item.ws.playerId=id;item.ws.roomCode=room.code;item.ws.rankedRoom=room.code;
   send(item.ws,{type:'roomState',ranked:true,selfId:id,code:room.code,mode:1,hostId:null,started:true,players:publicPlayers(room)});
  }
  room.hostId=room.players.keys().next().value;
  broadcast(room,{type:'gameStart',ranked:true,code:room.code,mode:1,hostId:room.hostId,players:publicPlayers(room)});
 }
}

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.playerId = null;
  ws.roomCode = null;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return send(ws, { type: 'error', message: 'Mensaje inválido.' }); }
    if (!msg || typeof msg.type !== 'string') return;

    if (msg.type === 'ping') return send(ws, { type: 'pong', time: Date.now() });
    if(msg.type==='rankedQueue'){
      removeFromRoom(ws);removeQueue(ws);
      const profile=profileFor(msg);if(!profile)return send(ws,{type:'error',message:'No se pudo crear tu perfil competitivo.'});
      const item={ws,profile,hero:cleanString(msg.hero,32,'Michael Jackson'),rp:profile.rp};rankedQueue.push(item);
      send(ws,{type:'rankedWaiting',message:'Buscando rival de rango parecido...'});tryMatch();return;
    }
    if(msg.type==='cancelRanked'){removeQueue(ws);send(ws,{type:'rankedWaiting',message:'Búsqueda cancelada.'});return;}
    if(msg.type==='rankedResult'){
      const room=ws.roomCode?rooms.get(ws.roomCode):null;if(!room||!room.ranked||room.results.has(ws.playerId))return;
      room.results.add(ws.playerId);
      const player=room.players.get(ws.playerId);if(!player)return;
      const profile=rankedProfiles.get(player.profileId);if(!profile)return;
      if(msg.win===true){profile.rp=Math.min(5000,profile.rp+25);profile.wins++;}else{profile.rp=Math.max(0,profile.rp-10);profile.losses++;}
      send(ws,{type:'rankedUpdate',rp:profile.rp,rank:rankName(profile.rp),wins:profile.wins,losses:profile.losses});
      if(room.results.size>=room.players.size){for(const p of room.players.values())p.ws.roomCode=null;rooms.delete(room.code);}
      return;
    }

    if (msg.type === 'createRoom') {
      removeFromRoom(ws);
      const mode = [1, 3, 5, 10].includes(Number(msg.mode)) ? Number(msg.mode) : 1;
      const room = { code: makeCode(), mode, hostId: null, players: new Map(), started: false };
      rooms.set(room.code, room);
      joinRoom(ws, room, {
        alias: cleanString(msg.alias, 14, 'Jugador'),
        hero: cleanString(msg.hero, 32, 'Michael Jackson')
      });
      if (ws.playerId) room.hostId = ws.playerId;
      roomUpdate(room);
      return;
    }

    if (msg.type === 'joinRoom') {
      removeFromRoom(ws);
      const code = cleanString(String(msg.code || '').toUpperCase(), 4, '');
      const room = rooms.get(code);
      if (!room) return send(ws, { type: 'error', message: 'No existe esa sala. Revisa el código.' });
      joinRoom(ws, room, {
        alias: cleanString(msg.alias, 14, 'Jugador'),
        hero: cleanString(msg.hero, 32, 'Michael Jackson')
      });
      roomUpdate(room);
      return;
    }

    const room = ws.roomCode ? rooms.get(ws.roomCode) : null;
    if (!room) return send(ws, { type: 'error', message: 'Primero crea o entra a una sala.' });

    if (msg.type === 'startGame') {
      if (ws.playerId !== room.hostId) return send(ws, { type: 'error', message: 'Solo quien creó la sala puede comenzar.' });
      if (room.started) return;
      room.started = true;
      broadcast(room, { type: 'gameStart', code: room.code, mode: room.mode, hostId: room.hostId, players: publicPlayers(room) });
      return;
    }

    if (msg.type === 'leaveRoom') {
      removeFromRoom(ws);
      return send(ws, { type: 'leftRoom' });
    }

    if (msg.type === 'states' && room.started && Array.isArray(msg.items)) {
      const isHost = ws.playerId === room.hostId;
      const items = [];
      for (const item of msg.items.slice(0, 22)) {
        if (!item || typeof item.id !== 'string') continue;
        const isBot = item.id.startsWith('bot-');
        if (isBot ? !isHost : item.id !== ws.playerId) continue;
        const nums = ['x', 'y', 'z', 'rotation', 'damage'];
        if (!nums.every(k => Number.isFinite(item[k]))) continue;
        items.push({
          id: item.id,
          x: Math.max(-14, Math.min(14, item.x)),
          y: Math.max(-2, Math.min(12, item.y)),
          z: Math.max(-14, Math.min(14, item.z)),
          rotation: item.rotation,
          damage: Math.max(0, Math.min(999, item.damage)),
          alive: item.alive === true
        });
      }
      if (items.length) broadcast(room, { type: 'states', items }, ws.playerId);
      return;
    }

    if (msg.type === 'attack' && room.started) {
      const actorId = String(msg.actorId || '');
      const isHost = ws.playerId === room.hostId;
      const actorAllowed = actorId === ws.playerId || (actorId.startsWith('bot-') && isHost);
      const targetExists = [...room.players.values()].some(p => p.id === msg.targetId) || (typeof msg.targetId === 'string' && msg.targetId.startsWith('bot-'));
      if (!actorAllowed || !targetExists) return;
      const damage = Math.max(1, Math.min(40, Number(msg.damage) || 1));
      const force = Math.max(1, Math.min(14, Number(msg.force) || 4));
      broadcast(room, {
        type: 'attack', actorId, targetId: String(msg.targetId), damage, force,
        special: msg.special === true,
        shape: cleanString(msg.shape, 12, 'glove'),
        color: Number.isInteger(msg.color) ? msg.color : 0xffaa33
      }, ws.playerId);
      return;
    }
  });

  ws.on('close', () => {removeQueue(ws);removeFromRoom(ws)});
  ws.on('error', () => {removeQueue(ws);removeFromRoom(ws)});
});

// Heartbeat: elimina conexiones muertas sin mantener datos de jugadores persistentes.
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);
wss.on('close', () => clearInterval(heartbeat));

server.listen(PORT, '0.0.0.0', () => console.log(`Famous Smash escuchando en puerto ${PORT}`));
