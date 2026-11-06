'use strict';

const CHUNK = 16 * 1024;
const BUFFER_HIGH = 4 * 1024 * 1024;
const BUFFER_LOW = 1024 * 1024;

const $ = (id) => document.getElementById(id);
const els = {
  tabManual: $('tabManual'), tabServer: $('tabServer'),
  panelManual: $('panelManual'), panelServer: $('panelServer'),
  btnOffer: $('btnOffer'), offerCode: $('offerCode'),
  offerInput: $('offerInput'), btnAnswer: $('btnAnswer'), answerCode: $('answerCode'),
  answerInput: $('answerInput'), btnConnect: $('btnConnect'),
  btnCreateRoom: $('btnCreateRoom'), roomInfo: $('roomInfo'), roomCode: $('roomCode'),
  roomJoin: $('roomJoin'), btnJoinRoom: $('btnJoinRoom'),
  connStatus: $('connStatus'), btnClose: $('btnClose'),
  dropzone: $('dropzone'), fileInput: $('fileInput'), fileList: $('fileList'),
  stats: $('stats'), log: $('log'),
};

const state = {
  pc: null,
  dc: null,
  role: null,          // 'host' | 'guest'
  useServer: false,
  ws: null,
  files: [],           // host: {id, file, sent, status}
  recvCurrent: null,
  sending: false,
  drainResolve: null,
  samples: [],         // {t, bytes} per calcolo velocità
};

function log(msg) {
  const line = document.createElement('div');
  const t = new Date().toLocaleTimeString('it-IT');
  line.innerHTML = '<span class="t"></span><span></span>';
  line.children[0].textContent = t;
  line.children[1].textContent = msg;
  els.log.appendChild(line);
  els.log.scrollTop = els.log.scrollHeight;
}

function fmtSize(n) {
  if (n < 1024) return n + ' B';
  const u = ['KB', 'MB', 'GB', 'TB'];
  let i = -1;
  do { n /= 1024; i++; } while (n >= 1024 && i < u.length - 1);
  return n.toFixed(1) + ' ' + u[i];
}

function encodePayload(obj) {
  const bytes = new TextEncoder().encode(JSON.stringify(obj));
  let bin = '';
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin);
}

function decodePayload(str) {
  const bin = atob(str.trim());
  const bytes = Uint8Array.from(bin, (c) => c.charCodeAt(0));
  return JSON.parse(new TextDecoder().decode(bytes));
}

function recordBytes(n) {
  state.samples.push({ t: performance.now(), bytes: n });
}

setInterval(() => {
  const now = performance.now();
  while (state.samples.length && now - state.samples[0].t > 3000) state.samples.shift();
  if (state.samples.length < 2) { els.stats.textContent = ''; return; }
  let bytes = 0;
  for (const s of state.samples) bytes += s.bytes;
  const span = (now - state.samples[0].t) / 1000;
  els.stats.textContent = span > 0 ? 'Velocità: ' + fmtSize(bytes / span) + '/s' : '';
}, 500);

/* ---------- PeerConnection ---------- */

function createPeer() {
  const pc = new RTCPeerConnection({
    iceServers: [{ urls: ['stun:stun.l.google.com:19302'] }],
  });
  pc.onconnectionstatechange = updateStatus;
  pc.oniceconnectionstatechange = updateStatus;
  if (state.useServer) {
    pc.onicecandidate = (e) => {
      if (e.candidate) serverSend({ cmd: 'signal', data: { candidate: e.candidate.toJSON() } });
    };
  }
  return pc;
}

function waitIceComplete(pc, timeout = 8000) {
  if (pc.iceGatheringState === 'complete') return Promise.resolve();
  return new Promise((resolve) => {
    const timer = setTimeout(done, timeout);
    function onChange() {
      if (pc.iceGatheringState === 'complete') done();
    }
    function done() {
      clearTimeout(timer);
      pc.removeEventListener('icegatheringstatechange', onChange);
      resolve();
    }
    pc.addEventListener('icegatheringstatechange', onChange);
  });
}

function updateStatus() {
  if (!state.pc) {
    els.connStatus.textContent = 'Stato: non connesso';
    els.connStatus.classList.remove('ok');
    return;
  }
  let s = 'Stato: ' + (state.pc.connectionState || '—') + ' · ICE: ' + (state.pc.iceConnectionState || '—');
  if (state.dc && state.dc.readyState === 'open') s += ' · Canale dati: aperto';
  els.connStatus.textContent = s;
  const open = state.dc && state.dc.readyState === 'open';
  els.connStatus.classList.toggle('ok', !!open);
  els.btnClose.hidden = !state.pc;
  if (state.role === 'host') els.dropzone.hidden = !open;
}

/* ---------- DataChannel ---------- */

function setupChannel(dc) {
  state.dc = dc;
  dc.binaryType = 'arraybuffer';
  dc.bufferedAmountLowThreshold = BUFFER_LOW;
  dc.onopen = () => {
    log('Canale dati aperto: trasferimento pronto');
    updateStatus();
    if (state.role === 'host') pump();
  };
  dc.onclose = () => { log('Canale dati chiuso'); updateStatus(); };
  dc.onerror = (e) => { log('Errore canale dati: ' + (e.error ? e.error.message : 'sconosciuto')); };
  dc.onbufferedamountlow = () => {
    if (state.drainResolve) { const r = state.drainResolve; state.drainResolve = null; r(); }
  };
  dc.onmessage = onMessage;
}

function sendJSON(obj) {
  state.dc.send(JSON.stringify(obj));
}

function waitDrain() {
  return new Promise((resolve) => {
    if (!state.dc || state.dc.bufferedAmount <= BUFFER_HIGH) return resolve();
    state.drainResolve = resolve;
  });
}

/* ---------- Invio (host) ---------- */

function addFiles(list) {
  let added = 0;
  for (const f of list) {
    const item = { id: 'f' + Math.random().toString(36).slice(2, 10), file: f, sent: 0, status: 'in coda' };
    state.files.push(item);
    addFileRow(item, 'out');
    added++;
  }
  if (added) log(added + ' file in coda');
  if (state.dc && state.dc.readyState === 'open') pump();
}

async function pump() {
  if (state.sending) return;
  if (!state.dc || state.dc.readyState !== 'open') return;
  state.sending = true;
  try {
    for (;;) {
      const next = state.files.find((f) => f.status === 'in coda');
      if (!next) break;
      if (state.dc.readyState !== 'open') break;
      await sendFile(next);
    }
    if (state.files.some((f) => f.status === 'in coda' || f.status === 'sending')) {
      sendJSON({ t: 'all-done' });
      log('Tutti i file sono stati inviati');
    }
  } catch (e) {
    log('Errore invio: ' + e.message);
  }
  state.sending = false;
  if (state.dc && state.dc.readyState === 'open' && state.files.some((f) => f.status === 'in coda')) pump();
}

async function sendFile(item) {
  item.status = 'sending';
  updateRow(item.id, 0, item.file.size);
  sendJSON({ t: 'meta', id: item.id, name: item.file.name, size: item.file.size, type: item.file.type || 'application/octet-stream' });
  const buf = await item.file.arrayBuffer();
  let offset = 0;
  while (offset < buf.byteLength) {
    if (state.dc.readyState !== 'open') throw new Error('canale chiuso durante l\'invio');
    await waitDrain();
    const end = Math.min(offset + CHUNK, buf.byteLength);
    const chunk = buf.slice(offset, end);
    state.dc.send(chunk);
    recordBytes(chunk.byteLength);
    offset = end;
    item.sent = offset;
    updateRow(item.id, offset, item.file.size);
  }
  sendJSON({ t: 'end', id: item.id });
  item.status = 'done';
  markRowDone(item.id);
  log('Inviato: ' + item.file.name + ' (' + fmtSize(item.file.size) + ')');
}

/* ---------- Ricezione (guest) ---------- */

function onMessage(e) {
  if (typeof e.data === 'string') {
    const msg = JSON.parse(e.data);
    if (msg.t === 'meta') {
      state.recvCurrent = { id: msg.id, name: msg.name, size: msg.size, type: msg.type, chunks: [], received: 0, row: null };
      state.recvCurrent.row = addFileRow(state.recvCurrent, 'in');
      log('Ricezione: ' + msg.name + ' (' + fmtSize(msg.size) + ')');
    } else if (msg.t === 'end') {
      if (state.recvCurrent) { finalizeFile(state.recvCurrent); state.recvCurrent = null; }
    } else if (msg.t === 'all-done') {
      log('Trasferimento completato dal mittente');
    }
    return;
  }
  const cur = state.recvCurrent;
  if (!cur) return;
  cur.chunks.push(e.data);
  cur.received += e.data.byteLength;
  recordBytes(e.data.byteLength);
  updateRow(cur.id, cur.received, cur.size);
}

function finalizeFile(cur) {
  const blob = new Blob(cur.chunks, { type: cur.type });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = cur.name;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 30000);
  markRowDone(cur.id);
  log('Salvato: ' + cur.name + ' (' + fmtSize(cur.size) + ')');
}

/* ---------- UI file ---------- */

function addFileRow(item, dir) {
  const li = document.createElement('li');
  li.id = 'row-' + item.id;
  const arrow = dir === 'out' ? '↗' : '↘';
  li.innerHTML =
    '<div class="fileHead"><span class="fileName"></span><span class="fileMeta"></span></div>' +
    '<div class="bar"><div></div></div>';
  li.querySelector('.fileName').textContent = arrow + ' ' + item.name;
  li.querySelector('.fileMeta').textContent = fmtSize(item.size || item.file.size) + ' · 0%';
  els.fileList.appendChild(li);
  return li;
}

function updateRow(id, done, total) {
  const li = $('row-' + id);
  if (!li) return;
  const pct = total > 0 ? Math.min(100, (done / total) * 100) : 100;
  li.querySelector('.bar > div').style.width = pct + '%';
  li.querySelector('.fileMeta').textContent = fmtSize(total) + ' · ' + pct.toFixed(0) + '%';
}

function markRowDone(id) {
  const li = $('row-' + id);
  if (li) li.classList.add('done');
}

/* ---------- Segnale manuale (copia-incolla) ---------- */

els.btnOffer.onclick = async () => {
  try {
    state.useServer = false;
    state.role = 'host';
    const pc = createPeer();
    state.pc = pc;
    setupChannel(pc.createDataChannel('files', { ordered: true }));
    await pc.setLocalDescription(await pc.createOffer());
    await waitIceComplete(pc);
    els.offerCode.value = encodePayload({ v: 1, type: pc.localDescription.type, sdp: pc.localDescription.sdp });
    log('Codice invito generato (passo 1)');
    updateStatus();
  } catch (e) { log('Errore offer: ' + e.message); }
};

els.btnAnswer.onclick = async () => {
  try {
    const data = decodePayload(els.offerInput.value);
    state.useServer = false;
    state.role = 'guest';
    const pc = createPeer();
    state.pc = pc;
    pc.ondatachannel = (e) => setupChannel(e.channel);
    await pc.setRemoteDescription({ type: 'offer', sdp: data.sdp });
    await pc.setLocalDescription(await pc.createAnswer());
    await waitIceComplete(pc);
    els.answerCode.value = encodePayload({ v: 1, type: pc.localDescription.type, sdp: pc.localDescription.sdp });
    log('Risposta generata (passo 2)');
    updateStatus();
  } catch (e) { log('Errore risposta: ' + e.message); }
};

els.btnConnect.onclick = async () => {
  try {
    const data = decodePayload(els.answerInput.value);
    await state.pc.setRemoteDescription({ type: 'answer', sdp: data.sdp });
    log('Risposta applicata, in attesa di connessione...');
  } catch (e) { log('Errore collegamento: ' + e.message); }
};

/* ---------- Segnale via server (stanza) ---------- */

let ws = null;
function serverSend(obj) { if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj)); }

function openWs(onOpen) {
  const proto = location.protocol === 'https:' ? 'wss://' : 'ws://';
  ws = new WebSocket(proto + location.host + '/api/ws');
  ws.onopen = () => { log('Server di segnalazione connesso'); onOpen(); };
  ws.onmessage = onWsMessage;
  ws.onclose = () => { log('Server di segnalazione disconnesso'); };
  ws.onerror = () => { log('Errore del server di segnalazione'); };
}

els.btnCreateRoom.onclick = () => {
  state.useServer = true;
  state.role = 'host';
  openWs(() => serverSend({ cmd: 'create' }));
};

els.btnJoinRoom.onclick = () => {
  const room = els.roomJoin.value.trim().toUpperCase();
  if (!room) return;
  state.useServer = true;
  state.role = 'guest';
  openWs(() => serverSend({ cmd: 'join', room }));
};

async function onWsMessage(ev) {
  const msg = JSON.parse(ev.data);
  switch (msg.cmd) {
    case 'created':
      els.roomCode.textContent = msg.room;
      els.roomInfo.hidden = false;
      log('Stanza creata: ' + msg.room + ' — condividi il codice');
      break;
    case 'joined':
      log('Unito alla stanza ' + msg.room);
      break;
    case 'peer':
      await onPeerJoined();
      break;
    case 'signal':
      await onServerSignal(msg.data);
      break;
    case 'peer-left':
      log('Il peer ha lasciato la stanza');
      break;
    case 'error':
      log('Errore: ' + msg.message);
      break;
  }
}

async function onPeerJoined() {
  const pc = createPeer();
  state.pc = pc;
  setupChannel(pc.createDataChannel('files', { ordered: true }));
  await pc.setLocalDescription(await pc.createOffer());
  serverSend({ cmd: 'signal', data: { sdp: pc.localDescription } });
  log('Offer inviata al peer');
}

async function onServerSignal(data) {
  if (!state.pc) {
    const pc = createPeer();
    state.pc = pc;
    pc.ondatachannel = (e) => setupChannel(e.channel);
    await pc.setRemoteDescription(data.sdp);
    await pc.setLocalDescription(await pc.createAnswer());
    serverSend({ cmd: 'signal', data: { sdp: pc.localDescription } });
    log('Offer ricevuta, risposta inviata');
  } else if (data.sdp) {
    await state.pc.setRemoteDescription(data.sdp);
    log('Risposta del peer applicata');
  } else if (data.candidate) {
    try { await state.pc.addIceCandidate(data.candidate); } catch (e) { console.warn(e); }
  }
  updateStatus();
}

/* ---------- Varie ---------- */

els.tabManual.onclick = () => {
  els.tabManual.classList.add('active');
  els.tabServer.classList.remove('active');
  els.panelManual.hidden = false;
  els.panelServer.hidden = true;
};
els.tabServer.onclick = () => {
  els.tabServer.classList.add('active');
  els.tabManual.classList.remove('active');
  els.panelServer.hidden = false;
  els.panelManual.hidden = true;
};

document.querySelectorAll('.copy').forEach((btn) => {
  btn.onclick = async () => {
    const ta = $(btn.dataset.target);
    try {
      await navigator.clipboard.writeText(ta.value);
      btn.textContent = 'Copiato!';
    } catch (e) {
      ta.select();
      document.execCommand('copy');
      btn.textContent = 'Copiato!';
    }
    setTimeout(() => { btn.textContent = 'Copia'; }, 1500);
  };
});

els.btnClose.onclick = () => {
  if (state.dc) state.dc.close();
  if (state.pc) state.pc.close();
  if (ws) ws.close();
  state.pc = null; state.dc = null; ws = null;
  log('Connessione chiusa');
  updateStatus();
};

els.fileInput.onchange = () => { addFiles(els.fileInput.files); els.fileInput.value = ''; };

['dragenter', 'dragover'].forEach((ev) =>
  els.dropzone.addEventListener(ev, (e) => { e.preventDefault(); els.dropzone.classList.add('over'); }));
['dragleave', 'drop'].forEach((ev) =>
  els.dropzone.addEventListener(ev, (e) => { e.preventDefault(); els.dropzone.classList.remove('over'); }));
els.dropzone.addEventListener('drop', (e) => { if (e.dataTransfer.files.length) addFiles(e.dataTransfer.files); });

updateStatus();
