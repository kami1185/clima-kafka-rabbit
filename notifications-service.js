// notifications-service.js — La "app" a la que se suscriben los usuarios.
//
// 1) API REST (/api/suscripciones) para suscribirse y cancelar.
// 2) Por cada usuario crea una cola propia 'usuario.<id>.app' enlazada al
//    exchange topic 'clima.alertas' SOLO con las routing keys que le interesan.
//    → Si el usuario está desconectado, sus alertas se ACUMULAN en su cola y
//      le llegan al volver a abrir la app (notificaciones push "pendientes").
// 3) Router: consume TODAS las alertas (#) y, para cada usuario que pidió
//    correo o SMS, publica un trabajo en el exchange direct 'notificaciones.canales'.
//    Los workers de email y SMS los procesan (colas de trabajo con reintentos).
// 4) Recibe el estado de cada entrega (fanout 'notificaciones.estado') y se lo
//    muestra al usuario en tiempo real por WebSocket.
const express = require('express');
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');
const {
  conectarRabbit, crearCanal, declararTopologia,
  EX, Q, NIVELES, TIPOS, ESTACIONES,
} = require('./rabbit');

const PUERTO = Number(process.env.PUERTO || 3000);
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const ARCHIVO_USUARIOS = path.join(DATA_DIR, 'usuarios.json');

const usuarios = new Map();        // id → datos de la suscripción
const sockets = new Map();         // id → Set<WebSocket> (puede tener varias pestañas abiertas)
const consumidoresApp = new Map(); // id → Promise<{consumerTag}> del consumo de su cola

let chPub; // canal para declarar y publicar
let chApp; // canal para consumir las colas de los usuarios

// --- Colas por usuario ---
const colaApp = (id) => `usuario.${id}.app`;
const ARGS_COLA_APP = {
  'x-max-length': 50,              // como máximo 50 alertas pendientes...
  'x-overflow': 'drop-head',       // ...si llegan más, se descartan las más antiguas
  'x-message-ttl': 60 * 60 * 1000, // una alerta de hace más de 1 hora ya no sirve
};

// --- Persistencia sencilla de las suscripciones (volumen de Docker) ---
function cargarUsuarios() {
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    if (fs.existsSync(ARCHIVO_USUARIOS)) {
      JSON.parse(fs.readFileSync(ARCHIVO_USUARIOS, 'utf8')).forEach((u) => usuarios.set(u.id, u));
    }
    console.log(`[Notificaciones] ${usuarios.size} suscripciones cargadas`);
  } catch (e) {
    console.error('[Notificaciones] No se pudieron cargar las suscripciones:', e.message);
  }
}
function guardarUsuarios() {
  fs.writeFileSync(ARCHIVO_USUARIOS, JSON.stringify([...usuarios.values()], null, 2));
}

// --- Reglas de suscripción ---
const nivelesDesde = (minimo) => NIVELES.slice(NIVELES.indexOf(minimo));

// Routing keys que se enlazan a la cola del usuario: <tipo>.<nivel>.<estacion>
function clavesDeEnrutamiento(u) {
  const claves = [];
  for (const tipo of u.tipos)
    for (const nivel of nivelesDesde(u.nivelMinimo))
      for (const estacion of u.estaciones)
        claves.push(`${tipo}.${nivel}.${estacion}`);
  return claves;
}

const coincide = (u, a) =>
  u.tipos.includes(a.tipo) &&
  u.estaciones.includes(a.stationId) &&
  NIVELES.indexOf(a.nivel) >= NIVELES.indexOf(u.nivelMinimo);

function validar(body = {}) {
  const errores = [];
  const nombre = String(body.nombre || '').trim().slice(0, 60);
  const canales = [...new Set((body.canales || []).filter((c) => ['email', 'sms'].includes(c)))];
  const email = String(body.email || '').trim();
  const telefono = String(body.telefono || '').replace(/[\s-]/g, '');
  const estaciones = [...new Set((body.estaciones || []).filter((e) => ESTACIONES.includes(e)))];
  const tipos = [...new Set((body.tipos || []).filter((t) => TIPOS.includes(t)))];
  const nivelMinimo = NIVELES.includes(body.nivelMinimo) ? body.nivelMinimo : 'naranja';

  if (!nombre) errores.push('Escribe tu nombre.');
  if (estaciones.length === 0) errores.push('Elige al menos una estación.');
  if (tipos.length === 0) errores.push('Elige al menos un tipo de alerta.');
  if (canales.includes('email') && !/^\S+@\S+\.\S+$/.test(email)) errores.push('Escribe un correo válido.');
  if (canales.includes('sms') && !/^\+?\d{7,15}$/.test(telefono)) errores.push('El teléfono debe tener entre 7 y 15 dígitos.');

  return {
    errores,
    datos: {
      nombre, estaciones, tipos, nivelMinimo, canales,
      email: canales.includes('email') ? email : null,
      telefono: canales.includes('sms') ? telefono : null,
    },
  };
}

async function prepararColaUsuario(u) {
  const cola = colaApp(u.id);
  await chPub.assertQueue(cola, { durable: true, arguments: ARGS_COLA_APP });
  for (const clave of clavesDeEnrutamiento(u)) {
    await chPub.bindQueue(cola, EX.ALERTAS, clave);
  }
}

// --- WebSocket: envío a todas las pestañas abiertas de un usuario ---
function enviarAUsuario(id, datos) {
  const conjunto = sockets.get(id);
  if (!conjunto) return 0;
  let enviados = 0;
  for (const ws of conjunto) {
    if (ws.readyState === ws.OPEN) {
      ws.send(JSON.stringify(datos));
      enviados++;
    }
  }
  return enviados;
}

// Solo se consume la cola del usuario mientras tiene la app abierta.
// Mientras tanto, RabbitMQ guarda sus alertas.
function iniciarConsumoApp(id) {
  if (consumidoresApp.has(id)) return consumidoresApp.get(id);
  const promesa = chApp.consume(colaApp(id), (msg) => {
    if (!msg) return; // el broker canceló el consumidor (por ejemplo, se borró la cola)
    const alerta = JSON.parse(msg.content.toString());
    const entregados = enviarAUsuario(id, { kind: 'alerta', alerta, reentregada: msg.fields.redelivered });
    if (entregados > 0) chApp.ack(msg);
    else {
      chApp.nack(msg, false, true); // nadie conectado: vuelve a la cola...
      detenerConsumoApp(id).catch(() => {}); // ...y dejamos de consumir hasta que vuelva
    }
  });
  consumidoresApp.set(id, promesa);
  return promesa;
}

async function detenerConsumoApp(id) {
  const promesa = consumidoresApp.get(id);
  if (!promesa) return;
  consumidoresApp.delete(id);
  const { consumerTag } = await promesa;
  await chApp.cancel(consumerTag);
}

// --- Router: alertas → trabajos de correo / SMS ---
async function iniciarRouter(conn) {
  const ch = await crearCanal(conn, 'router');
  await ch.prefetch(10);
  await ch.assertQueue(Q.ROUTER, { durable: true });
  await ch.bindQueue(Q.ROUTER, EX.ALERTAS, '#'); // '#' = todas las routing keys

  await ch.consume(Q.ROUTER, (msg) => {
    if (!msg) return;
    try {
      const alerta = JSON.parse(msg.content.toString());
      let trabajos = 0;
      for (const u of usuarios.values()) {
        if (!coincide(u, alerta)) continue;
        for (const canal of u.canales) {
          const trabajo = {
            notificacionId: crypto.randomUUID(),
            alertaId: alerta.id,
            usuarioId: u.id,
            canal,
            destino: canal === 'email' ? u.email : u.telefono,
            asunto: `Alerta ${alerta.nivel}: ${alerta.titulo} en ${alerta.stationId}`,
            mensaje: `Hola ${u.nombre}. ${alerta.titulo}: ${alerta.valor} ${alerta.unidad} en ${alerta.stationId}.`,
          };
          chPub.publish(EX.CANALES, canal, Buffer.from(JSON.stringify(trabajo)), {
            persistent: true,
            contentType: 'application/json',
            messageId: trabajo.notificacionId,
            headers: { 'x-intentos': 0 },
          });
          trabajos++;
        }
      }
      if (trabajos > 0) console.log(`[Router] ${alerta.message} → ${trabajos} trabajo(s) de correo/SMS`);
      ch.ack(msg);
    } catch (e) {
      console.error('[Router] Mensaje inválido, se envía a fallidas:', e.message);
      ch.nack(msg, false, false);
    }
  });
}

// --- Estado de entregas: workers → usuario ---
async function iniciarEstado(conn) {
  const ch = await crearCanal(conn, 'estado');
  // Cola temporal y exclusiva: solo existe mientras este servicio está conectado
  const { queue } = await ch.assertQueue('', { exclusive: true });
  await ch.bindQueue(queue, EX.ESTADO, '');
  await ch.consume(queue, (msg) => {
    const estado = JSON.parse(msg.content.toString());
    enviarAUsuario(estado.usuarioId, { kind: 'estado', ...estado });
  }, { noAck: true });
}

// --- HTTP + WebSocket ---
const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'app.html')));

app.post('/api/suscripciones', async (req, res) => {
  const { errores, datos } = validar(req.body);
  if (errores.length) return res.status(400).json({ errores });
  const usuario = { id: crypto.randomUUID().slice(0, 8), ...datos, creado: new Date().toISOString() };
  try {
    await prepararColaUsuario(usuario);
    usuarios.set(usuario.id, usuario);
    guardarUsuarios();
    console.log(`[API] Nueva suscripción ${usuario.id} (${usuario.nombre}): ${clavesDeEnrutamiento(usuario).length} bindings`);
    res.status(201).json(usuario);
  } catch (e) {
    console.error('[API] Error al suscribir:', e.message);
    res.status(500).json({ errores: ['No se pudo crear la suscripción. Inténtalo de nuevo.'] });
  }
});

app.get('/api/suscripciones', (req, res) => res.json([...usuarios.values()]));

app.get('/api/suscripciones/:id', (req, res) => {
  const u = usuarios.get(req.params.id);
  if (!u) return res.status(404).json({ errores: ['Suscripción no encontrada.'] });
  res.json({ ...u, routingKeys: clavesDeEnrutamiento(u) });
});

app.delete('/api/suscripciones/:id', async (req, res) => {
  const id = req.params.id;
  if (!usuarios.has(id)) return res.status(404).json({ errores: ['Suscripción no encontrada.'] });
  try {
    await detenerConsumoApp(id);
    await chPub.deleteQueue(colaApp(id)); // borra la cola y todos sus bindings
    usuarios.delete(id);
    guardarUsuarios();
    sockets.get(id)?.forEach((ws) => ws.close(4004, 'Suscripción cancelada'));
    sockets.delete(id);
    console.log(`[API] Suscripción ${id} cancelada`);
    res.status(204).end();
  } catch (e) {
    console.error('[API] Error al cancelar:', e.message);
    res.status(500).json({ errores: ['No se pudo cancelar la suscripción.'] });
  }
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', async (ws, req) => {
  const id = new URL(req.url, 'http://localhost').searchParams.get('usuario');
  const usuario = usuarios.get(id);
  if (!usuario) {
    ws.send(JSON.stringify({ kind: 'error', mensaje: 'Suscripción no encontrada.' }));
    return ws.close(4004, 'Suscripción no encontrada');
  }

  if (!sockets.has(id)) sockets.set(id, new Set());
  sockets.get(id).add(ws);

  ws.on('close', async () => {
    const conjunto = sockets.get(id);
    if (!conjunto) return;
    conjunto.delete(ws);
    if (conjunto.size === 0) {
      sockets.delete(id);
      await detenerConsumoApp(id).catch((e) => console.error(e.message));
      console.log(`[WS] ${usuario.nombre} se desconectó: sus alertas quedan en cola`);
    }
  });

  try {
    // assertQueue devuelve cuántos mensajes esperan en la cola
    const { messageCount } = await chPub.assertQueue(colaApp(id), { durable: true, arguments: ARGS_COLA_APP });
    if (ws.readyState !== ws.OPEN) return; // se cerró mientras esperábamos a RabbitMQ
    ws.send(JSON.stringify({ kind: 'bienvenida', usuario, pendientes: messageCount }));
    await iniciarConsumoApp(id);
    console.log(`[WS] ${usuario.nombre} conectado (${messageCount} alertas pendientes)`);
  } catch (e) {
    console.error('[WS] Error al iniciar el consumo:', e.message);
  }
});

const run = async () => {
  cargarUsuarios();
  const conn = await conectarRabbit('notificaciones');
  chPub = await crearCanal(conn, 'publicacion');
  chApp = await crearCanal(conn, 'app');
  await chApp.prefetch(20);
  await declararTopologia(chPub);

  // Si el servicio se reinicia, se vuelven a declarar las colas (operación idempotente)
  for (const u of usuarios.values()) await prepararColaUsuario(u);

  await iniciarRouter(conn);
  await iniciarEstado(conn);

  server.listen(PUERTO, () => console.log(`[Notificaciones] App en http://localhost:${PUERTO}`));
};

run().catch((e) => {
  console.error('[Notificaciones] Error fatal:', e);
  process.exit(1);
});
