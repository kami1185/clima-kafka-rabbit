// rabbit.js — Módulo compartido por todos los servicios que usan RabbitMQ.
// Centraliza la conexión (con reintentos) y la declaración de la topología,
// para que todos los servicios declaren exchanges y colas con los MISMOS argumentos.
const amqp = require('amqplib');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://clima:clima123@localhost:5672';

// --- Exchanges ---
const EX = {
  ALERTAS: 'clima.alertas',          // topic  → routing key: <tipo>.<nivel>.<estacion>  ej: temperatura.roja.STATION-02
  CANALES: 'notificaciones.canales', // direct → routing key: email | sms
  ESTADO:  'notificaciones.estado',  // fanout → resultado de cada entrega (enviado / reintento / fallido)
  DLX:     'notificaciones.dlx',     // fanout → Dead Letter Exchange: mensajes que agotaron sus reintentos
};

// --- Colas fijas ---
const Q = {
  ROUTER:   'notificaciones.router',   // recibe TODAS las alertas (#) y genera trabajos por canal
  FALLIDAS: 'notificaciones.fallidas', // Dead Letter Queue: para revisar a mano en la consola web
};

// --- Dominio ---
const NIVELES = ['amarilla', 'naranja', 'roja']; // de menor a mayor gravedad
const TIPOS = ['temperatura', 'viento', 'humedad'];
const ESTACIONES = ['STATION-01', 'STATION-02', 'STATION-03'];

const esperar = (ms) => new Promise((r) => setTimeout(r, ms));

// Conecta con reintentos. Si la conexión se cae después, el proceso termina
// y Docker lo reinicia (restart: on-failure). Es más simple y robusto que reconectar a mano.
async function conectarRabbit(nombre, intentos = 20) {
  for (let i = 1; i <= intentos; i++) {
    try {
      const conn = await amqp.connect(RABBITMQ_URL, {
        clientProperties: { connection_name: nombre }, // aparece en la consola de administración
      });
      conn.on('error', (e) => console.error(`[${nombre}] Error en la conexión con RabbitMQ:`, e.message));
      conn.on('close', () => {
        console.error(`[${nombre}] Conexión con RabbitMQ cerrada. Saliendo para que Docker reinicie el servicio.`);
        process.exit(1);
      });
      console.log(`[${nombre}] Conectado a RabbitMQ`);
      return conn;
    } catch (e) {
      console.error(`[${nombre}] RabbitMQ aún no está disponible (${i}/${intentos}): ${e.message}`);
      await esperar(3000);
    }
  }
  throw new Error('No se pudo conectar a RabbitMQ');
}

// Crea un canal. Con confirm: true el broker confirma cada publicación (publisher confirms).
async function crearCanal(conn, nombre, { confirm = false } = {}) {
  const ch = confirm ? await conn.createConfirmChannel() : await conn.createChannel();
  ch.on('error', (e) => console.error(`[${nombre}] Error en el canal:`, e.message));
  ch.on('close', () => {
    console.error(`[${nombre}] Canal cerrado inesperadamente. Saliendo.`);
    process.exit(1);
  });
  return ch;
}

async function declararTopologia(ch) {
  await ch.assertExchange(EX.ALERTAS, 'topic', { durable: true });
  await ch.assertExchange(EX.CANALES, 'direct', { durable: true });
  await ch.assertExchange(EX.ESTADO, 'fanout', { durable: true });
  await ch.assertExchange(EX.DLX, 'fanout', { durable: true });
  await ch.assertQueue(Q.FALLIDAS, { durable: true });
  await ch.bindQueue(Q.FALLIDAS, EX.DLX, '');
}

module.exports = {
  conectarRabbit, crearCanal, declararTopologia, esperar,
  EX, Q, NIVELES, TIPOS, ESTACIONES,
};
