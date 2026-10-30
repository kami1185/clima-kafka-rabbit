// notification-worker.js — Simula el envío de correos o SMS.
// El mismo archivo sirve para los dos canales: CANAL=email o CANAL=sms.
//
// Patrones de RabbitMQ que muestra:
//  • Cola de trabajo con consumidores competitivos: si escalas el servicio
//    (docker-compose up -d --scale worker-email=3), RabbitMQ reparte los mensajes.
//  • prefetch(1): cada worker recibe un mensaje nuevo solo cuando terminó el anterior.
//  • Reintentos diferidos SIN plugins: el mensaje fallido va a una cola de espera
//    con TTL; al expirar, su Dead Letter Exchange lo devuelve a la cola principal.
//  • Dead Letter Queue: tras MAX_INTENTOS fallos, nack(requeue=false) → 'notificaciones.fallidas'.
const os = require('os');
const { conectarRabbit, crearCanal, declararTopologia, esperar, EX } = require('./rabbit');

const CANAL = process.env.CANAL;
if (!['email', 'sms'].includes(CANAL)) {
  console.error('La variable CANAL debe ser "email" o "sms".');
  process.exit(1);
}
const TASA_FALLO = Number(process.env.TASA_FALLO ?? 0.2);           // probabilidad de fallo simulado
const MAX_INTENTOS = Number(process.env.MAX_INTENTOS ?? 3);
const RETRASO_REINTENTO_MS = Number(process.env.RETRASO_REINTENTO_MS ?? 5000);

const WORKER = os.hostname().slice(0, 12); // id del contenedor: sirve para ver el reparto
const ETIQUETA = CANAL === 'email' ? 'Correo' : 'SMS';
const COLA = `canal.${CANAL}`;
const COLA_REINTENTOS = `canal.${CANAL}.reintentos`;

const run = async () => {
  const conn = await conectarRabbit(`worker-${CANAL}-${WORKER}`);
  const ch = await crearCanal(conn, `worker-${CANAL}`);
  await declararTopologia(ch);

  // Cola principal: los mensajes rechazados sin reencolar van al DLX
  await ch.assertQueue(COLA, { durable: true, arguments: { 'x-dead-letter-exchange': EX.DLX } });
  await ch.bindQueue(COLA, EX.CANALES, CANAL);

  // Cola de espera: nadie la consume; al vencer el TTL el mensaje vuelve a la cola principal
  await ch.assertQueue(COLA_REINTENTOS, {
    durable: true,
    arguments: {
      'x-message-ttl': RETRASO_REINTENTO_MS,
      'x-dead-letter-exchange': EX.CANALES,
      'x-dead-letter-routing-key': CANAL,
    },
  });

  await ch.prefetch(1);

  const informar = (trabajo, estado, intento, extra = {}) =>
    ch.publish(EX.ESTADO, '', Buffer.from(JSON.stringify({
      notificacionId: trabajo.notificacionId,
      alertaId: trabajo.alertaId,
      usuarioId: trabajo.usuarioId,
      canal: CANAL,
      destino: trabajo.destino,
      estado,          // enviado | reintento | fallido
      intento,
      maxIntentos: MAX_INTENTOS,
      worker: WORKER,
      timestamp: new Date().toISOString(),
      ...extra,
    })));

  await ch.consume(COLA, async (msg) => {
    if (!msg) return;

    let trabajo;
    try {
      trabajo = JSON.parse(msg.content.toString());
    } catch {
      console.error(`[${ETIQUETA} ${WORKER}] Mensaje mal formado → cola de fallidas`);
      return ch.nack(msg, false, false);
    }

    const intento = (msg.properties.headers?.['x-intentos'] ?? 0) + 1;

    // Simula la llamada al proveedor externo (SMTP, pasarela SMS...)
    await esperar(300 + Math.random() * 900);
    const exito = Math.random() >= TASA_FALLO;

    if (exito) {
      ch.ack(msg);
      informar(trabajo, 'enviado', intento);
      console.log(`[${ETIQUETA} ${WORKER}] ✅ Enviado a ${trabajo.destino} (intento ${intento}): ${trabajo.mensaje}`);
      return;
    }

    if (intento < MAX_INTENTOS) {
      ch.sendToQueue(COLA_REINTENTOS, msg.content, {
        persistent: true,
        contentType: 'application/json',
        messageId: msg.properties.messageId,
        headers: { ...msg.properties.headers, 'x-intentos': intento },
      });
      ch.ack(msg);
      informar(trabajo, 'reintento', intento, { reintentoEnMs: RETRASO_REINTENTO_MS });
      console.warn(`[${ETIQUETA} ${WORKER}] ⚠️ Falló el intento ${intento} para ${trabajo.destino}, reintento en ${RETRASO_REINTENTO_MS / 1000} s`);
    } else {
      ch.nack(msg, false, false); // → notificaciones.dlx → notificaciones.fallidas
      informar(trabajo, 'fallido', intento);
      console.error(`[${ETIQUETA} ${WORKER}] ❌ ${trabajo.destino}: agotó ${MAX_INTENTOS} intentos → cola de fallidas`);
    }
  });

  console.log(`[${ETIQUETA} ${WORKER}] Esperando trabajos en '${COLA}' (fallo simulado: ${TASA_FALLO * 100}%)`);
};

run().catch((e) => {
  console.error(`[${ETIQUETA}] Error fatal:`, e);
  process.exit(1);
});
