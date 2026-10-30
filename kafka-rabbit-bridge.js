// kafka-rabbit-bridge.js — Puente entre los dos mundos.
//
//   Kafka 'weather-alerts'  ──►  RabbitMQ exchange 'clima.alertas' (topic)
//
// La routing key se arma como <tipo>.<nivel>.<estacion>, por ejemplo:
//   temperatura.roja.STATION-02
// Así RabbitMQ puede repartir cada alerta solo a quien le interesa.
//
// Garantía "al menos una vez": el offset de Kafka solo se confirma cuando
// RabbitMQ confirmó (publisher confirm) que guardó el mensaje. Si RabbitMQ
// falla, eachMessage lanza error, kafkajs NO avanza el offset y reintenta.
const { Kafka, logLevel } = require('kafkajs');
const { conectarRabbit, crearCanal, declararTopologia, EX } = require('./rabbit');

const kafka = new Kafka({
  clientId: 'puente-kafka-rabbitmq',
  brokers: ['kafka:9092'],
  retry: { retries: 10 },
  logLevel: logLevel.WARN,
});
const consumer = kafka.consumer({ groupId: 'puente-rabbitmq-group' });

const run = async () => {
  const conn = await conectarRabbit('puente');
  const ch = await crearCanal(conn, 'puente', { confirm: true });
  await declararTopologia(ch);

  await consumer.connect();
  // fromBeginning: false → la primera vez solo se notifican alertas NUEVAS.
  // No tiene sentido mandarle a un usuario alertas de hace horas.
  await consumer.subscribe({ topic: 'weather-alerts', fromBeginning: false });
  console.log("[Puente] Escuchando 'weather-alerts' en Kafka → publicando en RabbitMQ");

  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      let alerta;
      try {
        alerta = JSON.parse(message.value.toString());
      } catch {
        console.warn('[Puente] Mensaje mal formado, se ignora.');
        return; // si lanzáramos error, kafkajs lo reintentaría para siempre
      }
      if (!alerta.tipo || !alerta.nivel || !alerta.stationId) {
        console.warn('[Puente] Alerta sin tipo/nivel/estación (formato antiguo), se ignora.');
        return;
      }

      const routingKey = `${alerta.tipo}.${alerta.nivel}.${alerta.stationId}`;
      ch.publish(EX.ALERTAS, routingKey, Buffer.from(JSON.stringify(alerta)), {
        persistent: true,              // sobrevive a un reinicio de RabbitMQ
        contentType: 'application/json',
        messageId: alerta.id,
        timestamp: Math.floor(Date.now() / 1000),
        headers: { 'kafka-partition': partition, 'kafka-offset': message.offset },
      });
      await ch.waitForConfirms(); // espera el "ack" del broker antes de confirmar el offset en Kafka

      console.log(`[Puente] Kafka p${partition}@${message.offset} → RabbitMQ '${routingKey}'`);
    },
  });
};

const salir = async () => {
  try { await consumer.disconnect(); } finally { process.exit(0); }
};
process.on('SIGTERM', salir);
process.on('SIGINT', salir);

run().catch((e) => {
  console.error('[Puente] Error fatal:', e);
  process.exit(1);
});
