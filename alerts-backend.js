const { Kafka } = require('kafkajs');
const { WebSocketServer } = require('ws');

// --- Servidor de WebSockets para Alertas ---
const wss = new WebSocketServer({ port: 8081 });
console.log(" Panel de Alertas (WebSocket) escuchando en el puerto 8081");

wss.on('connection', ws => console.log('Cliente del panel de alertas conectado.'));

const broadcastAlert = (message) => {
  wss.clients.forEach(client => client.send(message));
};

// --- Consumidor de Kafka para Alertas ---
const kafka = new Kafka({ clientId: 'alerts-consumer', brokers: ['kafka:9092'] });
const consumer = kafka.consumer({ groupId: 'alerts-display-group' });

const run = async (retries = 5) => {
  try {
    await consumer.connect();
    // Intenta suscribirse aquí dentro del try/catch
    await consumer.subscribe({ topic: 'weather-alerts', fromBeginning: true });
    console.log("Consumidor de alertas suscrito a 'weather-alerts'.");

    await consumer.run({
      eachMessage: async ({ message }) => {
        broadcastAlert(message.value.toString());
      },
    });
  } catch (error) {
    console.error(` Falló la conexión/suscripción de alertas, reintentando... (${retries} intentos restantes)`);
    if (retries > 0) {
      // Espera 5 segundos antes de reintentar
      setTimeout(() => run(retries - 1), 5000);
    } else {
      console.error("No se pudo conectar a Kafka después de varios intentos.");
    }
  }
};


run().catch(console.error);