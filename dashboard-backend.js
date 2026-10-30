const { Kafka } = require('kafkajs');
const { WebSocketServer } = require('ws');

// --- Servidor de WebSockets para el Panel Principal ---
const wss = new WebSocketServer({ port: 8080 });
console.log("Panel en Tiempo Real (WebSocket) escuchando en el puerto 8080");

wss.on('connection', ws => console.log('Cliente del panel principal conectado.'));

const broadcastToDashboard = (message) => {
  wss.clients.forEach(client => client.send(message));
};

// --- Consumidor de Kafka para el Panel ---
const kafka = new Kafka({ clientId: 'dashboard-consumer', brokers: ['kafka:9092'] });
const consumer = kafka.consumer({ groupId: 'dashboard-group' });

const run = async (retries = 5) => {
  try {
    await consumer.connect();
    await consumer.subscribe({ topic: 'weather-data', fromBeginning: true });
    console.log("✅ Consumidor del panel suscrito a 'weather-data'.");

    await consumer.run({
      eachMessage: async ({ message }) => {
        // console.log("--- MENSAJE RECIBIDO EN DASHBOARD-BACKEND ---",message);
        broadcastToDashboard(message.value.toString());
      },
    });
  } catch (error) {
    console.error(`❌ Falló la conexión del consumidor, reintentando... (${retries} intentos restantes)`);
    if (retries > 0) {
      // Espera 5 segundos antes de reintentar
      setTimeout(() => run(retries - 1), 5000);
    } else {
      console.error("❌ No se pudo conectar a Kafka después de varios intentos.");
    }
  }
};

run().catch(console.error);