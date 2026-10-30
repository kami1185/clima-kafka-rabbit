// alerts-processor.js — Lee 'weather-data' (Kafka), evalúa umbrales y publica en 'weather-alerts' (Kafka).
// Cada alerta tiene un TIPO (temperatura, viento, humedad) y un NIVEL (amarilla, naranja, roja).
const crypto = require('crypto');
const { Kafka, logLevel } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'alerts-processor',
  brokers: ['kafka:9092'],
  retry: { retries: 10 },
  logLevel: logLevel.WARN,
});
const consumer = kafka.consumer({ groupId: 'alerts-processor-group' });
// idempotent: evita alertas duplicadas en Kafka si el productor reintenta un envío
const producer = kafka.producer({ idempotent: true, maxInFlightRequests: 1 });

// Umbrales por tipo, del nivel más grave al menos grave (se toma el primero que se supera)
const UMBRALES = {
  temperatura: { campo: 'temperature', unidad: '°C', titulo: 'Temperatura extrema',
                 niveles: [['roja', 33], ['naranja', 30], ['amarilla', 25]] },
  viento:      { campo: 'windSpeed', unidad: 'km/h', titulo: 'Vientos fuertes',
                 niveles: [['roja', 27], ['naranja', 24], ['amarilla', 20]] },
  humedad:     { campo: 'humidity', unidad: '%', titulo: 'Humedad muy alta',
                 niveles: [['roja', 98], ['naranja', 96], ['amarilla', 93]] },
};

const evaluar = (dato) => {
  const alertas = [];
  for (const [tipo, cfg] of Object.entries(UMBRALES)) {
    const valor = parseFloat(dato[cfg.campo]);
    const superado = cfg.niveles.find(([, umbral]) => valor > umbral);
    if (!superado) continue;
    const nivel = superado[0];
    alertas.push({
      id: crypto.randomUUID(),
      type: `ALERTA_${tipo.toUpperCase()}`, // se mantiene por compatibilidad con alerts.html
      tipo,
      nivel,
      stationId: dato.stationId,
      valor,
      unidad: cfg.unidad,
      titulo: cfg.titulo,
      message: `[${nivel.toUpperCase()}] ${cfg.titulo}: ${valor} ${cfg.unidad} en ${dato.stationId}`,
      medicionTimestamp: dato.timestamp,
      timestamp: new Date().toISOString(),
    });
  }
  return alertas;
};

const run = async () => {
  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({ topic: 'weather-data', fromBeginning: true });
  console.log("Procesador de Alertas suscrito a 'weather-data'.");

  await consumer.run({
    eachMessage: async ({ message }) => {
      let dato;
      try {
        dato = JSON.parse(message.value.toString());
      } catch {
        console.warn('Mensaje mal formado en weather-data, se ignora.');
        return;
      }

      const alertas = evaluar(dato);
      if (alertas.length === 0) return;

      await producer.send({
        topic: 'weather-alerts',
        messages: alertas.map((a) => ({ key: a.stationId, value: JSON.stringify(a) })),
      });
      alertas.forEach((a) => console.log(`Alerta generada: ${a.message}`));
    },
  });
};

run().catch((e) => {
  console.error('[Procesador] Error fatal:', e);
  process.exit(1);
});
