const { Kafka } = require('kafkajs');

async function runConsumer() {
  try {
    const kafka = new Kafka({
      clientId: 'my-consumer',
      brokers: ['localhost:9092'],
    });

    const consumer = kafka.consumer({ groupId: 'test-group' });
    await consumer.connect();
    await consumer.subscribe({ topic: 'test-topic', fromBeginning: true });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log(`Mensagem recebida: ${message.value.toString()}`);
      },
    });
  } catch (error) {
    console.error('Erro ao consumir mensagens:', error);
  }
}

runConsumer();
