const { Kafka } = require('kafkajs');

async function runProducer() {
  try {
    const kafka = new Kafka({
      clientId: 'my-producer',
      brokers: ['localhost:9092'],
    });

    const producer = kafka.producer();
    await producer.connect();

    const message_result = await producer.send({
      topic: 'test-topic',
      messages: [
        {
          value: 'Olá, usuário do KafkaJS!',
        },
      ],
    });

    console.log('Mensagem enviada:', JSON.stringify(message_result));
    await producer.disconnect();
  } catch (error) {
    console.error('Erro ao enviar mensagem:', error);
  } finally {
    process.exit(0);
  }
}

runProducer();
