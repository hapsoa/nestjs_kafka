import { Injectable } from '@nestjs/common';
import { Kafka, Producer } from 'kafkajs';

@Injectable()
export class AppService {
  private producer: Producer | null = null;

  constructor() {
    this.run().then(
      () => console.log('Done'),
      (err) => console.log(err),
    );
  }

  async run() {
    const kafka = new Kafka({ brokers: ['localhost:9092'] });

    // Consumer
    const consumer = kafka.consumer({ groupId: '' + Date.now() });
    // If you specify the same group id and run this process multiple times, KafkaJS
    // won't get the events. That's because Kafka assumes that, if you specify a
    // group id, a consumer in that group id should only read each message at most once.

    await consumer.connect();

    await consumer.subscribe({
      topic: 'quickstart-events',
      // fromBeginning: true,
    });
    await consumer.run({
      eachMessage: async (data) => {
        // console.log(data);

        // const currentDate = new Date();
        // new Date(data.message.timestamp);

        console.log(data.message.value.toString('utf8'));
      },
    });

    // Producer
    this.producer = kafka.producer();
    await this.producer.connect();
  }

  async produceEvent() {
    await this.producer.send({
      topic: 'quickstart-events',
      messages: [
        { value: 'new event', timestamp: new Date().getTime().toString() },
      ],
    });
  }

  calculateDaysBetweenDates(begin, end) {
    const date1 = new Date(begin);
    const date2 = new Date(end);

    const timeDiff = Math.abs(date2.getTime() - date1.getTime());
    const diffDays = Math.ceil(timeDiff / (1000 * 3600 * 24));

    return diffDays;
  }

  consumeEvent() {
    console.log('event consumed');
  }

  getHello(): string {
    return 'Hello World!';
  }
}
