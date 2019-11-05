import amqplib from 'amqplib';

export class MQConnection {
  constructor({ qos, url }) {
    this._consumerTags = [];
    this.channel = amqplib.connect(url)
      .then((connection) => connection.createChannel())
      .then((chan) => {
        chan.prefetch(qos || 10);
        return chan;
      });
  }

  assertQueue(queue, options) {
    return this.channel.then((chan) => chan.assertQueue(queue, options));
  }

  assertExchange(exchange, type, options) {
    return this.channel.then((chan) => chan.assertExchange(exchange, type, options));
  }

  bindQueue(queue, source, pattern) {
    return this.channel.then((chan) => chan.bindQueue(queue, source, pattern));
  }

  deleteQueue(queue) {
    return this.channel.then((chan) => chan.deleteQueue(queue));
  }

  cancel(queue) {
    const tag = this._consumerTags.find((i) => i.queue === queue);
    if (!tag) {
      throw new Error(`can not cancel nonexistent rabbit queue : ${queue} `);
    } else {
      const index = this._consumerTags.findIndex((i) => i.queue === queue);
      this._consumerTags.splice(index, 1);
      return this.channel.then((chan) => chan.cancel(tag.consumerTag));
    }
  }

  consume(queue, cb) {
    return this.channel.then(async (chan) => {
      const { consumerTag } = await chan.consume(queue, async (msg) => {
        if (!msg) {
          throw new Error(`rabbit message queue ${queue} deleted or channel cancelled in the server.`);
        } else {
          const content = msg.content.toString();
          try {
            const after = await cb(content);
            chan.ack(msg);
            await Promise.delay(100);
            if (typeof after === 'function') {
              try {
                await after();
              } catch (e) {
                throw new Error('failed to run after hook when message consumed');
              }
            }
          } catch (e) {
            chan.ack(msg);
            await Promise.delay(100);
            await this.publish(queue, content);
            throw new Error(`failed to process queue message ${content.slice(0, 100)}: `, e);
          }
        }
      });

      if (!this._consumerTags.find((i) => i.consumerTag === consumerTag && i.queue === queue)) {
        this._consumerTags.push({ consumerTag, queue });
      }
      return consumerTag;
    });
  }

  setQos(qos) {
    return this.channel.then((chan) => chan.prefetch(qos));
  }
}

