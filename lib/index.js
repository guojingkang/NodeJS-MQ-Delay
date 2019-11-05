"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.Delay = void 0;

var _mq = _interopRequireDefault(require("./mq"));

var _delayQueue = require("./delay-queue");

class Delay {
  constructor({
    url,
    qos
  }) {
    this._mq = new _mq.default({
      url,
      qos
    });

    (async () => {
      await this._init();
    })();
  }

  async _init() {
    const mq = this._mq;
    await mq.assertExchange('DLX'); // 初始化 exchange 消费队列，并绑定至 exchange

    await mq.assertQueue('OVERTIME_PRODUCER');
    await mq.bindQueue('OVERTIME_PRODUCER', 'DLX', 'DELAY'); // 初始化所有的延时队列

    const queues = (0, _delayQueue.delayQueues)();
    await Promise.map(queues, async singleQueue => {
      const queueName = singleQueue.queueName;
      const queueTTL = singleQueue.ttl; // messageTtl 单位为秒

      await mq.assertQueue(queueName, {
        messageTtl: queueTTL * 1000,
        deadLetterExchange: 'DLX',
        deadLetterRoutingKey: 'DELAY'
      });
    });
  }

  async delayMsg(target, msg, delayTime) {
    const mq = this._mq;

    if (!target) {
      throw new Error('target can not be nll');
    }

    if (!msg) {
      throw new Error('delay msg can not be null');
    }

    if (typeof msg !== 'string') {
      throw new Error('type of msg must be string');
    }

    if (!delayTime) {
      throw new Error('delay time can not be null ');
    } // 校验目标队列是否存在，不存在则新建


    await mq.assertQueue(target); // 根据延时时间选择该推入哪个队列

    const queueName = (0, _delayQueue.getQueueByTTL)(delayTime); // 封装在延时队列中流转的数据格式

    const message = {
      target,
      // 完成延时后的目标队列
      originMsg: msg,
      // 原始信息
      insertDate: new Date(),
      // 插入队列的时间，用于计算真实延时
      queueName,
      // 插入的队列，方便后面计算负载
      delayTime // 延时的时间

    };
    await mq.publish(queueName, JSON.stringify(message));
  }

  async onDelayMessage() {
    const mq = this._mq;
    mq.consume('OVERTIME_PRODUCER', async content => {
      const {
        target,
        originMsg,
        insertDate,
        delayTime
      } = JSON.parse(content); // 标记消息出延时队列的时间

      const consumeAt = new Date(); // 在队列中消耗的时间

      const timeInQueue = (consumeAt - new Date(insertDate)) / 1000; // 计算剩余的延时时间

      const currentDelayTime = delayTime - timeInQueue; // 因为延时的最小TTL为5s，所以如果小于5s的话直接推入目标队列，允许存在0~5S的误差

      if (currentDelayTime < 5) {
        await mq.publish(target, originMsg);
      } else {
        // 根据剩余的延时时间获取将要推入的队列
        const currentQueueName = (0, _delayQueue.getQueueByTTL)(currentDelayTime);
        const message = {
          target,
          originMsg,
          insertDate: new Date(),
          queueName: currentQueueName,
          delayTime: currentDelayTime
        };
        await mq.publish(currentQueueName, JSON.stringify(message));
      }
    });
  }

}

exports.Delay = Delay;