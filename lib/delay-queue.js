"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.delayQueues = delayQueues;
exports.getQueueByTTL = getQueueByTTL;

function delayQueues() {
  return [
  /**
   * 秒级队列
   */
  {
    queueName: 'DELAY_QUEUE_5S',
    // 队列名称
    ttl: 5 // 单位为s

  }, {
    queueName: 'DELAY_QUEUE_10S',
    ttl: 10
  }, {
    queueName: 'DELAY_QUEUE_30S',
    ttl: 30
  },
  /**
  * 分钟级别队列
  * queueName 为 `DELAY_QUEUE_N`,N 对应的是队列的TTL 时间为 2^n 次方分钟,N的值最大为19
  */
  {
    queueName: 'DELAY_QUEUE_0',
    ttl: 60
  }, {
    queueName: 'DELAY_QUEUE_1',
    ttl: 120
  }, {
    queueName: 'DELAY_QUEUE_2',
    ttl: 240
  }, {
    queueName: 'DELAY_QUEUE_3',
    ttl: 480
  }, {
    queueName: 'DELAY_QUEUE_4',
    ttl: 960
  }, {
    queueName: 'DELAY_QUEUE_5',
    ttl: 1920
  }, {
    queueName: 'DELAY_QUEUE_6',
    ttl: 3840
  }, {
    queueName: 'DELAY_QUEUE_7',
    ttl: 7680
  }, {
    queueName: 'DELAY_QUEUE_8',
    ttl: 15360
  }, {
    queueName: 'DELAY_QUEUE_9',
    ttl: 30720
  }, {
    queueName: 'DELAY_QUEUE_10',
    ttl: 61440
  }, {
    queueName: 'DELAY_QUEUE_11',
    ttl: 122880
  }, {
    queueName: 'DELAY_QUEUE_12',
    ttl: 245760
  }, {
    queueName: 'DELAY_QUEUE_13',
    ttl: 491520
  }, {
    queueName: 'DELAY_QUEUE_14',
    ttl: 983040
  }, {
    queueName: 'DELAY_QUEUE_15',
    ttl: 1966080
  }, {
    queueName: 'DELAY_QUEUE_16',
    ttl: 3932160
  }, {
    queueName: 'DELAY_QUEUE_17',
    ttl: 7864320
  }, {
    queueName: 'DELAY_QUEUE_18',
    ttl: 15728640
  }, {
    queueName: 'DELAY_QUEUE_19',
    ttl: 31457280
  }];
}
/**
 * 根据延时时间寻找改推入哪个延时队列
 * @param {*} ttl 延时时间，单位为秒
 */


function getQueueByTTL(ttl) {
  const sortedDelayQueues = delayQueues().sort((a, b) => b.ttl - a.ttl);
  const resultQueueIndex = sortedDelayQueues.findIndex(i => ttl >= i.ttl);
  return sortedDelayQueues[resultQueueIndex].queueName;
}