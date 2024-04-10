import { expect } from 'chai';
import Redis from 'ioredis';
import redismb from '../src/redismb.js';

describe('Redis Client Functions', () => {
  afterEach(async () => {
    await redismb.stop();
  });

  describe('bootstrap', () => {
    it('should establish a Redis connection', async () => {
      const redisUri = 'redis://localhost:6379';
      const redis = await redismb.bootstrap(redisUri);
      expect(redis).to.be.an.instanceof(Redis);
    });

    it('should throw an error if Redis connection is not established', async () => {
      const invalidRedisUri = 'redis://localhost:9999';
      let error;
      try {
        await redismb.bootstrap(invalidRedisUri, 2);
      } catch (err) {
        error = err;
      }
      expect(error.message).to.equal('TIMEOUT');
    });
  });

  describe('stop', () => {
    it('should terminate the Redis connection', async () => {
      const redisUri = 'redis://localhost:6379';
      await redismb.bootstrap(redisUri);
      const result = await redismb.stop();
      expect(result).to.equal('OK');
    });
  });

  describe('readRejectedMessages', () => {
    let redis, message;
    beforeEach(async () => {
      const redisUri = 'redis://localhost:6379';
      redis = await redismb.bootstrap(redisUri);
      message = await redis.xadd(
        'rejections',
        '*',
        'action',
        JSON.stringify({ foo: 'bar' }),
        'group',
        'redismb-channel'
      );
    });
    afterEach(async () => {
      await redis.xdel('rejections', message);
    });
    it('should read rejected messages based on specified criteria', async () => {
      const result = await redismb.readRejectedMessages({ all: true });
      expect(result.messages).to.be.an('array');
      expect(result.messages).to.have.lengthOf(1);
      expect(result.messages[0].action).to.equal('action');
      expect(result.messages[0].data.foo).to.equal('bar');
      expect(result.messages[0].group).to.equal('group');
      expect(result.messages[0].channel).to.equal('redismb-channel');
      expect(result.count).to.be.a('number');
      expect(result.count).to.equal(1);
    });
  });

  describe('reprocessRejectedMessages', () => {
    let redis;
    beforeEach(async () => {
      const redisUri = 'redis://localhost:6379';
      redis = await redismb.bootstrap(redisUri);
      await redis.xadd(
        'rejections',
        '*',
        'action',
        JSON.stringify({ foo: 'bar' }),
        'group',
        'redismb-channel'
      );
    });
    it('should reprocess rejected messages', async () => {
      const result = await redismb.reprocessRejectedMessages({ all: true });
      expect(result.succeeded).to.be.an('array');
      expect(result.succeeded).to.have.lengthOf(1);
      expect(result.succeeded[0].action).to.equal('action');
      expect(result.succeeded[0].data.foo).to.equal('bar');
      expect(result.succeeded[0].group).to.equal('group');
      expect(result.succeeded[0].channel).to.equal('redismb-channel');
      expect(result.failed).to.be.an('array');
      expect(result.failed).to.have.lengthOf(0);

      const messages = await redis.xrange('redismb-channel', '-', '+');
      expect(messages).to.have.lengthOf(1);
      await Promise.all(messages.map(async (message) => {
        expect(message[1][0]).to.equal('action');
        expect(JSON.parse(message[1][1]).foo).to.equal('bar');
        expect(message[1][3]).to.equal('group');
        await redis.xdel('redismb-channel', message[0]);
      }));
      const rejections = await redis.xrange('rejections', '-', '+');
      expect(rejections).to.have.lengthOf(0);
    });
  });
});
