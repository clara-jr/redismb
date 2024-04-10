import { expect } from 'chai';
import redismb from '../src/redismb.js';
import Publisher from '../src/publisher.js';

describe('Redis Publisher', () => {
  let redis;
  beforeEach(async () => {
    const redisUri = 'redis://localhost:6379';
    redis = await redismb.bootstrap(redisUri);
  });

  afterEach(async () => {
    await redismb.stop();
  });

  describe('constructor', () => {
    it('should throw an error if channel is not provided', () => {
      expect(() => new Publisher({})).to.throw('MISSED_VALUE');
    });
  });

  describe('publish', () => {
    it('should throw an error if Redis connection is not established', async () => {
      // Stop the Redis connection to simulate a connection failure.
      await redismb.stop();
      const publisher = new Publisher({ channel: 'channel' });

      let error;
      try {
        await publisher.publish('action', { foo: 'bar' });
      } catch (err) {
        error = err;
      }
      expect(error.message).to.equal('REDIS_CONNECTION');
    });
    it('should publish message to the specified channel', async () => {
      const publisher = new Publisher({ channel: 'publisher-channel' });
      const id = await publisher.publish('action', { foo: 'bar' });

      const messages = await redis.xrange('publisher-channel', '-', '+');
      expect(messages).to.have.lengthOf(1);
      await Promise.all(messages.map(async (message) => {
        expect(message[0]).to.equal(id);
        expect(message[1][0]).to.equal('action');
        expect(JSON.parse(message[1][1]).foo).to.equal('bar');
        await redis.xdel('publisher-channel', message[0]);
      }));
    });
  });
});
