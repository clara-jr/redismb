import * as chai from 'chai';
import { expect } from 'chai';
import { stub, spy, assert } from 'sinon';
import sinonChai from 'sinon-chai';

import redismb from '../src/redismb.js';
import Subscriber from '../src/subscriber.js';
chai.use(sinonChai);

describe('Redis Subscriber', () => {
  let redis;
  beforeEach(async () => {
    const redisUri = 'redis://localhost:6379';
    redis = await redismb.bootstrap(redisUri);
  });

  afterEach(async () => {
    await redismb.stop();
  });

  describe('Constructor', () => {
    it('should throw an error if channels are missing', () => {
      expect(() => new Subscriber({ group: 'group' })).to.throw('MISSED_VALUE');
    });

    it('should throw an error if group is missing', () => {
      expect(() => new Subscriber({ channels: ['channel'] })).to.throw('MISSED_VALUE');
    });

    it('should create a Subscriber instance with default parameters', async () => {
      const subscriber = new Subscriber({ channels: ['channel'], group: 'group' });
      await redis.xgroup('DESTROY', 'channel', 'group');
      expect(subscriber).to.be.an.instanceOf(Subscriber);
      expect(subscriber.channels).to.deep.equal(['channel']);
      expect(subscriber.group).to.equal('group');
      expect(subscriber.clientId).to.match(/^group:sub:\d+$/);
      expect(subscriber.timeout).to.equal(10000);
      expect(subscriber.interval).to.equal(0);
      expect(subscriber.messages).to.equal(1);
      expect(subscriber.retries).to.equal(3);
    });

    it('should create a Subscriber instance with provided parameters', async () => {
      const subscriber = new Subscriber({
        channels: ['channel1', 'channel2'],
        group: 'group',
        clientId: 'clientId',
        timeout: 5000,
        interval: 1000,
        messages: 5,
        retries: 2
      }, (err, channel, message) => {
        console.error({ err, channel, message });
      });
      expect(subscriber).to.be.an.instanceOf(Subscriber);
      expect(subscriber.channels).to.deep.equal(['channel1', 'channel2']);
      expect(subscriber.group).to.equal('group');
      expect(subscriber.clientId).to.equal('clientId');
      expect(subscriber.timeout).to.equal(5000);
      expect(subscriber.interval).to.equal(1000);
      expect(subscriber.messages).to.equal(5);
      expect(subscriber.retries).to.equal(2);
      await redis.xgroup('DESTROY', 'channel1', 'group');
      await redis.xgroup('DESTROY', 'channel2', 'group');
    });

    it('should throw an error if callback parameter is not a function', () => {
      expect(() => new Subscriber({ channels: ['channel'], group: 'group' }, 'not a function')).to.throw(TypeError);
    });

    it('should throw an error if callback parameter does not accept between 1 and 3 parameters', () => {
      expect(() => new Subscriber({ channels: ['channel'], group: 'group' }, (a, b, c, d) => {})).to.throw(Error);
    });
  });

  describe('subscribe', () => {
    let id, callback;
    const _sleep = (ms) => {
      return new Promise((resolve) => setTimeout(resolve, ms));
    };
    describe('when callback resolves', () => {
      beforeEach(async () => {
        id = await redis.xadd(
          'channel',
          'MAXLEN',
          '~',
          10,
          '*',
          'action',
          JSON.stringify({ foo: 'bar' })
        );
        callback = stub().resolves();
        spy(callback);
      });
      afterEach(async () => {
        await redis.xdel('channel', id);
        await redis.xgroup('DESTROY', 'channel', 'group');
      });
      it('should subscribe to streaming messages', async () => {
        const subscriber = new Subscriber({ channels: ['channel'], group: 'group' });
        subscriber.subscribe(callback);
        await _sleep(1000); // Wait for messages to be processed
        assert.calledOnce(callback);
        const [payload] = callback.args[0];
        expect(payload.channel).to.equal('channel');
        expect(payload.action).to.equal('action');
        expect(payload.data.foo).to.equal('bar');
        expect(payload.id).to.equal(id);
      });
    });
    describe('when callback rejects', () => {
      let logEventError, logEventErrorSpy;
      beforeEach(async () => {
        id = await redis.xadd(
          'channel',
          'MAXLEN',
          '~',
          10,
          '*',
          'action',
          JSON.stringify({ foo: 'bar' })
        );
        callback = stub().rejects();
        spy(callback);
        logEventError = (err, channel, message) => {
          console.error(err);
          expect(channel).to.equal('channel');
          expect(message.action).to.equal('action');
          expect(message.data.foo).to.equal('bar');
          expect(message.id).to.equal(id);
        };
        logEventErrorSpy = spy(logEventError);
      });
      afterEach(async () => {
        await redis.xdel('channel', id);
        await redis.xgroup('DESTROY', 'channel', 'group');
      });
      it('should subscribe to streaming messages and call logEventError', async () => {
        const subscriber = new Subscriber({ channels: ['channel'], group: 'group' }, logEventErrorSpy);
        subscriber.subscribe(callback);
        await _sleep(1000); // Wait for messages to be processed
        assert.calledOnce(callback);
        const [payload] = callback.args[0];
        expect(payload.channel).to.equal('channel');
        expect(payload.action).to.equal('action');
        expect(payload.data.foo).to.equal('bar');
        expect(payload.id).to.equal(id);
        /* eslint-disable */
        expect(logEventErrorSpy).to.have.been.calledOnce;
      });
    });
    describe('when callback throws sync exception', () => {
      let logEventError, logEventErrorSpy;
      beforeEach(async () => {
        id = await redis.xadd(
          'channel',
          'MAXLEN',
          '~',
          10,
          '*',
          'action',
          JSON.stringify({ foo: 'bar' })
        );
        callback = stub().throws(new Error('Oops! Something went wrong.'))
        spy(callback);
        logEventError = (err, channel, message) => {
          console.error(err);
          expect(channel).to.equal('channel');
          expect(message.action).to.equal('action');
          expect(message.data.foo).to.equal('bar');
          expect(message.id).to.equal(id);
        };
        logEventErrorSpy = spy(logEventError);
      });
      afterEach(async () => {
        await redis.xdel('channel', id);
        await redis.xgroup('DESTROY', 'channel', 'group');
      });
      it('should subscribe to streaming messages and call logEventError', async () => {
        const subscriber = new Subscriber({ channels: ['channel'], group: 'group' }, logEventErrorSpy);
        subscriber.subscribe(callback);
        await _sleep(1000); // Wait for messages to be processed
        assert.calledOnce(callback);
        const [payload] = callback.args[0];
        expect(payload.channel).to.equal('channel');
        expect(payload.action).to.equal('action');
        expect(payload.data.foo).to.equal('bar');
        expect(payload.id).to.equal(id);
        /* eslint-disable */
        expect(logEventErrorSpy).to.have.been.calledOnce;
      });
    });
  });

  describe('unsubscribe', () => {
    let callback, subscriber;
    beforeEach(async () => {
      callback = stub().resolves();
      spy(callback);
      subscriber = new Subscriber({ channels: ['channel'], group: 'group' });
      subscriber.subscribe(callback);
    });
    afterEach(async () => {
      await redis.xgroup('DESTROY', 'channel', 'group');
    });
    it('should unsubscribe from streaming messages', async () => {
      const { channels, result } = await subscriber.unsubscribe(1000);
      expect(channels).to.have.lengthOf(1);
      expect(channels[0]).to.equal('channel');
      expect(result).to.equal('OK');
    });
  });
});
