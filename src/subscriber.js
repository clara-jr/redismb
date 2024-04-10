import { redis } from './redismb.js';

export default class Subscriber {
  /**
   * Constructor for creating a stream subscriber.
   *
   * If the interval is set to 0, the stream will be read continuously.
   * If it is greater than 0, it will be read at the specified interval.
   *
   * @constructor
   *
   * @param {string[]} channels         Channels to connect to.
   * @param {string} group              A connsumer group identifier for subscribers.
   * @param {string} [clientId]         The subscriber ID for tracing. If not provided, it will be autogenerated.
   * @param {number} [timeout=10000]    Timeout in milliseconds to ACK a message.
   * @param {number} [interval=0]       Interval in milliseconds at which messages are checked.
   * @param {number} [messages=1]       Maximum number of messages carried with each stream check.
   * @param {number} [retries=3]        Number of retries to process a message.
   * @param {function} [callback]       Callback function to call when an error occurs. It should look like: (err, channel, message) => {...}
   */
  constructor ({ channels, group, clientId, timeout = 10000, interval = 0, messages = 1, retries = 3 },
    callback = (err, channel, message) => {
      console.error(err);
    }) {
    // Parameter validation
    if (!channels) throw new Error('MISSED_VALUE', 'No channels in Subscriber provided');
    if (!group) throw new Error('MISSED_VALUE', 'No group in Subscriber provided');
    if (callback) {
      if (typeof callback !== 'function') {
        throw new TypeError('Callback must be a function');
      }
      const callbackParamsCount = callback.length;
      if (callbackParamsCount < 1 || callbackParamsCount > 3) {
        throw new Error('Callback function must accept between 1 and 3 parameters: (err, channel?, message?)');
      }
    }

    // Assigning values
    this.channels = channels;
    this.group = group;
    this.clientId = clientId || `${group}:sub:${Date.now()}`;
    this.timeout = timeout;
    this.interval = interval;
    this.messages = messages;
    this.retries = retries;
    this.logEventError = callback;

    // Setting block parameter based on interval
    // BLOCK parameter in Redis Streams allows clients to wait synchronously:
    //    1. until new data is available in the stream
    //    2. or until a specified timeout expires
    // If continuous reading (every 0 ms) is chosen, the blocking time will be set to 3000 ms,
    // whereas if reading occurs less frequently, the blocking time will be shorter (1 ms)
    // due to a higher likelihood of messages being available in the stream, reducing the need for waiting.
    this.block = this.interval ? 1 : 3000;

    // Flag to control if we should continue/stop reading
    this.continueReading = true;

    // Creating Redis Streams consumer group
    this.#createGroup();
  }

  /**
   * Create new group or join to existing one.
   */
  #createGroup = async () => {
    if (!redis) throw new Error('REDIS_CONNECTION', 'No redis connection has been established');

    for (const channel of this.channels) {
      try {
        await redis.xgroup(
          'CREATE',
          channel,
          this.group,
          // We can use $ or an ID to start reading from the last message of the stream or from a particular one.
          // If we want the group's consumers to fetch the entire stream from the beginning, use 0 as the starting ID for the consumer group.
          '0',
          // MKSTREAM creates the stream if it doesn’t already exist.
          'MKSTREAM'
        );
        console.info(`Group ${this.group} have been created in stream ${channel}.`);
      } catch (err) {
        if (err.message.includes('BUSYGROUP')) {
          console.info(`Group ${this.group} already exists at stream ${channel}.`);
        } else {
          throw err;
        }
      };
    }
  };

  /**
   * Subscribe to streaming messages.
   *
   * @param {function} callback Processing messages callback.
   */
  subscribe = (callback) => {
    if (this.interval > 0) this.#intervalRead(callback);
    else this.#continualRead(callback);
  };

  /**
   * Delete consumer.
   *
   * @param {number} [timeout=60000] Milliseconds to wait after stop claiming messages and before deleting consumer.
   */
  unsubscribe = async (timeout = 60000) => {
    const _stopReading = () => {
      this.continueReading = false;
      if (this.readingInterval) clearInterval(this.readingInterval);
    };
    const _setTimeout = (timeout) => {
      return new Promise((resolve) => setTimeout(resolve, timeout));
    };

    // wait [timeout] milliseconds after stop claiming messages (and before deleting consumer)
    // in order to avoid the existence of pending messages that could become unclaimable.
    await Promise.all([_stopReading(), _setTimeout(timeout)]);

    for (const channel of this.channels) {
      await redis.xgroup('DELCONSUMER', channel, this.group, this.clientId);
      console.info(
        `Consumer ${this.clientId} has been removed from consumer group ${this.group} in channel ${channel}.`
      );
    }

    return { channels: this.channels, result: 'OK' };
  };

  /**
   * Strategy to read stream with provided intervals.
   *
   * @param {function} callback Processing message callback.
   */
  #intervalRead = (callback) => {
    console.info(`Client ${this.clientId} connecting to ${this.channels} channel for Interval Read`);
    this.readingInterval = setInterval(async () => {
      if (!this.continueReading) return;
      await this.#readMessages(callback);
      await this.#readPendingMessages(callback);
    }, this.interval);
  };

  /**
   * Strategy to read stream continually.
   *
   * @param {function} callback Processing message callback.
   */
  #continualRead = async (callback) => {
    const _recursiveCall = async (callback) => {
      if (!this.continueReading) return;
      await this.#readMessages(callback);
      await this.#readPendingMessages(callback);
      _recursiveCall(callback);
    };
    try {
      console.info(`Client ${this.clientId} connecting to ${this.channels} channel for Continual Read`);
      await _recursiveCall(callback);
    } catch (err) {
      this.logEventError(err);
    }
  };

  /**
   * Read live streaming messages.
   *
   * @param {function} callback Processing message callback.
   */
  #readMessages = async (callback) => {
    const streams = await redis.xreadgroup(
      'GROUP',
      this.group,
      this.clientId,
      'BLOCK',
      // BLOCK parameter in Redis Streams allows clients to wait synchronously:
      //    1. until new data is available in the stream
      //    2. or until a specified timeout expires
      this.block,
      'COUNT',
      // Set maximum number of messages carried with each stream check.
      this.messages,
      'STREAMS',
      ...this.channels,
      // The special character “>” at the end tells Redis Stream to fetch only data
      // entries that were never delivered to any other consumer (of the consumer group).
      // It just means, give me new messages.
      // So basically if the ID is not >, then the command will just let the client access
      // its pending entries: messages delivered to it, but not yet acknowledged.
      ...this.channels.map(() => '>')
    );

    if (streams) {
      for (const stream of streams) {
        const channel = stream[0];
        const messages = stream[1];
        if (messages) {
          const parsedMessages = this.#parseMessages(messages);
          const { receive, skip } = this.#filterMessages(parsedMessages);
          if (receive.length) {
            this.#processMessages(channel, receive, callback);
          }
          if (skip.length) {
            // Skip messages that should not be processed by this group. Skipped messages are directly acknowledge.
            this.#ackMessages(channel, skip, 'SKIPPED');
          }
        }
      }
    }
  };

  /**
   * Read pending messages (messages that have been taken for processing and have not yet been confirmed).
   *
   * @param {function} callback Processing message callback.
   */
  #readPendingMessages = async (callback) => {
    for (const channel of this.channels) {
      const pendingMessages = await redis.xpending(
        channel,
        this.group,
        'IDLE',
        // Read only messages that have not been confirmed in [timeout] time.
        this.timeout,
        '-', // Range starts with [init]
        '+', // Range finishes with [end]
        20 // Quantity of messages read per request from PEL
      );

      if (pendingMessages.length) {
        const { receive, reject } = this.#filterPendingMessages(pendingMessages);
        if (receive.length) {
          await this.#claimMessages(channel, receive, true, callback);
        }
        if (reject.length) {
          await this.#claimMessages(channel, reject, false, callback);
        }
      }
    }
  };

  /**
   * Adapt redis records to an established standard of a message structure.
   *
   * @param {[string,[string,string,string,string]}]} messages Array of redis records.
   *
   * @returns {[{
   *  id:string,
   *  action:string,
   *  data:JSON,
   *  date:number,
   *  clientId:string,
   *  group:string|undefined,
   *  }]} Parsed messages.
   */
  #parseMessages = (messages) => {
    return messages.map((msg) => {
      const id = msg[0];
      const value = msg[1];
      let data;
      try {
        data = JSON.parse(value[1]);
      } catch (_) {
        data = value[1];
      }
      return {
        id,
        action: value[0],
        data,
        date: new Date(+msg[0].split('-')[0]),
        clientId: this.clientId,
        group: value[3] // Unique group that should consume the message (if provided)
      };
    });
  };

  /**
   * Decide weather a live message should be received (processed) or skipped.
   *
   * @param {{
   *  group:string,
   *  }} parsedMessages
   *
   * @returns {{
   *  receive:[string],
   *  skip:[string],
   * }} Filtered messages ids for receiving (processing) and skipping messages.
   */
  #filterMessages = (parsedMessages) => {
    return parsedMessages.reduce(
      (acc, msg) => {
        const shouldReceiveMessage = !msg.group || msg.group === this.group;
        if (shouldReceiveMessage) acc.receive.push(msg);
        else acc.skip.push(msg);
        return acc;
      },
      { receive: [], skip: [] }
    );
  };

  /**
   * Decides if message should be received (processed) or rejected basing on the number of attempts of processing the message that redis PEL provides.
   *
   * @param {[string,,,number]} pendingMessages Message metadata that provides information about the number of attempts that have been made to process each message.
   *
   * @returns {{
   *  receive:[string],
   *  reject:[string],
   * }} Filtered messages ids for receiving (processing) and rejecting messages.
   */
  #filterPendingMessages = (pendingMessages) => {
    return pendingMessages.reduce(
      (acc, [id, , , attempts]) => {
        const hasExceededLimitOfAttempts = attempts > this.retries;
        if (hasExceededLimitOfAttempts) acc.reject.push(id);
        else acc.receive.push(id);
        return acc;
      },
      { receive: [], reject: [] }
    );
  };

  /**
   * Receiving messages to process them and handle errors.
   *
   * @param {string}   channel     Channel to parse messages from.
   * @param {[{
   *  id:string,
   *  action:string,
   *  data:JSON,
   *  date:number }]}  messages    Messages taken from channel.
   * @param {function} callback    Processing message callback.
   */
  #processMessages = (channel, messages, callback) => {
    Promise.all(
      messages.map((message) => {
        this.#logMessageStatus('RECEIVED', { channel, action: message.action, id: message.id });
        return callback({ channel, ...message })
          .then(() => this.#ackMessages(channel, [message], 'CONFIRMED'))
          .catch((err) => {
            this.logEventError(err, channel, message);
          });
      })
    );
  };

  /**
   * Claim pending messages either for confirm them or reject them.
   * Messages that have been taken for processing and have not yet been confirmed.
   *
   * @param {string} channel               Channel name.
   * @param {[string]} ids                 Array of redis messages.
   * @param {boolean} shouldProcessMessage Indicates if messages i claimed to be received or rejected.
   * @param {function} callback            Processing message callback.
   */
  #claimMessages = async (channel, ids, shouldProcessMessage, callback) => {
    const claimedMessages = await redis.xclaim(
      channel,
      this.group,
      this.clientId,
      this.timeout,
      ...ids
    );

    if (claimedMessages?.some((msg) => !!msg)) {
      const parsedMessages = this.#parseMessages(claimedMessages.filter((msg) => !!msg));
      if (shouldProcessMessage) {
        this.#processMessages(channel, parsedMessages, callback);
      } else {
        this.#rejectMessages(channel, parsedMessages);
      }
    }
  };

  /**
   * Confirm the message/s as processed.
   *
   * @param {string} channel  Channel name.
   * @param {[{
   *  action:string,
   *  id:string
   * }]} messages             Messages to acknowledge.
   * @param {string} status   Message status.
   */
  #ackMessages = async (channel, messages, status) => {
    await redis.xack(channel, this.group, ...messages.map(({ id }) => id));
    messages.forEach(({ action, id }) => {
      this.#logMessageStatus(status, { channel, action, id });
    });
  };

  /**
   * Reject and remove messages from PEL and move to specific stream of rejected messages.
   *
   * @param {string} channel Channel name.
   * @param {[{
   *  action:string,
   *  id:string
   * }]} messages            Messages.
   */
  #rejectMessages = async (channel, messages) => {
    await this.#ackMessages(channel, messages, 'REJECTED');

    Promise.all(
      messages.map(async (message) => {
        this.logEventError(new Error('MAX_RETRIES', 'Event exceed max retries'), channel, message);
        const { action, data } = message;
        return redis.xadd(
          'rejections',
          '*',
          action,
          JSON.stringify(data),
          this.group,
          channel
        );
      })
    );
  };

  /**
   * Log message status information.
   *
   * @param {string} status                                           Message status.
   * @param {{ channel:string, action:string, id:string }} message    Message channel, action and id.
   */
  #logMessageStatus = (status, { channel, action, id }) => {
    console.info(`[${new Date().toISOString()}] ${status} ${channel} ${action} ${id}`);
  };
}
