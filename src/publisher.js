import { redis } from './redis.js';

export default class Publisher {
  /**
   * Constructor for creating a stream publisher.
   *
   * @constructor
   *
   * @param {string} channel          Single channel name to connect to.
   * @param {number} [maxLength=5000] Maximum amount of held messages, both read and unread.
   */
  constructor ({ channel, maxLength = 5000 }) {
    // Parameter validation
    if (!channel) throw new Error('MISSED_VALUE', 'No channel in Publisher provided');

    // Assigning values
    this.channel = channel;
    this.maxLength = maxLength;
  }

  /**
   * Publish a message.
   *
   * @param {string} action   Action to perform.
   * @param {string} data     Data to publish.
   */
  publish = async (action, data) => {
    if (!redis) throw new Error('REDIS_CONNECTION', 'No redis connection has been established');

    data = JSON.stringify(data);
    const id = await redis.xadd(
      this.channel,
      'MAXLEN',
      // Although exact trimming is possible and is the default, due to the internal representation of streams
      // it is more efficient to add an entry and trim stream with XADD using almost exact trimming (the ~ argument).
      '~',
      // The old entries are automatically evicted when the specified length is reached, so that the stream is left at a constant size.
      this.maxLength,
      // The XADD command will auto-generate a unique ID for you if the ID argument specified is the * character.
      // However, while useful only in very rare cases, it is possible to specify a well-formed ID,
      // so that the new entry will be added exactly with the specified ID.
      '*',
      action,
      data
    );

    this.#logMessageStatus('PUBLISHED', { channel: this.channel, action, id });

    return id;
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
