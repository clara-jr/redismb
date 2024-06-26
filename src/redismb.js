import Redis from 'ioredis';

let redis;

/**
 * Set redis connection.
 *
 * @param {string} redisUri   Redis uri.
 * @param {number} [ttl]      Seconds waiting until Redis is connected.
 */
async function bootstrap (redisUri, ttl = 30) {
  let ready = false;

  redis = new Redis(redisUri);

  redis.on('ready', () => {
    ready = true;
  });

  redis.on('error', (error) => {
    console.error(error);
  });

  // Wait until redis is ready so the connection is not used before it is established
  let wait = ttl;
  const _sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  /* eslint-disable no-unmodified-loop-condition */
  while (!ready) {
    await _sleep(1000);
    if (--wait < 1) {
      throw new Error('TIMEOUT', `Redis is not connecting (waited for ${wait} seconds)`);
    }
  }

  return redis;
}

/**
 * Terminate redis connection.
 */
async function stop () {
  if (redis) {
    const result = await redis.quit();
    redis = null;
    return result;
  }
}

/**
 * Reads rejected messages based on specified criteria (IDs, time range, and optionally filtered by action).
 * If no filter-parameter is defined, reads all rejected messages.
 *
 * @param {string[]} [ids]               Array of message IDs to read.
 * @param {Date} [from]                  Start date/time for time range query.
 * @param {Date} [to]                    End date/time for time range query.
 * @param {string} [action]              Action type to filter messages.
 *
 * @returns {Promise<{ messages: {
 *    id:string,
 *    action:string,
 *    data:string,
 *    group:string,
 *    channel:string,
 *  }[], count: number }>} Promise object representing the result, containing an array of messages and their count.
 */
async function readRejectedMessages ({ ids, from, to, action } = {}) {
  if (!redis) throw new Error('REDIS_CONNECTION', 'No redis connection has been established');

  let messages = [];

  if (ids?.length) messages = await _readRejectedMessagesByIds(ids);
  else if (!!from && !!to) messages = await _readRejectedMessagesByTimeRange(from, to);
  else messages = await _readAllRejectedMessages();

  if (action) messages = messages.filter((event) => event.action === action);

  return { messages, count: messages.length };
}

/**
 * Reprocesses rejected messages based on specified criteria (IDs, time range, and optionally filtered by action).
 * If 'messages' parameter is provided, it will overwrite the existing messages based on their IDs.
 *
 * @param {[{
 *  id:string,
 *  action:string|undefined,
 *  data:object|undefined,
 *  group:string|undefined,
 *  channel:string|undefined
 * }]} [messages]                          Array of messages to overwrite current messages.
 * @param {string[]} [ids]                 Array of message IDs to reprocess.
 * @param {Date} [from]                    Start date/time for time range query.
 * @param {Date} [to]                      End date/time for time range query.
 * @param {string} [action]                Action type to filter messages.
 *
 * @returns {Promise<{ succeeded: Array, failed: Array }>} Promise object representing the result, containing arrays of succeeded and failed messages.
 */
async function reprocessRejectedMessages ({ messages, ids, from, to, action } = {}) {
  const { messages: messagesToReprocess } = await readRejectedMessages({ ids, from, to, action });
  const succeeded = [];
  const failed = [];

  for (const message of messagesToReprocess) {
    try {
      const newMessage = messages?.find(({ id }) => id === message.id);
      message.channel = message.channel || newMessage?.channel;
      message.group = message.group || newMessage?.group;
      message.data = newMessage?.data ? { ...message.data, ...newMessage.data } : message.data;

      const { id, action, data, group, channel } = message;

      // Send message to channel indicating the consumer group
      await redis.xadd(channel, '*', action, JSON.stringify(data), 'group', group);

      // Delete message from rejections channel after processing
      await redis.xdel('rejections', id);

      succeeded.push(message);
    } catch (err) {
      console.error(err);
      failed.push([message, err.message]);
    }
  }

  return { succeeded, failed };
}

async function _readRejectedMessagesByIds (ids) {
  return Promise.all(
    ids.map(async (id) => {
      const [message] = await _readRejectedMessages({ id });
      if (message) return message;
    })
  );
}

async function _readRejectedMessagesByTimeRange (from, to) {
  return _readRejectedMessages({ from: new Date(from).getTime(), to: new Date(to).getTime() });
}

async function _readAllRejectedMessages () {
  return await _readRejectedMessages();
}

async function _readRejectedMessages ({ id, from, to } = {}) {
  let params = [];

  if (id) params = [id, id];
  else if (!!from && !!to) params = [from, to];
  else params = ['-', '+'];

  const records = await redis.xrange('rejections', ...params);

  return records.map((record) => ({
    id: record[0],
    action: record[1][0],
    data: JSON.parse(record[1][1]),
    group: record[1][2],
    channel: record[1][3]
  }));
}

export default {
  bootstrap,
  stop,
  reprocessRejectedMessages,
  readRejectedMessages
};
export { redis };
