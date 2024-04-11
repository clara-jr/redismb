# redismb

[![npm](https://img.shields.io/npm/dt/redismb)](https://www.npmjs.com/package/redismb)
[![npm](https://img.shields.io/npm/dw/redismb)](https://www.npmjs.com/package/redismb)
[![GitHub license](https://img.shields.io/github/license/clara-jr/redismb)](https://github.com/clara-jr/redismb/blob/master/LICENSE)
[![npm](https://img.shields.io/npm/v/redismb)](https://www.npmjs.com/package/redismb)

Welcome to `redismb`! 🚀

This guide will help you get started with setting up a Redis message broker system based on [Redis Streams](https://redis.io/docs/latest/develop/data-types/streams/) using this powerful tool. Below are step-by-step instructions on initializing the Redis connection, creating subscribers and publishers, handling rejected messages, and more.

> [!NOTE]
> Make sure to replace placeholders such as `'channel'`, `'group'`, `'action'`, and `data` with your actual channel names, consumer groups, actions, and data objects.

## Installation

You can install the `redismb` library using npm:

```bash
npm install redismb
```

## Getting Started

Now let's dive into how you can use the `redismb` library in your Node.js applications.

### Initialize the Redis Connection

To start using the library, you need to initialize the Redis connection. Here's how you can do it:

```javascript
import redismb, { Subscriber, Publisher } from 'redismb';
await redismb.bootstrap('redis://localhost:6379');
```

### Create a Subscriber

Subscribers listen for messages on specific channels and consume them while assigned to a consumer group. You can create a subscriber instance like this:

```javascript
const subscriber = new Subscriber({ channels: ['channel'], group: 'group' });
subscriber.subscribe(async ({ channel, action, data, id }) => {
  // Process the received message...
});
```

### Create a Publisher

Publishers send messages to specific channels. Here's how you can create a publisher instance and publish a message:

```javascript
const publisher = new Publisher({ channel: 'channel' });
const data = { foo: 'bar' };
await publisher.publish('action', data);
```

### Unsubscribe and Stop the Redis Connection

When you're done with streaming messages, you can unsubscribe from channels and stop the Redis connection:

```javascript
await subscriber.unsubscribe();
await redismb.stop();
```

### Handle Rejected Messages

In case of rejected messages, you can read and reprocess them using the following methods:

```javascript
await redismb.readRejectedMessages({ ids, from, to, action });
await redismb.reprocessRejectedMessages({ messages, ids, from, to, action });
```

Parameters:

- `ids` (optional): An array of message IDs to filter rejected messages.
- `from` (optional): The start date from which to filter rejected messages.
- `to` (optional): The end date until which to filter rejected messages.
- `action` (optional): Parameter used to filter rejected messages by a specific `action`.

If none of the previous parameters are defined, all messages will be retrieved.

- `messages`(optional): Array of objects with optional properties: `id`, `channel`, `group`, and `data`. These properties are intended to republish the rejected messages (filtered by the previous parameters) to a different channel than they had, to a different consumer group from the one that revoked them, or with new properties in the data object.

## Conclusion

That's it! You've learned how to set up a Redis message broker system using the `redismb` library. Feel free to explore more features and customize it according to your project's needs.

You can also find [here](https://lp.redislabs.com/rs/915-NFD-128/images/DS-Redis-Streams.pdf) a cool cheatsheet ❤️ about Redis Streams.

Happy messaging! 📨
