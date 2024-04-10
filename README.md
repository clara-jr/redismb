# redis-message-broker

Welcome to `redis-message-broker`! 🚀

This guide will help you get started with setting up a Redis message broker system using this powerful tool. Below are step-by-step instructions on initializing the Redis connection, creating subscribers and publishers, handling rejected messages, and more.

> [!NOTE]
> Make sure to replace placeholders such as `'channel'`, `'group'`, `'action'`, and `data` with your actual channel names, consumer groups, actions, and data objects.

## Installation

First, make sure you have Node.js installed on your machine. You can install the `redis-message-broker` library using npm:

```bash
npm install redis-message-broker
```

## Getting Started

Now let's dive into how you can use the `redis-message-broker` library in your Node.js applications.

### Initialize the Redis Connection

To start using the library, you need to initialize the Redis connection. Here's how you can do it:

```javascript
import redismb, { Subscriber, Publisher } from 'redis-message-broker'
await redismb.bootstrap('redis://localhost:6379')
```

### Create a Subscriber

Subscribers listen for messages on specific channels and consume them. You can create a subscriber instance like this:

```javascript
const subscriber = new Subscriber({ channels: ['channel'], group: 'group' })
subscriber.subscribe(async ({ channel, action, data, id }) => {
  // Process the received message...
})
```

### Create a Publisher

Publishers send messages to specific channels. Here's how you can create a publisher instance and publish a message:

```javascript
const publisher = new Publisher({ channel: 'channel' })
const data = { foo: 'bar' };
await publisher.publish('action', data)
```

### Unsubscribe and stop the Redis connection

When you're done with streaming messages, you can unsubscribe from channels and stop the Redis connection:

```javascript
await subscriber.unsubscribe()
await redismb.stop()
```

### Handle rejected messages

In case of rejected messages, you can read and reprocess them using the following methods:

```javascript
await redismb.readRejectedMessages({ ids, from, to, action, all })
await redismb.reprocessRejectedMessages({ messages, ids, from, to, action, all })
```

Parameters:

- `ids` (optional): An array of message IDs to filter rejected messages.
- `from` (optional): The start date from which to filter rejected messages.
- `to` (optional): The end date until which to filter rejected messages.
- `action` (optional): Parameter used to filter rejected messages by a specific `action`.
- `all` (optional): A boolean to indicate if all rejected messages should be retrieved.
- `messages`(optional): Array of objects with optional properties: `channel`, `group`, and `data`. These properties are intended to republish the rejected messages to a different channel than they had, to a different consumer group from the one that revoked them, or with new properties in the data.

## Conclusion

That's it! You've learned how to set up a Redis message broker system using the `redis-message-broker` library. Feel free to explore more features and customize it according to your project's needs. Happy messaging! 📨
