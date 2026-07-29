Implementation Details
==========================

Client library
~~~~~~~~~~~~~~~~

`aio-pika <https://docs.aio-pika.dev/>`_ is used as the RabbitMQ client library.

Connections
~~~~~~~~~~~~~

A single AMQP connection is made with two long-lived channels:

- a **publish channel**, created with publisher confirms.
- a **consume channel**, created with a configurable QoS prefetch count.


Topology
~~~~~~~~~~

Two durable exchanges are declared:

- A **direct** exchange for point-to-point sends. Every Mersal transport binds its own input queue to it with a routing key equal to its own address.
- A **topic** exchange for pub/sub.

Queues are declared durable by default and messages are sent using a "persistent" delivery mode.

Mersal integration
~~~~~~~~~~~~~~~~~~~~

The library provides an implementation for Mersal two main protocols:

- :py:class:`mersal.transport.Transport`, via :py:class:`~mersal_rabbitmq.transport.RabbitMqTransport`
- :py:class:`mersal.subscription.SubscriptionStorage`, via :py:class:`~mersal_rabbitmq.subscription_storage.RabbitMqSubscriptionStorage`

Push-Pull bridge
-------------------

Mersal worker is designed to be a pull-based API. An infinite loop is run which
queries the transport for messages. It pulls one message at a time. This pulling
is run until a maximum number of messages are being handled by the Mersal worker
(configured via :code:`max_parallelism`).

This doesn't align well with push based transports such as RabbitMQ. `aio-pika`
has two main patterns for consuming messages, either via :code:`await
queue.consume(on_message_callback)` or by iterating over the queue iterator
provided by :code:`queue.iterator()`. The latter has been used to build a
push-pull bridge.

In this bridge, the Mersal worker `receive` API (which runs in an infinite loop)
is not allowed to return `None` when there are no messages. It awaits messages
being placed in a local buffer. This local buffer is filled by iterating over
messages using :code:`async for message in queue.iterator`.

So in short, we run an async task to consume the pushed messages and store them
in a buffer then the pull api running in another task consumes this buffer.


Missing features
~~~~~~~~~~~~~~~~~~~

1. Message expiry: tracked in `<https://github.com/mersal-org/mersal-rabbitmq/issues/1>`_
2. Deferred messages: tracked in `<https://github.com/mersal-org/mersal-rabbitmq/issues/2>`_
3. Replaying subscriptions if a queue is recreated.
