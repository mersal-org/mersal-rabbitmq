Usage
=====

Installation
------------

If you're using `uv`:

.. code-block:: bash

    uv add mersal_rabbitmq

Otherwise,

.. code-block:: bash

    pip install mersal_rabbitmq

Configuring the transport & pub/sub
-------------------------------------

Use the plugin :py:class:`~mersal_rabbitmq.plugin.RabbitMQPlugin`, to configure both the transport and pub/sub:

.. code-block:: python

    from mersal.app import Mersal
    from mersal_rabbitmq.plugin import RabbitMQPluginConfig

    rabbitmq_plugin_config = RabbitMQPluginConfig(
        connection_uri="amqp://guest:guest@localhost:5672",
        input_queue_name="my-app",
    )

    app = Mersal(
        "my-app",
        activator,
        plugins=[rabbitmq_plugin_config.plugin()],
    )

    await app.start()

Key ``RabbitMQPluginConfig`` fields:

``connection_uri``
    A standard AMQP URI, e.g. ``amqp://user:pass@host:5672/vhost``.

``input_queue_name``
    This app's own address - the name of the queue it consumes from.

``prefetch_count`` (default ``50``)
    How many unacknowledged messages the broker will push to this transport at once.

``passive_queue_check_interval`` (default ``60`` seconds)
    How often to passively verify the input queue still exists on the broker, and
    proactively recreate it if not. ``None`` disables the check entirely.

``should_declare_exchanges`` / ``should_declare_input_queue`` / ``should_bind_input_queue``
    Each defaults to ``True``, letting the transport manage its own topology. Set any of
    them to ``False`` if that piece of topology is managed externally (e.g. by
    infrastructure-as-code) instead.

``direct_exchange_name`` / ``topic_exchange_name`` (default ``"mersal.direct"`` / ``"mersal.topics"``)
    Names of the two exchanges the transport declares and uses. If you're also using
    :py:class:`~mersal_rabbitmq.subscription_storage.RabbitMqSubscriptionStorage`,
    its ``topic_exchange_name`` must match this one.

``input_queue_declaration_options`` / ``default_queue_declaration_options``
    :py:class:`~mersal_rabbitmq.transport.QueueDeclarationOptions` controlling
    durability/exclusivity/auto-delete/arguments - the first for this transport's own
    input queue, the second for any other queue this transport declares (e.g. via
    ``create_queue``). Both default to a durable, non-exclusive, non-auto-delete queue.

If for any reason, you don't want to use the plugin, the transport can be configured separately by providing an instance of :py:class:`~mersal_rabbitmq.transport.RabbitMqTransport` to the `transport` argument in the Mersal app constructor. Similarly, an instance of :py:class:`~mersal_rabbitmq.subscription_storage.RabbitMqSubscriptionStorage` can be given to the `subscription_config` argument.
