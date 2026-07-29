Welcome to Mersal RabbitMQ's documentation!
============================================

**mersal_rabbitmq** is the RabbitMQ implementation for Mersal. It allows using RabbitMQ as a transport that also supports Mersal pub/sub.


Quickstart
------------

.. code-block:: bash

    uv add mersal_rabbitmq

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

See :doc:`usage <./usage>` for more info.


.. toctree::
   :titlesonly:
   :caption: Documentation
   :hidden:

   Home <self>
   usage
   implementation_details
   reference


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
