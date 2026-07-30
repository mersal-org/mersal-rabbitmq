Mersal-RabbitMQ
==================

.. list-table::
   :header-rows: 1

   * - Project
     - Status
   * - CI/CD
     - .. image:: https://github.com/mersal-org/mersal-rabbitmq/actions/workflows/publish.yml/badge.svg
          :target: https://github.com/mersal-org/mersal-rabbitmq/actions/workflows/publish.yml
          :alt: Latest Release
       .. image:: https://github.com/mersal-org/mersal-rabbitmq/actions/workflows/ci.yml/badge.svg
          :target: https://github.com/mersal-org/mersal-rabbitmq/actions/workflows/ci.yml
          :alt: CI
       .. image:: https://github.com/mersal-org/mersal-rabbitmq/actions/workflows/docs.yml/badge.svg?branch=main
          :target: https://github.com/mersal-org/mersal-rabbitmq/actions/workflows/docs.yml
          :alt: Documentation Building
   * - Package
     - .. image:: https://img.shields.io/pypi/v/mersal-rabbitmq?labelColor=202235&color=1e4b94&logo=python&logoColor=white
          :target: https://badge.fury.io/py/mersal
          :alt: PyPI - Version
       .. image:: https://img.shields.io/pypi/pyversions/mersal-rabbitmq?labelColor=202235&color=1e4b94&logo=python&logoColor=white
          :alt: PyPI - Support Python Versions
       .. image:: https://img.shields.io/pypi/dm/mersal-rabbitmq?logo=python&label=package%20downloads&labelColor=202235&color=1e4b94&logoColor=white
          :alt: mersal-rabbitmq PyPI - Downloads
   * - Meta
     - .. image:: https://img.shields.io/badge/license-MIT-202235.svg?logo=python&labelColor=202235&color=1e4b94&logoColor=white
          :target: https://spdx.org/licenses/
          :alt: License - MIT
       .. image:: https://img.shields.io/badge/types-ty-202235.svg?logo=python&labelColor=202235&color=1e4b94&logoColor=white
          :target: https://github.com/astral-sh/ty
          :alt: types - ty
       .. image:: https://img.shields.io/badge/types-Basedpyright-202235.svg?logo=python&labelColor=202235&color=1e4b94&logoColor=white
          :target: https://github.com/DetachHead/basedpyright
          :alt: types - Basedpyright
       .. image:: https://img.shields.io/badge/types-Mypy-202235.svg?logo=python&labelColor=202235&color=1e4b94&logoColor=white
          :target: https://github.com/python/mypy
          :alt: types - Mypy
       .. image:: https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v2.json&labelColor=202235&color=1e4b94
          :target: https://github.com/astral-sh/ruff
          :alt: linting - Ruff


**mersal_rabbitmq** is the RabbitMQ implementation for Mersal. It allows using RabbitMQ as a transport that also supports Mersal pub/sub.


 Read the docs `here <https://mersal-rabbitmq.mersal.dev>`_


Acknowledgments
-----------------

Thanks to all contributors in `Rebus-RabbitMQ <https://github.com/rebus-org/Rebus.RabbitMq>`_. Many ideas were copied from there. Thanks also goes to `aio-pika <https://docs.aio-pika.dev/>`_ for providing an async client library for Rabbitmq.
