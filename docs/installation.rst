Installation
============

Requirements
------------

- Python 3.8 or later
- A running Hazelcast cluster (5.x recommended)

Installing from PyPI
--------------------

.. code-block:: bash

    pip install hazelcast-python-client

Installing with Optional Dependencies
-------------------------------------

For development tools:

.. code-block:: bash

    pip install hazelcast-python-client[dev]

For SSL/TLS support:

.. code-block:: bash

    pip install hazelcast-python-client[ssl]

For documentation building:

.. code-block:: bash

    pip install hazelcast-python-client[docs]

Installing from Source
----------------------

.. code-block:: bash

    git clone https://github.com/hazelcast/HazelPython.git
    cd HazelPython
    pip install -e .

Verifying Installation
----------------------

.. code-block:: python

    import hazelcast
    print(f"Hazelcast Python Client installed successfully")

Starting a Local Cluster
------------------------

Using Docker:

.. code-block:: bash

    docker run -d --name hazelcast -p 5701:5701 hazelcast/hazelcast:5.3

Using Hazelcast CLI:

.. code-block:: bash

    hz start
