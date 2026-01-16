Installation
============

Requirements
------------

* Python 3.8 or higher
* A running Hazelcast cluster (5.0+)

Installing with pip
-------------------

Install the Hazelcast Python client using pip:

.. code-block:: bash

   pip install hazelcast-python-client

Installing from Source
----------------------

Clone the repository and install in development mode:

.. code-block:: bash

   git clone https://github.com/hazelcast/hazelcast-python-client.git
   cd hazelcast-python-client
   pip install -e .

Optional Dependencies
---------------------

For additional features, install optional dependencies:

.. code-block:: bash

   # For YAML configuration support
   pip install pyyaml

   # For documentation building
   pip install sphinx sphinx-rtd-theme

   # For development and testing
   pip install pytest pytest-asyncio

Verifying Installation
----------------------

Verify the installation by importing the client:

.. code-block:: python

   import hazelcast
   print(f"Hazelcast Python Client version: {hazelcast.__version__}")

Starting a Test Cluster
-----------------------

For development, start a local Hazelcast cluster using Docker:

.. code-block:: bash

   docker run -p 5701:5701 hazelcast/hazelcast:5.3

Or download and run Hazelcast directly:

.. code-block:: bash

   # Download Hazelcast
   wget https://repository.hazelcast.com/download/hazelcast/hazelcast-5.3.6.zip
   unzip hazelcast-5.3.6.zip
   cd hazelcast-5.3.6
   
   # Start the cluster
   ./bin/hz start
