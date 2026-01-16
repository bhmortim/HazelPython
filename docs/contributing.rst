Contributing
============

We welcome contributions to the Hazelcast Python Client!

Development Setup
-----------------

1. Clone the repository:

   .. code-block:: bash

       git clone https://github.com/hazelcast/HazelPython.git
       cd HazelPython

2. Create a virtual environment:

   .. code-block:: bash

       python -m venv venv
       source venv/bin/activate  # On Windows: venv\Scripts\activate

3. Install development dependencies:

   .. code-block:: bash

       pip install -e ".[dev]"

Running Tests
-------------

Unit tests:

.. code-block:: bash

    pytest tests/ -v

Integration tests (requires Docker):

.. code-block:: bash

    pytest tests/integration/ -v

With coverage:

.. code-block:: bash

    pytest tests/ --cov=hazelcast --cov-report=html

Code Style
----------

We use the following tools for code quality:

* **black** for code formatting
* **isort** for import sorting
* **flake8** for linting
* **mypy** for type checking

Run all checks:

.. code-block:: bash

    black hazelcast tests
    isort hazelcast tests
    flake8 hazelcast tests
    mypy hazelcast

Building Documentation
----------------------

.. code-block:: bash

    cd docs
    pip install -e "..[docs]"
    make html

Documentation will be in ``docs/_build/html/``.

Submitting Changes
------------------

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

Code of Conduct
---------------

Please be respectful and constructive in all interactions.
