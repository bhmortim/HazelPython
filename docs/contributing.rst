Contributing
============

We welcome contributions to the Hazelcast Python Client!

Getting Started
---------------

1. Fork the repository on GitHub
2. Clone your fork locally:

   .. code-block:: bash

      git clone https://github.com/YOUR_USERNAME/HazelPython.git
      cd HazelPython

3. Install development dependencies:

   .. code-block:: bash

      pip install -e ".[dev]"

4. Create a branch for your changes:

   .. code-block:: bash

      git checkout -b feature/my-feature

Development
-----------

Running Tests
~~~~~~~~~~~~~

.. code-block:: bash

   # Run all unit tests
   pytest tests/ -v

   # Run with coverage
   pytest tests/ --cov=hazelcast --cov-report=html

   # Run integration tests (requires running cluster)
   pytest tests/integration/ -v --integration

Code Style
~~~~~~~~~~

We use Black for code formatting and isort for import sorting:

.. code-block:: bash

   # Format code
   black hazelcast/ tests/

   # Sort imports
   isort hazelcast/ tests/

   # Check types
   mypy hazelcast/

   # Lint
   flake8 hazelcast/ tests/

Building Documentation
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   cd docs
   make html

   # View at docs/_build/html/index.html

Pull Requests
-------------

1. Ensure all tests pass
2. Update documentation for any new features
3. Add entries to CHANGELOG.md
4. Submit a pull request to the main branch

Code of Conduct
---------------

Please be respectful and constructive in all interactions.

License
-------

By contributing, you agree that your contributions will be licensed
under the Apache License 2.0.
