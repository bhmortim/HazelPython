"""Integration tests for the Hazelcast Python client.

These tests require a running Hazelcast cluster. Use Docker Compose
to start a test cluster:

    docker-compose -f tests/integration/docker-compose.yml up -d

Run integration tests with:

    pytest tests/integration/ -v --integration
"""
