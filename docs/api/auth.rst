Authentication
==============

.. module:: hazelcast.auth
   :synopsis: Authentication support for Hazelcast client.

This module provides authentication credentials and services for connecting
to secured Hazelcast clusters.

Enumerations
------------

.. autoclass:: CredentialsType
   :members:
   :undoc-members:

Credentials Classes
-------------------

.. autoclass:: Credentials
   :members:
   :special-members: __init__

.. autoclass:: UsernamePasswordCredentials
   :members:
   :special-members: __init__

.. autoclass:: TokenCredentials
   :members:
   :special-members: __init__

.. autoclass:: KerberosCredentials
   :members:
   :special-members: __init__

.. autoclass:: LdapCredentials
   :members:
   :special-members: __init__

.. autoclass:: CustomCredentials
   :members:
   :special-members: __init__

Credentials Factories
---------------------

.. autoclass:: CredentialsFactory
   :members:
   :special-members: __init__

.. autoclass:: StaticCredentialsFactory
   :members:
   :special-members: __init__

.. autoclass:: CallableCredentialsFactory
   :members:
   :special-members: __init__

Authentication Service
----------------------

.. autoclass:: AuthenticationService
   :members:
   :special-members: __init__

Example Usage
-------------

Username/password authentication::

    from hazelcast.auth import UsernamePasswordCredentials

    creds = UsernamePasswordCredentials("admin", "secret123")

Token-based authentication::

    from hazelcast.auth import TokenCredentials, AuthenticationService

    credentials = TokenCredentials("my-auth-token")
    auth_service = AuthenticationService(credentials=credentials)

Custom credentials factory::

    from hazelcast.auth import CallableCredentialsFactory, UsernamePasswordCredentials

    def get_credentials():
        return UsernamePasswordCredentials("user", "pass")

    factory = CallableCredentialsFactory(get_credentials)
