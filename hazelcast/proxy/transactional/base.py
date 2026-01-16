"""Base class for transactional proxies."""

from abc import ABC
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from hazelcast.transaction import TransactionContext


class TransactionalProxy(ABC):
    """Base class for all transactional distributed object proxies.

    Transactional proxies provide operations that participate in a
    transaction managed by a TransactionContext. All operations on
    transactional proxies require an active transaction.

    Attributes:
        name: The name of the underlying distributed object.
        transaction_context: The transaction context managing this proxy.
    """

    def __init__(self, name: str, transaction_context: "TransactionContext"):
        self._name = name
        self._transaction_context = transaction_context

    @property
    def name(self) -> str:
        """Get the name of this distributed object."""
        return self._name

    @property
    def transaction_context(self) -> "TransactionContext":
        """Get the transaction context."""
        return self._transaction_context

    def _check_transaction_active(self) -> None:
        """Verify the transaction is active.

        Raises:
            TransactionNotActiveException: If the transaction is not active.
        """
        self._transaction_context._check_active()

    def _has_server_connection(self) -> bool:
        """Check if a server connection is available."""
        ctx = self._transaction_context._context
        return ctx is not None and ctx.invocation_service is not None

    def _invoke(self, request: Any) -> Any:
        """Send a request through the invocation service."""
        ctx = self._transaction_context._context
        if ctx and ctx.invocation_service:
            return ctx.invocation_service.invoke(request)
        return None

    def _to_data(self, obj: Any) -> bytes:
        """Serialize an object to binary data."""
        ctx = self._transaction_context._context
        if ctx and ctx.serialization_service:
            return ctx.serialization_service.to_data(obj)
        if obj is None:
            return b""
        if isinstance(obj, bytes):
            return obj
        return str(obj).encode("utf-8")

    def _to_object(self, data: bytes) -> Any:
        """Deserialize binary data to an object."""
        ctx = self._transaction_context._context
        if ctx and ctx.serialization_service:
            return ctx.serialization_service.to_object(data)
        if not data:
            return None
        return data

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self._name!r})"
