"""Base class for transactional distributed object proxies."""

from abc import ABC
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from hazelcast.transaction import TransactionContext


class TransactionalProxy(ABC):
    """Base class for transactional distributed object proxies.

    Transactional proxies provide access to distributed data structures
    within a transaction context. Operations on these proxies are part
    of the enclosing transaction and are committed or rolled back atomically.

    Subclasses implement specific data structure operations.
    """

    def __init__(self, name: str, transaction: "TransactionContext"):
        self._name = name
        self._transaction = transaction

    @property
    def name(self) -> str:
        """Get the name of this distributed object."""
        return self._name

    @property
    def transaction(self) -> "TransactionContext":
        """Get the transaction context."""
        return self._transaction

    def _check_transaction_active(self) -> None:
        """Verify the transaction is still active."""
        self._transaction._check_active()

    def _to_data(self, obj: Any) -> bytes:
        """Serialize an object to binary data."""
        if self._transaction._context and self._transaction._context.serialization_service:
            return self._transaction._context.serialization_service.to_data(obj)
        if obj is None:
            return b""
        if isinstance(obj, bytes):
            return obj
        return str(obj).encode("utf-8")

    def _to_object(self, data: bytes) -> Any:
        """Deserialize binary data to an object."""
        if self._transaction._context and self._transaction._context.serialization_service:
            return self._transaction._context.serialization_service.to_object(data)
        if not data:
            return None
        return data

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self._name!r})"
