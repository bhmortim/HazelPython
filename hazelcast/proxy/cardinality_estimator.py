"""CardinalityEstimator proxy for HyperLogLog-based cardinality estimation."""

from concurrent.futures import Future
from typing import Any, Optional, TYPE_CHECKING

from hazelcast.proxy.base import Proxy, ProxyContext

if TYPE_CHECKING:
    from hazelcast.protocol.client_message import ClientMessage

SERVICE_NAME_CARDINALITY_ESTIMATOR = "hz:impl:cardinalityEstimatorService"


class CardinalityEstimator(Proxy):
    """Proxy for distributed CardinalityEstimator.

    CardinalityEstimator is a distributed data structure that uses the
    HyperLogLog algorithm to estimate the number of distinct elements
    in a dataset. It provides probabilistic counting with low memory
    overhead.

    The standard error is typically around 1.04/sqrt(m) where m is the
    number of registers used internally.

    Example:
        >>> estimator = client.get_cardinality_estimator("visitors")
        >>> estimator.add("user-1")
        >>> estimator.add("user-2")
        >>> estimator.add("user-1")  # duplicate
        >>> count = estimator.estimate()  # ~2
    """

    def __init__(
        self,
        service_name: str,
        name: str,
        context: Optional[ProxyContext] = None,
    ):
        super().__init__(service_name, name, context)

    def add(self, value: Any) -> Future:
        """Add a value to the cardinality estimator.

        Args:
            value: The value to add. Will be serialized before adding.

        Returns:
            A Future that completes when the operation is done.

        Example:
            >>> estimator.add("user-123")
            >>> estimator.add(12345)
        """
        from hazelcast.protocol.builtin_codecs import CardinalityEstimatorCodec

        data = self._to_data(value)
        request = CardinalityEstimatorCodec.encode_add_request(self._name, data)
        return self._invoke(request)

    def add_async(self, value: Any) -> Future:
        """Add a value to the cardinality estimator asynchronously.

        Args:
            value: The value to add.

        Returns:
            A Future that completes when the operation is done.
        """
        return self.add(value)

    def estimate(self) -> Future:
        """Estimate the cardinality (number of distinct elements).

        Returns:
            A Future containing the estimated number of distinct elements
            that have been added to this estimator.

        Example:
            >>> future = estimator.estimate()
            >>> count = future.result()
            >>> print(f"Estimated distinct elements: {count}")
        """
        from hazelcast.protocol.builtin_codecs import CardinalityEstimatorCodec

        request = CardinalityEstimatorCodec.encode_estimate_request(self._name)
        return self._invoke(request, CardinalityEstimatorCodec.decode_estimate_response)

    def estimate_async(self) -> Future:
        """Estimate the cardinality asynchronously.

        Returns:
            A Future containing the estimated cardinality.
        """
        return self.estimate()


CardinalityEstimatorProxy = CardinalityEstimator
