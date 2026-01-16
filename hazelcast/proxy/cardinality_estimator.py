"""CardinalityEstimator proxy for HyperLogLog-based cardinality estimation.

This module provides the CardinalityEstimator distributed data structure
for probabilistic cardinality counting using the HyperLogLog algorithm.

Classes:
    HyperLogLog: Local HyperLogLog implementation for cardinality estimation.
    CardinalityEstimator: Proxy for the distributed CardinalityEstimator.

Example:
    Basic cardinality estimation::

        estimator = client.get_cardinality_estimator("unique-visitors")

        # Add elements
        estimator.add("user-1")
        estimator.add("user-2")
        estimator.add("user-1")  # duplicate

        # Get estimated count
        count = estimator.estimate().result()  # ~2
"""

import hashlib
import math
from concurrent.futures import Future
from typing import Any, List, Optional, TYPE_CHECKING

from hazelcast.proxy.base import Proxy, ProxyContext

if TYPE_CHECKING:
    from hazelcast.protocol.client_message import ClientMessage

SERVICE_NAME_CARDINALITY_ESTIMATOR = "hz:impl:cardinalityEstimatorService"

DEFAULT_HLL_PRECISION = 14


class HyperLogLog:
    """Local HyperLogLog implementation for cardinality estimation.

    HyperLogLog is a probabilistic data structure used to estimate the
    cardinality (number of distinct elements) of a multiset. It provides
    a very memory-efficient way to count unique items with a typical
    standard error of about 1.04/sqrt(m).

    Args:
        precision: The precision parameter (p). Number of registers = 2^p.
            Higher precision means more accuracy but more memory.
            Valid range is 4-16, default is 14.

    Attributes:
        precision: The precision parameter.
        register_count: Number of registers (2^precision).

    Example:
        >>> hll = HyperLogLog(precision=14)
        >>> hll.add("item1")
        >>> hll.add("item2")
        >>> hll.add("item1")  # duplicate
        >>> count = hll.estimate()  # approximately 2
    """

    def __init__(self, precision: int = DEFAULT_HLL_PRECISION):
        """Initialize the HyperLogLog with given precision.

        Args:
            precision: The precision parameter (4-16). Default is 14.
        """
        if precision < 4 or precision > 16:
            raise ValueError("Precision must be between 4 and 16")

        self._precision = precision
        self._register_count = 1 << precision
        self._registers: List[int] = [0] * self._register_count
        self._alpha = self._get_alpha(self._register_count)

    @property
    def precision(self) -> int:
        """Get the precision parameter."""
        return self._precision

    @property
    def register_count(self) -> int:
        """Get the number of registers."""
        return self._register_count

    @staticmethod
    def _get_alpha(m: int) -> float:
        """Get the alpha constant for bias correction.

        Args:
            m: Number of registers.

        Returns:
            The alpha correction factor.
        """
        if m == 16:
            return 0.673
        elif m == 32:
            return 0.697
        elif m == 64:
            return 0.709
        else:
            return 0.7213 / (1 + 1.079 / m)

    def _hash(self, value: Any) -> int:
        """Hash a value to a 64-bit integer.

        Args:
            value: The value to hash.

        Returns:
            A 64-bit hash value.
        """
        if isinstance(value, bytes):
            data = value
        elif isinstance(value, str):
            data = value.encode("utf-8")
        else:
            data = str(value).encode("utf-8")

        hash_bytes = hashlib.sha256(data).digest()[:8]
        return int.from_bytes(hash_bytes, byteorder="big")

    @staticmethod
    def _count_leading_zeros(value: int, max_bits: int = 64) -> int:
        """Count leading zeros in a value.

        Args:
            value: The integer value.
            max_bits: Maximum number of bits to consider.

        Returns:
            Number of leading zeros.
        """
        if value == 0:
            return max_bits

        count = 0
        for i in range(max_bits - 1, -1, -1):
            if (value >> i) & 1:
                break
            count += 1
        return count

    def add(self, value: Any) -> None:
        """Add a value to the HyperLogLog.

        Args:
            value: The value to add.
        """
        hash_value = self._hash(value)

        register_index = hash_value & (self._register_count - 1)

        remaining_bits = hash_value >> self._precision
        rank = self._count_leading_zeros(remaining_bits, 64 - self._precision) + 1

        if rank > self._registers[register_index]:
            self._registers[register_index] = rank

    def estimate(self) -> int:
        """Estimate the cardinality.

        Returns:
            The estimated number of distinct elements.
        """
        indicator = sum(2.0 ** (-r) for r in self._registers)
        raw_estimate = self._alpha * (self._register_count ** 2) / indicator

        if raw_estimate <= 2.5 * self._register_count:
            zeros = self._registers.count(0)
            if zeros > 0:
                raw_estimate = self._register_count * math.log(self._register_count / zeros)

        if raw_estimate > (1 << 32) / 30.0:
            raw_estimate = -(1 << 32) * math.log(1 - raw_estimate / (1 << 32))

        return int(round(raw_estimate))

    def merge(self, other: "HyperLogLog") -> None:
        """Merge another HyperLogLog into this one.

        Args:
            other: Another HyperLogLog to merge.

        Raises:
            ValueError: If precisions don't match.
        """
        if self._precision != other._precision:
            raise ValueError("Cannot merge HyperLogLogs with different precisions")

        for i in range(self._register_count):
            if other._registers[i] > self._registers[i]:
                self._registers[i] = other._registers[i]

    def clear(self) -> None:
        """Clear all registers."""
        self._registers = [0] * self._register_count


class CardinalityEstimator(Proxy):
    """Proxy for distributed CardinalityEstimator.

    CardinalityEstimator is a distributed data structure that uses the
    HyperLogLog algorithm to estimate the number of distinct elements
    in a dataset. It provides probabilistic counting with low memory
    overhead.

    The standard error is typically around 1.04/sqrt(m) where m is the
    number of registers used internally (default 2^14 = 16384 registers).

    Use Cases:
        - Counting unique website visitors
        - Counting distinct search queries
        - Estimating unique items in large datasets
        - Any scenario requiring approximate distinct counts

    Attributes:
        name: The name of this cardinality estimator.

    Example:
        Basic usage::

            estimator = client.get_cardinality_estimator("visitors")

            # Add elements (duplicates are handled)
            estimator.add("user-1")
            estimator.add("user-2")
            estimator.add("user-1")  # duplicate

            # Get estimate
            count = estimator.estimate().result()  # ~2

    Note:
        CardinalityEstimator provides probabilistic results. The actual
        count may differ slightly from the estimated value.
    """

    SERVICE_NAME = SERVICE_NAME_CARDINALITY_ESTIMATOR

    def __init__(
        self,
        service_name: str,
        name: str,
        context: Optional[ProxyContext] = None,
    ):
        """Initialize the CardinalityEstimator proxy.

        Args:
            service_name: The service name for this proxy.
            name: The name of the distributed cardinality estimator.
            context: The proxy context for service access.
        """
        super().__init__(service_name, name, context)
        self._local_hll = HyperLogLog(DEFAULT_HLL_PRECISION)

    def add(self, value: Any) -> Future:
        """Add a value to the cardinality estimator.

        Adds the specified value to the estimator. Duplicate values
        will not increase the estimated cardinality.

        Args:
            value: The value to add. Will be serialized before adding.

        Returns:
            A Future that completes when the operation is done.

        Raises:
            IllegalStateException: If the estimator has been destroyed.

        Example:
            >>> estimator.add("user-123")
            >>> estimator.add(12345)
            >>> estimator.add({"key": "value"})
        """
        self._check_not_destroyed()

        self._local_hll.add(value)

        if self._context is None or self._context.invocation_service is None:
            future: Future = Future()
            future.set_result(None)
            return future

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

    def add_all(self, values: List[Any]) -> Future:
        """Add multiple values to the cardinality estimator.

        Args:
            values: The values to add.

        Returns:
            A Future that completes when all values are added.

        Example:
            >>> estimator.add_all(["user-1", "user-2", "user-3"])
        """
        self._check_not_destroyed()

        for value in values:
            self._local_hll.add(value)

        if self._context is None or self._context.invocation_service is None:
            future: Future = Future()
            future.set_result(None)
            return future

        last_future: Future = Future()
        last_future.set_result(None)

        for value in values:
            last_future = self.add(value)

        return last_future

    def estimate(self) -> Future:
        """Estimate the cardinality (number of distinct elements).

        Returns the estimated number of distinct elements that have
        been added to this estimator. The estimate uses the HyperLogLog
        algorithm which provides probabilistic results.

        Returns:
            A Future containing the estimated number of distinct elements.

        Raises:
            IllegalStateException: If the estimator has been destroyed.

        Example:
            >>> future = estimator.estimate()
            >>> count = future.result()
            >>> print(f"Estimated distinct elements: {count}")
        """
        self._check_not_destroyed()

        if self._context is None or self._context.invocation_service is None:
            future: Future = Future()
            future.set_result(self._local_hll.estimate())
            return future

        from hazelcast.protocol.builtin_codecs import CardinalityEstimatorCodec

        request = CardinalityEstimatorCodec.encode_estimate_request(self._name)
        return self._invoke(request, CardinalityEstimatorCodec.decode_estimate_response)

    def estimate_async(self) -> Future:
        """Estimate the cardinality asynchronously.

        Returns:
            A Future containing the estimated cardinality.
        """
        return self.estimate()

    def get_local_estimate(self) -> int:
        """Get the local cardinality estimate.

        Returns the cardinality estimate based only on values added
        through this proxy instance, without querying the cluster.

        Returns:
            The local cardinality estimate.

        Example:
            >>> local_count = estimator.get_local_estimate()
        """
        return self._local_hll.estimate()

    def _on_destroy(self) -> None:
        """Called when the proxy is destroyed."""
        self._local_hll.clear()


CardinalityEstimatorProxy = CardinalityEstimator
