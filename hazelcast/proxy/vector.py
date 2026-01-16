"""Vector Collection proxy implementation."""

from typing import Any, Dict, List, Optional, TYPE_CHECKING

from hazelcast.proxy.base import Proxy
from hazelcast.protocol.codec import VectorCodec, VectorDocument, VectorSearchResult
from hazelcast.future import Future

if TYPE_CHECKING:
    from hazelcast.proxy.base import ProxyContext


class VectorCollection(Proxy):
    """Distributed Vector Collection for similarity search.

    VectorCollection stores vectors (embeddings) with associated keys and
    optional metadata. It supports efficient approximate nearest neighbor
    search for finding similar vectors.

    This data structure is ideal for:
    - Semantic search applications
    - Recommendation systems
    - Image/audio similarity search
    - RAG (Retrieval Augmented Generation) applications

    Example:
        >>> vc = client.get_vector_collection("embeddings")
        >>> # Store a vector
        >>> vc.put("doc-1", [0.1, 0.2, 0.3, 0.4], {"title": b"My Document"})
        >>> # Search for similar vectors
        >>> results = vc.search([0.15, 0.25, 0.35, 0.45], limit=10)
        >>> for result in results:
        ...     print(f"Score: {result.score}, Key: {result.key}")
    """

    def __init__(self, service_name: str, name: str, context: "ProxyContext"):
        """Initialize the VectorCollection proxy.

        Args:
            service_name: The service name for this distributed object.
            name: The name of the vector collection.
            context: The proxy context providing access to client services.
        """
        super().__init__(service_name, name, context)

    def put(
        self,
        key: Any,
        vector: List[float],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[Any]:
        """Store a vector with the given key.

        If a vector with the same key already exists, it will be replaced.

        Args:
            key: The unique identifier for the vector.
            vector: The vector data as a list of floats.
            metadata: Optional dictionary of metadata key-value pairs.
                Values will be serialized using the configured serializer.

        Returns:
            The previous key if it existed, None otherwise.

        Example:
            >>> vc.put("user-embedding-123", [0.1, 0.2, 0.3], {"user_id": b"123"})
        """
        return self.put_async(key, vector, metadata).result()

    def put_async(
        self,
        key: Any,
        vector: List[float],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Future:
        """Asynchronously store a vector with the given key.

        Args:
            key: The unique identifier for the vector.
            vector: The vector data as a list of floats.
            metadata: Optional dictionary of metadata key-value pairs.

        Returns:
            A Future that resolves to the previous key if it existed.
        """
        key_data = self._to_data(key)
        metadata_data = None
        if metadata:
            metadata_data = {k: self._to_data(v) for k, v in metadata.items()}

        request = VectorCodec.encode_put_request(self._name, key_data, vector, metadata_data)

        def decode_response(msg):
            result = VectorCodec.decode_put_response(msg)
            return self._to_object(result) if result else None

        return self._invoke(request, decode_response)

    def get(self, key: Any) -> Optional[VectorDocument]:
        """Retrieve a vector by its key.

        Args:
            key: The unique identifier for the vector.

        Returns:
            A VectorDocument containing the vector and metadata,
            or None if not found.

        Example:
            >>> doc = vc.get("user-embedding-123")
            >>> if doc:
            ...     print(f"Vector: {doc.vector}")
            ...     print(f"Metadata: {doc.metadata}")
        """
        return self.get_async(key).result()

    def get_async(self, key: Any) -> Future:
        """Asynchronously retrieve a vector by its key.

        Args:
            key: The unique identifier for the vector.

        Returns:
            A Future that resolves to a VectorDocument or None.
        """
        key_data = self._to_data(key)
        request = VectorCodec.encode_get_request(self._name, key_data)

        def decode_response(msg):
            doc = VectorCodec.decode_get_response(msg)
            if doc:
                doc.key = self._to_object(doc.key)
                doc.metadata = {k: self._to_object(v) for k, v in doc.metadata.items()}
            return doc

        return self._invoke(request, decode_response)

    def search(
        self,
        vector: List[float],
        limit: int = 10,
        include_vectors: bool = False,
        include_metadata: bool = True,
    ) -> List[VectorSearchResult]:
        """Search for vectors similar to the given query vector.

        Performs approximate nearest neighbor search to find vectors
        most similar to the query vector.

        Args:
            vector: The query vector to search for similar vectors.
            limit: Maximum number of results to return (default: 10).
            include_vectors: Whether to include vector data in results
                (default: False for performance).
            include_metadata: Whether to include metadata in results
                (default: True).

        Returns:
            A list of VectorSearchResult objects sorted by similarity
            score (highest first).

        Example:
            >>> results = vc.search([0.1, 0.2, 0.3], limit=5)
            >>> for result in results:
            ...     print(f"Key: {result.key}, Score: {result.score:.4f}")
        """
        return self.search_async(vector, limit, include_vectors, include_metadata).result()

    def search_async(
        self,
        vector: List[float],
        limit: int = 10,
        include_vectors: bool = False,
        include_metadata: bool = True,
    ) -> Future:
        """Asynchronously search for similar vectors.

        Args:
            vector: The query vector.
            limit: Maximum number of results.
            include_vectors: Whether to include vector data.
            include_metadata: Whether to include metadata.

        Returns:
            A Future that resolves to a list of VectorSearchResult objects.
        """
        request = VectorCodec.encode_search_request(
            self._name, vector, limit, include_vectors, include_metadata
        )

        def decode_response(msg):
            results = VectorCodec.decode_search_response(msg)
            for result in results:
                result.key = self._to_object(result.key)
                result.metadata = {k: self._to_object(v) for k, v in result.metadata.items()}
            return results

        return self._invoke(request, decode_response)

    def delete(self, key: Any) -> bool:
        """Delete a vector by its key.

        Args:
            key: The unique identifier of the vector to delete.

        Returns:
            True if the vector was deleted, False if it didn't exist.

        Example:
            >>> deleted = vc.delete("user-embedding-123")
            >>> print(f"Was deleted: {deleted}")
        """
        return self.delete_async(key).result()

    def delete_async(self, key: Any) -> Future:
        """Asynchronously delete a vector by its key.

        Args:
            key: The unique identifier of the vector to delete.

        Returns:
            A Future that resolves to True if deleted, False otherwise.
        """
        key_data = self._to_data(key)
        request = VectorCodec.encode_delete_request(self._name, key_data)

        def decode_response(msg):
            return VectorCodec.decode_delete_response(msg)

        return self._invoke(request, decode_response)

    def size(self) -> int:
        """Get the number of vectors in the collection.

        Returns:
            The number of vectors stored in the collection.

        Example:
            >>> count = vc.size()
            >>> print(f"Collection has {count} vectors")
        """
        return self.size_async().result()

    def size_async(self) -> Future:
        """Asynchronously get the size of the collection.

        Returns:
            A Future that resolves to the number of vectors.
        """
        request = VectorCodec.encode_size_request(self._name)

        def decode_response(msg):
            return VectorCodec.decode_size_response(msg)

        return self._invoke(request, decode_response)

    def clear(self) -> None:
        """Remove all vectors from the collection.

        Example:
            >>> vc.clear()
            >>> assert vc.size() == 0
        """
        return self.clear_async().result()

    def clear_async(self) -> Future:
        """Asynchronously clear all vectors from the collection.

        Returns:
            A Future that completes when the operation is done.
        """
        request = VectorCodec.encode_clear_request(self._name)
        return self._invoke(request)

    def optimize(self) -> None:
        """Optimize the vector index for better search performance.

        This operation rebuilds the internal index structure to improve
        search accuracy and performance. It should be called after
        bulk insertions or deletions.

        Note:
            This is a potentially expensive operation and should not
            be called frequently during normal operations.

        Example:
            >>> # After bulk insertion
            >>> for i in range(10000):
            ...     vc.put(f"doc-{i}", vectors[i])
            >>> vc.optimize()  # Rebuild index for better performance
        """
        return self.optimize_async().result()

    def optimize_async(self) -> Future:
        """Asynchronously optimize the vector index.

        Returns:
            A Future that completes when optimization is done.
        """
        request = VectorCodec.encode_optimize_request(self._name)
        return self._invoke(request)

    def _to_data(self, obj: Any) -> bytes:
        """Serialize an object to bytes."""
        if obj is None:
            return b""
        if isinstance(obj, bytes):
            return obj
        return self._context.serialization_service.to_data(obj)

    def _to_object(self, data: bytes) -> Any:
        """Deserialize bytes to an object."""
        if not data:
            return None
        return self._context.serialization_service.to_object(data)

    def _invoke(self, request, response_decoder=None) -> Future:
        """Invoke a request and return a future."""
        future = Future()

        def handle_response(response):
            try:
                if response_decoder:
                    result = response_decoder(response)
                else:
                    result = None
                future.set_result(result)
            except Exception as e:
                future.set_exception(e)

        try:
            self._context.invocation_service.invoke(request, handle_response)
        except Exception as e:
            future.set_exception(e)

        return future
