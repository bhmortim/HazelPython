#!/usr/bin/env python3
"""Vector Collection usage example.

Demonstrates how to use Hazelcast's VectorCollection for storing
and searching vector embeddings (approximate nearest neighbor search).

Use cases:
- Semantic search applications
- Recommendation systems
- RAG (Retrieval Augmented Generation)
- Image/audio similarity search
"""

from hazelcast import HazelcastClient, ClientConfig


def main():
    # Configure and connect to Hazelcast
    config = ClientConfig()
    config.cluster_name = "dev"
    config.cluster_members = ["localhost:5701"]

    client = HazelcastClient(config)

    try:
        client.start()

        # Get or create a vector collection
        vectors = client.get_vector_collection("document-embeddings")

        # -----------------------------------------------------------------
        # Storing vectors with metadata
        # -----------------------------------------------------------------
        print("=== Storing Vectors ===")

        # Store document embeddings with metadata
        documents = [
            {
                "key": "doc-1",
                "vector": [0.1, 0.2, 0.3, 0.4, 0.5],
                "metadata": {"title": b"Introduction to Hazelcast"},
            },
            {
                "key": "doc-2",
                "vector": [0.15, 0.25, 0.32, 0.41, 0.48],
                "metadata": {"title": b"Distributed Computing Basics"},
            },
            {
                "key": "doc-3",
                "vector": [0.8, 0.1, 0.05, 0.02, 0.03],
                "metadata": {"title": b"Machine Learning Overview"},
            },
            {
                "key": "doc-4",
                "vector": [0.12, 0.22, 0.31, 0.42, 0.51],
                "metadata": {"title": b"Hazelcast Vector Search"},
            },
            {
                "key": "doc-5",
                "vector": [0.7, 0.15, 0.08, 0.04, 0.03],
                "metadata": {"title": b"Deep Learning Fundamentals"},
            },
        ]

        for doc in documents:
            vectors.put(doc["key"], doc["vector"], doc["metadata"])
            print(f"  Stored: {doc['key']}")

        print(f"\nCollection size: {vectors.size()}")

        # -----------------------------------------------------------------
        # Retrieving a specific vector
        # -----------------------------------------------------------------
        print("\n=== Retrieving Vectors ===")

        doc = vectors.get("doc-1")
        if doc:
            print(f"  Key: doc-1")
            print(f"  Vector: {doc.vector}")
            print(f"  Metadata: {doc.metadata}")

        # -----------------------------------------------------------------
        # Similarity search
        # -----------------------------------------------------------------
        print("\n=== Similarity Search ===")

        # Query vector (similar to Hazelcast-related documents)
        query_vector = [0.11, 0.21, 0.31, 0.41, 0.49]

        # Search for top 3 most similar vectors
        results = vectors.search(
            vector=query_vector,
            limit=3,
            include_vectors=False,
            include_metadata=True,
        )

        print(f"Query: {query_vector}")
        print(f"Top {len(results)} similar documents:")
        for i, result in enumerate(results, 1):
            print(f"  {i}. Key: {result.key}, Score: {result.score:.4f}")
            if result.metadata:
                print(f"     Metadata: {result.metadata}")

        # -----------------------------------------------------------------
        # Search with vectors included
        # -----------------------------------------------------------------
        print("\n=== Search with Vector Data ===")

        results_with_vectors = vectors.search(
            vector=query_vector,
            limit=2,
            include_vectors=True,
            include_metadata=True,
        )

        for result in results_with_vectors:
            print(f"  Key: {result.key}")
            print(f"  Score: {result.score:.4f}")
            if result.vector:
                print(f"  Vector: {result.vector}")

        # -----------------------------------------------------------------
        # Updating a vector
        # -----------------------------------------------------------------
        print("\n=== Updating Vectors ===")

        # Update doc-1 with a new embedding
        new_vector = [0.2, 0.3, 0.4, 0.5, 0.6]
        old_key = vectors.put("doc-1", new_vector, {"title": b"Updated Document"})
        print(f"  Updated doc-1 (previous key: {old_key})")

        # -----------------------------------------------------------------
        # Deleting vectors
        # -----------------------------------------------------------------
        print("\n=== Deleting Vectors ===")

        deleted = vectors.delete("doc-5")
        print(f"  Deleted doc-5: {deleted}")
        print(f"  Collection size after delete: {vectors.size()}")

        # -----------------------------------------------------------------
        # Optimize index (after bulk operations)
        # -----------------------------------------------------------------
        print("\n=== Optimizing Index ===")
        vectors.optimize()
        print("  Index optimized for better search performance")

        # -----------------------------------------------------------------
        # Async operations example
        # -----------------------------------------------------------------
        print("\n=== Async Operations ===")

        # Async put
        future = vectors.put_async("doc-async", [0.5, 0.5, 0.5, 0.5, 0.5])
        future.result()  # Wait for completion
        print("  Async put completed")

        # Async search
        search_future = vectors.search_async([0.5, 0.5, 0.5, 0.5, 0.5], limit=2)
        async_results = search_future.result()
        print(f"  Async search found {len(async_results)} results")

        # -----------------------------------------------------------------
        # Cleanup
        # -----------------------------------------------------------------
        print("\n=== Cleanup ===")
        vectors.clear()
        print(f"  Collection cleared. Size: {vectors.size()}")

    finally:
        client.shutdown()
        print("\nClient shutdown complete.")


if __name__ == "__main__":
    main()
