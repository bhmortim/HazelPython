"""Jet Pipeline Example.

This example demonstrates how to build and submit Jet streaming/batch
processing pipelines using the Hazelcast Jet API.

Jet enables distributed stream and batch processing with a simple
pipeline API for transformations like map, filter, and aggregate.

Prerequisites:
    - A running Hazelcast cluster with Jet enabled

Usage:
    python jet_pipeline.py
"""

from hazelcast import HazelcastClient, ClientConfig
from hazelcast.jet import (
    JetService,
    Pipeline,
    Stage,
    Source,
    Sink,
    Job,
    JobConfig,
    JobStatus,
)
from hazelcast.jet.pipeline import (
    BatchSource,
    MapSource,
    ListSink,
    MapSink,
)


def basic_pipeline_example():
    """Create a simple transformation pipeline."""
    print("\n" + "=" * 50)
    print("Basic Pipeline Example")
    print("=" * 50)

    # Create a pipeline
    pipeline = Pipeline.create()
    print(f"Created pipeline: {pipeline.id[:8]}...")

    # Create a batch source with sample data
    numbers = list(range(1, 11))  # [1, 2, 3, ..., 10]
    source = Pipeline.from_list("numbers", numbers)
    print(f"Source: {source.name} with {len(numbers)} items")

    # Create a sink to collect results
    sink = Pipeline.to_list("results")
    print(f"Sink: {sink.name}")

    # Build the pipeline: source -> double -> filter evens -> sink
    stage = pipeline.read_from(source)
    stage = stage.map(lambda x: x * 2)       # Double each number
    stage = stage.filter(lambda x: x > 10)   # Keep only > 10
    pipeline = stage.write_to(sink)

    print("\nPipeline structure:")
    print("  source (1-10) -> map(x*2) -> filter(>10) -> sink")
    print(f"  Expected output: [12, 14, 16, 18, 20]")

    return pipeline


def map_transformation_example():
    """Demonstrate various map transformations."""
    print("\n" + "=" * 50)
    print("Map Transformation Example")
    print("=" * 50)

    pipeline = Pipeline.create()

    # Sample data: list of user dictionaries
    users = [
        {"name": "Alice", "age": 30, "city": "NYC"},
        {"name": "Bob", "age": 25, "city": "LA"},
        {"name": "Charlie", "age": 35, "city": "Chicago"},
    ]

    source = Pipeline.from_list("users", users)
    sink = Pipeline.to_list("user-ages")

    # Extract just the ages
    stage = pipeline.read_from(source)
    stage = stage.map(lambda user: user["age"])
    pipeline = stage.write_to(sink)

    print("Pipeline: users -> map(get age) -> ages")
    print(f"Input: {users}")
    print("Expected output: [30, 25, 35]")

    return pipeline


def filter_example():
    """Demonstrate filter operations."""
    print("\n" + "=" * 50)
    print("Filter Example")
    print("=" * 50)

    pipeline = Pipeline.create()

    # Sample data
    products = [
        {"name": "Laptop", "price": 999, "category": "Electronics"},
        {"name": "Book", "price": 29, "category": "Books"},
        {"name": "Phone", "price": 699, "category": "Electronics"},
        {"name": "Desk", "price": 199, "category": "Furniture"},
        {"name": "Tablet", "price": 449, "category": "Electronics"},
    ]

    source = Pipeline.from_list("products", products)
    sink = Pipeline.to_list("expensive-electronics")

    # Filter for expensive electronics
    stage = pipeline.read_from(source)
    stage = stage.filter(lambda p: p["category"] == "Electronics")
    stage = stage.filter(lambda p: p["price"] > 500)
    stage = stage.map(lambda p: p["name"])
    pipeline = stage.write_to(sink)

    print("Pipeline: products -> filter(Electronics) -> filter(price>500) -> names")
    print(f"Input: {len(products)} products")
    print("Expected output: ['Laptop', 'Phone']")

    return pipeline


def map_source_sink_example():
    """Demonstrate reading from and writing to IMaps."""
    print("\n" + "=" * 50)
    print("Map Source/Sink Example")
    print("=" * 50)

    pipeline = Pipeline.create()

    # Read from an IMap
    source = Pipeline.from_map("input-map")
    print(f"Source: {source.name} (IMap)")

    # Write to another IMap
    sink = Pipeline.to_map("output-map")
    print(f"Sink: {sink.name} (IMap)")

    # Transform key-value pairs
    stage = pipeline.read_from(source)
    # Map entries are (key, value) tuples
    stage = stage.map(lambda entry: (entry[0], entry[1].upper() if isinstance(entry[1], str) else entry[1]))
    pipeline = stage.write_to(sink)

    print("\nPipeline: input-map -> transform values -> output-map")
    print("This pipeline reads from 'input-map' and writes transformed values to 'output-map'")

    return pipeline


def job_submission_example(jet_service: JetService):
    """Demonstrate job submission and management."""
    print("\n" + "=" * 50)
    print("Job Submission Example")
    print("=" * 50)

    # Create a simple pipeline
    pipeline = Pipeline.create()
    source = Pipeline.from_list("data", [1, 2, 3, 4, 5])
    sink = Pipeline.to_list("output")
    pipeline.read_from(source).map(lambda x: x * x).write_to(sink)

    # Configure the job
    config = JobConfig()
    config.name = "square-numbers-job"
    config.split_brain_protection_enabled = False

    print(f"Job configuration:")
    print(f"  Name: {config.name}")
    print(f"  Auto-scaling: {config.auto_scaling_enabled}")

    # Submit the job
    print("\nSubmitting job...")
    job = jet_service.submit(pipeline, config)

    print(f"Job submitted:")
    print(f"  ID: {job.id}")
    print(f"  Name: {job.name}")
    print(f"  Status: {job.status.value}")
    print(f"  Submission time: {job.submission_time}")

    return job


def job_lifecycle_example(jet_service: JetService):
    """Demonstrate job lifecycle management."""
    print("\n" + "=" * 50)
    print("Job Lifecycle Example")
    print("=" * 50)

    # Submit a job
    pipeline = Pipeline.create()
    source = Pipeline.from_list("stream", list(range(100)))
    sink = Pipeline.to_list("results")
    pipeline.read_from(source).map(lambda x: x + 1).write_to(sink)

    config = JobConfig()
    config.name = "lifecycle-demo-job"

    job = jet_service.submit(pipeline, config)
    print(f"Submitted job: {job.name} (ID: {job.id})")

    # Check status
    print(f"\nInitial status: {job.status.value}")

    # Suspend the job
    print("\nSuspending job...")
    job.suspend()
    print(f"Status after suspend: {job.status.value}")

    # Resume the job
    print("\nResuming job...")
    job.resume()
    print(f"Status after resume: {job.status.value}")

    # Restart the job
    print("\nRestarting job...")
    job.restart()
    print(f"Status after restart: {job.status.value}")

    # Cancel the job
    print("\nCancelling job...")
    job.cancel()
    print(f"Status after cancel: {job.status.value}")


def job_retrieval_example(jet_service: JetService):
    """Demonstrate retrieving jobs."""
    print("\n" + "=" * 50)
    print("Job Retrieval Example")
    print("=" * 50)

    # Submit a few jobs
    for i in range(3):
        pipeline = Pipeline.create()
        source = Pipeline.from_list(f"source-{i}", [i])
        sink = Pipeline.to_list(f"sink-{i}")
        pipeline.read_from(source).write_to(sink)

        config = JobConfig()
        config.name = f"batch-job-{i}"
        jet_service.submit(pipeline, config)

    # Get all jobs
    print("\nAll jobs:")
    all_jobs = jet_service.get_jobs()
    for job in all_jobs:
        print(f"  {job.id}: {job.name} ({job.status.value})")

    # Get job by ID
    if all_jobs:
        first_job = all_jobs[0]
        retrieved = jet_service.get_job(first_job.id)
        print(f"\nRetrieved by ID ({first_job.id}): {retrieved.name if retrieved else 'None'}")

    # Get job by name
    job_by_name = jet_service.get_job_by_name("batch-job-1")
    print(f"Retrieved by name 'batch-job-1': {job_by_name.id if job_by_name else 'None'}")


def new_job_if_absent_example(jet_service: JetService):
    """Demonstrate submitting a job only if it doesn't exist."""
    print("\n" + "=" * 50)
    print("New Job If Absent Example")
    print("=" * 50)

    pipeline = Pipeline.create()
    source = Pipeline.from_list("singleton-source", [1, 2, 3])
    sink = Pipeline.to_list("singleton-sink")
    pipeline.read_from(source).write_to(sink)

    config = JobConfig()
    config.name = "singleton-job"

    # First submission
    print("First submission...")
    job1 = jet_service.new_job_if_absent(pipeline, config)
    print(f"  Job ID: {job1.id}")

    # Second submission (should return existing)
    print("\nSecond submission (should return existing)...")
    job2 = jet_service.new_job_if_absent(pipeline, config)
    print(f"  Job ID: {job2.id}")
    print(f"  Same job? {job1.id == job2.id}")


def main():
    print("Jet Pipeline Examples")
    print("=" * 50)
    print("""
Jet is Hazelcast's distributed stream/batch processing engine.
These examples demonstrate pipeline construction and job management.
    """)

    # Create and start Jet service (standalone demo)
    jet_service = JetService()
    jet_service.start()
    print(f"Jet service running: {jet_service.is_running}")

    # Pipeline construction examples
    basic_pipeline_example()
    map_transformation_example()
    filter_example()
    map_source_sink_example()

    # Job management examples
    job_submission_example(jet_service)
    job_lifecycle_example(jet_service)
    job_retrieval_example(jet_service)
    new_job_if_absent_example(jet_service)

    # Shutdown
    jet_service.shutdown()

    print("\n" + "=" * 50)
    print("Integration with HazelcastClient")
    print("=" * 50)
    print("""
To use Jet with a real Hazelcast cluster:

    config = ClientConfig()
    config.cluster_name = "dev"

    with HazelcastClient(config) as client:
        jet = client.get_jet()
        
        # Build pipeline
        pipeline = Pipeline.create()
        source = Pipeline.from_map("input-map")
        sink = Pipeline.to_map("output-map")
        
        pipeline.read_from(source) \\
            .map(lambda e: (e[0], e[1] * 2)) \\
            .write_to(sink)
        
        # Submit job
        config = JobConfig()
        config.name = "my-job"
        
        job = jet.submit(pipeline, config)
        print(f"Job status: {job.status}")
    """)


if __name__ == "__main__":
    main()
