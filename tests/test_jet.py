"""Unit tests for hazelcast/jet/service.py"""

import unittest
from unittest.mock import MagicMock, patch
from concurrent.futures import Future

from hazelcast.jet.service import JetService, JobStateListener
from hazelcast.jet.job import JobStatus, ProcessingGuarantee, JobConfig
from hazelcast.exceptions import IllegalArgumentException, IllegalStateException


class MockPipeline:
    def __init__(self, complete=True):
        self._complete = complete

    def is_complete(self):
        return self._complete


class TestJobStateListener(unittest.TestCase):
    def test_on_state_changed(self):
        listener = JobStateListener()
        mock_job = MagicMock()
        listener.on_state_changed(mock_job, JobStatus.RUNNING, JobStatus.COMPLETED)

    def test_on_job_completed(self):
        listener = JobStateListener()
        mock_job = MagicMock()
        listener.on_job_completed(mock_job)

    def test_on_job_failed(self):
        listener = JobStateListener()
        mock_job = MagicMock()
        listener.on_job_failed(mock_job, "Error message")


class TestJetService(unittest.TestCase):
    def test_init(self):
        service = JetService()
        self.assertFalse(service.is_running)
        self.assertIsNone(service._invocation_service)

    def test_init_with_services(self):
        inv = MagicMock()
        ser = MagicMock()
        service = JetService(invocation_service=inv, serialization_service=ser)
        self.assertEqual(service._invocation_service, inv)
        self.assertEqual(service._serialization_service, ser)

    def test_start(self):
        service = JetService()
        service.start()
        self.assertTrue(service.is_running)

    def test_shutdown(self):
        service = JetService()
        service.start()
        service.shutdown()
        self.assertFalse(service.is_running)

    def test_shutdown_clears_listeners(self):
        service = JetService()
        service.start()
        listener = JobStateListener()
        service.add_job_state_listener(listener)
        service.shutdown()
        self.assertEqual(len(service._state_listeners), 0)

    def test_is_running_property(self):
        service = JetService()
        self.assertFalse(service.is_running)
        service.start()
        self.assertTrue(service.is_running)

    def test_add_job_state_listener(self):
        service = JetService()
        listener = JobStateListener()
        reg_id = service.add_job_state_listener(listener)
        self.assertIsInstance(reg_id, str)
        self.assertIn("job-state", reg_id)

    def test_remove_job_state_listener(self):
        service = JetService()
        listener = JobStateListener()
        reg_id = service.add_job_state_listener(listener)
        result = service.remove_job_state_listener(reg_id)
        self.assertTrue(result)

    def test_remove_job_state_listener_not_found(self):
        service = JetService()
        result = service.remove_job_state_listener("unknown-id")
        self.assertFalse(result)

    def test_fire_state_change(self):
        service = JetService()
        listener = MagicMock()
        service.add_job_state_listener(listener)
        mock_job = MagicMock()
        service._fire_state_change(mock_job, JobStatus.STARTING, JobStatus.RUNNING)
        listener.on_state_changed.assert_called_once_with(mock_job, JobStatus.STARTING, JobStatus.RUNNING)

    def test_fire_state_change_completed(self):
        service = JetService()
        listener = MagicMock()
        service.add_job_state_listener(listener)
        mock_job = MagicMock()
        service._fire_state_change(mock_job, JobStatus.RUNNING, JobStatus.COMPLETED)
        listener.on_job_completed.assert_called_once_with(mock_job)

    def test_fire_state_change_failed(self):
        service = JetService()
        listener = MagicMock()
        service.add_job_state_listener(listener)
        mock_job = MagicMock()
        mock_job.failure_reason = "Test failure"
        service._fire_state_change(mock_job, JobStatus.RUNNING, JobStatus.FAILED)
        listener.on_job_failed.assert_called_once_with(mock_job, "Test failure")

    def test_fire_state_change_listener_exception(self):
        service = JetService()
        listener = MagicMock()
        listener.on_state_changed.side_effect = Exception("Listener error")
        service.add_job_state_listener(listener)
        mock_job = MagicMock()
        service._fire_state_change(mock_job, JobStatus.STARTING, JobStatus.RUNNING)

    def test_submit_not_running(self):
        service = JetService()
        pipeline = MockPipeline()
        with self.assertRaises(IllegalStateException):
            service.submit(pipeline)

    def test_submit_pipeline_none(self):
        service = JetService()
        service.start()
        with self.assertRaises(IllegalArgumentException):
            service.submit(None)

    def test_submit_pipeline_incomplete(self):
        service = JetService()
        service.start()
        pipeline = MockPipeline(complete=False)
        with self.assertRaises(IllegalArgumentException):
            service.submit(pipeline)

    def test_submit_success(self):
        service = JetService()
        service.start()
        pipeline = MockPipeline()
        job = service.submit(pipeline)
        self.assertIsNotNone(job)

    def test_submit_async_not_running(self):
        service = JetService()
        pipeline = MockPipeline()
        future = service.submit_async(pipeline)
        self.assertIsInstance(future, Future)
        with self.assertRaises(IllegalStateException):
            future.result()

    def test_submit_async_pipeline_none(self):
        service = JetService()
        service.start()
        future = service.submit_async(None)
        with self.assertRaises(IllegalArgumentException):
            future.result()

    def test_submit_async_pipeline_incomplete(self):
        service = JetService()
        service.start()
        pipeline = MockPipeline(complete=False)
        future = service.submit_async(pipeline)
        with self.assertRaises(IllegalArgumentException):
            future.result()

    def test_submit_async_success(self):
        service = JetService()
        service.start()
        pipeline = MockPipeline()
        future = service.submit_async(pipeline)
        job = future.result()
        self.assertIsNotNone(job)

    def test_new_job(self):
        service = JetService()
        service.start()
        pipeline = MockPipeline()
        job = service.new_job(pipeline)
        self.assertIsNotNone(job)

    def test_new_job_async(self):
        service = JetService()
        service.start()
        pipeline = MockPipeline()
        future = service.new_job_async(pipeline)
        job = future.result()
        self.assertIsNotNone(job)

    def test_new_job_if_absent_no_name_raises(self):
        service = JetService()
        service.start()
        pipeline = MockPipeline()
        config = JobConfig()
        with self.assertRaises(ValueError):
            service.new_job_if_absent(pipeline, config)

    def test_new_job_if_absent_creates_new(self):
        service = JetService()
        service.start()
        pipeline = MockPipeline()
        config = JobConfig(name="test-job")
        job = service.new_job_if_absent(pipeline, config)
        self.assertIsNotNone(job)

    def test_new_job_if_absent_returns_existing(self):
        service = JetService()
        service.start()
        pipeline = MockPipeline()
        config = JobConfig(name="test-job")
        job1 = service.new_job_if_absent(pipeline, config)
        job2 = service.new_job_if_absent(pipeline, config)
        self.assertIs(job1, job2)

    def test_new_job_if_absent_async_no_name(self):
        service = JetService()
        service.start()
        pipeline = MockPipeline()
        config = JobConfig()
        future = service.new_job_if_absent_async(pipeline, config)
        with self.assertRaises(ValueError):
            future.result()

    def test_new_job_if_absent_async_not_running(self):
        service = JetService()
        pipeline = MockPipeline()
        config = JobConfig(name="test")
        future = service.new_job_if_absent_async(pipeline, config)
        with self.assertRaises(IllegalStateException):
            future.result()

    def test_get_job_found(self):
        service = JetService()
        service.start()
        pipeline = MockPipeline()
        job = service.submit(pipeline)
        found = service.get_job(job.id)
        self.assertEqual(found, job)

    def test_get_job_not_found(self):
        service = JetService()
        service.start()
        result = service.get_job(999)
        self.assertIsNone(result)

    def test_get_job_async(self):
        service = JetService()
        service.start()
        pipeline = MockPipeline()
        job = service.submit(pipeline)
        future = service.get_job_async(job.id)
        found = future.result()
        self.assertEqual(found, job)

    def test_get_job_by_name_found(self):
        service = JetService()
        service.start()
        pipeline = MockPipeline()
        config = JobConfig(name="named-job")
        job = service.submit(pipeline, config)
        found = service.get_job_by_name("named-job")
        self.assertEqual(found, job)

    def test_get_job_by_name_not_found(self):
        service = JetService()
        service.start()
        result = service.get_job_by_name("nonexistent")
        self.assertIsNone(result)

    def test_get_job_by_name_async(self):
        service = JetService()
        service.start()
        pipeline = MockPipeline()
        config = JobConfig(name="async-job")
        job = service.submit(pipeline, config)
        future = service.get_job_by_name_async("async-job")
        found = future.result()
        self.assertEqual(found, job)

    def test_get_jobs(self):
        service = JetService()
        service.start()
        pipeline = MockPipeline()
        service.submit(pipeline)
        service.submit(pipeline)
        jobs = service.get_jobs()
        self.assertEqual(len(jobs), 2)

    def test_get_jobs_empty(self):
        service = JetService()
        service.start()
        jobs = service.get_jobs()
        self.assertEqual(jobs, [])

    def test_get_jobs_async(self):
        service = JetService()
        service.start()
        pipeline = MockPipeline()
        service.submit(pipeline)
        future = service.get_jobs_async()
        jobs = future.result()
        self.assertEqual(len(jobs), 1)

    def test_get_jobs_by_name(self):
        service = JetService()
        service.start()
        pipeline = MockPipeline()
        config = JobConfig(name="filter-test")
        service.submit(pipeline, config)
        jobs = service.get_jobs_by_name("filter-test")
        self.assertEqual(len(jobs), 1)

    def test_get_jobs_by_name_not_found(self):
        service = JetService()
        service.start()
        jobs = service.get_jobs_by_name("nonexistent")
        self.assertEqual(jobs, [])

    def test_get_jobs_by_name_async(self):
        service = JetService()
        service.start()
        pipeline = MockPipeline()
        config = JobConfig(name="async-filter")
        service.submit(pipeline, config)
        future = service.get_jobs_by_name_async("async-filter")
        jobs = future.result()
        self.assertEqual(len(jobs), 1)

    def test_new_light_job_not_running(self):
        service = JetService()
        pipeline = MockPipeline()
        with self.assertRaises(IllegalStateException):
            service.new_light_job(pipeline)

    def test_new_light_job_pipeline_none(self):
        service = JetService()
        service.start()
        with self.assertRaises(IllegalArgumentException):
            service.new_light_job(None)

    def test_new_light_job_pipeline_incomplete(self):
        service = JetService()
        service.start()
        pipeline = MockPipeline(complete=False)
        with self.assertRaises(IllegalArgumentException):
            service.new_light_job(pipeline)

    def test_new_light_job_success(self):
        service = JetService()
        service.start()
        pipeline = MockPipeline()
        job = service.new_light_job(pipeline)
        self.assertIsNotNone(job)
        self.assertTrue(job._light_job)

    def test_new_light_job_async(self):
        service = JetService()
        service.start()
        pipeline = MockPipeline()
        future = service.new_light_job_async(pipeline)
        job = future.result()
        self.assertTrue(job._light_job)

    def test_get_active_jobs(self):
        service = JetService()
        service.start()
        pipeline = MockPipeline()
        job = service.submit(pipeline)
        active = service.get_active_jobs()
        self.assertIn(job, active)

    def test_get_active_jobs_async(self):
        service = JetService()
        service.start()
        pipeline = MockPipeline()
        service.submit(pipeline)
        future = service.get_active_jobs_async()
        jobs = future.result()
        self.assertEqual(len(jobs), 1)

    def test_resume_job_not_found(self):
        service = JetService()
        service.start()
        with self.assertRaises(IllegalArgumentException):
            service.resume_job(999)

    def test_resume_job_async_not_found(self):
        service = JetService()
        service.start()
        future = service.resume_job_async(999)
        with self.assertRaises(IllegalArgumentException):
            future.result()

    def test_repr(self):
        service = JetService()
        service.start()
        pipeline = MockPipeline()
        service.submit(pipeline)
        result = repr(service)
        self.assertIn("JetService", result)
        self.assertIn("running=True", result)
        self.assertIn("jobs=1", result)
        self.assertIn("active=1", result)


class TestJetServiceWithConfig(unittest.TestCase):
    def test_submit_with_config(self):
        service = JetService()
        service.start()
        pipeline = MockPipeline()
        config = JobConfig(name="configured-job")
        job = service.submit(pipeline, config)
        self.assertEqual(job.name, "configured-job")

    def test_submit_without_config(self):
        service = JetService()
        service.start()
        pipeline = MockPipeline()
        job = service.submit(pipeline)
        self.assertIsNotNone(job)


if __name__ == "__main__":
    unittest.main()
