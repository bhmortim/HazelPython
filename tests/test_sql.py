"""Unit tests for hazelcast/sql/service.py"""

import unittest
from unittest.mock import MagicMock, patch
from concurrent.futures import Future

from hazelcast.sql.service import (
    SqlExplainResult,
    SqlServiceError,
    SqlService,
)
from hazelcast.sql.statement import SqlStatement, SqlExpectedResultType
from hazelcast.sql.result import SqlColumnType
from hazelcast.exceptions import IllegalStateException


class TestSqlExplainResult(unittest.TestCase):
    def test_init(self):
        result = SqlExplainResult(plan="Scan table", plan_nodes=[{"op": "scan"}], query="SELECT * FROM t")
        self.assertEqual(result.plan, "Scan table")
        self.assertEqual(result.plan_nodes, [{"op": "scan"}])
        self.assertEqual(result.query, "SELECT * FROM t")

    def test_init_defaults(self):
        result = SqlExplainResult(plan="Plan text")
        self.assertEqual(result.plan, "Plan text")
        self.assertEqual(result.plan_nodes, [])
        self.assertEqual(result.query, "")

    def test_str_returns_plan(self):
        result = SqlExplainResult(plan="Full table scan")
        self.assertEqual(str(result), "Full table scan")

    def test_repr(self):
        result = SqlExplainResult(plan="Plan", query="SELECT 1")
        self.assertEqual(repr(result), "SqlExplainResult(query='SELECT 1')")


class TestSqlServiceError(unittest.TestCase):
    def test_init_default(self):
        err = SqlServiceError()
        self.assertIn("SQL error", str(err))
        self.assertEqual(err.code, -1)
        self.assertIsNone(err.originating_member_id)
        self.assertIsNone(err.suggestion)

    def test_init_with_message(self):
        err = SqlServiceError("Custom error")
        self.assertIn("Custom error", str(err))

    def test_init_with_code(self):
        err = SqlServiceError("Error", code=42)
        self.assertEqual(err.code, 42)

    def test_init_with_originating_member_id(self):
        err = SqlServiceError("Error", originating_member_id="member-123")
        self.assertEqual(err.originating_member_id, "member-123")

    def test_init_with_suggestion(self):
        err = SqlServiceError("Error", suggestion="Try using index")
        self.assertEqual(err.suggestion, "Try using index")

    def test_error_code_constants(self):
        self.assertEqual(SqlServiceError.SQL_ERROR_CODE_CANCELLED, -1)
        self.assertEqual(SqlServiceError.SQL_ERROR_CODE_TIMEOUT, -2)
        self.assertEqual(SqlServiceError.SQL_ERROR_CODE_PARSING, -3)
        self.assertEqual(SqlServiceError.SQL_ERROR_CODE_GENERIC, -4)
        self.assertEqual(SqlServiceError.SQL_ERROR_CODE_DATA, -5)
        self.assertEqual(SqlServiceError.SQL_ERROR_CODE_MAP_DESTROYED, -6)
        self.assertEqual(SqlServiceError.SQL_ERROR_CODE_PARTITION_DISTRIBUTION, -7)
        self.assertEqual(SqlServiceError.SQL_ERROR_CODE_MAP_LOADING, -8)
        self.assertEqual(SqlServiceError.SQL_ERROR_CODE_RESTARTABLE, -9)

    def test_str_formatting_with_code(self):
        err = SqlServiceError("Test error", code=42)
        result = str(err)
        self.assertIn("Test error", result)
        self.assertIn("code=42", result)

    def test_str_formatting_with_suggestion(self):
        err = SqlServiceError("Error", suggestion="Use index")
        result = str(err)
        self.assertIn("suggestion=Use index", result)


class TestSqlService(unittest.TestCase):
    def test_init_defaults(self):
        service = SqlService()
        self.assertIsNone(service._invocation_service)
        self.assertIsNone(service._serialization_service)
        self.assertFalse(service.is_running)

    def test_init_with_services(self):
        inv = MagicMock()
        ser = MagicMock()
        service = SqlService(invocation_service=inv, serialization_service=ser)
        self.assertEqual(service._invocation_service, inv)
        self.assertEqual(service._serialization_service, ser)

    def test_start(self):
        service = SqlService()
        service.start()
        self.assertTrue(service.is_running)

    def test_shutdown(self):
        service = SqlService()
        service.start()
        service.shutdown()
        self.assertFalse(service.is_running)

    def test_is_running_property(self):
        service = SqlService()
        self.assertFalse(service.is_running)
        service.start()
        self.assertTrue(service.is_running)

    def test_expected_result_type_map(self):
        self.assertEqual(SqlService._EXPECTED_RESULT_TYPE_MAP[SqlExpectedResultType.ANY], 0)
        self.assertEqual(SqlService._EXPECTED_RESULT_TYPE_MAP[SqlExpectedResultType.ROWS], 1)
        self.assertEqual(SqlService._EXPECTED_RESULT_TYPE_MAP[SqlExpectedResultType.UPDATE_COUNT], 2)

    def test_sql_column_type_map(self):
        self.assertEqual(SqlService._SQL_COLUMN_TYPE_MAP[0], SqlColumnType.VARCHAR)
        self.assertEqual(SqlService._SQL_COLUMN_TYPE_MAP[1], SqlColumnType.BOOLEAN)
        self.assertEqual(SqlService._SQL_COLUMN_TYPE_MAP[4], SqlColumnType.INTEGER)
        self.assertEqual(SqlService._SQL_COLUMN_TYPE_MAP[5], SqlColumnType.BIGINT)

    def test_python_type_to_sql_type(self):
        self.assertEqual(SqlService._PYTHON_TYPE_TO_SQL_TYPE[str], SqlColumnType.VARCHAR)
        self.assertEqual(SqlService._PYTHON_TYPE_TO_SQL_TYPE[bool], SqlColumnType.BOOLEAN)
        self.assertEqual(SqlService._PYTHON_TYPE_TO_SQL_TYPE[int], SqlColumnType.BIGINT)
        self.assertEqual(SqlService._PYTHON_TYPE_TO_SQL_TYPE[float], SqlColumnType.DOUBLE)
        self.assertEqual(SqlService._PYTHON_TYPE_TO_SQL_TYPE[type(None)], SqlColumnType.NULL)

    def test_explain_not_running_raises(self):
        service = SqlService()
        with self.assertRaises(IllegalStateException):
            service.explain("SELECT 1")

    def test_explain_empty_sql_raises(self):
        service = SqlService()
        service.start()
        with self.assertRaises(SqlServiceError):
            service.explain("")

    def test_execute_not_running_raises(self):
        service = SqlService()
        with self.assertRaises(IllegalStateException):
            service.execute("SELECT 1")

    def test_execute_empty_sql_raises(self):
        service = SqlService()
        service.start()
        with self.assertRaises(SqlServiceError):
            service.execute("")

    def test_execute_statement_not_running_raises(self):
        service = SqlService()
        stmt = SqlStatement("SELECT 1")
        with self.assertRaises(IllegalStateException):
            service.execute_statement(stmt)

    def test_execute_statement_empty_sql_raises(self):
        service = SqlService()
        service.start()
        stmt = SqlStatement("")
        with self.assertRaises(SqlServiceError):
            service.execute_statement(stmt)

    def test_execute_async_not_running(self):
        service = SqlService()
        future = service.execute_async("SELECT 1")
        self.assertIsInstance(future, Future)
        with self.assertRaises(IllegalStateException):
            future.result()

    def test_execute_async_empty_sql(self):
        service = SqlService()
        service.start()
        future = service.execute_async("")
        with self.assertRaises(SqlServiceError):
            future.result()

    def test_execute_statement_async_not_running(self):
        service = SqlService()
        stmt = SqlStatement("SELECT 1")
        future = service.execute_statement_async(stmt)
        with self.assertRaises(IllegalStateException):
            future.result()

    def test_generate_query_id(self):
        service = SqlService()
        query_id = service._generate_query_id()
        self.assertIsInstance(query_id, bytes)
        self.assertEqual(len(query_id), 16)

    def test_serialize_parameters_empty(self):
        service = SqlService()
        result = service._serialize_parameters([])
        self.assertEqual(result, [])

    def test_serialize_parameters_none(self):
        service = SqlService()
        result = service._serialize_parameters([None])
        self.assertEqual(result, [b""])

    def test_serialize_primitive_none(self):
        service = SqlService()
        result = service._serialize_primitive(None)
        self.assertEqual(result, b"")

    def test_serialize_primitive_bool_true(self):
        service = SqlService()
        result = service._serialize_primitive(True)
        self.assertEqual(result, b"\x01")

    def test_serialize_primitive_bool_false(self):
        service = SqlService()
        result = service._serialize_primitive(False)
        self.assertEqual(result, b"\x00")

    def test_serialize_primitive_int(self):
        service = SqlService()
        result = service._serialize_primitive(42)
        import struct
        expected = struct.pack("<q", 42)
        self.assertEqual(result, expected)

    def test_serialize_primitive_float(self):
        service = SqlService()
        result = service._serialize_primitive(3.14)
        import struct
        expected = struct.pack("<d", 3.14)
        self.assertEqual(result, expected)

    def test_serialize_primitive_str(self):
        service = SqlService()
        result = service._serialize_primitive("hello")
        self.assertEqual(result, b"hello")

    def test_serialize_primitive_bytes(self):
        service = SqlService()
        result = service._serialize_primitive(b"data")
        self.assertEqual(result, b"data")

    def test_convert_timeout_negative(self):
        service = SqlService()
        result = service._convert_timeout(-1)
        self.assertEqual(result, -1)

    def test_convert_timeout_zero(self):
        service = SqlService()
        result = service._convert_timeout(0)
        self.assertEqual(result, 0)

    def test_convert_timeout_positive(self):
        service = SqlService()
        result = service._convert_timeout(5.5)
        self.assertEqual(result, 5500)

    def test_is_dml_query_insert(self):
        service = SqlService()
        self.assertTrue(service._is_dml_query("INSERT INTO t VALUES (1)"))

    def test_is_dml_query_update(self):
        service = SqlService()
        self.assertTrue(service._is_dml_query("UPDATE t SET x = 1"))

    def test_is_dml_query_delete(self):
        service = SqlService()
        self.assertTrue(service._is_dml_query("DELETE FROM t"))

    def test_is_dml_query_create(self):
        service = SqlService()
        self.assertTrue(service._is_dml_query("CREATE TABLE t (id INT)"))

    def test_is_dml_query_drop(self):
        service = SqlService()
        self.assertTrue(service._is_dml_query("DROP TABLE t"))

    def test_is_dml_query_select_false(self):
        service = SqlService()
        self.assertFalse(service._is_dml_query("SELECT * FROM t"))

    def test_is_dml_query_case_insensitive(self):
        service = SqlService()
        self.assertTrue(service._is_dml_query("insert into t values (1)"))

    def test_is_explain_query(self):
        service = SqlService()
        self.assertTrue(service._is_explain_query("EXPLAIN SELECT 1"))
        self.assertTrue(service._is_explain_query("explain plan for select 1"))
        self.assertFalse(service._is_explain_query("SELECT 1"))

    def test_create_default_metadata(self):
        service = SqlService()
        metadata = service._create_default_metadata("SELECT * FROM t")
        self.assertEqual(len(metadata.columns), 1)
        self.assertEqual(metadata.columns[0].name, "column_0")
        self.assertEqual(metadata.columns[0].type, SqlColumnType.OBJECT)

    def test_get_column_type_for_value_none(self):
        service = SqlService()
        result = service.get_column_type_for_value(None)
        self.assertEqual(result, SqlColumnType.NULL)

    def test_get_column_type_for_value_str(self):
        service = SqlService()
        result = service.get_column_type_for_value("test")
        self.assertEqual(result, SqlColumnType.VARCHAR)

    def test_get_column_type_for_value_int(self):
        service = SqlService()
        result = service.get_column_type_for_value(42)
        self.assertEqual(result, SqlColumnType.BIGINT)

    def test_get_column_type_for_value_bool(self):
        service = SqlService()
        result = service.get_column_type_for_value(True)
        self.assertEqual(result, SqlColumnType.BOOLEAN)

    def test_get_column_type_for_value_float(self):
        service = SqlService()
        result = service.get_column_type_for_value(3.14)
        self.assertEqual(result, SqlColumnType.DOUBLE)

    def test_get_column_type_for_value_unknown(self):
        service = SqlService()
        result = service.get_column_type_for_value([1, 2, 3])
        self.assertEqual(result, SqlColumnType.OBJECT)

    def test_get_active_query_count_empty(self):
        service = SqlService()
        self.assertEqual(service.get_active_query_count(), 0)

    def test_cancel_all_queries_empty(self):
        service = SqlService()
        result = service.cancel_all_queries()
        self.assertEqual(result, 0)

    def test_close_query_not_found(self):
        service = SqlService()
        service.close_query("unknown-query-id")

    def test_repr(self):
        service = SqlService()
        service.start()
        result = repr(service)
        self.assertIn("SqlService", result)
        self.assertIn("running=True", result)
        self.assertIn("active_queries=0", result)


class TestSqlServiceExecuteStatementSync(unittest.TestCase):
    def test_execute_dml_returns_update_count(self):
        service = SqlService()
        service.start()
        result = service.execute("INSERT INTO t VALUES (1)")
        self.assertEqual(result.update_count, 0)

    def test_execute_select_returns_metadata(self):
        service = SqlService()
        service.start()
        result = service.execute("SELECT * FROM t")
        self.assertIsNotNone(result.metadata)

    def test_serialize_parameters_with_service(self):
        mock_ser = MagicMock()
        mock_ser.to_data.return_value = b"serialized"
        service = SqlService(serialization_service=mock_ser)
        result = service._serialize_parameters(["test"])
        self.assertEqual(result, [b"serialized"])

    def test_serialize_parameters_with_service_exception(self):
        mock_ser = MagicMock()
        mock_ser.to_data.side_effect = Exception("Serialization failed")
        service = SqlService(serialization_service=mock_ser)
        result = service._serialize_parameters(["test"])
        self.assertEqual(result, [b"test"])


class TestSqlServiceExplain(unittest.TestCase):
    def test_explain_basic(self):
        service = SqlService()
        service.start()
        result = service.explain("SELECT * FROM users")
        self.assertIsInstance(result, SqlExplainResult)
        self.assertEqual(result.query, "SELECT * FROM users")


if __name__ == "__main__":
    unittest.main()
