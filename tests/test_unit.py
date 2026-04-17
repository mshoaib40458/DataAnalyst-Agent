"""
Unit tests for the DataAnalyst Agent project.
Covers: security utils, rate limiting, cleanup, profiler, models, and API endpoints.
"""
import os
import sys
import time
import json
import uuid
import tempfile
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

# Force tests to use a temporary file-based database so that connections across the test client share the data
_test_db_fd, _test_db_path = tempfile.mkstemp(suffix=".db")
os.environ["DATABASE_URL"] = f"sqlite:///{_test_db_path}"

from datetime import datetime, timezone

# ============================================================================
# SECURITY UTILS TESTS
# ============================================================================


class TestRedactSensitiveData:
    """Tests for app.utils.security.redact_sensitive_data"""

    def test_redacts_api_key(self):
        from app.utils.security import redact_sensitive_data
        text = "api_key=sk-abc123secret"
        result = redact_sensitive_data(text)
        assert "sk-abc123secret" not in result
        assert "[REDACTED]" in result

    def test_redacts_bearer_token(self):
        from app.utils.security import redact_sensitive_data
        text = "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.longtoken"
        result = redact_sensitive_data(text)
        assert "eyJhbGciOiJIUzI1NiJ9" not in result

    def test_redacts_database_url(self):
        from app.utils.security import redact_sensitive_data
        text = "connecting to postgres://user:pass@host:5432/db"
        result = redact_sensitive_data(text)
        assert "user:pass" not in result
        assert "[DATABASE_URL_REDACTED]" in result

    def test_redacts_password_field(self):
        from app.utils.security import redact_sensitive_data
        text = "password: mysecretpassword123"
        result = redact_sensitive_data(text)
        assert "mysecretpassword123" not in result

    def test_preserves_normal_text(self):
        from app.utils.security import redact_sensitive_data
        text = "This is a normal log message with no secrets."
        result = redact_sensitive_data(text)
        assert result == text


class TestValidateTableName:
    """Tests for app.utils.security.validate_table_name"""

    def test_valid_simple_name(self):
        from app.utils.security import validate_table_name
        assert validate_table_name("users") is True

    def test_valid_with_underscore(self):
        from app.utils.security import validate_table_name
        assert validate_table_name("user_profiles") is True

    def test_valid_starts_with_underscore(self):
        from app.utils.security import validate_table_name
        assert validate_table_name("_temp_table") is True

    def test_invalid_starts_with_number(self):
        from app.utils.security import validate_table_name
        assert validate_table_name("123table") is False

    def test_invalid_contains_dash(self):
        from app.utils.security import validate_table_name
        assert validate_table_name("user-table") is False

    def test_invalid_contains_space(self):
        from app.utils.security import validate_table_name
        assert validate_table_name("user table") is False

    def test_invalid_empty_string(self):
        from app.utils.security import validate_table_name
        assert validate_table_name("") is False

    def test_invalid_too_long(self):
        from app.utils.security import validate_table_name
        assert validate_table_name("a" * 64) is False

    def test_valid_at_max_length(self):
        from app.utils.security import validate_table_name
        assert validate_table_name("a" * 63) is True

    def test_invalid_sql_injection_attempt(self):
        from app.utils.security import validate_table_name
        assert validate_table_name("users; DROP TABLE users;--") is False


class TestSanitizeHtml:
    """Tests for app.utils.security.sanitize_html"""

    def test_allows_safe_tags(self):
        from app.utils.security import sanitize_html
        html = "<p>Hello <strong>world</strong></p>"
        result = sanitize_html(html)
        assert "<p>" in result
        assert "<strong>" in result

    def test_removes_script_tags(self):
        from app.utils.security import sanitize_html
        html = "<p>Hello</p><script>alert('xss')</script>"
        result = sanitize_html(html)
        assert "<script>" not in result
        assert "alert" not in result

    def test_removes_event_handlers(self):
        from app.utils.security import sanitize_html
        html = '<div onmouseover="alert(1)">hover</div>'
        result = sanitize_html(html)
        assert "onmouseover" not in result


class TestSanitizeMarkdownOutput:
    """Tests for app.utils.security.sanitize_markdown_output"""

    def test_strips_script_tags(self):
        from app.utils.security import sanitize_markdown_output
        md = "# Title\n<script>alert('xss')</script>\nHello"
        result = sanitize_markdown_output(md)
        assert "<script>" not in result
        assert "# Title" in result

    def test_strips_event_handlers(self):
        from app.utils.security import sanitize_markdown_output
        md = "Click <div onclick=alert(1)>here</div>"
        result = sanitize_markdown_output(md)
        assert "onclick" not in result

    def test_preserves_normal_markdown(self):
        from app.utils.security import sanitize_markdown_output
        md = "# Title\n\n- Item 1\n- Item 2\n\n**Bold text**"
        result = sanitize_markdown_output(md)
        assert result == md


class TestValidateFileSize:
    """Tests for app.utils.security.validate_file_size"""

    def test_valid_file_size(self):
        from app.utils.security import validate_file_size
        valid, msg = validate_file_size(1024, 1048576)
        assert valid is True
        assert msg == ""

    def test_zero_size_file(self):
        from app.utils.security import validate_file_size
        valid, msg = validate_file_size(0, 1048576)
        assert valid is False
        assert "greater than 0" in msg

    def test_over_max_size(self):
        from app.utils.security import validate_file_size
        valid, msg = validate_file_size(2000000, 1048576)
        assert valid is False
        assert "too large" in msg.lower()


# ============================================================================
# RATE LIMIT TESTS
# ============================================================================


class TestRateLimit:
    """Tests for app.utils.rate_limit"""

    def setup_method(self):
        """Reset rate limit stores before each test."""
        from app.utils.rate_limit import _rate_limit_store, _concurrent_jobs
        _rate_limit_store.clear()
        _concurrent_jobs.clear()

    def test_allows_under_limit(self):
        from app.utils.rate_limit import check_rate_limit
        allowed, msg = check_rate_limit("test_client", limit=5)
        assert allowed is True
        assert msg == ""

    def test_blocks_over_limit(self):
        from app.utils.rate_limit import check_rate_limit
        for _ in range(5):
            check_rate_limit("test_client", limit=5)
        allowed, msg = check_rate_limit("test_client", limit=5)
        assert allowed is False
        assert "Rate limit exceeded" in msg

    def test_different_clients_independent(self):
        from app.utils.rate_limit import check_rate_limit
        for _ in range(5):
            check_rate_limit("client_a", limit=5)
        # client_a is blocked
        allowed_a, _ = check_rate_limit("client_a", limit=5)
        assert allowed_a is False
        # client_b is still free
        allowed_b, _ = check_rate_limit("client_b", limit=5)
        assert allowed_b is True

    def test_concurrent_job_tracking(self):
        from app.utils.rate_limit import (
            can_start_analysis, increment_concurrent_job,
            decrement_concurrent_job
        )
        # Under limit
        allowed, _ = can_start_analysis("client_x")
        assert allowed is True

        # Fill up slots
        for _ in range(5):
            increment_concurrent_job("client_x")
        allowed, msg = can_start_analysis("client_x")
        assert allowed is False
        assert "Too many" in msg

        # Free one slot
        decrement_concurrent_job("client_x")
        allowed, _ = can_start_analysis("client_x")
        assert allowed is True

    def test_decrement_never_goes_negative(self):
        from app.utils.rate_limit import (
            _concurrent_jobs, decrement_concurrent_job
        )
        decrement_concurrent_job("nonexistent_client")
        assert _concurrent_jobs["nonexistent_client"] == 0

    def test_extract_client_id_forwarded_trusted(self):
        from app.utils.rate_limit import extract_client_id
        import os
        os.environ["TRUST_FORWARDED_IP"] = "true"
        mock_request = MagicMock()
        mock_request.headers.get.return_value = "1.2.3.4, 5.6.7.8"
        result = extract_client_id(mock_request)
        assert result == "1.2.3.4"
        
    def test_extract_client_id_forwarded_untrusted(self):
        from app.utils.rate_limit import extract_client_id
        import os
        os.environ["TRUST_FORWARDED_IP"] = "false"
        mock_request = MagicMock()
        mock_request.headers.get.return_value = "1.2.3.4, 5.6.7.8"
        mock_request.client.host = "10.0.0.1"
        result = extract_client_id(mock_request)
        assert result == "10.0.0.1"

    def test_extract_client_id_direct(self):
        from app.utils.rate_limit import extract_client_id
        mock_request = MagicMock()
        mock_request.headers.get.return_value = None
        mock_request.client.host = "127.0.0.1"
        result = extract_client_id(mock_request)
        assert result == "127.0.0.1"


# ============================================================================
# PROFILER TESTS
# ============================================================================


class TestProfiler:
    """Tests for app.services.profiler.profile_dataframe"""

    def test_basic_numeric_profiling(self):
        from app.services.profiler import profile_dataframe
        df = pd.DataFrame({"age": [25, 30, 35], "salary": [50000, 60000, 70000]})
        profile = profile_dataframe(df)

        assert profile["num_rows"] == 3
        assert profile["num_cols"] == 2
        assert "age" in profile["numeric_columns"]
        assert "salary" in profile["numeric_columns"]
        assert profile["columns"]["age"]["mean"] == 30.0

    def test_categorical_profiling(self):
        from app.services.profiler import profile_dataframe
        df = pd.DataFrame({"color": ["red", "blue", "red", "green"]})
        profile = profile_dataframe(df)

        assert "color" in profile["categorical_columns"]
        assert "top_values" in profile["columns"]["color"]

    def test_datetime_detection(self):
        from app.services.profiler import profile_dataframe
        df = pd.DataFrame({
            "date": pd.to_datetime(["2024-01-01", "2024-06-15", "2024-12-31"])
        })
        profile = profile_dataframe(df)
        assert "date" in profile["datetime_columns"]

    def test_datetime_string_detection(self):
        from app.services.profiler import profile_dataframe
        df = pd.DataFrame({
            "date_str": ["2024-01-01", "2024-06-15", "2024-12-31"]
        })
        profile = profile_dataframe(df)
        # Should detect as datetime_like since >80% parse
        assert "date_str" in profile["datetime_columns"]

    def test_missing_values_counted(self):
        from app.services.profiler import profile_dataframe
        df = pd.DataFrame({"val": [1, None, 3, None, 5]})
        profile = profile_dataframe(df)
        assert profile["columns"]["val"]["num_missing"] == 2

    def test_empty_dataframe(self):
        from app.services.profiler import profile_dataframe
        df = pd.DataFrame({"a": pd.Series(dtype="float64")})
        profile = profile_dataframe(df)
        assert profile["num_rows"] == 0

    def test_mixed_types(self):
        from app.services.profiler import profile_dataframe
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "score": [90.5, 85.0, 92.3]
        })
        profile = profile_dataframe(df)
        assert "id" in profile["numeric_columns"]
        assert "name" in profile["categorical_columns"]
        assert "score" in profile["numeric_columns"]


# ============================================================================
# CLEANUP TESTS
# ============================================================================


class TestCleanup:
    """Tests for app.utils.cleanup"""

    def test_cleanup_old_files(self):
        from app.utils.cleanup import cleanup_old_files
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a file and backdate it
            filepath = os.path.join(tmpdir, "old_file.csv")
            with open(filepath, "w") as f:
                f.write("test data")
            # Set mtime to 100 hours ago
            old_time = time.time() - (100 * 3600)
            os.utime(filepath, (old_time, old_time))

            deleted, freed = cleanup_old_files(tmpdir, cutoff_hours=72)
            assert deleted == 1
            assert freed > 0
            assert not os.path.exists(filepath)

    def test_cleanup_preserves_recent_files(self):
        from app.utils.cleanup import cleanup_old_files
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "recent.csv")
            with open(filepath, "w") as f:
                f.write("recent data")

            deleted, freed = cleanup_old_files(tmpdir, cutoff_hours=72)
            assert deleted == 0
            assert os.path.exists(filepath)

    def test_cleanup_dry_run(self):
        from app.utils.cleanup import cleanup_old_files
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "old_file.csv")
            with open(filepath, "w") as f:
                f.write("test data")
            old_time = time.time() - (100 * 3600)
            os.utime(filepath, (old_time, old_time))

            deleted, freed = cleanup_old_files(tmpdir, cutoff_hours=72, dry_run=True)
            assert deleted == 1
            assert os.path.exists(filepath)  # File NOT deleted in dry run

    def test_cleanup_nonexistent_directory(self):
        from app.utils.cleanup import cleanup_old_files
        deleted, freed = cleanup_old_files("/nonexistent/path", cutoff_hours=72)
        assert deleted == 0
        assert freed == 0

    def test_get_directory_size(self):
        from app.utils.cleanup import get_directory_size
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "data.txt")
            with open(filepath, "w") as f:
                f.write("A" * 1000)
            size = get_directory_size(tmpdir)
            assert size >= 1000


# ============================================================================
# LLM UTILS TESTS
# ============================================================================


class TestLLMUtils:
    """Tests for app.utils.llm_utils"""

    def test_estimate_tokens(self):
        from app.utils.llm_utils import estimate_prompt_tokens
        assert estimate_prompt_tokens("1234") == 1
        assert estimate_prompt_tokens("12345678") == 2

    def test_enforce_token_budget_under(self):
        from app.utils.llm_utils import enforce_token_budget
        text = "Short text"
        result = enforce_token_budget(text, max_tokens=2000)
        assert result == text

    def test_enforce_token_budget_over(self):
        from app.utils.llm_utils import enforce_token_budget
        text = "A" * 10000  # ~2500 tokens
        result = enforce_token_budget(text, max_tokens=500)
        assert len(result) == 2000  # 500 * 4


# ============================================================================
# DATABASE MODELS TESTS
# ============================================================================


class TestModels:
    """Tests for app.db.models"""

    def test_analysis_job_defaults(self):
        from app.db.models import AnalysisJob
        # SQLAlchemy defaults apply on flush, so we test the declarative column defaults
        assert AnalysisJob.status.default.arg == "uploaded"
        assert AnalysisJob.progress_logs.default.arg == ""
        assert AnalysisJob.error_message.default.arg == ""

    def test_analysis_memory_fields(self):
        from app.db.models import AnalysisMemory
        mem = AnalysisMemory(
            job_id="test-123",
            schema_fingerprint="abc123",
            insights_summary="Key finding: sales up 20%"
        )
        assert mem.job_id == "test-123"
        assert mem.schema_fingerprint == "abc123"


# ============================================================================
# FASTAPI ENDPOINT TESTS
# ============================================================================


class TestAPIEndpoints:
    """Tests for FastAPI endpoints using TestClient."""

    @pytest.fixture(autouse=True)
    def setup_client(self):
        """Create test client and ensure upload directory exists."""
        from fastapi.testclient import TestClient
        from app.main import app
        from app.db.database import Base, engine
        # Ensure fresh tables in our in-memory DB
        Base.metadata.drop_all(bind=engine)
        Base.metadata.create_all(bind=engine)
        
        self.client = TestClient(app)
        os.makedirs("data/uploads", exist_ok=True)

    def test_health_endpoint(self):
        res = self.client.get("/health")
        assert res.status_code == 200
        data = res.json()
        assert data["status"] == "ok"
        assert "version" in data

    def test_upload_csv_success(self):
        csv_content = b"name,age,score\nAlice,30,95\nBob,25,88\nCharlie,35,92\n"
        res = self.client.post(
            "/upload_dataset",
            files={"file": ("test_data.csv", csv_content, "text/csv")}
        )
        assert res.status_code == 200
        data = res.json()
        assert "job_id" in data
        assert data["message"] == "File uploaded successfully"

    def test_upload_non_csv_rejected(self):
        res = self.client.post(
            "/upload_dataset",
            files={"file": ("test.txt", b"hello world", "text/plain")}
        )
        assert res.status_code == 400
        assert "CSV" in res.json()["detail"]

    def test_upload_empty_csv_rejected(self):
        csv_content = b""
        res = self.client.post(
            "/upload_dataset",
            files={"file": ("empty.csv", csv_content, "text/csv")}
        )
        assert res.status_code == 400

    def test_start_analysis_missing_job(self):
        res = self.client.post(
            "/start_analysis",
            json={"job_id": "nonexistent-id-12345"}
        )
        assert res.status_code == 404

    def test_analysis_status_missing_job(self):
        res = self.client.get("/analysis_status/nonexistent-id-12345")
        assert res.status_code == 404

    def test_download_report_missing_job(self):
        res = self.client.get("/download_report/nonexistent-id-12345?format=json")
        assert res.status_code == 404

    def test_job_cleaning_missing_job(self):
        res = self.client.get("/job_cleaning/nonexistent-id-12345")
        assert res.status_code == 404

    def test_job_plan_missing_job(self):
        res = self.client.get("/job_plan/nonexistent-id-12345")
        assert res.status_code == 404

    def test_sql_upload_invalid_table_name(self):
        res = self.client.post(
            "/upload_sql_table",
            json={
                "database_url": "sqlite:///test.db",
                "table_name": "123-invalid;DROP",
                "limit": 1000
            }
        )
        assert res.status_code == 400
        assert "Invalid table name" in res.json()["detail"]

    def test_sql_upload_invalid_limit(self):
        res = self.client.post(
            "/upload_sql_table",
            json={
                "database_url": "sqlite:///test.db",
                "table_name": "valid_table",
                "limit": 50000
            }
        )
        assert res.status_code == 400
        assert "Limit" in res.json()["detail"]

    def test_full_upload_and_status_flow(self):
        """Integration: upload CSV → check status → verify uploaded state."""
        csv_content = b"x,y\n1,2\n3,4\n5,6\n"
        upload_res = self.client.post(
            "/upload_dataset",
            files={"file": ("flow_test.csv", csv_content, "text/csv")}
        )
        assert upload_res.status_code == 200
        job_id = upload_res.json()["job_id"]

        status_res = self.client.get(f"/analysis_status/{job_id}")
        assert status_res.status_code == 200
        assert status_res.json()["status"] == "uploaded"


# ============================================================================
# AGENT NODES VALIDATION TESTS
# ============================================================================


class TestNodeValidation:
    """Tests for the plan validation logic in agent nodes."""

    def test_validate_plan_filters_invalid_operations(self):
        from app.agent.nodes import _validate_plan
        raw_plan = [
            {"task": "Check missing", "operation": "missing_values", "params": {}},
            {"task": "Hack system", "operation": "exec_shell", "params": {}},
        ]
        result = _validate_plan(raw_plan)
        assert len(result) == 1
        assert result[0]["operation"] == "missing_values"

    def test_validate_plan_fixes_bad_freq(self):
        from app.agent.nodes import _validate_plan
        raw_plan = [
            {
                "task": "Trend analysis",
                "operation": "time_series_trend",
                "params": {"date_col": "date", "value_col": "sales", "freq": "Monthly"}
            },
        ]
        result = _validate_plan(raw_plan)
        assert len(result) == 1
        assert result[0]["params"]["freq"] == "M"  # Auto-corrected

    def test_validate_plan_preserves_valid_freq(self):
        from app.agent.nodes import _validate_plan
        raw_plan = [
            {
                "task": "Trend analysis",
                "operation": "time_series_trend",
                "params": {"date_col": "date", "value_col": "sales", "freq": "W"}
            },
        ]
        result = _validate_plan(raw_plan)
        assert result[0]["params"]["freq"] == "W"  # Kept as-is

    def test_validate_plan_empty_input(self):
        from app.agent.nodes import _validate_plan
        result = _validate_plan([])
        assert result == []

    def test_validate_plan_skips_bad_params(self):
        from app.agent.nodes import _validate_plan
        raw_plan = [
            {"task": "Bad step", "operation": "describe_numeric", "params": "not_a_dict"},
        ]
        with pytest.raises(ValueError, match="No valid analysis steps"):
            _validate_plan(raw_plan)
