# Coverage Metrics and Verification

This document tracks test coverage metrics for the Hazelcast Python Client
and provides verification steps for maintaining coverage standards.

## Coverage Requirements

| Metric | Threshold | Description |
|--------|-----------|-------------|
| Line Coverage | 95% | Minimum percentage of lines executed |
| Branch Coverage | 90% | Minimum percentage of branches taken |
| Missing Lines | < 50 | Maximum uncovered lines per module |

## Running Coverage Validation

### Quick Validation

```bash
# Run with default 95% threshold
python scripts/run_coverage.py

# Run with verbose output showing missing lines
python scripts/run_coverage.py --verbose

# Generate HTML report
python scripts/run_coverage.py --html
```

### CI Pipeline Command

```bash
pytest --cov=hazelcast --cov-report=term-missing --cov-fail-under=95 tests/
```

### Full Coverage Report

```bash
# Generate comprehensive report
python scripts/run_coverage.py --verbose --html --xml

# View HTML report
open htmlcov/index.html
```

## Module Coverage Summary

| Module | Target | Status | Notes |
|--------|--------|--------|-------|
| `hazelcast.config` | 95% | ✅ | Configuration classes fully tested |
| `hazelcast.exceptions` | 95% | ✅ | All exception types covered |
| `hazelcast.auth` | 95% | ✅ | All credential types and factories |
| `hazelcast.metrics` | 95% | ✅ | All metric types and registry |
| `hazelcast.failover` | 95% | ✅ | Cluster failover and CNAME resolver |
| `hazelcast.logging_config` | 95% | ✅ | Logger configuration utilities |
| `hazelcast.near_cache` | 95% | ✅ | Cache and eviction strategies |
| `hazelcast.serialization.api` | 95% | ✅ | Serialization interfaces |
| `hazelcast.serialization.builtin` | 95% | ✅ | Built-in type serializers |
| `hazelcast.serialization.compact` | 95% | ✅ | Compact serialization |
| `hazelcast.serialization.json` | 95% | ✅ | JSON value handling |
| `hazelcast.serialization.service` | 95% | ✅ | Serialization service |
| `hazelcast.service.partition` | 95% | ✅ | Partition service |
| `hazelcast.service.executor` | 95% | ✅ | Executor service |
| `hazelcast.service.client_service` | 95% | ✅ | Client service |

## Known Coverage Gaps

### Acceptable Exclusions

These patterns are intentionally excluded from coverage requirements:

```python
# In .coveragerc or pyproject.toml
[coverage:report]
exclude_lines =
    pragma: no cover
    def __repr__
    raise NotImplementedError
    if TYPE_CHECKING:
    if __name__ == .__main__.:
    @abstractmethod
```

### Platform-Specific Code

Some code paths are platform-specific and may not execute on all systems:

| Code Path | Reason |
|-----------|--------|
| SSL/TLS initialization | Requires OpenSSL |
| Kerberos authentication | Requires Kerberos libraries |
| Cloud discovery (AWS/Azure/GCP) | Requires cloud SDK |

### Error Handling Branches

Some error branches require specific failure conditions:

| Module | Branch | Testing Approach |
|--------|--------|------------------|
| `failover.CNAMEResolver` | DNS failure | Mock `socket.gethostbyname_ex` |
| `near_cache.NearCache` | Serialization error | Mock serialization service |
| `metrics.GaugeMetric` | Value function error | Mock with raising callable |

## Verification Steps

### Before Each Release

1. **Run Full Coverage Check**
   ```bash
   python scripts/run_coverage.py --verbose --strict
   ```

2. **Verify Branch Coverage**
   ```bash
   pytest --cov=hazelcast --cov-branch --cov-report=term-missing tests/
   ```

3. **Generate Coverage Report**
   ```bash
   python scripts/run_coverage.py --html --xml
   ```

4. **Review Uncovered Lines**
   - Open `htmlcov/index.html`
   - Check each module with coverage < 95%
   - Add tests for critical paths

### CI Integration

Add to your CI configuration:

**GitHub Actions:**
```yaml
- name: Run Tests with Coverage
  run: |
    pip install pytest pytest-cov
    python scripts/run_coverage.py --xml --threshold 95

- name: Upload Coverage Report
  uses: codecov/codecov-action@v3
  with:
    files: coverage.xml
    fail_ci_if_error: true
```

**GitLab CI:**
```yaml
test:
  script:
    - pip install pytest pytest-cov
    - python scripts/run_coverage.py --xml --threshold 95
  artifacts:
    reports:
      coverage_report:
        coverage_format: cobertura
        path: coverage.xml
```

## Test File Mapping

| Source Module | Test File |
|---------------|-----------|
| `hazelcast/config.py` | `tests/test_config.py` |
| `hazelcast/exceptions.py` | `tests/test_exceptions.py` |
| `hazelcast/auth.py` | `tests/test_security.py` |
| `hazelcast/metrics.py` | `tests/test_metrics.py` |
| `hazelcast/failover.py` | `tests/test_failover.py` |
| `hazelcast/logging_config.py` | `tests/test_logging.py` |
| `hazelcast/near_cache.py` | `tests/test_near_cache.py` |
| `hazelcast/serialization/*.py` | `tests/test_serialization.py`, `tests/test_compact.py`, `tests/test_json_serialization.py` |
| `hazelcast/service/*.py` | `tests/test_services.py` |
| `hazelcast/network/*.py` | `tests/test_network.py` |

## Coverage Improvement Checklist

When coverage drops below threshold:

- [ ] Identify uncovered lines using `--cov-report=term-missing`
- [ ] Prioritize error handling and edge cases
- [ ] Add unit tests for untested code paths
- [ ] Add integration tests for complex workflows
- [ ] Mock external dependencies for isolated testing
- [ ] Document any legitimate exclusions

## Historical Coverage Trends

Track coverage over time to identify regressions:

| Version | Line Coverage | Branch Coverage | Date |
|---------|---------------|-----------------|------|
| 0.1.0 | 95%+ | 90%+ | Current |

## Additional Resources

- [pytest-cov documentation](https://pytest-cov.readthedocs.io/)
- [Coverage.py documentation](https://coverage.readthedocs.io/)
- [Hazelcast Python Client Tests](../tests/)
