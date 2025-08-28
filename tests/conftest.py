"""
Pytest configuration for market_data_etl CLI tests.

This file contains:
- Global pytest configuration and markers
- Essential fixtures for CLI end-to-end testing
"""

import pytest


# Global pytest configuration
def pytest_configure(config):
    """Configure pytest with custom markers and settings."""
    config.addinivalue_line(
        "markers", "unit: mark test as unit test"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "external_api: mark test as requiring external API access"
    )


# Mark for organization - default marker for CLI tests
pytestmark = pytest.mark.integration