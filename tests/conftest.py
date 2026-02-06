"""Pytest configuration and custom mark registration."""


def pytest_configure(config):
    config.addinivalue_line("markers", "integration: requires a running Houdini bridge")
