import os

import pytest


def integration_test():
    enabled = os.environ.get("INTEGRATION_TESTS", "TRUE")
    return pytest.mark.skipif(
        enabled != "TRUE", reason="Integration tests are disabled for current test"
    )
