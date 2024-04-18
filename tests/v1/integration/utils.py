import inspect
import json
import os

import pytest
from datadoc_model.model import MetadataContainer


def integration_test() -> pytest.MarkDecorator:
    # Tests annotated with integration_test will run if `INTEGRATION_TESTS` env variable is unset or `TRUE`
    # This is used to disable integration tests in the `test.yaml` workflow, since these tests need additional configuration.
    enabled = os.environ.get("INTEGRATION_TESTS", "TRUE")
    return pytest.mark.skipif(
        enabled != "TRUE", reason="Integration tests are disabled for current test"
    )


def get_expected_datadoc_metadata_container(
    calling_function_name: str,
) -> MetadataContainer:
    """Helper function that returns the expected MetadataContainer for a given calling function."""
    expected_metadata_json_file = (
        f"tests/data/datadoc/expected_metadata_{calling_function_name}.json"
    )
    with open(expected_metadata_json_file) as json_file:
        expected_datadoc_json = json.load(json_file)
    return MetadataContainer(**expected_datadoc_json)


def get_calling_function_name() -> str:
    """Retrieves the name of the function that called this function.

    Returns:
        str: The name of the function that called this function.

    Raises:
        RuntimeError: If the calling frame cannot be determined.
    """
    frame = inspect.currentframe().f_back  # type: ignore
    if frame is not None:
        function_name = frame.f_code.co_name
        return function_name
    else:
        # If no frame is found, raise an exception to fail the test
        raise RuntimeError(
            "Failed to get the calling frame, which is required for this test."
        )
