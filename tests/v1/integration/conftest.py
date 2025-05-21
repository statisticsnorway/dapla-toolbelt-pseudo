import os
import subprocess
from collections.abc import Generator

import pytest_cases


@pytest_cases.fixture()
def setup() -> Generator[None, None, None]:
    os.environ["PSEUDO_SERVICE_URL"] = "https://pseudo-service.test.ssb.no"
    # Setup step that runs when integration test are ran on local machine
    # This will not run in GH actions since GITHUB_ACTIONS is set to `true` per default
    # https://docs.github.com/en/actions/learn-github-actions/variables
    if os.environ.get("GITHUB_ACTIONS") != "true":
        # Setup step
        # Could not find a way to generate id tokes without a SA to impersonate.
        # Subprocess `glcoud auth` as a temporary workaround

        # Need to disable local file logging to avoid getting gcloud perm error
        subprocess.run(["gcloud", "config", "set", "core/disable_file_logging", "True"])
        id_token = subprocess.getoutput("gcloud auth print-identity-token")
        os.environ["PSEUDO_SERVICE_AUTH_TOKEN"] = id_token
        yield
        # Teardown step
        os.unsetenv("PSEUDO_SERVICE_URL")
        os.unsetenv("PSEUDO_SERVICE_AUTH_TOKEN")
    else:
        # If ran from GitHub action no setup is required
        yield
