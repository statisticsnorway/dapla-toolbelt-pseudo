"""This file tests the _BasePseudonymizer class."""

from unittest.mock import ANY
from unittest.mock import Mock

import polars as pl
import pytest_cases
from pytest_cases import fixture_ref
from pytest_mock import MockerFixture

from dapla_pseudo.constants import PseudoOperation
from dapla_pseudo.v1.baseclasses import _BasePseudonymizer
from dapla_pseudo.v1.models.api import PseudoFieldRequest
from dapla_pseudo.v1.models.api import PseudoFieldResponse
from dapla_pseudo.v1.models.api import RawPseudoMetadata
from dapla_pseudo.v1.models.core import PseudoFunction
from dapla_pseudo.v1.models.core import PseudoRule

PKG = "dapla_pseudo.v1.baseclasses"


@pytest_cases.parametrize(
    "dataset",
    [
        fixture_ref("df_personer"),
        fixture_ref("df_personer_hierarchical"),
    ],
)
@pytest_cases.parametrize(
    "pseudo_op",
    PseudoOperation.__members__.values(),  # list of all possible PseudoOperations
)
def test_execute_pseudo_operation_field(
    pseudo_op: PseudoOperation,
    dataset: pl.DataFrame,
    mocker: MockerFixture,
) -> None:
    """Purpose: Ensure that supported dataset types are handled and actually perform pseudonymization."""
    mocker.patch(f"{PKG}.build_pseudo_field_request", return_value=Mock())
    mock_pseudo_field = mocker.patch(
        f"{PKG}._BasePseudonymizer._pseudonymize_field", return_value=Mock()
    )
    mock_pseudo_field.return_value.raw_metadata = []
    base = _BasePseudonymizer(
        pseudo_operation=pseudo_op, dataset=dataset, hierarchical=False
    )

    rules = [PseudoRule(name="dummy", pattern="dummy", func=Mock(spec=PseudoFunction))]
    base._execute_pseudo_operation(
        rules=rules,
        timeout=ANY,
        custom_keyset=ANY,
        target_custom_keyset=ANY,
        target_rules=rules,
    )

    mock_pseudo_field.assert_called_once()


def test_pseudonymize_field(
    df_personer: pl.DataFrame,
    pseudo_func_sid: PseudoFunction,
    mocker: MockerFixture,
) -> None:
    """Purpose: Ensure that JSON responses are deserialized into Result-type objects as expected.

    Only tests for 'PseudoFieldRequest', not depseudo/repseudo as the deserialization is agnostic to the type of request.
    """
    sid_req = PseudoFieldRequest(
        pseudo_func=pseudo_func_sid,
        name="fnr",
        pattern="fnr*",
        values=list(df_personer["fnr"]),
    )

    mocked_data = ["jJuuj0i", "ylc9488", "yeLfkaL"]
    mocked_metadata = RawPseudoMetadata(
        field_name="fnr",
        logs=[],
        metrics=[{"MAPPED_SID": 3}],
        datadoc=[
            {
                "datadoc_metadata": {
                    "datadoc": {
                        "document_version": "5.0.1",
                        "variables": [
                            {
                                "short_name": "fnr",
                                "data_element_path": "fnr",
                                "pseudonymization": {
                                    "stable_identifier_type": "FREG_SNR",
                                    "stable_identifier_version": "2023-08-31",
                                    "encryption_algorithm": "TINK-FPE",
                                    "encryption_key_reference": "papis-common-key-1",
                                    "encryption_algorithm_parameters": [
                                        {"keyId": "papis-common-key-1"},
                                        {"strategy": "skip"},
                                    ],
                                },
                            }
                        ],
                    }
                }
            }
        ],
    )

    mocked_asyncio_run = mocker.patch(
        "dapla_pseudo.v1.baseclasses.asyncio.run",
    )
    mocked_asyncio_run.return_value = [("fnr", mocked_data, mocked_metadata)]
    base = _BasePseudonymizer(
        pseudo_operation=PseudoOperation.PSEUDONYMIZE,
        dataset=df_personer,
        hierarchical=False,
    )

    response = base._pseudonymize_field([sid_req], timeout=ANY)
    metadata = response.raw_metadata[0]
    assert isinstance(response, PseudoFieldResponse)

    assert metadata.datadoc == mocked_metadata.datadoc
    assert metadata.logs == mocked_metadata.logs
    assert metadata.metrics == mocked_metadata.metrics
