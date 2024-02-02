import pandas as pd

from dapla_pseudo import Pseudonymize

JSON_FILE = "tests/data/personer_pseudonymized.json"


def test_pseudonymize() -> None:
    df = pd.read_json(
        JSON_FILE,
        dtype={
            "fnr": "string",
            "fornavn": "string",
            "etternavn": "string",
            "kjonn": "category",
            "fodselsdato": "string",
        },
    )
    expected_data = {
        "fnr": [
            "AWIRfKIwWmlxO9zxQ7DM0a31ZDCpfHTg…",
            "AWIRfKLuAuP/Y0C3MAXO8eXIgDIX7Ss1…",
            "AWIRfKIDQ0LedgTUaq7GVql7ZwVZeVC/…",
        ],
        "fornavn": ["Donald", "Mikke", "Anton"],
        "etternavn": ["Duck", "Mus", "Duck"],
        "kjonn": ["M", "M", "M"],
        "fodselsdato": ["020995", "060970", "180999"],
    }
    expected_df = pd.DataFrame(expected_data)

    result = (
        Pseudonymize.from_pandas(df)  ### GENERAL WORKFLOW ###  # Select dataset
        .on_fields("fnr")  # Select fields in dataset
        .with_default_encryption()  # Select encryption method on fields
        .run()  # Apply pseudonymization
    )
    assert result.to_pandas().equals(expected_df)
