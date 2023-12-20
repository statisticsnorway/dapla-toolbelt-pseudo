"""Type declarations for dapla-toolbelt-pseudo."""

import io
import typing as t
from pathlib import Path

import fsspec
import pandas as pd
import polars as pl
import gcsfs
from dapla_pseudo.v1.models import Field


FieldDecl = str | dict[str, str] | Field
BinaryFileDecl = t.Union[
    io.BufferedReader,
    fsspec.spec.AbstractBufferedFile,
    gcsfs.core.GCSFile,
]
DatasetDecl = pd.DataFrame | BinaryFileDecl | str | Path
HierarchDatasetDecl = BinaryFileDecl | str | Path
FileSpecDecl = tuple[t.Optional[str], BinaryFileDecl | str, str]
