"""Type declarations for dapla-toolbelt-pseudo."""

import io
import typing as t
from pathlib import Path

import fsspec
import pandas as pd

from dapla_pseudo.v1.models import Field


FieldDecl = t.Union[str, dict[str, str], Field]
BinaryFileDecl = t.Union[t.BinaryIO, io.BufferedReader, fsspec.spec.AbstractBufferedFile]
DatasetDecl = t.Union[pd.DataFrame, BinaryFileDecl, str, Path]
FileSpecDecl = tuple[t.Optional[str], t.Union[BinaryFileDecl, str], str]
