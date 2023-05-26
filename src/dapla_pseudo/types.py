"""Type declarations for dapla-toolbelt-pseudo."""

import io
import typing as t
from pathlib import Path

import fsspec
import pandas as pd

from dapla_pseudo.v1.models import Field


_FieldDecl = t.Union[str, dict, Field]
_BinaryFileDecl = t.Union[t.BinaryIO, io.BufferedReader, fsspec.spec.AbstractBufferedFile]
_DatasetDecl = t.Union[pd.DataFrame, _BinaryFileDecl, str, Path]
_FileSpecDecl = tuple[t.Optional[str], t.Union[_BinaryFileDecl, str], str]
