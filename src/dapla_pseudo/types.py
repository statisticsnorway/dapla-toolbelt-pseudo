"""Type declarations for dapla-toolbelt-pseudo."""

import io
from pathlib import Path
from typing import TypeAlias

import fsspec
import gcsfs
import pandas as pd

from dapla_pseudo.v1.api_models import Field

FieldDecl: TypeAlias = str | dict[str, str] | Field
BinaryFileDecl: TypeAlias = (
    io.BufferedReader | fsspec.spec.AbstractBufferedFile | gcsfs.core.GCSFile
)

DatasetDecl: TypeAlias = pd.DataFrame | BinaryFileDecl | str | Path
FileLikeDatasetDecl: TypeAlias = BinaryFileDecl | str | Path
FileSpecDecl: TypeAlias = tuple[str | None, BinaryFileDecl | str, str]
# FileSpecDecl is derived from the "files" argument in multi-part requests from the "Requests"-library
# The tuple semantically means: ('filename', fileobj, 'content_type')
# See "files" in https://requests.readthedocs.io/en/latest/api/#requests.request
