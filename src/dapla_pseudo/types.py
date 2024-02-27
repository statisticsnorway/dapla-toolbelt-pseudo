"""Type declarations for dapla-toolbelt-pseudo."""

import io
import typing as t
from pathlib import Path

import fsspec
import gcsfs
import pandas as pd

from dapla_pseudo.v1.api_models import Field

FieldDecl = str | dict[str, str] | Field
BinaryFileDecl = t.Union[
    io.BufferedReader,
    fsspec.spec.AbstractBufferedFile,
    gcsfs.core.GCSFile,
]
DatasetDecl = pd.DataFrame | BinaryFileDecl | str | Path
FileLikeDatasetDecl = BinaryFileDecl | str | Path
FileSpecDecl = tuple[t.Optional[str], BinaryFileDecl | str, str]
# FileSpecDecl is derived from the "files" argument in multi-part requests from the "Requests"-library
# The tuple semantically means: ('filename', fileobj, 'content_type')
# See "files" in https://requests.readthedocs.io/en/latest/api/#requests.request
