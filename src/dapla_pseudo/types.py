"""Type declarations for dapla-toolbelt-pseudo."""

import io
from pathlib import Path

import fsspec
import gcsfs
import pandas as pd

BinaryFileDecl = (
    io.BufferedReader | fsspec.spec.AbstractBufferedFile | gcsfs.core.GCSFile
)

DatasetDecl = pd.DataFrame | BinaryFileDecl | str | Path
FileLikeDatasetDecl = BinaryFileDecl | str | Path
FileSpecDecl = tuple[str | None, BinaryFileDecl | str, str]
# FileSpecDecl is derived from the "files" argument in multi-part requests from the "Requests"-library
# The tuple semantically means: ('filename', fileobj, 'content_type')
# See "files" in https://requests.readthedocs.io/en/latest/api/#requests.request
