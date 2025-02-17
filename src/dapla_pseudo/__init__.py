"""Pseudonymization extensions for Dapla Toolbelt.

We import all methods from the v1 module as start. Later, if we need to support different versions,
we can use this mechanism to define a default version. Users should typically not care about the
underlying implementation, but they have the option to lock their implementation explicitly to a
specific version if the need should arise, e.g. due to compatibility reasons.

One should import functions like so:
from dapla_pseudo import pseudonymize
(which would resolve to the default implementation)

Alternatively, lock the implementation to a specific version, like so:
from dapla_pseudo.v1 import pseudonymize
(which would always resolve to the v1 implementation)
"""

from importlib.metadata import version

# Avoid having to define the version multiple places.
# Ref: https://github.com/python-poetry/poetry/issues/144#issuecomment-1488038660
__version__ = version("dapla_toolbelt_pseudo")

from dapla_pseudo.globberator.traverser import SchemaTraverser
from dapla_pseudo.v1.client import PseudoClient
from dapla_pseudo.v1.depseudo import Depseudonymize
from dapla_pseudo.v1.models.core import PseudoKeyset
from dapla_pseudo.v1.models.core import PseudoRule
from dapla_pseudo.v1.pseudo import Pseudonymize
from dapla_pseudo.v1.repseudo import Repseudonymize
from dapla_pseudo.v1.validation import Validator

__all__ = [
    "Depseudonymize",
    "PseudoClient",
    "PseudoKeyset",
    "PseudoRule",
    "Pseudonymize",
    "Repseudonymize",
    "SchemaTraverser",
    "Validator",
]
