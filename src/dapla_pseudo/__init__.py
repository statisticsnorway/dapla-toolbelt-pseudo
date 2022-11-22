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
__version__ = "0.0.1"

from .v1 import PseudoClient
from .v1 import depseudonymize
from .v1 import pseudonymize
from .v1 import repseudonymize


__all__ = ["PseudoClient", "pseudonymize", "depseudonymize", "repseudonymize"]
