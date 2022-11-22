"""Sphinx configuration."""
project = "Pseudonymization extensions for Dapla Toolbelt"
author = "Team Skyinfrastruktur"
copyright = "2022, Team Skyinfrastruktur"
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_click",
    "myst_parser",
]
autodoc_typehints = "description"
html_theme = "furo"
