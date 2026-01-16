"""Sphinx configuration for Hazelcast Python Client documentation."""

import os
import sys

# Add the project root to the path for autodoc
sys.path.insert(0, os.path.abspath(".."))

# -- Project information -----------------------------------------------------

project = "Hazelcast Python Client"
copyright = "2024, Hazelcast, Inc."
author = "Hazelcast, Inc."
release = "0.1.0"
version = "0.1.0"

# -- General configuration ---------------------------------------------------

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "sphinx.ext.napoleon",
    "sphinx_autodoc_typehints",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# Source file suffix
source_suffix = ".rst"

# Master document
master_doc = "index"

# -- Options for autodoc -----------------------------------------------------

autodoc_default_options = {
    "members": True,
    "member-order": "bysource",
    "special-members": "__init__",
    "undoc-members": False,
    "exclude-members": "__weakref__",
    "show-inheritance": True,
}

autodoc_typehints = "description"
autodoc_typehints_format = "short"

# Don't skip __init__ docstrings
autoclass_content = "both"

# Generate autosummary
autosummary_generate = True

# -- Options for Napoleon (Google/NumPy docstrings) --------------------------

napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = True
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = True
napoleon_use_admonition_for_examples = True
napoleon_use_admonition_for_notes = True
napoleon_use_admonition_for_references = False
napoleon_use_ivar = False
napoleon_use_param = True
napoleon_use_rtype = True
napoleon_preprocess_types = True
napoleon_type_aliases = None
napoleon_attr_annotations = True

# -- Options for intersphinx -------------------------------------------------

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}

# -- Options for HTML output -------------------------------------------------

html_theme = "sphinx_rtd_theme"

html_theme_options = {
    "logo_only": False,
    "display_version": True,
    "prev_next_buttons_location": "bottom",
    "style_external_links": True,
    "collapse_navigation": False,
    "sticky_navigation": True,
    "navigation_depth": 4,
    "includehidden": True,
    "titles_only": False,
}

html_static_path = ["_static"]

html_context = {
    "display_github": True,
    "github_user": "hazelcast",
    "github_repo": "HazelPython",
    "github_version": "main",
    "conf_py_path": "/docs/",
}

# -- Options for LaTeX output ------------------------------------------------

latex_elements = {
    "papersize": "letterpaper",
    "pointsize": "10pt",
}

latex_documents = [
    (
        master_doc,
        "HazelcastPythonClient.tex",
        "Hazelcast Python Client Documentation",
        "Hazelcast, Inc.",
        "manual",
    ),
]

# -- Options for manual page output ------------------------------------------

man_pages = [
    (
        master_doc,
        "hazelcastpythonclient",
        "Hazelcast Python Client Documentation",
        [author],
        1,
    ),
]

# -- Options for Texinfo output ----------------------------------------------

texinfo_documents = [
    (
        master_doc,
        "HazelcastPythonClient",
        "Hazelcast Python Client Documentation",
        author,
        "HazelcastPythonClient",
        "Python client for Hazelcast in-memory data grid.",
        "Miscellaneous",
    ),
]
