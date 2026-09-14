# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
import sys
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SOURCE_DIRECTORY = REPOSITORY_ROOT / "BrAinPI"

sys.path.insert(0, str(SOURCE_DIRECTORY))

# Autodoc imports application modules. Select the committed templates
# explicitly so documentation builds never depend on deployment-only files and
# normal application startup never has to guess that it is running under Sphinx.
os.environ.setdefault(
    "BRAINPI_SETTINGS",
    str(SOURCE_DIRECTORY / "template_settings.ini"),
)
os.environ.setdefault(
    "BRAINPI_GROUPS",
    str(SOURCE_DIRECTORY / "template_groups.ini"),
)

project = 'BrainPi Document'
copyright = '2024, Alan M Watson, Kelin He'
author = 'Alan M Watson, Kelin He'
release = '1.0.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ["sphinx.ext.autodoc",
              "sphinx.ext.viewcode",
              "sphinx.ext.napoleon",
              "sphinx.ext.autosummary"]

# templates_path = ['_templates']
exclude_patterns = ['_build', '_templates', '_static','Thumbs.db', '.DS_Store']



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
# html_static_path = ['_static']
