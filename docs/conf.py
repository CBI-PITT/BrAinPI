# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
import sys
from sphinx.ext.apidoc import main as sphinx_apidoc_main

sys.path.insert(0, os.path.abspath('../BrAinPI'))
print(sys.path)

def run_apidoc(_):
    module_path = os.path.abspath('../BrAinPI')
    output_path = os.path.abspath('.')
    sphinx_apidoc_main([
        '-o', output_path, module_path, '--force'
    ])

def setup(app):
    app.connect('builder-inited', run_apidoc)

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

