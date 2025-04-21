# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))

project = 'DataPy'
copyright = '2025, MuliMuri'
author = 'MuliMuri'

release = 'unknow'
init_path = os.path.join(os.path.dirname(__file__), "..", "..", "src", "datapy", "__init__.py")
with open(init_path) as f:
    for line in f.readlines():
        if line.startswith("__version__"):
            delim = '"' if '"' in line else "'"
            release = line.split(delim)[1]
            break

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx_rtd_theme"
]

templates_path = ['_templates']

exclude_patterns = []

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'

html_static_path = []
