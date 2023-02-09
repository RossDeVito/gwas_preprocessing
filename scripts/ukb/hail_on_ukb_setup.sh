#!/bin/bash

# see https://stackoverflow.com/questions/73797873/importlib-metadata-packagenotfounderror-no-package-metadata-was-found-for-pypro
pip install --force-reinstall -v "pyproj==3.3.1"

# Git pull and fix ownership issure
git clone https://github.com/RossDeVito/gwas_qc
git config --global --add safe.directory /home/dnanexus/gwas_qc

# install gwas_qc
cd gwas_qc
pip install --user -e .
bgen-to-mt
pip install -e .
bgen-to-mt
