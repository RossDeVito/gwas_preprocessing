#!/bin/bash

# see https://stackoverflow.com/questions/73797873/importlib-metadata-packagenotfounderror-no-package-metadata-was-found-for-pypro
pip install --force-reinstall -v "pyproj==3.3.1"

# For VEP anno
# pip install gnomad

# For parquet
pip install pyarrow fsspec

# Git pull and fix ownership issure
git clone https://github.com/RossDeVito/gwas_preprocessing
git config --global --add safe.directory /home/dnanexus/gwas_preprocessing

# install gwas_qc
cd gwas_preprocessing
pip install --user -e .
bgen-to-mt
pip install -e .
bgen-to-mt
pip install --user -e .
bgen-to-mt
