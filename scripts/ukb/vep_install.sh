#!/bin/bash

sudo apt install libdbi-perl

git clone https://github.com/Ensembl/ensembl-vep.git
cd ensembl-vep

perl INSTALL.pl