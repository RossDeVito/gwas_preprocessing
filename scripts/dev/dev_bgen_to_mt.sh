#!/bin/bash

GENO_DATA_DIR='../../dev_data/bulk_data'

bgen-to-mt \
	-w ../../dev_data/proj_data/geno.mt \
	-i \
	-s \
	-e ../../dev_data/proj_data/excluded_samples.tsv \
	-b "$GENO_DATA_DIR/geno.bgen"