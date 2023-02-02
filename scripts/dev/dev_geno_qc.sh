#!/bin/bash

DATA_DIR='../../dev_data/proj_data'

compute-variant-qc \
	-m "$DATA_DIR/geno.mt" \
	-w "$DATA_DIR/geno_qc.ht"