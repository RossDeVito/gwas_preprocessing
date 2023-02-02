#!/bin/bash

GENO_DATA_DIR='/mnt/project/Bulk/Exome sequences/Population level exome OQFE variants, BGEN format - final release'

bgen-to-mt \
	-w chr19_geno.mt \
	-i \
	-s \
	-e /mnt/project/rdevito/project1_data/excluded_samples.tsv \
	-u rdevito_p1_db \
	--split_multi \
	-b "$GENO_DATA_DIR/ukb23159_c19_b0_v1.bgen"
	
