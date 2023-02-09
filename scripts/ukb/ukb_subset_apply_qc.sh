#!/bin/bash

apply-geno-qc \
	-m split_subset_geno.mt \
	-v split_subset_variant_qc.ht \
	-w subset_qced.mt \
	-l /home/dnanexus/subset_loci.tsv \
	-j ../../scripts/qc_params/dev_general.json \
	-u rdevito_p1_db

# Automatic uploads have to be fixed
# dx upload ~/subset_common_loci.tsv
