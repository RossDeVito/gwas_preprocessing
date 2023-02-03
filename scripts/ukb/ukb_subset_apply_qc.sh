#!/bin/bash

apply-geno-qc \
	-m split_subset_geno.mt \
	-v split_subset_variant_qc.ht \
	-w subset_common_qced.mt \
	-l /mnt/project/rdevito/project1_data/subset_common_loci.tsv \
	-j ../../scripts/qc_params/dev_common.json \
	-u rdevito_p1_db

apply-geno-qc \
	-m split_subset_geno.mt \
	-v split_subset_variant_qc.ht \
	-w subset_rare_qced.mt \
	-l /mnt/project/rdevito/project1_data/subset_rare_loci.tsv \
	-j ../../scripts/qc_params/dev_rare.json \
	-u rdevito_p1_db