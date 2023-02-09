#!/bin/bash

apply-geno-qc \
	-m chr19_geno.mt \
	-v chr19_variant_qc.ht \
	-w chr19_qced.mt \
	-l /home/dnanexus/chr19_loci.tsv \
	-j ../../scripts/qc_params/ukb_v1.json \
	-u rdevito_p1_db

# Automatic uploads have to be fixed
# dx upload ~/chr19_loci.tsv
