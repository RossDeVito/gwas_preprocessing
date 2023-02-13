#!/bin/bash

vep-anno \
	-v /mnt/project/rdevito/project1_data/chr19/chr19_loci.tsv \
	-c /opt/notebooks/gwas_preprocessing/scripts/vep_specs/vep_ukb_rap.json \
	-o chr19_vep_anno.ht \
	-u rdevito_p1_db
