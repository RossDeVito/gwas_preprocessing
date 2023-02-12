#!/bin/bash

vep-anno \
	-v /mnt/project/rdevito/project1_data/subset/subset_rare_loci.tsv \
	-c /../../scripts/vep_specs/vep_ukb_rap.json \
	-o subset_vep_anno.ht \
	-u rdevito_p1_db
