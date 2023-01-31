#!/bin/bash

bgen-to-mt \
	-b /mnt/project/Bulk/Exome sequences/Population level exome OQFE variants, BGEN format - final release/ \
	-w ../../dev_data/proj_data/geno.mt \
	-i \
	-s \
	-e ../../dev_data/proj_data/excluded_samples.tsv