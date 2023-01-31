#!/bin/bash

bgen-to-mt \
	-b ../../dev_data/bulk_data/geno.bgen \
	-w ../../dev_data/proj_data/geno.mt \
	-i \
	-s \
	-e ../../dev_data/proj_data/excluded_samples.tsv