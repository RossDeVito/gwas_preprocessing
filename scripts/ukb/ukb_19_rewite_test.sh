#!/bin/bash

rewrite-agg-table \
	-p '/mnt/project/rdevito/project1_data/chr19/agg_tables/chr19_common_lof_by_gene_id_oms.parquet/'
	-s /home/dnanexus/rr_chr19_common_lof_by_gene_id_oms.parquet
	-c 2