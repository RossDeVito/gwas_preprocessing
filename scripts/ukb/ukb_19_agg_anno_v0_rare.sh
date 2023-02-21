#!/bin/bash

agg-anno \
	-m chr19_qced.mt \
	-a chr19_vep_anno.ht \
	-j /home/dnanexus/gwas_preprocessing/scripts/vep_agg_json/v0_rare_lof_by_gene_id_oms.json \
	-s /home/dnanexus/chr19_rare_lof_by_gene_id_oms.parquet \
	-u rdevito_p1_db

dx upload -r /home/dnanexus/chr19_rare_lof_by_gene_id_oms.parquet \
	--path "project-GG25fB8Jv7B928vqK7k6vYY6:/rdevito/project1_data/chr19/agg_tables/"