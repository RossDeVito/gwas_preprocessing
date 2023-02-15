#!/bin/bash

agg-anno \
	-m chr19_qced.mt \
	-a chr19_vep_anno.ht \
	-j ../../scripts/vep_agg_json/common_lof_by_gene_id_oms.json \
	-s /home/dnanexus/chr19_common_lof_by_gene_id_oms.parquet \
	-u rdevito_p1_db
