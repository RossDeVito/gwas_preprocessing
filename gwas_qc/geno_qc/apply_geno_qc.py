"""Apply variant QC, calculate and apply sample QC, and save.

Will save QC'd data as MT and locus/alleles as BGEN (to make it easy to
use with VEP on UKB RAP)
"""

import os
import argparse
import json

import hail as hl

"""
run apply_geno_qc.py -m split_subset_geno.mt -v split_subset_variant_qc.ht \
	-w split_subset_qced.mt -l /mnt/project/rdevito/project1_data/split_subset_qced_loc.vcf.gz \
	-j ../../scripts/qc_params/dev.json -u rdevito_p1_db
"""


if __name__ == '__main__':
	"""Apply variant QC, calculate and apply sample QC, and save.
	
	Args:
		mt_path (str, required): Path to saved MatrixTable.
		variant_qc_path (str, required): Path to saved variant QC
			Hail Table.
		write_path_gc_geno (str, required): Path to write QC'd data as MT.
		write_path_gc_locus_bgen (str): Path to write BGEN with
			locus/alleles.
		qc_params_json (str, required): Path to JSON file with QC
			parameters.
		ref_genome (str): Reference genome. Default: 'GRCh38'.
		ukb_db_name (str, or None): If not None, will init Hail with
			spark as required when using ukb RAP and will use
			dnax://{ukb_db_name's id}/{mt_path} as path to saved
			MatrixTable, dnax://{ukb_db_name's id}/{variant_qc_path}
			as path to saved variant QC Hail Table, and 
			dnax://{ukb_db_name's id}/{write_path_gc_geno} as path to
			write QC'd data as MT. If write_path_gc_locus_bgen is not
			None, will also use file://{write_path_gc_locus_bgen} as
			path to write VCF with locus/alleles.
	"""

	parser = argparse.ArgumentParser()

	parser.add_argument(
		'-m',
		'--mt_path',
		type=str,
		required=True,
		help='Path to saved MatrixTable.',
	)
	parser.add_argument(
		'-v',
		'--variant_qc_path',
		type=str,
		required=True,
		help='Path to saved variant QC Hail Table.',
	)
	parser.add_argument(
		'-w',
		'--write_path_gc_geno',
		type=str,
		required=True,
		help='Path to write QC\'d data as MT.',
	)
	parser.add_argument(
		'-l',
		'--write_path_gc_locus_bgen',
		type=str,
		default=None,
		help='Path to write BGEN with locus/alleles.',
	)
	parser.add_argument(
		'-j',
		'--qc_params_json',
		type=str,
		required=True,
		help='Path to JSON file with QC parameters.',
	)
	parser.add_argument(
		'-r',
		'--ref_genome',
		type=str,
		default='GRCh38',
		help='Reference genome. Default: GRCh38.',
	)
	parser.add_argument(
		'-u',
		'--ukb_db_name',
		type=str,
		default=None,
		help='Name of UKB database. If not None, will init Hail with'
			'spark as required when using ukb RAP and will use'
			'dnax://{ukb_db_name\'s id}/{mt_path} as path to saved'
			'MatrixTable, dnax://{ukb_db_name\'s id}/{variant_qc_path}'
			'as path to saved variant QC Hail Table, and '
			'dnax://{ukb_db_name\'s id}/{write_path_gc_geno} as path to'
			'write QC\'d data as MT. If write_path_gc_locus_vcf is not'
			'None, will also use file://{write_path_gc_locus_vcf} as'
			'path to write VCF with locus/alleles.',
	)
	args = parser.parse_args()

	# Init Hail
	if args.ukb_db_name is not None:
		import pyspark
		import dxpy

		db_uri = dxpy.find_one_data_object(
			name=args.ukb_db_name, 
			project=dxpy.find_one_project()["id"]
		)["id"]

		builder = (
			pyspark.sql.SparkSession
			.builder
			.enableHiveSupport()
			.config("spark.shuffle.mapStatus.compression.codec", "lz4") 
		)
		spark = builder.getOrCreate()
		
		hl.init(
			sc=spark.sparkContext, # sc
			default_reference=args.ref_genome,
			# tmp_dir=f'dnax://{args.ukb_db_name}/tmp/'
		)

		mt_path = f'dnax://{db_uri}/{args.mt_path}'
		variant_qc_path = f'dnax://{db_uri}/{args.variant_qc_path}'
		write_path = f'dnax://{db_uri}/{args.write_path_gc_geno}'
	else:
		hl.init(default_reference=args.ref_genome)
		mt_path = args.mt_path
		variant_qc_path = args.variant_qc_path
		write_path = args.write_path

	# Load QC parameters from JSON
	with open(args.qc_params_json, 'r') as f:
		qc_params = json.load(f)

	# Load data
	mt = hl.read_matrix_table(mt_path)

	# Load variant QC data
	variant_qc = hl.read_table(variant_qc_path)

	# Apply variant QC filters to variant_QC
	# var_mask = None

	# for qc_key, qc_val in qc_params['variant'].items():
	# 	if 'dp_stats' in qc_key:
	# 		if 'min' in qc_key:
	# 			var_mask = (
	# 				(variant_qc[qc_key] >= qc_val) &
	# 				var_mask
	# 			)

	variant_qc = variant_qc.filter(
		(variant_qc.variant_qc.call_rate >= qc_params['variant']['call_rate']) &
		(variant_qc.variant_qc.p_value_hwe >= qc_params['variant']['p_value_hwe'])
	)

	# Filter data to variants in variant_QC
	mt = mt.semi_join_rows(variant_qc)

	# Compute sample QC
	mt = hl.sample_qc(mt)

	# Apply sample QC filters to sample_QC
	mt = mt.filter_cols(
		(mt.sample_qc.call_rate >= qc_params['sample']['call_rate'])
	)

	# Drop sample QC data and save as MT
	mt = mt.drop('sample_qc')
	mt.write(write_path, overwrite=True)
	
	# Save locus/alleles as BGEN
	if args.write_path_gc_locus_bgen is not None:
		if args.ukb_db_name is not None:
			write_path_bgen = f'file://{args.write_path_gc_locus_vcf}'
		else:
			write_path_bgen = args.write_path_gc_locus_vcf
		sites_mt = hl.MatrixTable.from_rows_table(mt.rows())
		sites_mt = sites_mt.drop(
			*list(sites_mt.row_value.keys())
		)
		hl.export_bgen(
			sites_mt,
			write_path_bgen,
			compresssion_codec='zlib'	
		)

		# Index BGEN
		hl.index_bgen(write_path_bgen)