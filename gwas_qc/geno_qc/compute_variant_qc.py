"""Use Hail to compute and save variant QC metrics for saved MatrixTable."""

import os
import argparse

import hail as hl


def main():
	"""Compute and save variant QC metrics for saved MatrixTable
	and save as Hail Table.
	
	Args:
		mt_path (str, required): Path to saved MatrixTable.
		write_path (str, required): Path to write variant QC metrics.
		ref_genome (str): Reference genome. Default: 'GRCh38'.
		ukb_db_name (str, or None): If not None, will init Hail with
			spark as required when using ukb RAP and will use
			dnax://{ukb_db_name's id}/{mt_path} as path to saved
			MatrixTable and dnax://{ukb_db_name's id}/{write_path} as
			path to write variant QC metrics.
		dp10_frac (bool, default False): If True (flag present), will
			compute fraction of samples with DP >= 10 as a sub-field
			of variant_qc.
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
		'-w',
		'--write_path',
		type=str,
		required=True,
		help='Path to write variant QC metrics.',
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
		"dnax://{ukb_db_name's id}/{mt_path} as path to saved"
		"MatrixTable and dnax://{ukb_db_name's id}/{write_path} as"
		'path to write variant QC metrics.',
	)
	parser.add_argument(
		'-d',
		'--dp10_frac',
		action='store_true',
		help='If True (flag present), will compute fraction of samples with DP >= 10 as a sub-field of variant_qc.',
	)

	args = parser.parse_args()

	# Initialize Hail
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
		write_path = f'dnax://{db_uri}/{args.write_path}'
	else:
		hl.init(
			default_reference=args.ref_genome,
		)
		mt_path = args.mt_path
		write_path = args.write_path

	# Load MatrixTable
	mt = hl.read_matrix_table(mt_path)

	# Compute variant QC metrics
	mt = hl.variant_qc(mt)

	# Percent with 10 read depth, suggested here:
	# dnanexus.gitbook.io/uk-biobank-rap/science-corner/whole-exome-sequencing-oqfe-protocol/generation-and-utilization-of-quality-control-set-90pct10dp-on-oqfe-data/details-on-processing-the-300k-exome-data-to-generate-the-quality-control-set
	if args.dp10_frac:
		mt = mt.annotate_rows(
			variant_qc=mt.variant_qc.annotate(pct_10dp=hl.agg.filter(
				hl.is_defined(mt.GT),
				hl.agg.fraction(mt.DP >= 10)
			))
		)

	# Save variant QC metrics
	var_qc = mt.select_rows('variant_qc').rows()
	var_qc.write(write_path, overwrite=True)


if __name__ == '__main__':
	main()