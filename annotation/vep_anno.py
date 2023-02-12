"""Annotate loci,allele pairs with VEP"""

import argparse
import os
import json

import hail as hl
import gnomad


def main():
	"""Annotate loci,variant pairs with VEP, preprocess, and save
	as Hail Table.
	
	Args:
		variant_table (str): Path to table with columns 'locus' and 'alleles'.
		vep_config (str): Path to VEP configuration JSON file.
		output_ht (str): Path to output Hail table.
		ref_genome (str): Reference genome. Default: 'GRCh38'.
		ukb_db_name (str, or None): If not None, will init Hail with
			spark as required when using ukb RAP and will use 'file://'
			before variant_table and vep_config. For output_ht will use
			dnax://{ukb_db_name's id}/{output_ht}.
	"""

	parser = argparse.ArgumentParser()

	parser.add_argument(
		'-v',
		'--variant_table',
		type=str,
		required=True,
		help='Path to table with columns "locus" and "alleles".',
	)
	parser.add_argument(
		'-c',
		'--vep_config',
		type=str,
		required=True,
		help='Path to VEP configuration JSON file.',
	)
	parser.add_argument(
		'-o',
		'--output_ht',
		type=str,
		required=True,
		help='Path to output Hail table.',
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
		'spark as required when using ukb RAP and will use "file://"'
		'before variant_table and vep_config. For output_ht will use'
		"dnax://{ukb_db_name's id}/{output_ht}.",
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
		)
		file_prefix = 'file://'
	else:
		hl.init(
			default_reference=args.ref_genome,
		)
		file_prefix = ''

	# Load variant table
	dtype_map = {'locus': hl.tstr, 'alleles': hl.tarray(hl.tstr)}
	variant_table = hl.import_table(
		file_prefix + args.variant_table,
		types=dtype_map,
	}

	variant_table = variant_table.annotate(
		locus=hl.parse_locus(variant_table.locus)
	)
	variant_table = variant_table.key_by('locus', 'alleles')

	# Annotate
	anno_table = hl.vep(
		variant_table,
		file_prefix + args.vep_config,
	)

	# Process results

	# Filter to just canonical variants with some consequence
	anno_table = gnomad.utils.vep.filter_vep_transcript_csqs(
		anno_table,
		synonymous=False,
		canonical=True,
		filter_empty_csq=True
	)
	anno_table = gnomad.utils.vep.get_most_severe_consequence_for_summary(
		anno_table
	)

	# Explode by consequence for loci with multiple consequences
	anno_table = anno_table.explode(anno_table.vep.transcript_consequences)

	# Label most severe consequences
	anno_table = anno_table.annotate(
		is_most_severe=exp_mt.vep.transcript_consequences.consequence_terms.contains(
			exp_mt.most_severe_csq
		)
	)

	# Get consequences and info on what transcript, protein, gene, etc is impacted
	anno_table = anno_table.select(
		anno_table.most_severe_csq,
		anno_table.protein_coding,
		anno_table.lof,
		anno_table.no_lof_flags,
		anno_table.is_most_severe,
		transcript_id=anno_table.vep.transcript_consequences.transcript_id,
		protein_id=anno_table.vep.transcript_consequences.protein_id,
		gene_id=anno_table.vep.transcript_consequences.gene_id,
		gene_symbol=anno_table.vep.transcript_consequences.gene_symbol,
		biotype=anno_table.vep.transcript_consequences.biotype
	)

	# Save as Hail Table
	if args.ukb_db_name is not None:
		print(
			"Writing to dnax://{}/{}".format(db_uri, args.output_ht),
			flush=True
		)
		anno_table.write(
			f'dnax://{db_uri}/{args.output_ht}',
			overwrite=True,
		)
	else:
		anno_table.write(args.output_ht, overwrite=True)


if __name__ == '__main__':
	main()