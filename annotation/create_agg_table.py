"""
Aggregate vap annotations into a table grouped by impacted feature and
predicted consequence.

A JSON file will contain information on how to filter and aggregate the
annotations. It should contain the following keys:

	"group_by_feature": One 'transcript_id', 'protein-id', 'gene_id', 
		or 'gene_symbol'. This is the genomic feature for which variants
		of different predicted consequences will be aggregated.
		NOTE: 'protein_id' contains missing values, so will first filter
		to only variants with a protein_id.

	"consequence": One of 'most_severe_csq' or 'lof'. This is the
		predicted consequence of the variant that will be counted per
		group_by_feature.
		NOTE: 'lof' contains missing values, so will first filter to only
		variants that are predicted some lof. If predicted lof variant
		was flagged by loftee, it's category will be "{loftee_csq}_flagged".

	"only_most_severe": True or False. If True, will only count variants
		with the most severe predicted consequence for a variant
		loci,allele pair.

	"only_protein_coding": True or False. If True, will only count
		variants where protein_coding field is True.

	"MAF_lt": Float. If included, will only count variants with
		MAF < MAF_lt.

	"MAF_geq": Float. If included, will only count variants with
		MAF >= MAF_geq.

"""
import argparse
import json
import os

import hail as hl
import pyarrow	# Won't be used directly,
import fsspec	# but better to crash now


def main():
	"""
	Aggregate vap annotations into a table grouped by impacted feature and
	predicted consequence.

	Args:
		mt_path (str, required): Path to saved QCed MatrixTable.
		anno_ht_path (str, required): Path to saved VEP annotated Hail Table.
		agg_json_path (str, required): Path to JSON file containing
			information on how to aggregate annotations.
		save_path (str, required): Path to save aggregated annotations as
			parquet file.
		ukb_db_name (str, optional): If not None, will init Hail with
			spark as required when using ukb RAP. For mt_path and
			anno_ht_path, will use path 'dnax://{ukb_db_name's id}/{mt_path}'
			and 'dnax://{ukb_db_name's id}/{anno_ht_path}'. For agg_json_path
			and save_path, will prepend 'file://'.
	"""

	parser = argparse.ArgumentParser()

	parser.add_argument(
		'-m',
		'--mt_path',
		type=str,
		required=True,
		help='Path to saved QCed MatrixTable.',
	)
	parser.add_argument(
		'-a',
		'--anno_ht_path',
		type=str,
		required=True,
		help='Path to saved VEP annotated Hail Table.',
	)
	parser.add_argument(
		'-j',
		'--agg_json_path',
		type=str,
		required=True,
		help='Path to JSON file containing information on how to aggregate annotations.',
	)
	parser.add_argument(
		'-s',
		'--save_path',
		type=str,
		required=True,
		help='Path to save aggregated annotations.',
	)
	parser.add_argument(
		'-u',
		'--ukb_db_name',
		type=str,
		default=None,
		help='If not None, will init Hail with spark as required when using'
			'ukb RAP. For mt_path and anno_ht_path, will use path'
			'"dnax://{ukb_db_name\'s id}/{mt_path}" and'
			'"dnax://{ukb_db_name\'s id}/{anno_ht_path}". For agg_json_path'
			'and save_path, will prepend "file://".',
	)

	args = parser.parse_args()

	# Init hail
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
		
		hl.init(sc=spark.sparkContext)

		mt_path = f'dnax://{db_uri}/{args.mt_path}'
		anno_ht_path = f'dnax://{db_uri}/{args.anno_ht_path}'
		agg_json_path = f'file://{args.agg_json_path}'
		save_path = f'file://{args.save_path}'
	else:
		hl.init()

		mt_path = args.mt_path
		anno_ht_path = args.anno_ht_path
		agg_json_path = args.agg_json_path
		save_path = args.save_path

	# Load agg_json
	with open(agg_json_path, 'r') as f:
		agg_json = json.load(f)

	# Load genotype data
	geno_mt = hl.read_matrix_table(mt_path)

	# Filter genotype data by MAF if either MAF_lt or MAF_geq is specified
	if 'MAF_lt' in agg_json:
		geno_mt = geno_mt.filter_rows(geno_mt.MAF < agg_json['MAF_lt'])

	if 'MAF_geq' in agg_json:
		geno_mt = geno_mt.filter_rows(geno_mt.MAF >= agg_json['MAF_geq'])

	# Load VEP annotations
	anno_ht = hl.read_table(anno_ht_path)

	# Filter annotations to just included variants
	anno_ht = anno_ht.semi_join(geno_mt.rows())

	# Apply additional filters if specified
	if 'only_most_severe' in agg_json and agg_json['only_most_severe'] is True:
		anno_ht = anno_ht.filter(anno_ht.is_most_severe)

	if 'only_protein_coding' in agg_json and agg_json['only_protein_coding'] is True:
		anno_ht = anno_ht.filter(anno_ht.protein_coding)

	# If specified 'group_by_feature' or 'consequence' needs filtering
	# of None values, do so
	if agg_json['group_by_feature'] == 'protein_id':
		anno_ht = anno_ht.filter(hl.is_defined(anno_ht.protein_id))

	if agg_json['consequence'] == 'lof':
		anno_ht = anno_ht.filter(hl.is_defined(anno_ht.lof))

		# Also create now field that combines predicted lof and if flagged
		anno_ht = anno_ht.annotate(
			lof=hl.delimit(
				[anno_ht.lof, hl.if_else(
						anno_ht.no_lof_flags,
						'',
						'_flagged'
					)
				],
				delimiter=''
			)
		)

	# Join genotype data to annotations
	geno_mt = geno_mt.annotate_rows(
		anno=anno_ht[geno_mt.row_key]
	)

	# Aggregate annotations
	agg_mt = geno_mt.group_rows_by(
		geno_mt.anno[agg_json['group_by_feature']],
		geno_mt.anno[agg_json['consequence']]
	).aggregate(
		var_count=hl.agg.sum(geno_mt.GT.n_alt_alleles())
	)

	# Reformat
	agg_table = agg_mt.entries()
	agg_table = agg_table.select(
		agg_table.var_count,
		feat_name=hl.delimit(
			[
				agg_table[agg_json['group_by_feature']], 
				agg_table[agg_json['consequence']]
			],
			delimiter='*-*'
		)
	)
	agg_table = agg_table.to_spark().to_pandas_on_spark()

	agg_table = agg_table.pivot(
		index='s', columns='feat_name', values='var_count'
	)

	# Save as parquet
	agg_table.to_parquet(
		save_path,
		index_col='s',
		partition_cols='s',
	)


if __name__ == '__main__':
	main()