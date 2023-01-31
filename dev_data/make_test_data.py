"""Make test data."""

import os

import hail as hl
import pyspark
from pyspark.sql import SparkSession


if __name__ == '__main__':
	# builder = (
	# 	SparkSession
	# 	.builder
	# 	# .enableHiveSupport()
	# )
	# spark = builder.getOrCreate()
	# hl.init(sc=spark.sparkContext)
	hl.init(
		default_reference='GRCh38',
		global_seed=147,
	)

	# Simulate with balding nichols
	bn_mt = hl.balding_nichols_model(4, 1000, 1000,
         pop_dist=[0.1, 0.2, 0.3, 0.4],
         fst=[.02, .06, .04, .12],
         af_dist=hl.rand_beta(a=0.01, b=2.0, lower=0.001, upper=1.0))

	# Rename column key to 's' and make it a string
	bn_mt = bn_mt.key_cols_by(s=hl.str(bn_mt.sample_idx))

	# Create demo bulk_data dir
	bulk_data_dir = 'bulk_data'
	if not os.path.exists(bulk_data_dir):
		os.mkdir(bulk_data_dir)

	# Save as BGEN
	hl.export_bgen(
		bn_mt, 
		os.path.join(bulk_data_dir, 'geno'),
		compression_codec='zstd'
	)