""" Rewrite agg table parquet with known divisions, an index, and a
_metadata file. Speeds up downstream use of the table.

Suggested to do this before saving a parquet file agg table to storage.
"""

import argparse
import os

import dask.dataframe as dd
from dask.distributed import Client, progress


def main():
	"""
	Rewrite agg table parquet with known divisions, an index, and a
	_metadata file. Speeds up downstream use of the table.

	Suggested to do this before saving a parquet file agg table to storage.

	Args:
		path (str): Path to parquet file.
		save_path (str): Path to save rewritten parquet file.
		index_col (str): Column to use as index. Default is 's'.
		write_metadata (bool): If True, will write a _metadata file.
			Default is True.
		core_reduce_factor (float): Factor to reduce the number of cores
			used by dask (round(n / core_reduce_factor)). Default is 1.
			Useful if workers are running out of memory so you want more
			memory per worker.
		non_int_index (bool): If True, will not convert index to int.
	"""
    
	parser = argparse.ArgumentParser()

	parser.add_argument(
		'-p',
		'--path',
		type=str,
		required=True,
		help='Path to parquet file.',
	)
	parser.add_argument(
		'-s',
		'--save_path',
		type=str,
		required=True,
		help='Path to save rewritten parquet file.',
	)
	parser.add_argument(
		'-i',
		'--index_col',
		type=str,
		default='s',
		help='Column to use as index. Default is "s".',
	)
	parser.add_argument(
		'--no_metadata_file',
		action='store_true',
		help='If flag is set, will not write a _metadata file.',
	)
	parser.add_argument(
		'-c',
		'--core_reduce_factor',
		type=float,
		default=1.,
		help='Factor to reduce the number of cores used by dask '
		'(round(n / core_reduce_factor)). Default is 1. Useful if '
		'workers are running out of memory so you want more memory '
		'per worker.',
	)
	parser.add_argument(
		'--non_int_index',
		action='store_true',
		help='If flag is set, will not convert index to int.',
	)

	args = parser.parse_args()

	# Set up dask client
	n_cores = round(os.cpu_count() / args.core_reduce_factor)

	client = Client(
		processes=True,
		threads_per_worker=1,
		n_workers=n_cores,
		dashboard_address=':8787'
	)

	# Read in parquet file
	df = dd.read_parquet(args.path)

	# Set index
	if not args.non_int_index:
		df[args.index_col] = df[args.index_col].astype('int')
	
	df = df.set_index(args.index_col, partition_size=100 * 1000000)

	# Persist and show progress
	df = df.persist()
	progress(df)

	# Write parquet file
	df.to_parquet(
		args.save_path,
		write_index=True,
		overwrite=True,
		write_metadata=not args.no_metadata_file,
		compute=True,
	)

	# Close client
	client.close()


if __name__ == '__main__':
	main()