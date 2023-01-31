"""Save BGEN file as MatrixTable."""

import argparse
import os

import hail as hl
import pandas as pd


def main():
	"""
	Args:
		bgen_path (list, required): Path to BGEN file.
		write_path (str, required): Path to write MatrixTable.
		ref_genome (str): Reference genome. Default: 'GRCh38'.
		index_bgen_local (bool): If flag included, creates local copy
			BGEN index. If BGEN files must have corresponding index in
			same directory, don't use flag (False).
		sample_file (bool): If -s flag is set, will include BGEN .sample
			file in BGEN read command using sample_path.
		sample_path (str): Path to sample file. If None, assumes .sample
			file is in same directory as BGEN file and has same name as
			BGEN file (pre '.') with '.sample' extension. Ignored if
			sample_file is False.
		exclude_samples (str): Path to containing sample IDs (one
			per line) to exclude from MT output. Ignored if
			sample_file is None (default).
		sample_idx_name (str, deault: 's'): Name of sample index
			column in BGEN file. Default: 's'. Only used if exclude_samples
			is not None.
	"""

	parser = argparse.ArgumentParser()

	parser.add_argument(
		'-b',
		'--bgen_path',
		type=str,
		required=True,
		help='Path to BGEN file.',
	)
	parser.add_argument(
		'-w',
		'--write_path',
		type=str,
		required=True,
		help='Path to write MatrixTable.',
	)
	parser.add_argument(
		'-r',
		'--ref_genome',
		type=str,
		default='GRCh38',
		help='Reference genome. Default: GRCh38.',
	)
	parser.add_argument(
		'-i',
		'--index_bgen_local',
		default=False,
		action='store_true',
		help='If True (flag present), creates local copy BGEN index. If BGEN '
		'files must have corresponding index in same directory, do not '
		'include flag.'
	)
	parser.add_argument(
		'-s',
		'--sample_file',
		default=False,
		action='store_true',
		help='If -s flag is set, will include sample file in BGEN read '
		'command using sample_path.',
	)
	parser.add_argument(
		'-p',
		'--sample_path',
		type=str,
		default=None,
		help='Path to sample file. If None, assumes sample file is in '
		'same directory as BGEN file and has same name as BGEN file '
		'(pre ".") with ".sample" extension. Ignored if sample_file is '
		'False.',
	)
	parser.add_argument(
		'-e',
		'--exclude_samples',
		type=str,
		default=None,
		help='Path to containing sample IDs (one per line) to exclude '
		'from MT output. Ignored if sample_file is None (default).',
	)
	parser.add_argument(
		'-n',
		'--sample_idx_name',
		type=str,
		default='s',
		help='Name of sample index column in BGEN file. Default: s. '
		'Only used if exclude_samples is not None.',
	)

	args = parser.parse_args()

	# Initialize Hail
	hl.init(
		default_reference=args.ref_genome,
	)

	# Create BGEN indices if necessary
	if args.index_bgen_local:
		bgen_to_idx = {
			bgen_path: os.path.basename(bgen_path) + '.idx2'
			for bgen_path in [args.bgen_path]
		}
		hl.index_bgen(
			args.bgen_path,
			index_file_map=bgen_to_idx,
		)

	# Create sample file path if necessary
	if args.sample_file:
		if args.sample_path is None:
			args.sample_path = os.path.join(
				os.path.dirname(args.bgen_path),
				os.path.basename(args.bgen_path).split('.')[0] + '.sample',
			)

	# Load BGEN as MatrixTable
	bgen_mt = hl.import_bgen(
		args.bgen_path,
		index_file_map=bgen_to_idx if args.index_bgen_local else None,
		entry_fields=['GT', 'GP', 'dosage'],
		sample_file=args.sample_path if args.sample_file else None,
	)

	# Load sample IDs to exclude
	if args.exclude_samples is not None:
		excluded_samples = pd.read_csv(
			args.exclude_samples,
			header=None,
			names=[args.sample_idx_name],
		)

		# Make string format
		excluded_samples[args.sample_idx_name] = (
			excluded_samples[args.sample_idx_name].astype(str)
		)

		# Convert to Hail Table
		excluded_samples = hl.Table.from_pandas(excluded_samples)
		excluded_samples = excluded_samples.key_by(
			s=excluded_samples[args.sample_idx_name],
		)

		# Filter out excluded samples
		bgen_mt = bgen_mt.anti_join_cols(excluded_samples)

	# Write MatrixTable
	bgen_mt.write(args.write_path, overwrite=True)