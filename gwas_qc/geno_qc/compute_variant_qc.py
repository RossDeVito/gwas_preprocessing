"""Use Hail to compute and save variant QC metrics for saved MatrixTable."""

import os
import argparse

import hail as hl


if __name__ == '__main__':
	"""Compute and save variant QC metrics for saved MatrixTable.
	
	Args:
		mt_path (str, required): Path to saved MatrixTable.
		write_path (str, required): Path to write variant QC metrics.
		ref_genome (str): Reference genome. Default: 'GRCh38'.
		
	"""