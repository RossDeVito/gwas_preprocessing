from setuptools import setup, find_packages

setup(
	name='gwas_preprocessing',
	version='0.1.0',
	packages=find_packages(include=['qc', 'annotation']),
	entry_points={
		'console_scripts': [
			'bgen-to-mt = qc.geno_qc.bgen_to_mt:main',
			'compute-variant-qc = qc.geno_qc.compute_variant_qc:main',
			'apply-geno-qc = qc.geno_qc.apply_geno_qc:main',
			'vep-anno = annotation.vep_anno:main',
			'agg-anno = annotation.create_agg_table:main',
            'rewrite-agg-table = annotation.rewrite_agg_table:main',
		]
	}
)