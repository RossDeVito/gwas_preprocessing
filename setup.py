from setuptools import setup, find_packages

setup(
	name='gwas_qc',
	version='0.1.0',
	packages=find_packages(include=['gwas_qc']),
	entry_points={
		'console_scripts': [
			'bgen-to-mt = gwas_qc.geno_qc.bgen_to_mt:main',
		]
	}
)