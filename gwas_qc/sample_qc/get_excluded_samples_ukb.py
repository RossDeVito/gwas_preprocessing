"""Find and save samples in UKB that should be excluded from GWAS."""

import os

import dxpy
import dxdata


# This function is used to grab all field names (e.g. "p<field_id>_iYYY_aZZZ") of a list of field IDs
def field_names_for_ids(field_ids, participant_db):
	from distutils.version import LooseVersion
	fields = []
	for field_id in field_ids:
		field_id = 'p' + str(field_id)
		fields += participant_db.find_fields(
			name_regex=r'^{}(_i\d+)?(_a\d+)?$'.format(field_id)
		)
	return sorted(
		[field.name for field in fields], 
		key=lambda n: LooseVersion(n)
	)


if __name__ == '__main__':
	# Automatically discover dispensed dataset ID and load the dataset
	dispensed_dataset_id = dxpy.find_one_data_object(
		typename='Dataset',
		name='app*.dataset',
		folder='/',
		name_mode='glob'
	)['id']
	dataset = dxdata.load_dataset(id=dispensed_dataset_id)

	print(f'Dataset ID: {dispensed_dataset_id}')

	participant_db = dataset['participant']

	# Get fields used for preliminary exclusion of samples
	exclude_fields = [
		'22020', # met UKB PCA QC checks
		'22019', # sex chromosome aneuploidy
		'22027', # outliers for heterozygosity or missing rate
	]

	field_names = ['eid'] + field_names_for_ids(
		exclude_fields, participant_db
	)

	# Connect to Spark, retrieve data, and convert to DataFrame
	df = participant_db.retrieve_fields(
		names=field_names,
		coding_values='replace',
		engine=dxdata.connect(),
	)
	pdf = df.toPandas()

	# Get samples to exclude
	exclude_samples = pdf.loc[
		(~pdf['p22019'].isnull()) &
		(pdf['p22020'].isnull()) &
		(~pdf['p22027'].isnull())
	]['eid']

	# Save samples to exclude
	exclude_samples.to_csv(
		'excluded_samples.tsv',
		header=False,
		index=False,
		sep='\t',
	)

	# run dx upload excludes_samples.tsv
	os.system(
		'dx upload excluded_samples.tsv --path /rdevito/project1_data/excluded_samples.tsv'
	)
