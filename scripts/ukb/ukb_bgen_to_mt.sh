#!/bin/bash

GENO_DATA_DIR='/mnt/project/Bulk/Exome sequences/Population level exome OQFE variants, BGEN format - final release'

bgen-to-mt \
	-w chr19_geno.mt \
	-i \
	-s \
	-u rdevito_p1_db
	-b \
		# "$GENO_DATA_DIR/ukb23159_c1_b0_v1.bgen" \
		# "$GENO_DATA_DIR/ukb23159_c2_b0_v1.bgen" \
		# "$GENO_DATA_DIR/ukb23159_c3_b0_v1.bgen" \
		# "$GENO_DATA_DIR/ukb23159_c4_b0_v1.bgen" \
		# "$GENO_DATA_DIR/ukb23159_c5_b0_v1.bgen" \
		# "$GENO_DATA_DIR/ukb23159_c6_b0_v1.bgen" \
		# "$GENO_DATA_DIR/ukb23159_c7_b0_v1.bgen" \
		# "$GENO_DATA_DIR/ukb23159_c8_b0_v1.bgen" \
		# "$GENO_DATA_DIR/ukb23159_c9_b0_v1.bgen" \
		# "$GENO_DATA_DIR/ukb23159_c10_b0_v1.bgen" \
		# "$GENO_DATA_DIR/ukb23159_c11_b0_v1.bgen" \
		# "$GENO_DATA_DIR/ukb23159_c12_b0_v1.bgen" \
		# "$GENO_DATA_DIR/ukb23159_c13_b0_v1.bgen" \
		# "$GENO_DATA_DIR/ukb23159_c14_b0_v1.bgen" \
		# "$GENO_DATA_DIR/ukb23159_c15_b0_v1.bgen" \
		# "$GENO_DATA_DIR/ukb23159_c16_b0_v1.bgen" \
		# "$GENO_DATA_DIR/ukb23159_c17_b0_v1.bgen" \
		# "$GENO_DATA_DIR/ukb23159_c18_b0_v1.bgen" \
		# "$GENO_DATA_DIR/ukb23159_c19_b0_v1.bgen" \
		# "$GENO_DATA_DIR/ukb23159_c20_b0_v1.bgen" \
		"$GENO_DATA_DIR/ukb23159_c21_b0_v1.bgen" \
		"$GENO_DATA_DIR/ukb23159_c22_b0_v1.bgen" \
	
	
