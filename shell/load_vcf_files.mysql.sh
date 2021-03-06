#!/bin/bash

# This shell script contains a record of the exact command lines that were used
# to call the perl script when the database was first loaded in April 2016.
# This script is helpful to demonstrate how command lines could be constructed.

ES_SCRIPTS=`dirname $0`/../scripts/elasticsearch

perl $ES_SCRIPTS/load_vcf_file.mysql.pl \
  -data_collection phase3 \
  -dbpass $RESEQTRACK_PASS \
  -analysis_group integrated_calls \
  -file /nfs/1000g-archive/vol1/ftp/release/20130502/ALL.chr10.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/release/20130502/ALL.chr11.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/release/20130502/ALL.chr12.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/release/20130502/ALL.chr13.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/release/20130502/ALL.chr14.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/release/20130502/ALL.chr15.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/release/20130502/ALL.chr16.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/release/20130502/ALL.chr17.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/release/20130502/ALL.chr18.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/release/20130502/ALL.chr19.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/release/20130502/ALL.chr1.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/release/20130502/ALL.chr20.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/release/20130502/ALL.chr21.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/release/20130502/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/release/20130502/ALL.chr2.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/release/20130502/ALL.chr3.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/release/20130502/ALL.chr4.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/release/20130502/ALL.chr5.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/release/20130502/ALL.chr6.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/release/20130502/ALL.chr7.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/release/20130502/ALL.chr8.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/release/20130502/ALL.chr9.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/release/20130502/ALL.chrMT.phase3_callmom.20130502.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/release/20130502/ALL.chrX.phase3_shapeit2_mvncall_integrated_v1b.20130502.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/release/20130502/ALL.chrY.phase3_integrated_v2a.20130502.genotypes.vcf.gz

perl $ES_SCRIPTS/load_vcf_file.mysql.pl \
  -data_collection phase1 \
  -dbpass $RESEQTRACK_PASS \
  -analysis_group integrated_calls \
  -file /nfs/1000g-archive/vol1/ftp/phase1/analysis_results/integrated_call_sets/ALL.chr10.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/phase1/analysis_results/integrated_call_sets/ALL.chr11.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/phase1/analysis_results/integrated_call_sets/ALL.chr12.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/phase1/analysis_results/integrated_call_sets/ALL.chr13.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/phase1/analysis_results/integrated_call_sets/ALL.chr14.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/phase1/analysis_results/integrated_call_sets/ALL.chr15.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/phase1/analysis_results/integrated_call_sets/ALL.chr16.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/phase1/analysis_results/integrated_call_sets/ALL.chr17.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/phase1/analysis_results/integrated_call_sets/ALL.chr18.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/phase1/analysis_results/integrated_call_sets/ALL.chr19.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/phase1/analysis_results/integrated_call_sets/ALL.chr1.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/phase1/analysis_results/integrated_call_sets/ALL.chr20.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/phase1/analysis_results/integrated_call_sets/ALL.chr21.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/phase1/analysis_results/integrated_call_sets/ALL.chr22.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/phase1/analysis_results/integrated_call_sets/ALL.chr2.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/phase1/analysis_results/integrated_call_sets/ALL.chr3.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/phase1/analysis_results/integrated_call_sets/ALL.chr4.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/phase1/analysis_results/integrated_call_sets/ALL.chr5.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/phase1/analysis_results/integrated_call_sets/ALL.chr6.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/phase1/analysis_results/integrated_call_sets/ALL.chr7.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/phase1/analysis_results/integrated_call_sets/ALL.chr8.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/phase1/analysis_results/integrated_call_sets/ALL.chr9.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/phase1/analysis_results/integrated_call_sets/ALL.chrMT.phase1_samtools_si.20101123.snps.low_coverage.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/phase1/analysis_results/integrated_call_sets/ALL.chrX.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/phase1/analysis_results/integrated_call_sets/ALL.chrY.genome_strip_hq.20101123.svs.low_coverage.genotypes.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/phase1/analysis_results/integrated_call_sets/ALL.chrY.phase1_samtools_si.20101123.snps.low_coverage.genotypes.vcf.gz \

perl $ES_SCRIPTS/load_vcf_file.mysql.pl \
  -data_collection phase3 \
  -analysis_group hd_genotype_chip \
  -dbpass $RESEQTRACK_PASS \
  -file /nfs/1000g-archive/vol1/ftp/release/20130502/supporting/hd_genotype_chip/ALL.wgs.nhgri_coriell_affy_6.20140825.genotypes_no_ped.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/release/20130502/supporting/hd_genotype_chip/ALL.wgs.nhgri_coriell_affy_6.20140825.genotypes_no_ped.vcf.gz \
  -file /nfs/1000g-archive/vol1/ftp/release/20130502/supporting/hd_genotype_chip/ALL.chip.omni_broad_sanger_combined.20140818.snps.genotypes.vcf.gz
