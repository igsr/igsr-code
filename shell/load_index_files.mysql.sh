#!/bin/bash

ES_SCRIPTS=`dirname $0`/../scripts/elasticsearch

perl $ES_SCRIPTS/load_index_file.mysql.pl \
  -file /nfs/1000g-archive/vol1/ftp/data_collections/1000_genomes_project/1000genomes.sequence.index \
  -data_collection 1000genomes \
  -data_type sequence \
  -use_column_headers \
  -url_column FASTQ_ENA_PATH \
  -md5_column MD5 \
  -withdrawn_column WITHDRAWN \
  -sample_column SAMPLE_NAME \
  -analysis_group_column ANALYSIS_GROUP \
  -dbpass $RESEQTRACK_PASS

perl $ES_SCRIPTS/load_index_file.mysql.pl \
  -file /nfs/1000g-archive/vol1/ftp/data_collections/1000_genomes_project/1000genomes.high_coverage.GRCh38DH.alignment.index \
  -data_collection 1000genomes \
  -data_type alignment \
  -use_column_headers \
  -url_column CRAM \
  -md5_column CRAM_MD5 \
  -url_column CRAI \
  -md5_column CRAI_MD5 \
  -url_column BAS \
  -md5_column BAS_MD5 \
  -infer_sample \
  -analysis_group high_coverage \
  -dbpass $RESEQTRACK_PASS

perl $ES_SCRIPTS/load_index_file.mysql.pl \
  -file /nfs/1000g-archive/vol1/ftp/data_collections/1000_genomes_project/1000genomes.low_coverage.GRCh38DH.alignment.index \
  -data_collection 1000genomes \
  -data_type alignment \
  -use_column_headers \
  -url_column CRAM \
  -md5_column CRAM_MD5 \
  -url_column CRAI \
  -md5_column CRAI_MD5 \
  -url_column BAS \
  -md5_column BAS_MD5 \
  -infer_sample \
  -analysis_group low_coverage \
  -dbpass $RESEQTRACK_PASS

perl $ES_SCRIPTS/load_index_file.mysql.pl \
  -file /nfs/1000g-archive/vol1/ftp/data_collections/1000_genomes_project/1000genomes.exome.GRCh38DH.alignment.index \
  -data_collection 1000genomes \
  -data_type alignment \
  -use_column_headers \
  -url_column CRAM \
  -md5_column CRAM_MD5 \
  -url_column CRAI \
  -md5_column CRAI_MD5 \
  -url_column BAS \
  -md5_column BAS_MD5 \
  -infer_sample \
  -analysis_group exome \
  -dbpass $RESEQTRACK_PASS

perl $ES_SCRIPTS/load_index_file.mysql.pl \
  -file /nfs/1000g-archive/vol1/ftp/data_collections/hgsv_sv_discovery/illumina_rna.sequence.index \
  -data_collection hgsv_sv_discovery \
  -data_type sequence \
  -use_column_headers \
  -url_column ENA_FILE_PATH \
  -md5_column MD5SUM \
  -sample_column SAMPLE_NAME \
  -analysis_group_column ANALYSIS_GROUP \
  -dbpass $RESEQTRACK_PASS

perl $ES_SCRIPTS/load_index_file.mysql.pl \
  -file /nfs/1000g-archive/vol1/ftp/data_collections/hgsv_sv_discovery/illumina_wgs.GRCh38.alignment.index \
  -data_collection hgsv_sv_discovery \
  -data_type alignment \
  -use_column_headers \
  -url_column CRAM \
  -md5_column CRAM_MD5 \
  -url_column CRAI \
  -md5_column CRAI_MD5 \
  -url_column BAS \
  -md5_column BAS_MD5 \
  -infer_sample \
  -analysis_group high_coverage \
  -dbpass $RESEQTRACK_PASS

perl $ES_SCRIPTS/load_index_file.mysql.pl \
  -file /nfs/1000g-archive/vol1/ftp/data_collections/hgsv_sv_discovery/illumina_wgs.sequence.index \
  -data_collection hgsv_sv_discovery \
  -data_type sequence \
  -use_column_headers \
  -url_column ENA_FILE_PATH \
  -md5_column MD5SUM \
  -sample_column SAMPLE_NAME \
  -analysis_group_column ANALYSIS_GROUP \
  -dbpass $RESEQTRACK_PASS

perl $ES_SCRIPTS/load_index_file.mysql.pl \
  -file /nfs/1000g-archive/vol1/ftp/data_collections/hgsv_sv_discovery/illumina_wgs.sequence.index \
  -data_collection hgsv_sv_discovery \
  -data_type sequence \
  -use_column_headers \
  -url_column ENA_FILE_PATH \
  -md5_column MD5SUM \
  -sample_column SAMPLE_NAME \
  -analysis_group_column ANALYSIS_GROUP \
  -dbpass $RESEQTRACK_PASS

perl $ES_SCRIPTS/load_index_file.mysql.pl \
  -file /nfs/1000g-archive/vol1/ftp/data_collections/hgsv_sv_discovery/smrt.sequence.index \
  -data_collection hgsv_sv_discovery \
  -data_type sequence \
  -use_column_headers \
  -url_column ENA_FILE_PATH \
  -md5_column MD5SUM \
  -sample_column SAMPLE_NAME \
  -analysis_group_column ANALYSIS_GROUP \
  -dbpass $RESEQTRACK_PASS

perl $ES_SCRIPTS/load_index_file.mysql.pl \
  -file /nfs/1000g-archive/vol1/ftp/data_collections/illumina_platinum_pedigree/illumina_platinum_ped.sequence.index \
  -data_collection illumina_platinum_ped \
  -data_type sequence \
  -use_column_headers \
  -url_column FASTQ_FILE \
  -md5_column MD5 \
  -sample_column SAMPLE_NAME \
  -withdrawn_column WITHDRAWN \
  -analysis_group_column ANALYSIS_GROUP \
  -dbpass $RESEQTRACK_PASS

perl $ES_SCRIPTS/load_index_file.mysql.pl \
  -file /nfs/1000g-archive/vol1/ftp/data_collections/illumina_platinum_pedigree/illumina_platinum_ped.GRCh38DH.alignment.index \
  -data_collection illumina_platinum_ped \
  -data_type alignment \
  -use_column_headers \
  -url_column CRAM_FILE \
  -md5_column CRAM_MD5 \
  -url_column CRAI_FILE \
  -md5_column CRAI_MD5 \
  -url_column BAS_FILE \
  -md5_column BAS_MD5 \
  -infer_sample \
  -analysis_group illumina_platinum_ped \
  -dbpass $RESEQTRACK_PASS

perl $ES_SCRIPTS/load_index_file.mysql.pl \
  -file /nfs/1000g-archive/vol1/ftp/phase3/20130502.phase3.sequence.index \
  -data_collection phase3 \
  -data_type sequence \
  -use_column_headers \
  -url_column FASTQ_FILE \
  -md5_column MD5 \
  -sample_column SAMPLE_NAME \
  -withdrawn_column WITHDRAWN \
  -analysis_group_column ANALYSIS_GROUP \
  -root ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase3/ \
  -dbpass $RESEQTRACK_PASS

perl $ES_SCRIPTS/load_index_file.mysql.pl \
  -file /nfs/1000g-archive/vol1/ftp/phase3/20130502.phase3.exome.alignment.index \
  -data_collection phase3 \
  -data_type alignment \
  -use_column_headers \
  -url_column "BAM FILE" \
  -md5_column "BAM MD5" \
  -url_column "BAI FILE" \
  -url_column "BAI FILE" \
  -md5_column "BAS MD5" \
  -md5_column "BAS MD5" \
  -infer_sample \
  -analysis_group exome \
  -root ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase3/ \
  -dbpass $RESEQTRACK_PASS

perl $ES_SCRIPTS/load_index_file.mysql.pl \
  -file  /nfs/1000g-archive/vol1/ftp/phase3/20130502.phase3.low_coverage.alignment.index \
  -data_collection phase3 \
  -data_type alignment \
  -use_column_headers \
  -url_column "BAM FILE" \
  -md5_column "BAM MD5" \
  -url_column "BAI FILE" \
  -url_column "BAI FILE" \
  -md5_column "BAS MD5" \
  -md5_column "BAS MD5" \
  -infer_sample \
  -analysis_group exome \
  -root ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase3/ \
  -dbpass $RESEQTRACK_PASS

perl $ES_SCRIPTS/load_index_file.mysql.pl \
  -file /nfs/1000g-archive/vol1/ftp/phase3/20130725.phase3.cg_sra.index \
  -data_collection phase3 \
  -data_type sequence \
  -use_column_headers \
  -url_column "SRA Relative Path" \
  -sample_column Sample \
  -analysis_group cg \
  -root ftp://ftp-trace.ncbi.nih.gov/sra/sra-instant/reads/ByRun/sra/SRR/ \
  -dbpass $RESEQTRACK_PASS

perl $ES_SCRIPTS/load_index_file.mysql.pl \
  -file /nfs/1000g-archive/vol1/ftp/phase3/20130820.phase3.cg_data_index \
  -data_collection phase3 \
  -url_column 0 \
  -md5_column 1 \
  -analysis_group cg \
  -infer_sample \
  -root ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase3/ \
  -dbpass $RESEQTRACK_PASS

perl $ES_SCRIPTS/load_index_file.mysql.pl \
  -file /nfs/1000g-archive/vol1/ftp/phase1/phase1.alignment.index \
  -data_collection phase1 \
  -data_type alignment \
  -use_column_headers \
  -url_column "BAM FILE" \
  -md5_column "BAM MD5" \
  -url_column "BAI FILE" \
  -url_column "BAI FILE" \
  -md5_column "BAS MD5" \
  -md5_column "BAS MD5" \
  -infer_sample \
  -analysis_group low_coverage \
  -root ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase1/ \
  -dbpass $RESEQTRACK_PASS

perl $ES_SCRIPTS/load_index_file.mysql.pl \
  -file   /nfs/1000g-archive/vol1/ftp/phase1/phase1.exome.alignment.index \
  -data_collection phase1 \
  -data_type alignment \
  -use_column_headers \
  -url_column "BAM FILE" \
  -md5_column "BAM MD5" \
  -url_column "BAI FILE" \
  -url_column "BAI FILE" \
  -md5_column "BAS MD5" \
  -md5_column "BAS MD5" \
  -infer_sample \
  -analysis_group exome \
  -root ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase1/ \
  -dbpass $RESEQTRACK_PASS

perl $ES_SCRIPTS/load_index_file.mysql.pl \
  -file /nfs/1000g-archive/vol1/ftp/pilot_data/pilot_data.sequence.index \
  -data_collection pilot \
  -data_type sequence \
  -url_column 0 \
  -md5_column 1 \
  -sample_column 9 \
  -withdrawn_column 20 \
  -analysis_group pilot_data \
  -root ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/pilot_data/ \
  -dbpass $RESEQTRACK_PASS

perl $ES_SCRIPTS/load_index_file.mysql.pl \
  -file /nfs/1000g-archive/vol1/ftp/pilot_data/pilot_data.alignment.index \
  -data_collection pilot \
  -data_type alignment \
  -url_column 0 \
  -md5_column 1 \
  -url_column 4 \
  -md5_column 5 \
  -url_column 6 \
  -md5_column 7 \
  -sample_column 3 \
  -analysis_group pilot_data \
  -root ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/pilot_data/ \
  -dbpass $RESEQTRACK_PASS
