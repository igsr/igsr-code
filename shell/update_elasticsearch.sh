#!/bin/bash

# This is the cron job that is run every night by user reseq_adm

ES_SCRIPTS=`dirname $0`/../scripts/elasticsearch

# Load the most recent current tree into the mysql database.
# Exit early if the current tree has not changed recently.
perl $ES_SCRIPTS/load_current_tree.mysql.pl \
  -dbpass $RESEQTRACK_PASS \
  -ftp -check_timestamp


# Create the file index in elasticsearch. Exit early if the current tree has not changed recently.
perl $ES_SCRIPTS/load_files.es.pl \
  -dbpass $RESEQTRACK_PASS \
  -check_timestamp \
  -es_host ves-hx-e4 \
  -es_index_name igsr_beta

# Take a snapshot of the index to disk in Hinxton. Copy the snapshot to Hemel.
# Restore the new snapshot into elasticsearch in Hemel.
perl $GCA_ELASTICSEARCH/scripts/sync_hx_hh.es.pl \
  -from ves-hx-e4 \
  -to ves-pg-e4 \
  -to ves-oy-e4 \
  -repo igsr_repo \
  -snap_index igsr_beta \
  -snap_index igsr \
  -restore_only igsr_beta
