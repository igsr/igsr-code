#!/bin/bash

source ~/.bashrc

# This is the cron job that is run every night by user vg_igsr_adm

# Load the most recent current tree into the mysql database.
# Exit early if the current tree has not changed recently.
perl $ES_SCRIPTS/load_current_tree.mysql.pl \
  -dbpass $RESEQTRACK_PASS \
  -ftp -check_timestamp


# Create the file index in elasticsearch. Exit early if the current tree has not changed recently.
perl $ES_SCRIPTS/load_files.es.pl \
  -dbpass $RESEQTRACK_PASS \
  -check_timestamp \
  -es_host wp-np2-1b \
  -es_index_name igsr_beta

# Take a snapshot of the index to disk in Hinxton. Copy the snapshot to Hemel.
# Restore the new snapshot into elasticsearch in Hemel.
perl $GCA_ELASTICSEARCH/scripts/sync_hx_hh.es.pl \
  -from_es_host wp-np2-1a \
  -to_es_host wp-p1m-a1 \
  -to_es_host wp-p2m-3a \
  -repo igsr_repo \
  -snap_index igsr_beta \
  -restore_only igsr_beta

echo "Finishing updating the ES index"
