#!/bin/bash

ES_SCRIPTS=`dirname $0`/../scripts/elasticsearch

perl $ES_SCRIPTS/load_current_tree.mysql.pl \
  -dbpass $RESEQTRACK_PASS \
  -ftp -check_timestamp

perl $ES_SCRIPTS/load_files.es.pl \
  -dbpass $RESEQTRACK_PASS \
  -check_timestamp \
  -es_host ves-hx-e4 \
  -es_index_name igsr_beta

perl $ES_SCRIPTS/sync_hx_hh.es.pl \
  -dbpass $RESEQTRACK_PASS \
  -check_timestamp \
  -from_es_host ves-hx-e4 \
  -to ves-pg-e4 \
  -to ves-oy-e4 \
  -repo hx_hh_sync \
  -es_index_name igsr_beta
