#!/bin/sh
#usage ./mysqlimport.sh --host=$HOST --port=$PORT --user=$USER --pass=$PASS $DBNAME

mysqlimport $@ --local --columns=code,title,short_title,display_order,reuse_policy,reuse_policy_precedence --fields-terminated-by='\t' data_collection.txt
mysqlimport $@ --local --columns=code,description,short_title,table_display_order --fields-terminated-by='\t' analysis_group.txt
mysqlimport $@ --local --columns=superpopulation_id,code,name --fields-terminated-by='\t' superpopulation.txt
mysqlimport $@ --local --columns=superpopulation_id,code,name,description --fields-terminated-by='\t' population.txt
mysqlimport $@ --local --columns=code --fields-terminated-by='\t' data_type.txt
