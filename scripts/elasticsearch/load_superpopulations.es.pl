#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use Search::Elasticsearch;
use DBI;

my ($dbname, $dbhost, $dbuser, $dbport, $dbpass) = ('igsr_website_v2', 'mysql-igsr-web', 'g1kro', 4641, undef);
my $es_host = 'ves-hx-e4:9200';
my $es_index_name = 'igsr_beta';

&GetOptions(
  'dbpass=s'      => \$dbpass,
  'dbport=i'      => \$dbport,
  'dbuser=s'      => \$dbuser,
  'dbhost=s'      => \$dbhost,
  'dbname=s'      => \$dbname,
  'es_host=s' =>\$es_host,
  'es_index_name=s' =>\$es_index_name,
);
my $es = Search::Elasticsearch->new(nodes => $es_host, client => '1_0::Direct');

my $dbh = DBI->connect("DBI:mysql:$dbname;host=$dbhost;port=$dbport", $dbuser, $dbpass) or die $DBI::errstr;
my $select_all_superpops_sql = 'SELECT sp.*
    FROM superpopulation sp
    GROUP BY sp.superpopulation_id';
my $select_files_sql = 'SELECT dt.code data_type, ag.description analysis_group, dc.title data_collection, dc.data_collection_id, dc.reuse_policy
    FROM file f LEFT JOIN data_type dt ON f.data_type_id = dt.data_type_id
    LEFT JOIN analysis_group ag ON f.analysis_group_id = ag.analysis_group_id
    INNER JOIN sample_file sf ON sf.file_id=f.file_id
    INNER JOIN file_data_collection fdc ON f.file_id=fdc.file_id
    INNER JOIN data_collection dc ON fdc.data_collection_id=dc.data_collection_id
    INNER JOIN sample s ON sf.sample_id=s.sample_id
    WHERE s.population_id=?
    GROUP BY dt.data_type_id, ag.analysis_group_id, dc.data_collection_id';
my $sth_population = $dbh->prepare($select_all_superpops_sql) or die $dbh->errstr;
#my $sth_files = $dbh->prepare($select_files_sql) or die $dbh->errstr;

$sth_population->execute() or die $sth_population->errstr;
my %indexed_pops;
FILE:
while (my $row = $sth_population->fetchrow_hashref()) {
  my %es_doc = (
      code => $row->{code},
      name => $row->{name},
			display_colour => $row->{display_colour},
      display_order => $row->{display_order},
  );


  eval{$es->index(
    index => $es_index_name,
    type => 'superpopulation',
    id => $row->{elastic_id},
    body => \%es_doc,
  );};
  if (my $error = $@) {
    die "error indexing population in $es_index_name index:".$error->{text};
  }
  $indexed_pops{$row->{elastic_id}} = 1;
}

my $scroll = $es->scroll_helper(
    index => $es_index_name,
    type => 'superpopulation',
    search_type => 'scan',
    size => 500,
);
SCROLL:
while (my $es_doc = $scroll->next) {
  next SCROLL if $indexed_pops{$es_doc->{_id}};
  $es->delete(
    index => $es_index_name,
    type => 'superpopulation',
    id => $es_doc->{_id},
  );
}

=pod

=head1 NAME

igsr-code/scripts/elasticsearch/load_populations.es.pl

=head1 SYNONPSIS

This script is for creating the population index in elasticsearch. Run this script whenever you want to add, delete or modify populations in the portal.

The script roughly does this:

    Selects all rows in the mysql population table.
    Works out which samples, analysis_groups, data_types, data_collections are connected to the population via the sample, sample_file, file, and file_data_collection tables.
    Index all of those populations in elasticsearch
    Scans through all existing populations in elasticsearch. If it finds one that should not be there, then deletes it.

=head1 WHAT'S IN THE INDEX?

  # list them (10 only by default):
  curl http://www.internationalgenome.org/api/beta/population/_search | python -m json.tool
  # pick one out by name:
  curl http://www.internationalgenome.org/api/beta/population/GWD | python -m json.tool

=head1 OPTIONS

    -dbhost, the name of the mysql-host
    -dbname, the name of the mysql database
    -dbuser, the name of the mysql user
    -dbpass, the database password if appropriate
    -dbport, the port the mysql instance is running on
    -es_host, host and port of the elasticsearch index you are loading into, e.g. ves-hx-e3:9200
    -es_index_name, the elasticsearch index name, e.g. igsr_beta

=cut

