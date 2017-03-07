#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use Search::Elasticsearch;
use DBI;

my ($dbname, $dbhost, $dbuser, $dbport, $dbpass) = ('igsr_website', 'mysql-g1kdcc-public', 'g1kro', 4197, undef);
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
my $select_all_dcs_sql = 'SELECT * from data_collection';
my $count_samples_sql = 'SELECT count(samples.sample_id) AS num_samples FROM (SELECT DISTINCT sf.sample_id FROM sample_file sf, file_data_collection fdc WHERE sf.file_id=fdc.file_id AND fdc.data_collection_id=?) AS samples';
my $count_pops_sql = 'SELECT count(*) AS num_populations FROM (SELECT DISTINCT s.population_id FROM sample_file sf, file_data_collection fdc, sample s WHERE s.sample_id=sf.sample_id AND sf.file_id=fdc.file_id AND fdc.data_collection_id=?) AS populations';
my $select_files_sql = 'SELECT dt.code data_type, ag.description analysis_group
    FROM file f LEFT JOIN data_type dt ON f.data_type_id = dt.data_type_id
    LEFT JOIN analysis_group ag ON f.analysis_group_id = ag.analysis_group_id
    INNER JOIN file_data_collection fdc ON f.file_id=fdc.file_id
    WHERE fdc.data_collection_id=?
    GROUP BY dt.data_type_id, ag.analysis_group_id';
my $sth_dcs = $dbh->prepare($select_all_dcs_sql) or die $dbh->errstr;
my $sth_samples = $dbh->prepare($count_samples_sql) or die $dbh->errstr;
my $sth_pops = $dbh->prepare($count_pops_sql) or die $dbh->errstr;
my $sth_files = $dbh->prepare($select_files_sql) or die $dbh->errstr;

$sth_dcs->execute() or die $sth_dcs->errstr;
my %indexed_dcs;
while (my $row = $sth_dcs->fetchrow_hashref()) {
  my %es_doc = (
    title => $row->{title},
    shortTitle => $row->{short_title},
    dataReusePolicy => $row->{reuse_policy},
    displayOrder => $row->{display_order},
  );
  if ($row->{website}) {
    $es_doc{website} = $row->{website};
  }
  if ($row->{publication}) {
    $es_doc{publication} = $row->{publication};
  }

  my $es_id = lc($row->{short_title});
  $es_id =~ s/ /-/g;

  $sth_samples->bind_param(1, $row->{data_collection_id});
  $sth_samples->execute() or die $sth_samples->errstr;
  if (my $samples_row = $sth_samples->fetchrow_hashref()) {
    $es_doc{samples}{count} = $samples_row->{num_samples};
  }
  else {
    $es_doc{samples}{count} = 0;
  }

  $sth_pops->bind_param(1, $row->{data_collection_id});
  $sth_pops->execute() or die $sth_pops->errstr;
  if (my $pops_row = $sth_pops->fetchrow_hashref()) {
    $es_doc{populations}{count} = $pops_row->{num_populations};
  }
  else {
    $es_doc{populations}{count} = 0;
  }

  $sth_files->bind_param(1, $row->{data_collection_id});
  $sth_files->execute() or die $sth_files->errstr;
  while (my $file_row = $sth_files->fetchrow_hashref()) {
    $es_doc{dataTypes}{$file_row->{data_type}} = 1;
    push(@{$es_doc{$file_row->{data_type}}}, $file_row->{analysis_group});
  }
  $es_doc{dataTypes} = [keys %{$es_doc{dataTypes}}];

  eval{$es->index(
    index => $es_index_name,
    type => 'data-collection',
    id => $es_id,
    body => \%es_doc,
  );};
  if (my $error = $@) {
    die "error indexing data_collection in $es_index_name index:".$error->{text};
  }
  $indexed_dcs{$es_id} = 1;
}

my $scroll = $es->scroll_helper(
    index => $es_index_name,
    type => 'data-collection',
    search_type => 'scan',
    size => 500,
);
SCROLL:
while (my $es_doc = $scroll->next) {
  next SCROLL if $indexed_dcs{$es_doc->{_id}};
  $es->delete(
    index => $es_index_name,
    type => 'data-collection',
    id => $es_doc->{_id},
  );
}

=pod

=head1 NAME

igsr-code/scripts/elasticsearch/load_data_collections.es.pl

=head1 SYNONPSIS

This script is for creating the data-collection index in elasticsearch. Run this script whenever you want to add, delete or modify data-collections in the portal.

The script roughly does this:

    Selects all rows in the mysql data_collection table.
        Counts the number of samples and populations belonging to the data collection, via the sample_file and file_data_collection tables.
        Selects data_type and analysis_group rows that belong to the data collection, via the file and file_data_collection tables.
    Index all of those data collections in elasticsearch
    Scans through all existing data collections in elasticsearch. If it finds one that should not be there, then deletes it.

=head1 WHAT'S IN THE INDEX?

  # list them (10 only by default):
  curl http://www.internationalgenome.org/api/beta/data-collection/_search | python -m json.tool
  # pick one out by name:
  curl http://www.internationalgenome.org/api/beta/data-collection/grch38 | python -m json.tool

=head1 OPTIONS

    -dbhost, the name of the mysql-host
    -dbname, the name of the mysql database
    -dbuser, the name of the mysql user
    -dbpass, the database password if appropriate
    -dbport, the port the mysql instance is running on
    -es_host, host and port of the elasticsearch index you are loading into, e.g. ves-hx-e3:9200
    -es_index_name, the elasticsearch index name, e.g. igsr_beta

=cut

