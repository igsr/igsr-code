#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use Search::Elasticsearch;
use DBI;

my ($dbname, $dbhost, $dbuser, $dbport, $dbpass) = ('igsr_website', 'mysql-g1kdcc-public', 'g1krw', 4197, undef);
my $check_timestamp;
my $es_index_name = 'igsr';
my $es_host = 'ves-hx-e4';

&GetOptions(
  'dbpass=s'      => \$dbpass,
  'dbport=i'      => \$dbport,
  'dbuser=s'      => \$dbuser,
  'dbhost=s'      => \$dbhost,
  'dbname=s'      => \$dbname,
  'check_timestamp!' => \$check_timestamp,
  'es_host=s' =>\$es_host,
  'es_index_name=s' =>\$es_index_name,
);
my $es = Search::Elasticsearch->new(nodes => "$es_host:9200", client => '1_0::Direct');
my $es_bulk = $es->bulk_helper(index => $es_index_name, type => 'file');

my $dbh = DBI->connect("DBI:mysql:$dbname;host=$dbhost;port=$dbport", $dbuser, $dbpass) or die $DBI::errstr;
my $select_new_files_sql = 'SELECT f.file_id, f.url, f.md5, dt.code data_type, ag.description analysis_group
    FROM file f LEFT JOIN data_type dt ON f.data_type_id = dt.data_type_id
    LEFT JOIN analysis_group ag ON f.analysis_group_id = ag.analysis_group_id
    WHERE (f.foreign_file IS TRUE OR f.in_current_tree IS TRUE) AND f.indexed_in_elasticsearch IS NOT TRUE
    ORDER BY file_id';
my $select_old_files_sql = 'SELECT f.file_id FROM file f
    WHERE f.foreign_file IS NOT TRUE AND f.in_current_tree IS NOT TRUE AND f.indexed_in_elasticsearch IS TRUE
    ORDER BY file_id';
my $select_sample_sql = 'SELECT s.name, p.code AS pop_code from sample s
    INNER JOIN sample_file sf ON sf.sample_id=s.sample_id 
    LEFT JOIN population p ON p.population_id=s.population_id
    WHERE sf.file_id=?';
my $select_data_collection_sql = 'SELECT dc.title, dc.reuse_policy from data_collection dc, file_data_collection fdc
    WHERE fdc.data_collection_id=dc.data_collection_id AND fdc.file_id=?
    ORDER BY dc.reuse_policy_precedence';
my $update_file_sql = 'UPDATE file SET indexed_in_elasticsearch = (foreign_file IS TRUE OR in_current_tree IS TRUE)';
my $sth_new_files = $dbh->prepare($select_new_files_sql) or die $dbh->errstr;
my $sth_old_files = $dbh->prepare($select_old_files_sql) or die $dbh->errstr;
my $sth_file_update = $dbh->prepare($update_file_sql) or die $dbh->errstr;
my $sth_sample = $dbh->prepare($select_sample_sql) or die $dbh->errstr;
my $sth_data_collection = $dbh->prepare($select_data_collection_sql) or die $dbh->errstr;

my $timestamp_sql =
   'SELECT current_tree_log_id, loaded_into_elasticsearch FROM current_tree_log
    ORDER BY loaded_into_db DESC limit 1';
my $sth_timestamp = $dbh->prepare($timestamp_sql) or die $dbh->errstr;
$sth_timestamp->execute() or die $sth_timestamp->errstr;
my $row_timestamp = $sth_timestamp->fetchrow_hashref();
exit if $check_timestamp && (!$row_timestamp || $row_timestamp->{loaded_into_elasticsearch});
my $current_tree_log_id = $row_timestamp ? $row_timestamp->{current_tree_log_id} : undef;

eval{$es->indices->put_settings(
  index => $es_index_name,
  body => {
    'index.refresh_interval' => -1,
    'index.number_of_replicas' => 0,
  }
);};
if (my $error = $@) {
  die "error changing settings for elasticsearch index $es_index_name: ".$error->{text};
}


$sth_new_files->execute() or die $sth_new_files->errstr;
FILE:
while (my $row_file = $sth_new_files->fetchrow_hashref()) {

  my %es_doc = (
    url => $row_file->{url},
    md5 => $row_file->{md5},
  );
  if (my $analysis_group = $row_file->{analysis_group}) {
    $es_doc{analysisGroup} = $analysis_group;
  }
  if (my $data_type = $row_file->{data_type}) {
    $es_doc{dataType} = $data_type;
  }

  $sth_sample->bind_param(1, $row_file->{file_id});
  $sth_sample->execute() or die $sth_sample->errstr;
  my %file_populations;
  while (my $row = $sth_sample->fetchrow_hashref()) {
    push(@{$es_doc{samples}}, $row->{name});
    if ($row->{pop_code}) {
      $file_populations{$row->{pop_code}} = 1;
    }
  }
  if (scalar keys %file_populations) {
    $es_doc{populations} = [keys %file_populations];
  }

  $sth_data_collection->bind_param(1, $row_file->{file_id});
  $sth_data_collection->execute() or die $sth_data_collection->errstr;
  while (my $row = $sth_data_collection->fetchrow_hashref()) {
    $es_doc{dataReusePolicy} //= $row->{reuse_policy};
    push(@{$es_doc{dataCollections}}, $row->{title});
  }

  eval {$es_bulk->index({
    id => sprintf('%.9d', $row_file->{file_id}),
    source => \%es_doc,
  });};
  if (my $error = $@) {
    die "error bullk indexing file in $es_index_name index:".$error->{text};
  }
}

$sth_old_files->execute() or die $sth_old_files->errstr;
FILE:
while (my $row_file = $sth_old_files->fetchrow_hashref()) {
  $es_bulk->delete({
    id => sprintf('%.9d', $row_file->{file_id}),
  });
}


$es_bulk->flush();

eval{$es->indices->put_settings(
  index => $es_index_name,
  body => {
    'index.refresh_interval' => '1s',
    'index.number_of_replicas' => 1,
  }
);};
if (my $error = $@) {
  die "error changing settings for elasticsearch index $es_index_name: ".$error->{text};
}

$sth_file_update->execute() or die $sth_file_update->errstr;

if ($current_tree_log_id) {
    my $log_sql = 'UPDATE current_tree_log SET loaded_into_elasticsearch=now() WHERE current_tree_log_id=?';
    my $sth_log = $dbh->prepare($log_sql) or die $dbh->errstr;
    $sth_log->bind_param(1, $current_tree_log_id);
    $sth_log->execute() or die $sth_log->errstr;
}
