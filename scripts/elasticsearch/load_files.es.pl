#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use Search::Elasticsearch;
use DBI;

my ($dbname, $dbhost, $dbuser, $dbport, $dbpass) = ('igsr_website', 'mysql-g1kdcc-public', 'g1krw', 4197, undef);
my $check_timestamp;
my @es_host;

&GetOptions(
  'dbpass=s'      => \$dbpass,
  'dbport=i'      => \$dbport,
  'dbuser=s'      => \$dbuser,
  'dbhost=s'      => \$dbhost,
  'dbname=s'      => \$dbname,
  'check_timestamp=s' => \$check_timestamp,
  'es_host=s' =>\@es_host,
);
my @es = map {Search::Elasticsearch->new(nodes => $_, client => '1_0::Direct')} @es_host;
my @es_bulks = map {$_->bulk_helper(index => 'igsr', type => 'file')} @es;

my $dbh = DBI->connect("DBI:mysql:$dbname;host=$dbhost;port=$dbport", $dbuser, $dbpass) or die $DBI::errstr;
my $select_all_files_sql = 'SELECT f.file_id, f.url, f.md5, f.foreign_file, f.in_current_tree,  dt.code data_type, ag.description analysis_group
    FROM file f LEFT JOIN data_type dt ON f.data_type_id = dt.data_type_id
    LEFT JOIN analysis_group ag ON f.analysis_group_id = ag.analysis_group_id
    ORDER BY file_id';
my $select_sample_sql = 'SELECT s.name from sample s, sample_file sf
    WHERE sf.sample_id=s.sample_id AND sf.file_id=?';
my $select_data_collection_sql = 'SELECT dc.description, dc.reuse_policy from data_collection dc, file_data_collection fdc
    WHERE fdc.data_collection_id=dc.data_collection_id AND fdc.file_id=?
    ORDER BY dc.reuse_policy_precedence';
my $insert_file_sql = 'INSERT INTO file(url, url_crc, md5, foreign_file) VALUES(?, ?, ?, ?)';
my $sth_all_files = $dbh->prepare($select_all_files_sql) or die $dbh->errstr;
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

foreach my $es (@es) {
  $es->indices->put_settings(
    index => 'igsr',
    body => {
      'index.refresh_interval' => -1,
      'index.number_of_replicas' => 0,
    }
  );
}


$sth_all_files->execute() or die $sth_all_files->errstr;
FILE:
while (my $row_file = $sth_all_files->fetchrow_hashref()) {
  next FILE if !($row_file->{foreign_file} || $row_file->{in_current_tree});

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
  while (my $row = $sth_sample->fetchrow_hashref()) {
    push(@{$es_doc{samples}}, $row->{name});
  }

  $sth_data_collection->bind_param(1, $row_file->{file_id});
  $sth_data_collection->execute() or die $sth_data_collection->errstr;
  while (my $row = $sth_data_collection->fetchrow_hashref()) {
    $es_doc{dataReusePolicy} //= $row->{reuse_policy};
    push(@{$es_doc{dataCollections}}, $row->{description});
  }

  foreach my $es_bulk (@es_bulks) {
    $es_bulk->index({
      id => sprintf('%.9d', $row_file->{file_id}),
      source => \%es_doc,
    });
  }
}

foreach my $es (@es) {
  $es->indices->put_settings(
    index => 'igsr',
    body => {
      'index.refresh_interval' => '1s',
      'index.number_of_replicas' => 1,
    }
  );
}

foreach my $es_bulk (@es_bulks) {
  $es_bulk->flush();
}

if ($current_tree_log_id) {
    my $log_sql = 'UPDATE current_tree_log SET loaded_into_elasticsearch=now() WHERE current_tree_log_id=?';
    my $sth_log = $dbh->prepare($log_sql) or die $dbh->errstr;
    $sth_log->bind_param(1, $current_tree_log_id);
    $sth_log->execute() or die $sth_log->errstr;
}
