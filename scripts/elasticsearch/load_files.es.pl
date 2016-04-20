#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use Search::Elasticsearch;
use DBI;
use File::stat;

my ($dbname, $dbhost, $dbuser, $dbport, $dbpass) = ('igsr_website', 'mysql-g1kdcc-public', 'g1krw', 4197, undef);
my $current_tree = '/nfs/1000g-archive/vol1/ftp/current.tree';
my $root = 'ftp://ftp.1000genomes.ebi.ac.uk/vol1/';
my $check_timestamp;
my @es_host;

&GetOptions(
  'current_tree=s' => \$current_tree,
  'root=s'        => \$root,
  'dbpass=s'      => \$dbpass,
  'dbport=i'      => \$dbport,
  'dbuser=s'      => \$dbuser,
  'dbhost=s'      => \$dbhost,
  'dbname=s'      => \$dbname,
  'check_timestamp=s' => \$check_timestamp,
  'es_host=s' =>\@es_host,
);
my @es = map {Search::Elasticsearch->new(nodes => $_, client => '1_0::Direct')} @es_host;

my $dbh = DBI->connect("DBI:mysql:$dbname;host=$dbhost;port=$dbport", $dbuser, $dbpass) or die $DBI::errstr;
my $select_file_sql = 'SELECT f.file_id, f.url, f.url_crc, dt.code data_type, ag.description analysis_group
    FROM file f LEFT JOIN data_type dt ON f.data_type_id = dt.data_type_id
    LEFT JOIN analysis_group ag ON f.analysis_group_id = ag.analysis_group_id
    WHERE f.url_crc=crc32(?) AND f.url=?';
my $select_all_files_sql = 'SELECT f.file_id, f.url, f.url_crc, f.md5, dt.code data_type, ag.description analysis_group
    FROM file f LEFT JOIN data_type dt ON f.data_type_id = dt.data_type_id
    LEFT JOIN analysis_group ag ON f.analysis_group_id = ag.analysis_group_id';
my $select_sample_sql = 'SELECT s.name from sample s, sample_file sf
    WHERE sf.sample_id=s.sample_id AND sf.file_id=?';
my $select_data_collection_sql = 'SELECT dc.description, dc.reuse_policy from data_collection dc, file_data_collection fdc
    WHERE fdc.data_collection_id=dc.data_collection_id AND fdc.file_id=?
    ORDER BY dc.reuse_policy_precedence';
my $sth_file = $dbh->prepare($select_file_sql) or die $dbh->errstr;
my $sth_all_files = $dbh->prepare($select_all_files_sql) or die $dbh->errstr;
my $sth_sample = $dbh->prepare($select_sample_sql) or die $dbh->errstr;
my $sth_data_collection = $dbh->prepare($select_data_collection_sql) or die $dbh->errstr;

my $st = stat($current_tree) or die "could not stat $current_tree $!";
if ($check_timestamp) {
  my $timestamp_sql = 'SELECT count(*) FROM log WHERE current_tree_mtime = FROM_UNIXTIME(?) AND complete_run_time IS NOT NULL';
  my $sth_timestamp = $dbh->prepare($timestamp_sql) or die $dbh->errstr;
  $sth_timestamp->bind_param(1, $st->mtime);
  $sth_timestamp->execute() or die $sth_timestamp->errstr;
  my $rows = $sth_timestamp->fetchall_arrayref();
  exit if @$rows;
}
my $log_sql = 'INSERT INTO log(current_tree_mtime, start_run_time) VALUES (FROM_UNIXTIME(?), now())';
my $sth_log = $dbh->prepare($log_sql) or die $dbh->errstr;
$sth_log->bind_param(1, $st->mtime);
$sth_log->execute() or die $sth_log->errstr;
my $log_id = $sth_log->{mysql_insertid};


my %processed_files;
open my $fh, '<', $current_tree or die "could not open current_tree $!";
LINE:
while (my $line = <$fh>) {
  my @split_line = split("\t", $line);
  next LINE if $split_line[1] ne 'file';
  my $url = $root.$split_line[0];

  $sth_file->bind_param(1, $url);
  $sth_file->bind_param(2, $url);
  $sth_file->execute() or die $sth_file->errstr;
  my $file_rows = $sth_file->fetchall_arrayref({});
  next LINE if !@$file_rows;
  die "found more than one matching file for $url" if @$file_rows >1;

  $processed_files{$file_rows->[0]{file_id}} = 1;
  add_file($file_rows->[0], $split_line[4]);
}
close $fh;

$sth_all_files->execute() or die $sth_all_files->errstr;
FILE:
while (my $row = $sth_all_files->fetchrow_hashref()) {
  next FILE if $processed_files{$row->{file_id}};
  add_file($row);
}


$log_sql = 'UPDATE log SET complete_run_time=now() WHERE log_id=?';
$sth_log = $dbh->prepare($log_sql) or die $dbh->errstr;
$sth_log->bind_param(1, $log_id);
$sth_log->execute() or die $sth_log->errstr;


sub add_file {
  my ($file_mysql_row, $md5) = @_;
  my %es_doc = (
    url => $file_mysql_row->{url},
    analysisGroup => $file_mysql_row->{analysis_group},
    dataType => $file_mysql_row->{data_type},
    md5 => $md5 // $file_mysql_row->{md5},
  );

  $sth_sample->bind_param(1, $file_mysql_row->{file_id});
  $sth_sample->execute() or die $sth_sample->errstr;
  while (my $row = $sth_sample->fetchrow_hashref()) {
    push(@{$es_doc{samples}}, $row->{name});
  }

  $sth_data_collection->bind_param(1, $file_mysql_row->{file_id});
  $sth_data_collection->execute() or die $sth_data_collection->errstr;
  while (my $row = $sth_data_collection->fetchrow_hashref()) {
    $es_doc{dataReusePolicy} //= $row->{reuse_policy};
    push(@{$es_doc{dataCollections}}, $row->{description});
  }

  foreach my $es (@es) {
    $es->index(
      index => 'igsr',
      type => 'file',
      id => $file_mysql_row->{url_crc} || crc32($file_mysql_row->{url}),
      body => \%es_doc,
    );
  }
}
