#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use Search::Elasticsearch;
use DBI;

my ($dbname, $dbhost, $dbuser, $dbport, $dbpass) = ('igsr_website', 'mysql-g1kdcc-public', 'g1kro', 4197, undef);
my $current_tree = '/nfs/1000g-archive/vol1/ftp/current.tree';
my $root = 'ftp://ftp.1000genomes.ebi.ac.uk/vol1/';
my @es_host;

&GetOptions(
  'current_tree=s' => \$current_tree,
  'root=s'        => \$root,
  'dbpass=s'      => \$dbpass,
  'dbport=i'      => \$dbport,
  'dbuser=s'      => \$dbuser,
  'dbhost=s'      => \$dbhost,
  'dbname=s'      => \$dbname,
  'es_host=s' =>\@es_host,
);
my @es = map {Search::Elasticsearch->new(nodes => $_, client => '1_0::Direct')} @es_host;

my $dbh = DBI->connect("DBI:mysql:$dbname;host=$dbhost;port=$dbport", $dbuser, $dbpass) or die $DBI::errstr;
my $select_file_sql = 'SELECT f.file_id, f.url, f.url_crc, dt.code data_type, ag.description analysis_group
    FROM file f, data_type dt, analysis_group ag
    WHERE f.data_type_id=dt.data_type_id AND f.analysis_group_id=ag.analysis_group_id
    AND f.url_crc=crc32(?) AND f.url=?';
my $select_all_files_sql = 'SELECT f.file_id, f.url, f.url_crc, f.md5, dt.code data_type, ag.description analysis_group
    FROM file f, data_type dt, analysis_group ag
    WHERE f.data_type_id=dt.data_type_id AND f.analysis_group_id=ag.analysis_group_id';
my $select_sample_sql = 'SELECT s.name from sample s, sample_file sf
    WHERE sf.sample_id=s.sample_id AND sf.file_id=?';
my $select_data_collection_sql = 'SELECT dc.description from data_collection dc, file_data_collection fdc
    WHERE fdc.data_collection_id=dc.data_collection_id AND fdc.file_id=?';
my $sth_file = $dbh->prepare($select_file_sql) or die $dbh->errstr;
my $sth_all_files = $dbh->prepare($select_all_files_sql) or die $dbh->errstr;
my $sth_sample = $dbh->prepare($select_sample_sql) or die $dbh->errstr;
my $sth_data_collection = $dbh->prepare($select_data_collection_sql) or die $dbh->errstr;

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
  add_file($file_rows->[0], $split_line[5]);
}
close $fh;

$sth_all_files->execute() or die $sth_all_files->errstr;
FILE:
while (my $row = $sth_all_files->fetchrow_hashref()) {
  next FILE if $processed_files{$row->{file_id}};
  add_file($row);
}

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
