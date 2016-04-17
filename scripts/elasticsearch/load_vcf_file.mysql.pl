#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use DBI;

my @vcf_file;
my $data_collection = '1000genomes';
my $data_type = 'variants';
my $analysis_group = 'release';
my ($dbname, $dbhost, $dbuser, $dbport, $dbpass) = ('igsr_website', 'mysql-g1kdcc-public', 'g1krw', 4197, undef);
my $trim = '/nfs/1000g-archive/';
my $root = 'ftp://ftp.1000genomes.ebi.ac.uk/';

&GetOptions(
  'file|vcf_file=s'      => \@vcf_file,
  'data_collection=s' => \$data_collection,
  'data_type=s' => \$data_type,
  'analysis_group=s' => \$analysis_group,
  'root=s'        => \$root,
  'trim=s'        => \$trim,
  'dbpass=s'      => \$dbpass,
  'dbport=i'      => \$dbport,
  'dbuser=s'      => \$dbuser,
  'dbhost=s'      => \$dbhost,
  'dbname=s'      => \$dbname,
);

my $dbh = DBI->connect("DBI:mysql:$dbname;host=$dbhost;port=$dbport", $dbuser, $dbpass) or die $DBI::errstr;
my $insert_file_sql = 'INSERT INTO file(url, url_crc ) VALUES(?, crc32(?)) ON DUPLICATE KEY UPDATE file_id=LAST_INSERT_ID(file_id)';
my $update_type_sql = 'UPDATE file SET data_type_id=(SELECT data_type_id FROM data_type WHERE code=?) WHERE file_id=?';
my $update_analysis_group_sql = 'UPDATE file SET analysis_group_id=(SELECT analysis_group_id FROM analysis_group WHERE code=?) WHERE file_id=?';
my $insert_data_collection_sql = 'INSERT IGNORE INTO file_data_collection(file_id, data_collection_id) SELECT ?, data_collection_id from data_collection where code=?';
my $insert_sample_sql = 'INSERT IGNORE INTO sample_file(file_id, sample_id) SELECT ?, sample_id from sample where name=?';
my $sth_file = $dbh->prepare($insert_file_sql) or die $dbh->errstr;
my $sth_type = $dbh->prepare($update_type_sql) or die $dbh->errstr;
my $sth_analysis_group = $dbh->prepare($update_analysis_group_sql) or die $dbh->errstr;
my $sth_data_collection = $dbh->prepare($insert_data_collection_sql) or die $dbh->errstr;
my $sth_sample = $dbh->prepare($insert_sample_sql) or die $dbh->errstr;

foreach my $vcf (@vcf_file) {
  my $vcf_header = `tabix -H $vcf | tail -1`;
  die "error opening $vcf" if !$vcf_header;
  chomp $vcf_header;
  my @split_line = split("\t", $vcf_header);
  my @samples = @split_line[9..$#split_line];

  $vcf =~ s/^$trim//;
  my $url = $root.$vcf;
  foreach my $file ($url, "$url.tbi") {
    $sth_file->bind_param(1, $url);
    $sth_file->bind_param(2, $url);
    $sth_file->execute() or die $sth_file->errstr;
    my $file_id = $sth_file->{mysql_insertid};

    if ($analysis_group) {
      $sth_analysis_group->bind_param(1, $analysis_group);
      $sth_analysis_group->bind_param(2, $file_id);
      $sth_analysis_group->execute() or die $sth_analysis_group->errstr;
    }

    if ($data_type) {
      $sth_type->bind_param(1, $data_type);
      $sth_type->bind_param(2, $file_id);
      $sth_type->execute() or die $sth_type->errstr;
    }

    if ($data_collection) {
      $sth_data_collection->bind_param(1, $file_id);
      $sth_data_collection->bind_param(2, $data_collection);
      $sth_data_collection->execute() or die $sth_data_collection->errstr;
    }

    foreach my $sample (@samples) {
      $sth_sample->bind_param(1, $file_id);
      $sth_sample->bind_param(2, $sample);
      $sth_sample->execute() or die $sth_sample->errstr;
    }
  }
}
