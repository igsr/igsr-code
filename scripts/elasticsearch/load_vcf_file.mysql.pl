#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use VCF;
use DBI;

my @vcf_file;
my $data_collection = '1000genomes';
my $data_type = 'variants';
my $analysis_group;
my ($dbname, $dbhost, $dbuser, $dbport, $dbpass) = ('igsr_website_v2', 'mysql-igsr-web', 'g1krw', 4641, undef);
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
my $insert_file_sql =
    'INSERT INTO file(url, url_crc, foreign_file)
      SELECT f2.url, crc32(f2.url), f2.foreign_file
      FROM (SELECT ? AS url, ? AS foreign_file) f2
    ON DUPLICATE KEY UPDATE file_id=LAST_INSERT_ID(file_id), indexed_in_elastic_search=0';
my $update_type_sql = 'UPDATE file SET data_type_id=(SELECT data_type_id FROM data_type WHERE code=?) WHERE file_id=?';
my $update_analysis_group_sql = 'UPDATE file SET analysis_group_id=(SELECT analysis_group_id FROM analysis_group WHERE code=?) WHERE file_id=?';
my $insert_data_collection_sql = 'INSERT IGNORE INTO file_data_collection(file_id, data_collection_id) SELECT ?, data_collection_id from data_collection where code=?';
my $insert_sample_sql = 'INSERT IGNORE INTO sample_file(file_id, sample_id) SELECT ?, sample_id from sample where name=?';
my $sth_file = $dbh->prepare($insert_file_sql) or die $dbh->errstr;
my $sth_type = $dbh->prepare($update_type_sql) or die $dbh->errstr;
my $sth_analysis_group = $dbh->prepare($update_analysis_group_sql) or die $dbh->errstr;
my $sth_data_collection = $dbh->prepare($insert_data_collection_sql) or die $dbh->errstr;
my $sth_sample = $dbh->prepare($insert_sample_sql) or die $dbh->errstr;

$dbh->{AutoCommit} = 0;
foreach my $vcf (grep {$_} @vcf_file) {
  my $vcf_obj = VCF->new(file => $vcf);
  $vcf_obj->parse_header();
  my @samples = $vcf_obj->get_samples();
  $vcf->close();

  $vcf =~ s/^$trim//;
  my $url = $root.$vcf;
  my $is_foreign = $url =~ /ftp\.1000genomes\.ebi\.ac\.uk/;
  foreach my $file ($url, "$url.tbi") {
    $sth_file->bind_param(1, $file);
    $sth_file->bind_param(2, $is_foreign);
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
$dbh->commit;

=pod

=head1 NAME

igsr-code/scripts/elasticsearch/load_vcf_file.mysql.pl

=head1 SYNONPSIS

This is another way of loading the file table. This script should be used for "final" vcf files - i.e. the ones you want to be advertised on the data portal as belonging to samples, and data collections. Take a look at the bash script shell/load_vcf_files.mysql.sh in the igsr-code git repo. It contains a record of the exact command lines that were used to call the perl script when the database was first loaded in April 2016. This bash script is helpful to demostrate how command lines could be constructed.

    For each vcf file passed in on the command line, it does the following for the vcf file and for the .tbi file
        1. Read the vcf header to get a list of samples.
        2. Inserts the file into file table, and check for duplicate_key on the url column
        3. If there was a duplicate_key, then the file was already in the file table. So it sets indexed_indexed_elasticsearch=0 to mark that you are modifying this file.
        4. Updates the analysis_group_id in the file table if you used the --analysis_group arguments
        5. Updates the data_type_id in the file table if you used the --data_type argument
        6. Inserts a row in the file_data_collection table if you used the --data_collection argument
        7. Inserts a row in the sample_file table for each sample in the header. Ignores duplicate key errors.

=head1 OPTIONS

    -dbhost, the name of the mysql-host
    -dbname, the name of the mysql database
    -dbuser, the name of the mysql user
    -dbpass, the database password if appropriate
    -dbport, the port the mysql instance is running on
    -es_host, host and port of the elasticsearch index you are loading into, e.g. ves-hx-e3:9200
    -es_index_name, the elasticsearch index name, e.g. igsr_beta
    --file=/path/to/file.vcf.gz the vcf file you want to load. Can be specified many times and it will load all of them.
    --data_collection=grc38 optional, must match the code column of the data_collection table.
    --data_type=variants. must match the code column of the data_type table.
    --analysis_group=exome. must match the code column of the analysis_group table.
    --trim=/nfs/1000g-archive/ this is already set as a default. This is trimmed from the file path before converting it into a url.
    --root=ftp://ftp.1000genomes.ebi.ac.uk/. This is already set by default. It is prepended to the file path to turn it into a url.

=cut

