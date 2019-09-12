#!/usr/bin/env perl

use strict;
use warnings;
use Text::Delimited;
use Getopt::Long;
use BioSD;
use DBI;
use List::Util qw();

my $sample_file = '/nfs/1000g-archive/vol1/ftp/technical/working/20130606_sample_info/20130606_sample_info.txt';
my $biosample_group = 'SAMEG305842';
my ($dbname, $dbhost, $dbuser, $dbport, $dbpass) = ('igsr_website_v2', 'mysql-igsr-web', 'g1krw', 4641, undef);

&GetOptions(
  'sample_file=s'      => \$sample_file,
  'biosample_group=s'      => \$biosample_group,
  'dbpass=s'      => \$dbpass,
  'dbport=s'      => \$dbport,
  'dbuser=s'      => \$dbuser,
  'dbhost=s'      => \$dbhost,
  'dbname=s'      => \$dbname,
);

my %biosamples;
foreach my $biosample (@{BioSD::fetch_group($biosample_group)->samples}) {
  $biosamples{$biosample->property('Sample Name')->values->[0]} = $biosample;
}

my $dbh = DBI->connect("DBI:mysql:$dbname;host=$dbhost;port=$dbport", $dbuser, $dbpass) or die $DBI::errstr;
my $insert_sql = 'INSERT INTO sample (name, biosample_id, population_id, sex)
  SELECT name, b_id, pop, sex FROM
    (SELECT ? AS name, ? AS b_id, population_id AS pop, ? AS sex FROM population where code=?) AS s2
  ON DUPLICATE KEY UPDATE biosample_id=s2.b_id, population_id=pop, sex=s2.sex';
my $sth = $dbh->prepare($insert_sql) or die $dbh->errstr;


my %samples;
my $sample_fh = new Text::Delimited;
$sample_fh->delimiter("\t");
$sample_fh->open($sample_file) or die "could not open $sample_file $!";
while( my $line = $sample_fh->read) {
  my $biosample = $biosamples{$line->{Sample}};
  my $sex = $line->{Gender} ? uc(substr($line->{Gender}, 0, 1)) : undef;
  $sth->bind_param(1, $line->{Sample});
  $sth->bind_param(2, $biosample->id);
  $sth->bind_param(3, $sex);
  $sth->bind_param(4, $line->{Population});
  my $rv = $sth->execute() or die $sth->errstr;
}
  
$sample_fh->close();

=pod

=head1 NAME

igsr-code/scripts/elasticsearch/populate_samples.mysql.pl

=head1 SYNONPSIS

This is how samples originally entered the mysql database. This script was written because the best source of sample information (in April 2016) was the text-delimited file /nfs/1000g-archive/vol1/ftp/technical/working/20130606_sample_info/20130606_sample_info.txt

This is what the script does:

    Fetches the biosamples group SAMEG305842 using the xml API. This is to find the biosamples id of each sample.
    For each sample in the file, create a new row in the sample table. On duplicate keys, it updates the row in the sample table.

This script should probably never be used again for loading samples. New data collections to IGSR will have different sources of information. At best, this script could be a template for what a sample-loading script might look like.

=cut

