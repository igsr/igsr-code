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
my ($dbname, $dbhost, $dbuser, $dbport, $dbpass) = ('igsr_website', 'mysql-g1kdcc-public', 'g1krw', 4197, undef);

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
my $insert_sql = 'INSERT INTO sample(name, biosample_id, population_id, sex) SELECT ?, ?, population_id, ? FROM population WHERE code=?';
my $sth = $dbh->prepare($insert_sql) or die $dbh->errstr;


my %samples;
my $sample_fh = new Text::Delimited;
$sample_fh->delimiter("\t");
$sample_fh->open($sample_file) or die "could not open $sample_file $!";
while( my $line = $sample_fh->read) {
  my $biosample = $biosamples{$line->{Sample}};
  $sth->bind_param(1, $line->{Sample});
  $sth->bind_param(2, $biosample->id);
  $sth->bind_param(3, $line->{Sex});
  $sth->bind_param(4, $line->{Population});
  my $rv = $sth->execute() or die $sth->errstr;
  die "did not recognise population ".$line->{Population} if $rv !=1;
}
  
$sample_fh->close();
