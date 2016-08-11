#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use Search::Elasticsearch;
use DBI;

my ($dbname, $dbhost, $dbuser, $dbport, $dbpass) = ('igsr_website', 'mysql-g1kdcc-public', 'g1kro', 4197, undef);
my @es_host;
my $es_index_name = 'igsr_beta';

&GetOptions(
  'dbpass=s'      => \$dbpass,
  'dbport=i'      => \$dbport,
  'dbuser=s'      => \$dbuser,
  'dbhost=s'      => \$dbhost,
  'dbname=s'      => \$dbname,
  'es_host=s' =>\@es_host,
  'es_index_name=s' =>\$es_index_name,
);
my @es = map {Search::Elasticsearch->new(nodes => $_, client => '1_0::Direct')} @es_host;

my $dbh = DBI->connect("DBI:mysql:$dbname;host=$dbhost;port=$dbport", $dbuser, $dbpass) or die $DBI::errstr;
my $select_all_dcs_sql = 'SELECT * from data_collection';
my $count_samples_sql = 'SELECT count(samples.sample_id) AS num_samples FROM (SELECT DISTINCT sf.sample_id FROM sample_file sf, file_data_collection fdc WHERE sf.file_id=fdc.file_id AND fdc.data_collection_id=?) AS samples';
my $count_pops_sql = 'SELECT count(*) AS num_populations FROM (SELECT DISTINCT s.population_id FROM sample_file sf, file_data_collection fdc, sample s WHERE s.sample_id=sf.sample_id AND sf.file_id=fdc.file_id AND fdc.data_collection_id=?) AS populations';
my $sth_dcs = $dbh->prepare($select_all_dcs_sql) or die $dbh->errstr;
my $sth_samples = $dbh->prepare($count_samples_sql) or die $dbh->errstr;
my $sth_pops = $dbh->prepare($count_pops_sql) or die $dbh->errstr;

$sth_dcs->execute() or die $sth_dcs->errstr;
FILE:
while (my $row = $sth_dcs->fetchrow_hashref()) {
  my %es_doc = (
    title => $row->{title},
    shortTitle => $row->{short_title},
    dataReusePolicy => $row->{reuse_policy},
    displayOrder => $row->{display_order},
  );

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

  foreach my $es (@es) {
    eval{$es->index(
      index => $es_index_name,
      type => 'data_collection',
      id => $es_id,
      body => \%es_doc,
    );};
    if (my $error = $@) {
      die "error indexing data_collection in $es_index_name index:".$error->{text};
    }
  }
}
