#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use Search::Elasticsearch;
use DBI;

my ($dbname, $dbhost, $dbuser, $dbport, $dbpass) = ('igsr_website', 'mysql-g1kdcc-public', 'g1kro', 4197, undef);
my @es_host;

&GetOptions(
  'dbpass=s'      => \$dbpass,
  'dbport=i'      => \$dbport,
  'dbuser=s'      => \$dbuser,
  'dbhost=s'      => \$dbhost,
  'dbname=s'      => \$dbname,
  'es_host=s' =>\@es_host,
);
my @es = map {Search::Elasticsearch->new(nodes => $_, client => '1_0::Direct')} @es_host;

my $dbh = DBI->connect("DBI:mysql:$dbname;host=$dbhost;port=$dbport", $dbuser, $dbpass) or die $DBI::errstr;
my $select_all_pops_sql = 'SELECT p.*, sp.code superpop_code, sp.name superpop_name
    FROM population p, superpopulation sp
    WHERE p.superpopulation_id=sp.superpopulation_id';
my $select_files_sql = 'SELECT dt.code data_type, ag.description analysis_group, dc.description data_collection, dc.reuse_policy
    FROM file f LEFT JOIN data_type dt ON f.data_type_id = dt.data_type_id
    LEFT JOIN analysis_group ag ON f.analysis_group_id = ag.analysis_group_id
    INNER JOIN sample_file sf ON sf.file_id=f.file_id
    INNER JOIN file_data_collection fdc ON f.file_id=fdc.file_id
    INNER JOIN data_collection dc ON fdc.data_collection_id=dc.data_collection_id
    INNER JOIN sample s ON sf.sample_id=s.sample_id
    WHERE s.population_id=?
    GROUP BY dt.data_type_id, ag.analysis_group_id, dc.data_collection_id';
my $sth_population = $dbh->prepare($select_all_pops_sql) or die $dbh->errstr;
my $sth_files = $dbh->prepare($select_files_sql) or die $dbh->errstr;

$sth_population->execute() or die $sth_population->errstr;
FILE:
while (my $row = $sth_population->fetchrow_hashref()) {
  my %es_doc = (
    code => $row->{code},
    name => $row->{name},
    description => $row->{description},
    superpopulation => {
      code => $row->{superpop_code},
      name => $row->{superpop_name},
    },
  );

  $sth_files->bind_param(1, $row->{population_id});
  $sth_files->execute() or die $sth_files->errstr;
  my %data_collections;
  while (my $file_row = $sth_files->fetchrow_hashref()) {
    $data_collections{$file_row->{data_collection}} //= {
      dataCollection => $file_row->{data_collection},
      dataReusePolicy => $file_row->{reuse_policy}
    };
    $data_collections{$file_row->{data_collection}}{dataTypes}{$file_row->{data_type}} = 1;
    push(@{$data_collections{$file_row->{data_collection}}{$file_row->{data_type}}}, $file_row->{analysis_group});
  }
  while (my ($data_collection, $dc_hash) = each %data_collections) {
    $dc_hash->{dataTypes} = [keys %{$dc_hash->{dataTypes}}];
    push(@{$es_doc{dataCollections}}, $dc_hash);
  }

  foreach my $es (@es) {
    $es->index(
      index => 'igsr',
      type => 'population',
      id => $row->{code},
      body => \%es_doc,
    );
  }
}