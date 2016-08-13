#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use Search::Elasticsearch;
use DBI;

my ($dbname, $dbhost, $dbuser, $dbport, $dbpass) = ('igsr_website', 'mysql-g1kdcc-public', 'g1kro', 4197, undef);
my @es_host;
my $es_index_name = 'igsr';

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
my $select_all_pops_sql = 'SELECT p.*, count(s.sample_id) num_samples, sp.code superpop_code, sp.name superpop_name
    FROM population p
    LEFT JOIN sample s on p.population_id=s.population_id
    INNER JOIN superpopulation sp on p.superpopulation_id=sp.superpopulation_id
    GROUP BY p.population_id';
my $select_files_sql = 'SELECT dt.code data_type, ag.description analysis_group, dc.title data_collection, dc.data_collection_id, dc.reuse_policy
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
    samples => {
      count => $row->{num_samples},
    },
  );

  $sth_files->bind_param(1, $row->{population_id});
  $sth_files->execute() or die $sth_files->errstr;
  my %data_collections;
  while (my $file_row = $sth_files->fetchrow_hashref()) {
    $data_collections{$file_row->{data_collection_id}} //= {
      title => $file_row->{data_collection},
      dataReusePolicy => $file_row->{reuse_policy}
    };
    $data_collections{$file_row->{data_collection_id}}{dataTypes}{$file_row->{data_type}} = 1;
    push(@{$data_collections{$file_row->{data_collection_id}}{$file_row->{data_type}}}, $file_row->{analysis_group});
  }
  foreach my $data_collection_id (sort keys %data_collections) {
    my $dc_hash = $data_collections{$data_collection_id};
    $dc_hash->{dataTypes} = [keys %{$dc_hash->{dataTypes}}];
    push(@{$es_doc{dataCollections}}, $dc_hash);
  }

  foreach my $es (@es) {
    eval{$es->index(
      index => $es_index_name,
      type => 'population',
      id => $row->{code},
      body => \%es_doc,
    );};
    if (my $error = $@) {
      die "error indexing population in $es_index_name index:".$error->{text};
    }
  }
}
