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
    FROM population p, superpopulation sp,
    WHERE p.superpopulation_id=sp.superpopulation_id';
my $select_files_sql = 'SELECT dt.code data_type, ag.description analysis_group, dc.description data_collection, dc.reuse_policy
    FROM sample_file sf, sample s, file f, data_type dt, analysis_group ag, file_data_collection fdc, data_collection dc
    WHERE sf.file_id=f.file_id AND f.data_type_id=dt.data_type_id AND f.analysis_group_id=ag.analysis_group_id
    AND f.file_id=fdc.file_id AND fdc.data_collection_id=dc.data_collection_id AND s.sample_id=sf.sample_id
    AND s.population_id=?
    GROUP BY dt.data_type_id, ag.analysis_group_id, dc.data_collection_id';
my $sth_population = $dbh->prepare($select_all_pops_sql) or die $dbh->errstr;
my $sth_files = $dbh->prepare($select_files_sql) or die $dbh->errstr;

$sth_population->execute() or die $sth_population->errstr;
FILE:
while (my $row = $sth_population->fetchrow_hashref()) {
  my %es_doc = (
    name => $row->{name},
    population => {
      code => $row->{code},
      name => $row->{name},
      description => $row->{description},
    },
    superpopulation => {
      code => $row->{superpop_code},
      name => $row->{superpop_name},
    },
  );

  $sth_files->bind_param(1, $row->{population_id});
  $sth_files->execute() or die $sth_files->errstr;
  my %datasets;
  while (my $file_row = $sth_files->fetchrow_hashref()) {
    push(@{$datasets{$file_row->{data_collection}}{analysis_groups}{$file_row->{analysis_group}}}, $file_row->{data_type});
    $datasets{$file_row->{data_collection}}{data_reuse_policy} = $file_row->{reuse_policy};
  }
  while (my ($data_collection, $dc_hash) = each %datasets) {
    my @analysis_groups;
    while (my ($analysis_group, $data_type_arr) = each %{$dc_hash->{analysis_groups}}) {
      push(@analysis_groups, {
        analysisGroup => $analysis_group,
        analysisGroupData => $data_type_arr
      });
    }
    push(@{$es_doc{dataCollections}}, {
      dataCollection => $data_collection,
      dataReusePolicy => $dc_hash->{data_reuse_policy},
      analysisGroups => \@analysis_groups,
    });
  }

  foreach my $es (@es) {
    $es->index(
      index => 'igsr',
      type => 'population',
      id => $row->{name},
      body => \%es_doc,
    );
  }
}
