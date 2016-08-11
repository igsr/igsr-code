#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use Search::Elasticsearch;
use DBI;
use feature qw(fc);

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
my $select_all_samples_sql = 'SELECT s.*, p.code pop_code, p.name pop_name, p.description pop_description,
    sp.code superpop_code, sp.name superpop_name
    FROM sample s, population p, superpopulation sp
    WHERE s.population_id=p.population_id AND p.superpopulation_id=sp.superpopulation_id';
my $select_files_sql = 'SELECT dt.code data_type, ag.description analysis_group, dc.title data_collection, dc.data_collection_id, dc.reuse_policy
    FROM file f LEFT JOIN data_type dt ON f.data_type_id = dt.data_type_id
    LEFT JOIN analysis_group ag ON f.analysis_group_id = ag.analysis_group_id
    INNER JOIN sample_file sf ON sf.file_id=f.file_id
    INNER JOIN file_data_collection fdc ON f.file_id=fdc.file_id
    INNER JOIN data_collection dc ON fdc.data_collection_id=dc.data_collection_id
    WHERE sf.sample_id=?
    GROUP BY dt.data_type_id, ag.analysis_group_id, dc.data_collection_id';
my $select_relationship_sql = 'SELECT s.name, sr.type FROM sample s, sample_relationship sr WHERE sr.relation_sample_id=s.sample_id AND sr.subject_sample_id=?';
my $sth_sample = $dbh->prepare($select_all_samples_sql) or die $dbh->errstr;
my $sth_files = $dbh->prepare($select_files_sql) or die $dbh->errstr;
my $sth_relationship = $dbh->prepare($select_relationship_sql) or die $dbh->errstr;

$sth_sample->execute() or die $sth_sample->errstr;
FILE:
while (my $row = $sth_sample->fetchrow_hashref()) {
  my %es_doc = (
    name => $row->{name},
    population => {
      code => $row->{pop_code},
      name => $row->{pop_name},
      description => $row->{pop_description},
    },
    superpopulation => {
      code => $row->{superpop_code},
      name => $row->{superpop_name},
    },
    sex => $row->{sex},
    biosampleId => $row->{biosample_id},
  );
  if (my $sex_letter = $row->{sex}) {
    $es_doc{sex} = fc($sex_letter) eq fc('M') ? 'male'
                  : fc($sex_letter) eq fc('F') ? 'female'
                  : 'unknown';
  }

  $sth_files->bind_param(1, $row->{sample_id});
  $sth_files->execute() or die $sth_files->errstr;
  my %data_collections;
  while (my $file_row = $sth_files->fetchrow_hashref()) {
    $data_collections{$file_row->{data_collection_id}} //= {
      dataCollection => $file_row->{data_collection},
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

  $sth_relationship->bind_param(1, $row->{sample_id});
  $sth_relationship->execute() or die $sth_relationship->errstr;
  while (my $relationship_row = $sth_relationship->fetchrow_hashref()) {
    push(@{$es_doc{relatedSample}}, {relatedSampleName => $relationship_row->{name}, relationship => $relationship_row->{type}});
  }

  foreach my $es (@es) {
    eval{$es->index(
      index => $es_index_name,
      type => 'sample',
      id => $row->{name},
      body => \%es_doc,
    );};
    if (my $error = $@) {
      die "error indexing sample in $es_index_name index:".$error->{text};
    }
  }
}
