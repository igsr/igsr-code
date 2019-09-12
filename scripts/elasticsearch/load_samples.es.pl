#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use Search::Elasticsearch;
use DBI;
use Data::Dumper;
use feature qw(fc);

my ($dbname, $dbhost, $dbuser, $dbport, $dbpass) = ('igsr_website_v2', 'mysql-igsr-web', 'g1kro', 4641, undef);
my $es_host = 'ves-hx-e4:9200';
my $es_index_name = 'igsr_v2';

&GetOptions(
  'dbpass=s'      => \$dbpass,
  'dbport=i'      => \$dbport,
  'dbuser=s'      => \$dbuser,
  'dbhost=s'      => \$dbhost,
  'dbname=s'      => \$dbname,
  'es_host=s' =>\$es_host,
  'es_index_name=s' =>\$es_index_name,
);
my $es = Search::Elasticsearch->new(nodes => $es_host, client => '1_0::Direct');

my $dbh = DBI->connect("DBI:mysql:$dbname;host=$dbhost;port=$dbport", $dbuser, $dbpass) or die $DBI::errstr;
#my $select_all_samples_sql = 'SELECT s.*, p.code pop_code, p.name pop_name, p.description pop_description,
#    sp.code superpop_code, sp.name superpop_name
#    FROM sample s, population p, superpopulation sp
#    WHERE s.population_id=p.population_id AND p.superpopulation_id=sp.superpopulation_id';

my $select_all_samples_sql = 'select s.sample_id sample_id, s.name name, s.biosample_id biosample_id, s.sex sex from sample s';

my $select_source_sql = 'select s_source.sample_source_id sample_source_id, s_source.name source_name, s_source.description source_desc, s_source.url source_url from sample s, sample_source s_source where s.sample_source_id=s_source.sample_source_id and s.sample_id = ?';

my $select_pops_sql = 'select distinct population.population_id population_id, population.code pop_code, population.name pop_name, population.description pop_desc, population.latitude lat, population.longitude lng, population.elastic_id pop_elastic_id, population.superpopulation_id superpop_id, superpopulation.code superpop_code, superpopulation.name superpop_name, superpopulation.display_colour superpop_display_colour, superpopulation.display_order superpop_display_order from dc_sample_pop_assign, population, superpopulation where dc_sample_pop_assign.sample_id = ? and dc_sample_pop_assign.population_id =population.population_id and population.superpopulation_id =superpopulation.superpopulation_id';

my $select_files_sql = 'SELECT dt.code data_type, ag.description analysis_group, dc.title data_collection, dc.data_collection_id, dc.reuse_policy
    FROM file f LEFT JOIN data_type dt ON f.data_type_id = dt.data_type_id
    LEFT JOIN analysis_group ag ON f.analysis_group_id = ag.analysis_group_id
    INNER JOIN sample_file sf ON sf.file_id=f.file_id
    INNER JOIN file_data_collection fdc ON f.file_id=fdc.file_id
    INNER JOIN data_collection dc ON fdc.data_collection_id=dc.data_collection_id
    WHERE sf.sample_id=?
    GROUP BY dt.data_type_id, ag.analysis_group_id, dc.data_collection_id';

my $select_relationship_sql = 'SELECT s.name, sr.type FROM sample s, sample_relationship sr WHERE sr.relation_sample_id=s.sample_id AND sr.subject_sample_id=?';

my $select_synonyms_sql = 'select synonym from sample_synonym where sample_id = ?';

my $sth_sample = $dbh->prepare($select_all_samples_sql) or die $dbh->errstr;
my $sth_source = $dbh->prepare($select_source_sql) or die $dbh->errstr;
my $sth_pops = $dbh->prepare($select_pops_sql) or die $dbh->errstr;
my $sth_files = $dbh->prepare($select_files_sql) or die $dbh->errstr;
my $sth_relationship = $dbh->prepare($select_relationship_sql) or die $dbh->errstr;
my $sth_synonym = $dbh->prepare($select_synonyms_sql) or die $dbh->errstr;

$sth_sample->execute() or die $sth_sample->errstr;
my %indexed_samples;
#for each of the samples...
SAMPLE:
while (my $row = $sth_sample->fetchrow_hashref()) {
  my %es_doc = (
    name => $row->{name},
    sex => $row->{sex},
    biosampleId => $row->{biosample_id},
	);

  if (my $sex_letter = $row->{sex}) {
    $es_doc{sex} = fc($sex_letter) eq fc('M') ? 'male'
                  : fc($sex_letter) eq fc('F') ? 'female'
                  : 'unknown';
  }

	#source
	$sth_source->bind_param(1, $row->{sample_id});
	$sth_source->execute() or die $sth_source->errstr;
	my %source;
	while(my $source_row = $sth_source->fetchrow_hashref()) {
		$source{$source_row->{sample_source_id}} //= {
			name => $source_row->{source_name},
			description => $source_row->{source_desc},
			url => $source_row->{source_url},
		};
	}
	foreach my $sample_source_id (sort keys %source) {
		my $ss_hash = $source{$sample_source_id};
		push(@{$es_doc{source}}, $ss_hash);
	}

	#populations and superpops
	$sth_pops->bind_param(1, $row->{sample_id});
	$sth_pops->execute() or die $sth_pops->errstr;
	my %pops;
	while(my $pop_row = $sth_pops->fetchrow_hashref()) {
		$pops{$pop_row->{population_id}} //= {
			code => $pop_row->{pop_code},
			elasticId => $pop_row->{pop_elastic_id},
			description => $pop_row->{pop_desc},
			name => $pop_row->{pop_name},
			superpopulationCode => $pop_row->{superpop_code},
			superpopulationName => $pop_row->{superpop_name},
		}
	}
	foreach my $pop_id (sort keys %pops) {
		my $pop_hash = $pops{$pop_id};
		push(@{$es_doc{populations}}, $pop_hash);
	}
  

  $sth_files->bind_param(1, $row->{sample_id});
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

  $sth_relationship->bind_param(1, $row->{sample_id});
  $sth_relationship->execute() or die $sth_relationship->errstr;
  while (my $relationship_row = $sth_relationship->fetchrow_hashref()) {
    push(@{$es_doc{relatedSample}}, {relatedSampleName => $relationship_row->{name}, relationship => $relationship_row->{type}});
  }

	#synonyms
	$sth_synonym->bind_param(1, $row->{sample_id});
	$sth_synonym->execute() or die $sth_synonym->errstr;
	while (my $synonym_row = $sth_synonym->fetchrow_hashref()) {
		push(@{$es_doc{synonyms}},[$synonym_row->{synonym}]);
	}

	#print Dumper(%es_doc);

  eval{$es->index(
    index => $es_index_name,
    type => 'sample',
    id => $row->{name},
    body => \%es_doc,
  );};
  if (my $error = $@) {
    die "error indexing sample in $es_index_name index:".$error->{text};
  }
  $indexed_samples{$row->{name}} = 1;
}

my $scroll = $es->scroll_helper(
    index => $es_index_name,
    type => 'sample',
    search_type => 'scan',
    size => 500,
);
SCROLL:
while (my $es_doc = $scroll->next) {
  next SCROLL if $indexed_samples{$es_doc->{_id}};
  $es->delete(
    index => $es_index_name,
    type => 'sample',
    id => $es_doc->{_id},
  );
}

=pod

=head1 NAME

igsr-code/scripts/elasticsearch/load_samples.es.pl

=head1 SYNONPSIS

This script is for creating the sample index in elasticsearch. Run this script whenever you want to add, delete or modify samples in the portal.

The script roughly does this:

    1. Selects all rows in the mysql sample table.
    2. Works out which population, analysis_groups, data_types, data_collections are connected to the sample via the sample_file, file, and file_data_collection tables.
    3. Index all of those samples in elasticsearch
    4. Scans through all existing samples in elasticsearch. If it finds one that should not be there, then deletes it.

=head1 WHAT'S IN THE INDEX?

  # list them (10 only by default):
  curl http://www.internationalgenome.org/api/beta/sample/_search | python -m json.tool
  # pick one out by name:
  curl http://www.internationalgenome.org/api/beta/sample/NA12878 | python -m json.tool

=head1 OPTIONS

    -dbhost, the name of the mysql-host
    -dbname, the name of the mysql database
    -dbuser, the name of the mysql user
    -dbpass, the database password if appropriate
    -dbport, the port the mysql instance is running on
    -es_host, host and port of the elasticsearch index you are loading into, e.g. ves-hx-e3:9200
    -es_index_name, the elasticsearch index name, e.g. igsr_beta

=cut

