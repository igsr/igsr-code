#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use Search::Elasticsearch;
use DBI;
use Data::Dumper;


my ($dbname, $dbhost, $dbuser, $dbport, $dbpass) = ('igsr_website_v2', 'mysql-igsr-web', 'g1kro', 4641, undef);
my $es_host = 'ves-hx-e4:9200';
my $es_index_name = 'igsr_beta';

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
#my $select_all_pops_sql = 'SELECT p.*, count(s.sample_id) num_samples, sp.code superpop_code, sp.name superpop_name, sp.display_colour superpop_display_colour, sp.display_order superpop_display_order
#    FROM population p
#    LEFT JOIN sample s on p.population_id=s.population_id
#    INNER JOIN superpopulation sp on p.superpopulation_id=sp.superpopulation_id
#    GROUP BY p.population_id';
my $select_all_pops_sql = 'select p.*, count(distinct sample_id) num_samples, sp.code superpop_code, sp.name superpop_name, sp.display_colour superpop_display_colour, sp.display_order superpop_display_order from population p, superpopulation sp, dc_sample_pop_assign dcsp where p.superpopulation_id = sp.superpopulation_id and p.population_id=dcsp.population_id group by p.population_id';

#my $select_files_sql = 'SELECT dt.code data_type, ag.description analysis_group, dc.title data_collection, dc.data_collection_id, dc.reuse_policy
#    FROM file f LEFT JOIN data_type dt ON f.data_type_id = dt.data_type_id
#    LEFT JOIN analysis_group ag ON f.analysis_group_id = ag.analysis_group_id
#    INNER JOIN sample_file sf ON sf.file_id=f.file_id
#    INNER JOIN file_data_collection fdc ON f.file_id=fdc.file_id
#    INNER JOIN data_collection dc ON fdc.data_collection_id=dc.data_collection_id
#    INNER JOIN sample s ON sf.sample_id=s.sample_id
#    WHERE s.population_id=?
#    GROUP BY dt.data_type_id, ag.analysis_group_id, dc.data_collection_id';

my $select_files_sql = 'SELECT dt.code data_type, ag.description analysis_group, dc.title data_collection, dc.data_collection_id, dc.reuse_policy FROM population p, dc_sample_pop_assign dspa, sample_file sf, file f, analysis_group ag, data_type dt, file_data_collection fdc, data_collection dc WHERE p.population_id=? and p.population_id=dspa.population_id and dspa.sample_id=sf.sample_id and sf.file_id=f.file_id and f.analysis_group_id=ag.analysis_group_id and f.data_type_id=dt.data_type_id and f.file_id=fdc.file_id and fdc.data_collection_id=dc.data_collection_id and dspa.data_collection_id=dc.data_collection_id GROUP BY dt.data_type_id, ag.analysis_group_id, dc.data_collection_id';

my $select_overlap_pops = 'select population_id pop_id, description pop_desc, elastic_id pop_elastic_id, count(*) sample_count from (select distinct population.population_id, population.description, population.elastic_id, sample.name from (select sample_id, population_id from dc_sample_pop_assign where population_id = ?) as t1, dc_sample_pop_assign, population, sample where t1.sample_id = dc_sample_pop_assign.sample_id and dc_sample_pop_assign.population_id != t1.population_id and dc_sample_pop_assign.population_id=population.population_id and dc_sample_pop_assign.sample_id=sample.sample_id order by description) as t3 group by description';

my $select_overlap_pop_samples = 'select distinct sample.sample_id s_id, sample.name s_name from (select sample_id si from dc_sample_pop_assign where population_id = ?) as t1, dc_sample_pop_assign, sample where dc_sample_pop_assign.sample_id = t1.si and dc_sample_pop_assign.population_id=? and dc_sample_pop_assign.sample_id=sample.sample_id';

#'select distinct population.population_id pop_id, population.description pop_desc, sample.sample_id s_id, sample.name s_name from (select sample_id, population_id from dc_sample_pop_assign where population_id = ?) as t1, dc_sample_pop_assign, population, sample where t1.sample_id = dc_sample_pop_assign.sample_id and dc_sample_pop_assign.population_id != t1.population_id and dc_sample_pop_assign.population_id=population.population_id and dc_sample_pop_assign.sample_id=sample.sample_id order by description;';
    
my $sth_population = $dbh->prepare($select_all_pops_sql) or die $dbh->errstr;
my $sth_files = $dbh->prepare($select_files_sql) or die $dbh->errstr;
my $sth_overlaps = $dbh->prepare($select_overlap_pops) or die $dbh->errstr;
my $sth_samples = $dbh->prepare($select_overlap_pop_samples) or die $dbh->errstr;

$sth_population->execute() or die $sth_population->errstr;
my %indexed_pops;

my $pop_counter = 0;

#Looping through the populations returned by population SQL
POPS:
while (my $row = $sth_population->fetchrow_hashref()) {

  my %es_doc = (
    code => $row->{code},
		elasticId => $row->{elastic_id},
    name => $row->{name},
    description => $row->{description},
		display_order => $row->{display_order},
    latitude => $row->{latitude},
    longitude => $row->{longitude},
    superpopulation => {
      code => $row->{superpop_code},
      name => $row->{superpop_name},
			display_colour => $row->{superpop_display_colour},
      display_order => $row->{superpop_display_order},
    },
    samples => {
      count => $row->{num_samples},
    },
  );

	$pop_counter++;
	print "processing pops: $row->{elastic_id} pop $pop_counter\n";

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

	$sth_overlaps->bind_param(1, $row->{population_id});
	$sth_overlaps->execute() or die $sth_overlaps->errstr;
	my %overlapping_pops;
	while (my $overlap_row = $sth_overlaps->fetchrow_hashref()) {

		#for each overlapping population, get the samples that are shared
		$sth_samples->bind_param(1, $row->{population_id});
		$sth_samples->bind_param(2, $overlap_row->{pop_id});
		$sth_samples->execute() or die $sth_samples->errstr;

		#create the structure for the overlapping population
		$overlapping_pops{$overlap_row->{pop_id}} //= {
			populationDescription => $overlap_row->{pop_desc},
			populationElasticId => $overlap_row->{pop_elastic_id},
			sharedSampleCount => $overlap_row->{sample_count},
		};
		#add the list of shared samples
		my @samples;
		while (my $sample_row = $sth_samples->fetchrow_hashref()) {
			push(@samples, $sample_row->{s_name});
		}
		$overlapping_pops{$overlap_row->{pop_id}}{sharedSamples} = [@samples];
	}
	foreach my $overlap_pop_id (sort keys %overlapping_pops){
		my $op_hash = $overlapping_pops{$overlap_pop_id};
		push(@{$es_doc{overlappingPopulations}}, $op_hash);
	}

	#print Dumper(%es_doc);

  eval{$es->index(
    index => $es_index_name,
    type => 'population',
    id => $row->{elastic_id},
    body => \%es_doc,
  );};
  if (my $error = $@) {
    die "error indexing population in $es_index_name index:".$error->{text};
  }
  $indexed_pops{$row->{elastic_id}} = 1;
}

my $scroll = $es->scroll_helper(
    index => $es_index_name,
    type => 'population',
    search_type => 'scan',
    size => 500,
);
SCROLL:
while (my $es_doc = $scroll->next) {
  next SCROLL if $indexed_pops{$es_doc->{_id}};
  $es->delete(
    index => $es_index_name,
    type => 'population',
    id => $es_doc->{_id},
  );
}

=pod

=head1 NAME

igsr-code/scripts/elasticsearch/load_populations.es.pl

=head1 SYNONPSIS

This script is for creating the population index in elasticsearch. Run this script whenever you want to add, delete or modify populations in the portal.

The script roughly does this:

    Selects all rows in the mysql population table.
    Works out which samples, analysis_groups, data_types, data_collections are connected to the population via the sample, sample_file, file, and file_data_collection tables.
    Index all of those populations in elasticsearch
    Scans through all existing populations in elasticsearch. If it finds one that should not be there, then deletes it.

=head1 WHAT'S IN THE INDEX?

  # list them (10 only by default):
  curl http://www.internationalgenome.org/api/beta/population/_search | python -m json.tool
  # pick one out by name:
  curl http://www.internationalgenome.org/api/beta/population/GWD | python -m json.tool

=head1 OPTIONS

    -dbhost, the name of the mysql-host
    -dbname, the name of the mysql database
    -dbuser, the name of the mysql user
    -dbpass, the database password if appropriate
    -dbport, the port the mysql instance is running on
    -es_host, host and port of the elasticsearch index you are loading into, e.g. ves-hx-e3:9200
    -es_index_name, the elasticsearch index name, e.g. igsr_beta

=cut

