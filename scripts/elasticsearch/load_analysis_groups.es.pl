#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use Search::Elasticsearch;
use DBI;

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
my $select_all_ags_sql = 'SELECT ag.* from file f
    INNER JOIN analysis_group ag ON f.analysis_group_id = ag.analysis_group_id
    INNER JOIN sample_file sf ON sf.file_id=f.file_id
    GROUP BY ag.analysis_group_id';
my $sth_ags = $dbh->prepare($select_all_ags_sql) or die $dbh->errstr;

$sth_ags->execute() or die $sth_ags->errstr;
my %indexed_ags;
while (my $row = $sth_ags->fetchrow_hashref()) {
  my %es_doc = (
    title => $row->{description},
    shortTitle => $row->{short_title},
    displayOrder => $row->{table_display_order},
		longDescription => $row->{long_description},
  );
  if ($row->{table_display_order}) {
    $es_doc{displayOrder} = $row->{table_display_order};
  }

  my $es_id = lc($row->{short_title});
  $es_id =~ s/[^\w]/-/g;

  eval{$es->index(
    index => $es_index_name,
    type => 'analysis-group',
    id => $es_id,
    body => \%es_doc,
  );};
  if (my $error = $@) {
    die "error indexing analysis group in $es_index_name index:".$error->{text};
  }
  $indexed_ags{$es_id} = 1;
}

my $scroll = $es->scroll_helper(
    index => $es_index_name,
    type => 'analysis-group',
    search_type => 'scan',
    size => 500,
);
SCROLL:
while (my $es_doc = $scroll->next) {
  next SCROLL if $indexed_ags{$es_doc->{_id}};
  $es->delete(
    index => $es_index_name,
    type => 'analysis-group',
    id => $es_doc->{_id},
  );
}

=pod

=head1 NAME

igsr-code/scripts/elasticsearch/load_analysis_groups.es.pl

=head1 SYNONPSIS

This script is for creating the analysis-group index in elasticsearch. Run this script whenever you want to add, delete or modify analysis-groups in the portal.

The script roughly does this:

    Selects all rows in the mysql analysis_group table. (It ignore a row if it is not connected to a sample via the sample_file and file tables)
    Index all of those analysis groups in elasticsearch
    Scans through all existing analysis groups in elasticsearch. If it finds one that should not be there, then deletes it.

=head1 WHAT'S IN THE INDEX?

  # list them (10 only by default):
  curl http://www.internationalgenome.org/api/beta/analysis-group/_search | python -m json.tool
  # pick one out by name:
  curl http://www.internationalgenome.org/api/beta/analysis-group/low-cov-wgs | python -m json.tool

=head1 OPTIONS

    -dbhost, the name of the mysql-host
    -dbname, the name of the mysql database
    -dbuser, the name of the mysql user
    -dbpass, the database password if appropriate
    -dbport, the port the mysql instance is running on
    -es_host, host and port of the elasticsearch index you are loading into, e.g. ves-hx-e3:9200
    -es_index_name, the elasticsearch index name, e.g. igsr_beta

=cut

