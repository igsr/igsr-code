#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use Search::Elasticsearch;
use DBI;

my ($dbname, $dbhost, $dbuser, $dbport, $dbpass) = ('igsr_website', 'mysql-g1kdcc-public', 'g1kro', 4197, undef);
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
my $select_all_ags_sql = 'SELECT * from analysis_group';
my $sth_ags = $dbh->prepare($select_all_ags_sql) or die $dbh->errstr;

$sth_ags->execute() or die $sth_ags->errstr;
my %indexed_ags;
while (my $row = $sth_ags->fetchrow_hashref()) {
  my %es_doc = (
    title => $row->{description},
    shortTitle => $row->{short_title},
    displayOrder => $row->{table_display_order},
  );

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
