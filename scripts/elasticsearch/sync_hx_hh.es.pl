#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use Search::Elasticsearch;
use File::Rsync;

my $from_es_host;
my @to_es_host;
my $repo = 'hx_hh_sync';
my $index_name = 'igsr';

&GetOptions(
  'from_es_host=s' =>\$from_es_host,
  'to_es_host=s' =>\@to_es_host,
  'repo=s' =>\$repo,
  'index_name=s' =>\$index_name,
);
my @es_to = map {Search::Elasticsearch->new(nodes => "$_:9200", client => '1_0::Direct')} @to_es_host;
my $es_from = Search::Elasticsearch->new(nodes => "$from_es_host:9200", client => '1_0::Direct');

my ($sec,$min,$hour,$day,$month,$year) = localtime();
my $snapshot_name = sprintf("%s_%04d%02d%02d%02d%02d%02d", $index_name, $year+1900, $month+1, $day, $hour, $min, $sec);

my $repo_res = $es_from->snapshot->get_repository(
    repository => $repo,
);
my $repo_dir = $repo_res->{$repo}{settings}{location} || die "did not get repo directory for $repo";
$repo_dir =~ s{/+$}{};

$es_from->snapshot->create(
    repository => $repo,
    snapshot => $snapshot_name,
    wait_for_completion => 1,
    body => {
        indicies => $index_name,
    }
);

foreach my $host (@to_es_host) {
  my $rsync = new File::Rsync;
  $rsync->exec({
    archive => 1,
    src => $repo_dir,
    dest => "$host:$repo_dir",
  }) or die join("\t", $rsync->err, $rsync->lastcmd);
}

foreach my $es (@es_to) {

  my $get_alias_res = $es->indices->get_alias(
    index => $index_name,
  );
  my @existing_aliases = grep {$_->{aliases}{$index_name}} keys %$get_alias_res;
  die "unexpected number of existing aliases @existing_aliases" if scalar @existing_alises != 1;
  my $old_index_name = $existing_aliases[0];

  $es->snapshot->restore(
    repository => $repo,
    snapshot => $snapshot_name,
    wait_for_completion => 1,
    body => {
        indicies => $index_name,
        rename_pattern => $index_name,
        rename_replacement => $snapshot_name,
    }
  );

  $es->indices->update_aliases(
    body => {
      actions => [
        {add => {alias => $index_name, index => $snapshot_name}},
        {remove => {alias => $index_name, index => $old_index_name}},
      ]
    }
  );

  $es->indices->delete(
    index => $old_index_name,
  );
}
