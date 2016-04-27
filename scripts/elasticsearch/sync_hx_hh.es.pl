#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use Search::Elasticsearch;
use File::Rsync;
use DBI;

my $from_es_host = 'ves-hx-e4';
my @to_es_host;
my $repo = 'hx_hh_sync';
my @snap_indices;
my @restore_indices;
my $snapshot_prefix = 'igsr';

&GetOptions(
  'from_es_host=s' =>\$from_es_host,
  'to_es_host=s' =>\@to_es_host,
  'repo=s' =>\$repo,
  'snapshot_prefix=s' =>\$snapshot_prefix,
  'snap_index=s' =>\@snap_indices,
  'restore_index=s' =>\@restore_indices,
);

# Some defaults:
if (!scalar @to_es_host) {
  @to_es_host = ('ves-pg-e4', 'ves-oy-e4');
}
if (!scalar @snap_indices) {
  @snap_indices = ('igsr', 'igsr_beta');
}

my @es_to = map {Search::Elasticsearch->new(nodes => "$_:9200", client => '1_0::Direct')} @to_es_host;
my $es_from = Search::Elasticsearch->new(nodes => "$from_es_host:9200", client => '1_0::Direct');

my ($sec,$min,$hour,$day,$month,$year) = localtime();
my $snapshot_name = sprintf("%s_%04d%02d%02d_%02d%02d%02d", $snapshot_prefix, $year+1900, $month+1, $day, $hour, $min, $sec);

my $repo_res = $es_from->snapshot->get_repository(
    repository => $repo,
);
my $repo_dir = $repo_res->{$repo}{settings}{location} || die "did not get repo directory for $repo";
$repo_dir .= '/'; # important for rsync
$repo_dir =~ s{//}{/}g;

eval{$es_from->snapshot->create(
    repository => $repo,
    snapshot => $snapshot_name,
    wait_for_completion => 1,
    body => {
        indices => join(',', @snap_indices),
        include_global_state => 0,
    }
);};
if (my $error = $@) {
  die "error creating snapshot $snapshot_name in $repo for indices @snap_indices: ".$error->{text};
}

my $rsync = File::Rsync->new({archive=>1});
foreach my $host (@to_es_host) {
  $rsync->exec({archive => 1, src => $repo_dir, dest => "$host:$repo_dir"})
      or die join("\n", "error syncing $repo_dir to $host", $rsync->err);
}

exit if ! scalar @restore_indices;

foreach my $es (@es_to) {

  eval{$es->snapshot->restore(
    repository => $repo,
    snapshot => $snapshot_name,
    wait_for_completion => 1,
    body => {
        indices => join(',', @restore_indices),
        include_global_state => 0,
    }
  );};
  if (my $error = $@) {
    die "error restoring snapshot $snapshot_name from $repo: ".$error->{text};
  }

}
