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
my $snapshot_suffix = sprintf("_%04d%02d%02d_%02d%02d%02d", $year+1900, $month+1, $day, $hour, $min, $sec);
my $snapshot_name = $snapshot_prefix.$snapshot_suffix;

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

foreach my $es (@es_to) {
  foreach my $restore_index (@restore_indices) {

    my $get_alias_res = eval{return $es->indices->get_alias(
      index => $restore_index,
    );};
    if (my $error = $@) {
      die "error getting index alias for $restore_index: ".$error->{text};
    }
    my @existing_aliases = grep {$_->{aliases}{$restore_index}} keys %$get_alias_res;
    die "unexpected number of existing aliases @existing_aliases" if scalar @existing_aliases != 1;
    my $old_index_name = $existing_aliases[0];

    my $new_index_name = $restore_index.$snapshot_suffix;
    eval{$es->snapshot->restore(
      repository => $repo,
      snapshot => $snapshot_name,
      wait_for_completion => 1,
      body => {
          indices => join(',', @restore_indices),
          include_global_state => 0,
          rename_pattern => $restore_index,
          rename_replacement => $new_index_name,
      }
    );};
    if (my $error = $@) {
      die "error restoring snapshot $snapshot_name from $repo for $restore_index: ".$error->{text};
    }

    eval{$es->indices->update_aliases(
      body => {
        actions => [
          {add => {alias => $restore_index, index => $new_index_name}},
          {remove => {alias => $restore_index, index => $old_index_name}},
        ]
      }
    );};
    if (my $error = $@) {
      die "error changing alias from $old_index_name to $new_index_name for index $restore_index: ".$error->{text};
    }

    eval{$es->indices->delete(
      index => $old_index_name,
    );};
    if (my $error = $@) {
      die "error deleting old index $old_index_name: ".$error->{text};
    }

  }

}
