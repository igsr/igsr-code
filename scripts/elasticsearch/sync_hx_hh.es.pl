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
my $es_index_name = 'igsr';
my ($dbname, $dbhost, $dbuser, $dbport, $dbpass) = ('igsr_website', 'mysql-g1kdcc-public', 'g1krw', 4197, undef);
my $check_timestamp;

&GetOptions(
  'from_es_host=s' =>\$from_es_host,
  'to_es_host=s' =>\@to_es_host,
  'repo=s' =>\$repo,
  'es_index_name=s' =>\$es_index_name,
  'dbpass=s'      => \$dbpass,
  'dbport=i'      => \$dbport,
  'dbuser=s'      => \$dbuser,
  'dbhost=s'      => \$dbhost,
  'dbname=s'      => \$dbname,
  'check_timestamp!' => \$check_timestamp,
);

# Some defaults:
if (!scalar @to_es_host) {
  @to_es_host = ('ves-pg-e4', 'ves-oy-e4');
}

my $dbh = DBI->connect("DBI:mysql:$dbname;host=$dbhost;port=$dbport", $dbuser, $dbpass) or die $DBI::errstr;
my $timestamp_sql =
   'SELECT current_tree_log_id, synced_hx_to_hh FROM current_tree_log
    ORDER BY loaded_into_elasticsearch DESC limit 1';
my $sth_timestamp = $dbh->prepare($timestamp_sql) or die $dbh->errstr;
$sth_timestamp->execute() or die $sth_timestamp->errstr;
my $row_timestamp = $sth_timestamp->fetchrow_hashref();
exit if $check_timestamp && (!$row_timestamp || $row_timestamp->{synced_hh_to_hx});
my $current_tree_log_id = $row_timestamp ? $row_timestamp->{current_tree_log_id} : undef;

my @es_to = map {Search::Elasticsearch->new(nodes => "$_:9200", client => '1_0::Direct')} @to_es_host;
my $es_from = Search::Elasticsearch->new(nodes => "$from_es_host:9200", client => '1_0::Direct');

my ($sec,$min,$hour,$day,$month,$year) = localtime();
my $snapshot_name = sprintf("%s_%04d%02d%02d%02d%02d%02d", $es_index_name, $year+1900, $month+1, $day, $hour, $min, $sec);

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
        indicies => $es_index_name,
    }
);};
if (my $error = $@) {
  die "error creating snapshot $snapshot_name in $repo for index $es_index_name: ".$error->{text};
}

my $rsync = File::Rsync->new({archive=>1});
foreach my $host (@to_es_host) {
  $rsync->exec({archive => 1, src => $repo_dir, dest => "$host:$repo_dir"})
      or die join("\n", "error syncing $repo_dir to $host", $rsync->err);
}

foreach my $es (@es_to) {

  my $get_alias_res = eval{return $es->indices->get_alias(
    index => $es_index_name,
  );};
  if (my $error = $@) {
    die "error getting index alias for $es_index_name: ".$error->{text};
  }
  my @existing_aliases = grep {exists $get_alias_res->{$_}->{aliases}{$es_index_name}} keys %$get_alias_res;
  die "unexpected number of existing aliases @existing_aliases" if scalar @existing_aliases != 1;
  my $old_index_name = $existing_aliases[0];

  eval{$es->snapshot->restore(
    repository => $repo,
    snapshot => $snapshot_name,
    wait_for_completion => 1,
    body => {
        indices => $es_index_name,
        rename_pattern => $es_index_name,
        rename_replacement => $snapshot_name,
    }
  );};
  if (my $error = $@) {
    die "error restoring snapshot $snapshot_name from $repo: ".$error->{text};
  }

  eval{$es->indices->update_aliases(
    body => {
      actions => [
        {add => {alias => $es_index_name, index => $snapshot_name}},
        {remove => {alias => $es_index_name, index => $old_index_name}},
      ]
    }
  );};
  if (my $error = $@) {
    die "error changing alias from $old_index_name to $snapshot_name for index $es_index_name: ".$error->{text};
  }

  eval{$es->indices->delete(
    index => $old_index_name,
  );};
  if (my $error = $@) {
    die "error deleting old index $old_index_name: ".$error->{text};
  }
}

if ($current_tree_log_id) {
    my $log_sql = 'UPDATE current_tree_log SET synced_hx_to_hh=now() WHERE current_tree_log_id=?';
    my $sth_log = $dbh->prepare($log_sql) or die $dbh->errstr;
    $sth_log->bind_param(1, $current_tree_log_id);
    $sth_log->execute() or die $sth_log->errstr;
}
