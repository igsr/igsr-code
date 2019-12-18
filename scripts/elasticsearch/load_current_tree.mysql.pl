#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use File::stat;
use Net::FTP;
use IO::Pipe;
use DBI;

my ($dbname, $dbhost, $dbuser, $dbport, $dbpass) = ('igsr_website_v2', 'mysql-igsr-web', 'g1krw', 4641, undef);
my $current_tree = '/nfs/1000g-archive/vol1/ftp/current.tree';
my $ftp_current_tree = '/vol1/ftp/current.tree';
my $ftp_host= 'ftp.1000genomes.ebi.ac.uk';
my $root = 'ftp://ftp.1000genomes.ebi.ac.uk/vol1/';
my $check_timestamp;
my $use_ftp;

&GetOptions(
  'current_tree=s' => \$current_tree,
  'root=s'        => \$root,
  'dbpass=s'      => \$dbpass,
  'dbport=i'      => \$dbport,
  'dbuser=s'      => \$dbuser,
  'dbhost=s'      => \$dbhost,
  'dbname=s'      => \$dbname,
  'check_timestamp!' => \$check_timestamp,

  'ftp!'        => \$use_ftp,
  'ftphost=s'   => \$ftp_host,
  'ftp_current_tree=s'   => \$ftp_current_tree,
);


my $ftp;
my $mtime;
if ($use_ftp) {
  $ftp = Net::FTP->new($ftp_host) or die "cannot connect to $ftp_host $@";
  $ftp->login("anonymous",'-anonymous@') or die "Cannot login ", $ftp->message;
  $mtime = $ftp->mdtm($ftp_current_tree) or die "could not get mtime", $ftp->message;
}
else {
  my $st = stat($current_tree) or die "could not stat $current_tree $!";
  $mtime = $st->mtime;
}

my $fh;
my $fork_pid;
if ($use_ftp) {
  $fh = IO::Pipe->new();
# Fork must happen before database connection, or else errors occur on closing child process
  $fork_pid = fork();
  if ($fork_pid) { #parent
    $fh->reader();
  }
  elsif (defined $fork_pid) { #child
    $fh->writer();
    eval{$ftp->get($ftp_current_tree, $fh) or die "could not get $ftp_current_tree ", $ftp->message;};
    if (my $error = $@) {
      $fh->close;
      die $error;
    }
    $fh->close;
    exit(0);
  }
  else {
    die "error forking: $!";
  }
}
else {
  open $fh, '<', $current_tree or die "could not open current_tree $!";
}

my $dbh = DBI->connect("DBI:mysql:$dbname;host=$dbhost;port=$dbport", $dbuser, $dbpass) or die $DBI::errstr;
if ($check_timestamp) {
  my $timestamp_sql = 'SELECT count(*) AS loaded FROM current_tree_log WHERE current_tree_mtime = FROM_UNIXTIME(?) AND loaded_into_db IS NOT NULL';
  my $sth_timestamp = $dbh->prepare($timestamp_sql) or die $dbh->errstr;
  $sth_timestamp->bind_param(1, $mtime);
  $sth_timestamp->execute() or die $sth_timestamp->errstr;
  my $row = $sth_timestamp->fetchrow_hashref();
  exit if $row && $row->{loaded};
}

$dbh->{AutoCommit} = 0;
my $reset_sql = 'UPDATE file SET in_current_tree=0';
my $select_file_sql =
  'SELECT f.file_id, f.md5, f.indexed_in_elasticsearch
    FROM file f, (SELECT ? AS url) f2
    WHERE f.url_crc=crc32(f2.url) AND f.url=f2.url';
my $insert_file_sql =
  'INSERT INTO file(url, url_crc, md5, foreign_file, in_current_tree)
  SELECT f2.url, crc32(f2.url), f2.md5, 0, 1
    FROM (SELECT ? AS url, ? AS md5) f2';
my $update_file_sql = 'UPDATE file SET md5=?, in_current_tree=1, indexed_in_elasticsearch=? WHERE file_id=?';
my $log_sql = 'INSERT INTO current_tree_log(current_tree_mtime, loaded_into_db) VALUES (FROM_UNIXTIME(?), now())';
my $sth_reset = $dbh->prepare($reset_sql) or die $dbh->errstr;
my $sth_select = $dbh->prepare($select_file_sql) or die $dbh->errstr;
my $sth_insert = $dbh->prepare($insert_file_sql) or die $dbh->errstr;
my $sth_update = $dbh->prepare($update_file_sql) or die $dbh->errstr;
my $sth_log = $dbh->prepare($log_sql) or die $dbh->errstr;
$sth_reset->execute() or die $sth_reset->errstr;


LINE:
while (my $line = <$fh>) {
	chomp $line;
  my @split_line = split("\t", $line);
  next LINE if $split_line[1] ne 'file';
  next LINE if !$split_line[0];
  my $url = $root.$split_line[0];

  $sth_select->bind_param(1, $url);
  $sth_select->execute() or die $sth_select->errstr;
  if (my $row = $sth_select->fetchrow_hashref()) {
    $sth_update->bind_param(1, $split_line[4]);
    $sth_update->bind_param(2, $split_line[4] && $row->{md5} && $split_line[4] eq $row->{md5} ? $row->{indexed_in_elasticsearch} : 0);
    $sth_update->bind_param(3, $row->{file_id});
    $sth_update->execute() or die $sth_update->errstr;
  }
  else {
    $sth_insert->bind_param(1, $url);
    $sth_insert->bind_param(2, $split_line[4]);
    $sth_insert->execute() or die $sth_insert->errstr;
  }

}
close $fh;

if ($use_ftp) {
  waitpid($fork_pid, 0);
  exit($?) if($?);
}

$sth_log->bind_param(1, $mtime);
$sth_log->execute() or die $sth_log->errstr;

$dbh->commit;

=pod

=head1 NAME

igsr-code/scripts/elasticsearch/load_current_tree.mysql.pl

=head1 SYNONPSIS

This script gets run every night by a cron job. Here is what it does:

    Gets the current_tree file, either by FTP if running in Hinxton (default) or by opening the file directly if in Hemel.
    Uses unix timestamp to find out when the current_tree was last modified. Compares to the database current_tree_log table to work out if the file has changed since the script last run. Exits early if file is unchanged and if the --check_timestamp flag is set.
    Sets in_current_tree=0 for all files in the mysql file table
    For each file in current tree, checks if it is already in the file table of mysql database:
        Inserts a new row if the file was missing, setting in_current_tree=1
        Updates the md5 and sets in_current_tree=1 if the file is already in the table.

=head1 OPTIONS

    -dbhost, the name of the mysql-host
    -dbname, the name of the mysql database
    -dbuser, the name of the mysql user
    -dbpass, the database password if appropriate
    -dbport, the port the mysql instance is running on
    -es_host, host and port of the elasticsearch index you are loading into, e.g. ves-hx-e3:9200
    -es_index_name, the elasticsearch index name, e.g. igsr_beta
    --current_tree=/nfs/1000g-archive/vol1/ftp/current.tree where the file lives. Set by default. Only used if running the script in Hemel.
    --ftp_current_tree=/vol1/ftp/current.tree where the file lives on the FTP site. Set by default. Only used if running the script in Hinxton.
    --ftphost=ftp.1000genomes.ebi.ac.uk. Connection details, set by default.
    --root=ftp://ftp.1000genomes.ebi.ac.uk/vol1/ - set by default. This is how the url should be named when the file goes into the portal.
    --check_timestamp - a boolean flag. Not set by default. The script will exit early if this is set if the current tree has not changed since the script was last run.
    --ftp - boolean flag. Set to true by default. Tells the script that you are in Hinxton, so connect by FTP. Set to --noftp to cancel it (if you are in Hemel).

=cut

