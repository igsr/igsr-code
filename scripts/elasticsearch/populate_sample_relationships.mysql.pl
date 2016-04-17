#!/usr/bin/env perl

use strict;
use warnings;
use Text::Delimited;
use Getopt::Long;
use DBI;
use List::Util qw();

my $sample_ped = '/nfs/1000g-archive/vol1/ftp/release/20130502/integrated_call_samples_v2.20130502.ALL.ped';
my ($dbname, $dbhost, $dbuser, $dbport, $dbpass) = ('igsr_website', 'mysql-g1kdcc-public', 'g1krw', 4197, undef);

&GetOptions(
  'sample_ped=s'      => \$sample_ped,
  'dbpass=s'      => \$dbpass,
  'dbport=s'      => \$dbport,
  'dbuser=s'      => \$dbuser,
  'dbhost=s'      => \$dbhost,
  'dbname=s'      => \$dbname,
);

my $dbh = DBI->connect("DBI:mysql:$dbname;host=$dbhost;port=$dbport", $dbuser, $dbpass) or die $DBI::errstr;
my $insert_sql = 'INSERT INTO sample_relationship(subject_sample_id, relation_sample_id, type) SELECT s1.sample_id, s2.sample_id, ? from sample s1, sample s2 where s1.name=? and s2.name=?';
my $sth = $dbh->prepare($insert_sql) or die $dbh->errstr;

my @second_orders;
my @third_orders;
my %inserted_relationships;
my $ped_fh = new Text::Delimited;
$ped_fh->delimiter("\t");
$ped_fh->open($sample_ped) or die "could not open $sample_ped $!";
LINE:
while( my $line = $ped_fh->read) {
  my $sample_id = $line->{'Individual ID'};
  my ($father, $mother, $siblings, $second_orders, $third_orders, $children)
      = map {$_ || ''} @{$line}{'Paternal ID', 'Maternal ID', 'Siblings', 'Second Order', 'Third Order', 'Children'};
  insert_relationship($sample_id, $father, 'Father');
  insert_relationship($sample_id, $mother, 'Mother');
  foreach my $child (split(',', $children)) {
    insert_relationship($sample_id, $child, 'Child');
    insert_relationship($mother, $child, 'Grandchild');
    insert_relationship($father, $child, 'Grandchild');
    insert_relationship($child, $father, $line->{'Gender'} == 1 ? 'Paternal Grandfather' :
          $line->{'Gender'} == 2 ? 'Maternal Grandfather' : 'Grandfather');
    insert_relationship($child, $mother, $line->{'Gender'} == 1 ? 'Paternal Grandmother' :
          $line->{'Gender'} == 2 ? 'Maternal Grandmother' : 'Grandmother');
  }
  foreach my $sibling (split(/ *, */, $siblings)) {
    if ($sibling =~ /2nd order to (\w+)/) {
      push(@second_orders, [$sample_id, $1]);
    }
    else {
      insert_relationship($sample_id, $sibling, 'Sibling');
    }
  }
  foreach my $second_order (split(',', $second_orders)) {
    $second_order =~ s/ *$//;
    $second_order =~ s/.* //;
    push(@second_orders, [$sample_id, $second_order]);
  }
  foreach my $third_order (split(',', $third_orders)) {
    $third_order =~ s/ *$//;
    $third_order =~ s/.* //;
    push(@second_orders, [$sample_id, $third_order]);
  }

}
$ped_fh->close();

SECOND_ORDER:
foreach my $relationship (@second_orders) {
  next SECOND_ORDER if $inserted_relationships{$relationship->[0]}{$relationship->[1]};
  insert_relationship(@$relationship, 'Second Order');
}
THIRD_ORDER:
foreach my $relationship (@third_orders) {
  next THIRD_ORDER if $inserted_relationships{$relationship->[0]}{$relationship->[1]};
  insert_relationship(@$relationship, 'Third Order');
}

sub insert_relationship {
  my ($subject, $relation, $type) = @_;
  return if !$relation;
  return if !$subject;
  $sth->bind_param(1, $type);
  $sth->bind_param(2, $subject);
  $sth->bind_param(3, $relation);
  my $rv = $sth->execute() or die $sth->errstr;
  print STDERR "skipping relationship between unknown samples $subject $relation\n" if $rv !=1;
  $inserted_relationships{$subject}{$relation} = 1;
}
