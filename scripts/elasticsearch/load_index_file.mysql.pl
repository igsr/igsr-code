#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use DBI;

my $index_file = '/nfs/1000g-archive/vol1/ftp/data_collections/1000_genomes_project/1000genomes.sequence.index';
my $data_collection = '1000genomes';
my $data_type = 'sequence';
my $analysis_group;
my ($use_column_headers, $infer_sample);
my (@url_column, @md5_column);
my ($analysis_group_column, $withdrawn_column, $sample_column);
my ($dbname, $dbhost, $dbuser, $dbport, $dbpass) = ('igsr_website', 'mysql-g1kdcc-public', 'g1krw', 4197, undef);
my $root = '';

&GetOptions(
  'file|index_file=s'      => \$index_file,
  'data_collection=s' => \$data_collection,
  'data_type=s' => \$data_type,
  'analysis_group=s' => \$analysis_group,
  'root=s'        => \$root,
  'dbpass=s'      => \$dbpass,
  'dbport=i'      => \$dbport,
  'dbuser=s'      => \$dbuser,
  'dbhost=s'      => \$dbhost,
  'dbname=s'      => \$dbname,

  'url_column=s' => \@url_column,
  'md5_column=s' => \@md5_column,
  'withdrawn_column=s' => \$withdrawn_column,
  'sample_column=s' => \$sample_column,
  'analysis_group_column=s' => \$analysis_group_column,

  'use_column_headers' => \$use_column_headers,
  'infer_sample' => \$infer_sample,
);
die "did not get url column on command line" if ! scalar @url_column;
die "need one md5 column for each url column" if @md5_column && (scalar @url_column != scalar @md5_column);

my $dbh = DBI->connect("DBI:mysql:$dbname;host=$dbhost;port=$dbport", $dbuser, $dbpass) or die $DBI::errstr;
$dbh->{AutoCommit} = 0;
#my $insert_file_sql = 'INSERT INTO file(url, url_crc, md5) VALUES(?, crc32(?), ?) ON DUPLICATE KEY UPDATE file_id=LAST_INSERT_ID(file_id)';
my $insert_file_sql =
      'INSERT INTO file(url, url_crc, md5, foreign_file)
        SELECT f2.url, crc32(f2.url), f2.md5, f2.foreign_file
        FROM (SELECT ? AS url, ? AS md5, ? AS foreign_file) AS f2
      ON DUPLICATE KEY UPDATE file_id=LAST_INSERT_ID(file_id), indexed_in_elasticsearch=0';
my $update_type_sql = 'UPDATE file SET data_type_id=(SELECT data_type_id FROM data_type WHERE code=?) WHERE file_id=?';
my $update_analysis_group_sql = 'UPDATE file SET analysis_group_id=(SELECT analysis_group_id FROM analysis_group WHERE code=?) WHERE file_id=?';
my $insert_data_collection_sql = 'INSERT IGNORE INTO file_data_collection(file_id, data_collection_id) SELECT ?, data_collection_id from data_collection where code=?';
my $insert_sample_sql = 'INSERT IGNORE INTO sample_file(file_id, sample_id) SELECT ?, sample_id from sample where name=?';
my $sth_file = $dbh->prepare($insert_file_sql) or die $dbh->errstr;
my $sth_type = $dbh->prepare($update_type_sql) or die $dbh->errstr;
my $sth_analysis_group = $dbh->prepare($update_analysis_group_sql) or die $dbh->errstr;
my $sth_data_collection = $dbh->prepare($insert_data_collection_sql) or die $dbh->errstr;
my $sth_sample = $dbh->prepare($insert_sample_sql) or die $dbh->errstr;

open my $fh, '<', $index_file or die "could not open index_file $!";

my (@i_url, @i_md5, $i_analysis_group, $i_withdrawn, $i_sample);

if ($use_column_headers) {
  LINE:
  while (my $line = <$fh>) {
    next LINE if $line =~ /^##/;
    chomp $line;
    $line =~ s/^#//;
    my @split_line = split("\t", $line);
    my %cols;
    foreach my $i (0..$#split_line) {
      $cols{uc($split_line[$i])} = $i;
    }
    @i_url = map {$cols{uc($_)}} @url_column;
    @i_md5 = map {$cols{uc($_)}} @md5_column;
    $i_analysis_group = $analysis_group_column ? $cols{$analysis_group_column} : undef;
    $i_withdrawn = $withdrawn_column ? $cols{$withdrawn_column} : undef;
    $i_sample = $sample_column ? $cols{$sample_column} : undef;
    last LINE;
  }
}
else {
  @i_url = @url_column;
  @i_md5 = @md5_column;
  ($i_analysis_group, $i_withdrawn, $i_sample) = ($analysis_group_column, $withdrawn_column, $i_sample);
}

die "do not know column of url" if grep {!defined $_} @i_url;
die "do not know column of md5" if grep {!defined $_} @i_md5;


LINE:
while (my $line = <$fh>) {
  chomp $line;
  my @split_line = split("\t", $line);
  next LINE if defined $i_withdrawn && $split_line[$i_withdrawn];
  foreach my $j (0..$#i_url) {
    my ($i_url, $i_md5) = ($i_url[$j], $i_md5[$j]);
    next LINE if !$split_line[$i_url];
    my $url = $root.$split_line[$i_url];

    #temporary line until index files are fixed:
    $url =~ s{ftp:/ftp}{ftp://ftp};

    $sth_file->bind_param(1, $url);
    $sth_file->bind_param(2, defined $i_md5 ? $split_line[$i_md5] : undef);
    $sth_file->bind_param(3, ! scalar $url =~ /ftp\.1000genomes\.ebi\.ac\.uk/);
    $sth_file->execute() or die $sth_file->errstr;
    my $file_id = $sth_file->{mysql_insertid};

    if (my $file_analysis_group = $analysis_group ? $analysis_group
                        : defined($i_analysis_group) ? lc($split_line[$i_analysis_group]) : undef) {
      $file_analysis_group =~ s/ /_/g;
      $sth_analysis_group->bind_param(1, $file_analysis_group);
      $sth_analysis_group->bind_param(2, $file_id);
      $sth_analysis_group->execute() or die $sth_analysis_group->errstr;
    }

    if ($data_type) {
      $sth_type->bind_param(1, $data_type);
      $sth_type->bind_param(2, $file_id);
      $sth_type->execute() or die $sth_type->errstr;
    }

    if ($data_collection) {
      $sth_data_collection->bind_param(1, $file_id);
      $sth_data_collection->bind_param(2, $data_collection);
      $sth_data_collection->execute() or die $sth_data_collection->errstr;
    }

    my $sample = defined $i_sample ? $split_line[$i_sample] : undef;
    if (!$sample && $infer_sample) {
      ($sample) = $url =~ m{/data/(?:[A-Z]{3}/)?([^\./]+)/};
    }
    if ($sample) {
      $sth_sample->bind_param(1, $file_id);
      $sth_sample->bind_param(2, $sample);
      $sth_sample->execute() or die $sth_sample->errstr;
    }
  }
}
close $fh;
$dbh->commit;
