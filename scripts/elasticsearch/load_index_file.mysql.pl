#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use DBI;

my $index_file = '/nfs/1000g-archive/vol1/ftp/data_collections/1000_genomes_project/1000genomes.sequence.index';
my $data_collection = '1000genomes';
my $data_type = 'sequence';
my $analysis_group;
my ($use_column_headers, $infer_sample, $infer_data_type);
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
  'infer_data_type' => \$infer_data_type,
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
    $i_analysis_group = $analysis_group_column ? $cols{uc($analysis_group_column)} : undef;
    $i_withdrawn = $withdrawn_column ? $cols{uc($withdrawn_column)} : undef;
    $i_sample = $sample_column ? $cols{uc($sample_column)} : undef;
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

    if ($infer_data_type) {
      $data_type = $url =~ /\.vcf(?:\.gz)?(?:\.tbi)?$/ ? 'variants'
                : $url =~ /\.bam(?:\.[a-z]+)?$/ ? 'alignment'
                : $url =~ /\.cram(?:\.[a-z]+)?$/ ? 'alignment'
                : 'sequence';
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

=pod

=head1 NAME

igsr-code/scripts/elasticsearch/load_index_file.mysql.pl

=head1 SYNONPSIS

This the primary way for loading the file table. It can be used for loading foreign files, or for annotating current tree files to indicate that they belong to a sample or data_collection or analysis_group. Take a look at the bash script shell/load_index_files.mysql.sh in the igsr-code git repo. It contains a record of the exact command lines that were used to call the perl script when the database was first loaded in April 2016. This bash script is helpful to demostrate how command lines could be constructed.

    For every file listed in your tab-delimited index file:
        1. Inserts the file into file table, and check for duplicate_key on the url column
        2. If there was a duplicate_key, then the file was already in the file table. So it sets indexed_indexed_elasticsearch=0 to mark that you are modifying this file.
        3. Updates the analysis_group_id in the file table if you used the --analysis_group or --analysis_group_column arguments
        4. Updates the data_type_id in the file table if you used the --data_type or --infer_data_type arguments
        5. Inserts a row in the file_data_collection table if you used the --data_collection argument
        6. Inserts a row in the sample_file table if you used the --sample_columns or --infer_sample argument


=head1 OPTIONS

    -dbhost, the name of the mysql-host
    -dbname, the name of the mysql database
    -dbuser, the name of the mysql user
    -dbpass, the database password if appropriate
    -dbport, the port the mysql instance is running on
    -es_host, host and port of the elasticsearch index you are loading into, e.g. ves-hx-e3:9200
    -es_index_name, the elasticsearch index name, e.g. igsr_beta
    --file=/path/to/my/file the tab delimited index file you want to load
    --route=ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase3/ this is optional. If your index file does not have full urls, then this string is prepended.
    --use_column_headers boolean flag. Use this flag if your index file has a header row that defines the columns
    --url_column=0 or --url_column=BAM. Can be used multiple times if your index file has many files per row (e.g. a bam and a bas and a bai). The first format --url_column=0 mean the first column in the file. Use the second format --url_column=BAM if you have set the --use_column_headers flag.
    --md5_column=4 or --md5_column=BAM_MD5. Same as for the url_column. Can be specified many times.
    --withdrawn_column=5 or --withdrawn_column=WITHDRAWN. Optional. Tells the script to ignore these rows of your index file.
    --sample_column=7 or --sample_column=SAMPLE. Optional. The column containing the sample name.
    --analysis_group_column=8 or --analysis_group_column=ANALYSIS_GROUP. Optional. The column contents must match the code column of the analysis_group table.
    --analysis_group=exome this is optional, but it must match the code column of the analysis_group table. It overrides the flag --analysis_group_column
    --data_collection=grch38 this is optional, but it must match the code column of the data_collection table.
    --data_type=sequence this is optional, but it must match the code column of the data_type table.
    --infer_sample, boolean flag. Use this if your index file does not contain a sample column, but the samples can be inferred from the file paths. The script will guess the sample, by matching the file path against a regex.
    --infer_data_type, boolean flag. Use this if you are not using the --data_type flag. The script assigns a sensible data type if the file looks like a vcf, bam, or cram.

=head1  THE INDEX FILE

    It must be tab delimited.
    It is allowed to have header lines starting with '##'. These header lines will be ignored.
    If you use the --use_column_headers option then it must have a columns definition line. This must be the first line immediately after any '##' lines. Optionally, it is allowed to start with a single '#', which will be ignored.

=cut

