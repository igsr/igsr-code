#!/usr/bin/env perl

use strict;
use warnings;

use ReseqTrack::Tools::Exception;
use ReseqTrack::DBSQL::DBAdaptor;
use ReseqTrack::Tools::RunMetaInfoUtils qw(create_index_line create_suppressed_index_line);
use ReseqTrack::Tools::SequenceIndexUtils qw(return_header_string return_header_desc assign_files);
use ReseqTrack::Tools::FileSystemUtils qw(get_lines_from_file);
use ReseqTrack::Tools::ERAUtils qw(get_erapro_conn);
use ReseqTrack::Tools::RunSeqtk;
use File::Basename;
use Getopt::Long;

$| = 1;  # filehandle special variable, If set to nonzero, forces an fflush(3) after every write or print on the currently selected output channel.

my $dbhost;
my $dbuser;
my $dbpass;
my $dbport = 4175;
my $dbname;
my @era_params;
my $collection_type; ##the analysis sequence index includes only fastq files with a "FQ_OK" type in the collection table.
my $file_type;
my $clob_read_length = 66000;
my $output_file;
my $single_run_id;
my @skip_study_ids;
my $current_index ;
my @print_status;
my $help;
my $seqtk_program;

my %current_hash;
my %skip_study_id;
my $meta_infos;
my %file_hash;
my %era_ftp_path_hash; 
my %index_lines;
my %runs_pass_qa;

&GetOptions(
	    'dbhost=s'      		=> \$dbhost,
	    'dbname=s'      		=> \$dbname,
	    'dbuser=s'      		=> \$dbuser,
	    'dbpass=s'      		=> \$dbpass,
	    'dbport=s'      		=> \$dbport,
	    'era_dbuser=s'          => \$era_params[0],
        'era_dbpass=s'          => \$era_params[1],
        'era_dbname=s'          => \$era_params[2],
	    'collection_type=s'		=> \$collection_type,
	    'file_type=s'			=> \$file_type,	    
	    'help!' 				=> \$help,
	    'output_file=s' 		=> \$output_file,
	    'skip_study_id=s@' 		=> \@skip_study_ids,
	    'run_id=s' 				=> \$single_run_id,
	    'current_index=s'     	=>\$current_index,
        'print_status=s'  		=> \@print_status,
        'seqtk_program:s'		=> \$seqtk_program,
	   );

if($help){
  useage();
}

$seqtk_program = "/nfs/1000g-work/G1K/work/bin/seqtk/seqtk" if (!$seqtk_program);

if (!@print_status) {
  push(@print_status, 'public');
}
  
my $db = ReseqTrack::DBSQL::DBAdaptor->new(
  -host   => $dbhost,
  -user   => $dbuser,
  -port   => $dbport,
  -dbname => $dbname,
  -pass   => $dbpass,
    );
  
if ($current_index){
  my $current_si_lines = get_lines_from_file($current_index);
  foreach (@$current_si_lines) {
    my @aa = split /\t/;
    if ($aa[20] && $aa[20] eq '1'){
      $current_hash{$aa[2]}{withdrawn}      = $aa[20];
      $current_hash{$aa[2]}{withdrawn_date} = $aa[21] if($aa[21]);
      $current_hash{$aa[2]}{comment}        = $aa[22];
    }
  }
}

foreach my $study_id(@skip_study_ids){
  $skip_study_id{$study_id} = 1;
}

### get meta info ###
my $rmi_a = $db->get_RunMetaInfoAdaptor;

if($single_run_id){
  my $rmi = $rmi_a->fetch_by_run_id($single_run_id);
  $meta_infos = [$rmi];
}else{
  $meta_infos = $rmi_a->fetch_all;
}
my @sorted = sort{$a->run_id cmp $b->run_id} @$meta_infos;

my $collections = $db->get_CollectionAdaptor->fetch_by_type($collection_type);
throw("Cannot find any collection of type $collection_type") if (!$collections || @$collections == 0);

#print "number of collection with type $collection_type is " . scalar @$collections . "\n";

### find fastq files of specified file type, populate file_hash and find fastq file ENA ftp path ####

my $files = $db->get_FileAdaptor->fetch_by_type($file_type);

my $era_db = get_erapro_conn(@era_params);
$era_db->dbc->db_handle->{LongReadLen} = $clob_read_length;
my $info_sql = 'select file_name, dir_name, md5, volume_name, bytes from fastq_file where run_id = ?';
my $era_sth = $era_db->dbc->prepare($info_sql);

foreach my $file ( @$files ) {		
	my ($run_id) = $file->filename =~ /([E|S]RR\d+)/;
    #print "run_id from file name is $run_id\n";
    push(@{$file_hash{$run_id}}, $file);
    
    $era_sth->execute($run_id);
    while(my $hashref = $era_sth->fetchrow_hashref){
    	my $era_filename = $hashref->{DIR_NAME}."/".$hashref->{FILE_NAME};
      	my $era_md5 = $hashref->{MD5};
      	if (basename($era_filename) eq basename($file->name) && $era_md5 eq $file->md5) {
      	    #print "hash key is " . $good_file->name . "\n";
      	    #print "ENA base name " . basename($era_filename) . " " . $era_md5 . " db basename " . basename($good_file->name) . " " . $good_file->md5 . "\n";
        	$era_ftp_path_hash{$file->name} =  "ftp://ftp.sra.ebi.ac.uk/vol1/". $era_filename;	
    	}
  	}    
}	

foreach my $collection ( @$collections ) {
	$runs_pass_qa{$collection->name} = 1; 
}	

META_INFO:foreach my $meta_info(@sorted){
	#print "Have ".$meta_info->run_id."\n";
	my $analysis_group = $meta_info->library_strategy; 
	
	#next if ($analysis_group eq "RNA-Seq");
	if(keys(%skip_study_id)){
     next META_INFO if($skip_study_id{$meta_info->study_id});
   }

   #print "Running dump on ".$meta_info->run_id."\n";
   $index_lines{$meta_info->run_id} = [] unless($index_lines{$meta_info->run_id});
   if (grep {$_ eq $meta_info->status} @print_status) {
       
		my $files = $file_hash{$meta_info->run_id};
	  
		if(!$files || @$files == 0){
	       my $warning = $meta_info->run_id." seems to have no files associated with it";
	       $warning .= " for collection type ".$collection_type if($collection_type);
	       print STDERR $warning,"\n";
	
	       my $new_comment = "NOT YET AVAILABLE FROM ARCHIVE";
	       my $time = "";
	
			if (defined $current_hash{$meta_info->run_id}{withdrawn}){
	       		print STDERR "current index has it withdrawn as well: " . $meta_info->run_id, "\t",$current_hash{$meta_info->run_id}{comment},"\n";
	     	}
	     	
	       #print STDERR "CREATEING A SUPPRESSED INDEX LINE\n";
	        my $line = create_suppressed_index_line($meta_info, $new_comment, $time, $analysis_group);
	        push(@{$index_lines{$meta_info->run_id}}, $line);
		}
		else{
	       print STDERR "Have ".@$files." for ".$meta_info->run_id."\n";
		   my ($mate1, $mate2, $frag) = assign_files($files);   
		   print STDERR "Mate 1 ".$mate1->name."\n" if($mate1);
		   print STDERR "Mate 2 ".$mate2->name."\n" if($mate2);
		   print STDERR "Frag ".$frag->name."\n" if($frag);
	       if ($runs_pass_qa{$meta_info->run_id}) { 
		       
		       	my $read_count = $meta_info->archive_read_count;
		       	my $base_count = $meta_info->archive_base_count;  	#### In run_meta_info_vw table, the archive_read_count and archive_base_count are for individual fastq files if no frag fragment exist;
		       														#### If frag fastq file exists, the archive_read_count is the number of read pairs plus count of reads in the fragment file

		       
		       if($frag){
		         my ($frag_read_cnt, $frag_base_cnt) = count_fastq_by_seqtk($frag->name);  
		         my $line = create_index_line($era_ftp_path_hash{$frag->name}, $frag->md5, $meta_info, undef, 0,
		                                      undef, undef, $frag_read_cnt, $frag_base_cnt, 
		                                      $analysis_group);                         
		         push(@{$index_lines{$meta_info->run_id}}, $line);
		         $read_count = $read_count - $frag_read_cnt;
		         $base_count = $base_count - $frag_base_cnt;
		       }
		       if($mate1 && $mate2){
		          my $mate1_line = create_index_line($era_ftp_path_hash{$mate1->name}, $mate1->md5, $meta_info, 
		                                             $era_ftp_path_hash{$mate2->name}, 0, undef, undef, 
		                                             $read_count, $base_count,
		                                             $analysis_group);
		          my $mate2_line = create_index_line($era_ftp_path_hash{$mate2->name}, $mate2->md5, $meta_info, 
		                                             $era_ftp_path_hash{$mate1->name}, 0, undef, undef,
		                                             $read_count, $base_count,
		                                             $analysis_group);
		          push(@{$index_lines{$meta_info->run_id}}, ($mate1_line, $mate2_line));
				}
				
				if (defined $current_hash{$meta_info->run_id}{withdrawn}){
		       		print STDERR "Passed now, but withdrawn in phase3: " . $meta_info->run_id, "\t",$current_hash{$meta_info->run_id}{comment},"\n"; 
		     	}
				
			}
			else {	
				my $colls = $db->get_CollectionAdaptor->fetch_by_name($meta_info->run_id);

	        	my $new_comment;				
				foreach my $coll ( @$colls ) {
					next if ($coll->type =~ /FASTQ/i);
					next if $coll->type =~ /FQ_OK/;
					$new_comment = $coll->type;
				}	 

	       		my $time = "";
	       
	     		my $md5 = "................................";
	     		
	     		if ($frag) {
	     		    print "Failed QA frag " . $era_ftp_path_hash{$frag->name} ."\n";
		     		my $line = create_index_line($era_ftp_path_hash{$frag->name}, $md5, $meta_info,, undef, 1, $new_comment, $time, undef, undef, $analysis_group);
	     			push(@{$index_lines{$meta_info->run_id}}, $line);
	     		}
	     		if($mate1 && $mate2){	
	     		    print "Failed QA mate 1 " . $era_ftp_path_hash{$mate1->name} . "\n";
	     		    my $line1 = create_index_line($era_ftp_path_hash{$mate1->name}, $md5, $meta_info,, undef, 1, $new_comment, $time, undef, undef, $analysis_group);
	     		    my $line2 = create_index_line($era_ftp_path_hash{$mate2->name}, $md5, $meta_info,, undef, 1, $new_comment, $time, undef, undef, $analysis_group);
	     			push(@{$index_lines{$meta_info->run_id}}, $line1);
	     			push(@{$index_lines{$meta_info->run_id}}, $line2);
	     		}
	     		
	     		if (defined $current_hash{$meta_info->run_id}{withdrawn}){
	       			print STDERR "Current index has it withdrawn as well: " . $meta_info->run_id, "\t",$current_hash{$meta_info->run_id}{comment},"\n";  
	     		}
     			#my $line = create_suppressed_index_line($meta_info, $new_comment, $time, $analysis_group);
     			#push(@{$index_lines{$meta_info->run_id}}, $line);
	     		#print "run didn't pass QA\n";
	     	}
	    }	     
   }  
   elsif($meta_info->status eq 'suppressed' || $meta_info->status eq 'cancelled' || $meta_info->status eq 'killed'){
     my $line = create_suppressed_index_line($meta_info, undef, undef, $analysis_group);
     print STDERR "suppressed or canceled line: " . $line."\n";
     push(@{$index_lines{$meta_info->run_id}}, $line);
   }
}

my $fh = \*STDOUT;

if($output_file){
  open(FH, ">".$output_file) or throw("Failed to open ".$output_file." $!");
  $fh = \*FH;
}

print $fh return_header_desc();

=head"##Date=
##Project=The 1000 Genomes Project
##FASTQ_ENA_PATH=an ENA ftp path from which the FASTQ file can be downloaded
##MD5=md5 for the fastq file
##RUN_ID=ENA/SRA assigned accession for the run
##STUDY_ID=ENA/SRA assigned accession for the study
##STUDY_NAME=name of the study
##CENTER_NAME=sequencing center that produced and submitted the sequence data
##SUBMISSION_ID=ENA/SRA assigned accession for the submission
##SUBMISSION_DATE=date of the data was submitted to ENA/SRA
##SAMPLE_ID=ENA/SRA assigned accession for the sample
##SAMPLE_NAME=sample identifier given by Coriell
##POPULATION=three letter population code for the sample
##EXPERIMENT_ID=ENA/SRA assigned accession for the experiment
##INSTRUMENT_PLATFORM=type of sequencing machine used in the experiment
##INSTRUMENT_MODEL=model of the sequencing machine used in the experiment
##LIBRARY_NAME=identifier for the library
##RUN_NAME=run name assigned by the sequencing machine
##INSERT_SIZE=submitter specified insert size of the library
##LIBRARY_LAYOUT=Library layout, this can be either PAIRED or SINGLE
##PAIRED_FASTQ=Name of mate pair file if exists
##WITHDRAWN=0/1 to indicate if the file has been withdrawn, only present if a file has been withdrawn
##WITHDRAWN_DATE=this is generally the date the index file is generated on
##COMMENT=comment about reasons for withdrawing from variant calling. \"TOO SHORT\" means reads are shorter than 70bp for WGS data or less than 68bp for WXS data; \"NOT_ILLUMINA\" are data produced on platformats other than Illumina; \"SUPPRESSED IN ARCHIVE\" are runs that are no longer available from ENA/SRA
##READ_COUNT=number of reads in the fastq file
##BASE_COUNT=number of bases in the fastq file
##ANALYSIS_GROUP=the analysis group of the sequence, this reflects sequencing strategy. Currently this includes low coverage whole genome sequence (WGS), 
exome sequence (WXS), high coverage whole genome sequence (HC_WGS)";
=cut

my $header = return_header_string(); 
print $fh $header;
foreach my $meta_info(@sorted){
  my $lines = $index_lines{$meta_info->run_id};
  if($lines && @$lines > 0){
    foreach my $line(@$lines){                                                                                                        
      print $fh $line."\n";
    }
  }else{
    print STDERR "Have no lines for ".$meta_info->run_id." " . $meta_info->center_name . "\n" unless($single_run_id);
  }
}
close($fh);

##### SUBS #####
sub count_fastq_by_seqtk {
	my ($input) = @_;
	my $output_dir = "/tmp";
	#create RunSeqtk object
	my $run_seqtk_fqchk = ReseqTrack::Tools::RunSeqtk->new
  	(
		-input_files => [$input],
		-program => $seqtk_program,
		-working_dir => $output_dir,
	);

	$run_seqtk_fqchk->run_fqchk;
	my $outs = $run_seqtk_fqchk->output_files;

	foreach my $out ( @$outs ) {

        my $lines = get_lines_from_file($out);

        my ($min_len_tmp, $max_len_tmp, $avg_len_tmp) = split(/;/, $lines->[0]);
        my ($name1, $min_len) = split(/: /, $min_len_tmp);
        my ($name2, $max_len) = split(/: /, $max_len_tmp);
        my ($name3, $avg_len) = split(/: /, $avg_len_tmp);

        my ($tmp, $total_bases) = split(/\t/, $lines->[2]);

        my $total_reads = $total_bases/$avg_len;
        print "By seqtk: read cnt is $total_reads\n";
        return ($total_reads, $total_bases);
	}
}	     

=pod

=head1 NAME

dump_igsr_sequence_index.pl

=head1 SYNOPSIS

This script dumps a sequence.index file based on the run_meta_info_vw table in a given database. Runs with status other than "public"
are labled as "SUPPRESSED IN ARCHIVE". Only runs associated with collection of appropriate collection type (such as FQ_OK) have active index lines, otherwise 
they are labeled as "TOO SHORT OR NON-ILLUMINA".  

The fastq file path is ENA ftp path queried from ERAPRO

=head1 OPTIONS

-dbhost, the name of the mysql-host
-dbname, the name of the mysql database
-dbuser, the name of the mysql user
-dbpass, the database password if appropriate
-dbport, the port the mysql instance is running on, this defaults to 4197 the 
    standard port for mysql-g1kdcc.ebi.ac.uk

-era_dbuser, user name to connect to ERAPRO
-era_dbpass, password to connect to ERAPRO

-collection_type, this is the type string you will use to fetch from the collection table
-file_type, this is the type string you will use to fetch from thefile table

-skip_study_id, this is a option to allow specific studies to be skipped, it can appear multiple times on the commandline

-output_file, this is where the sequence.index file is dumped out to

-single_run_id, this is to allow a single runs index to be dumped which is useful for debugging purposes

-current_index, path to existing sequence.index file. Withdrawn date and reasons will be output to log file for comparison; nothing from the 
				current index is written to the index to be dumped

-print_status, lines will be printed if the run_meta_info status is equal to this.
  				Default is 'public'.  Can be specified multiple times for multiple acceptable statuses.

-help, binary flag to get the perldocs printed

=head1 Examples

$ perl /nfs/1000g-work/G1K/work/zheng/igsr_code/scripts/dump_igsr_sequence_index.pl \
-dbhost mysql-g1kdcc-public -dbuser g1krw -dbpass xxxxxxxx -dbport 4197 -dbname zheng_map_hc_samples_to_38 \
-era_dbuser ops\$laura -era_dbpass xxxxxxxx \
-current_index  /nfs/1000g-archive/vol1/ftp/sequence.index \
-collection_type FQ_OK \
-file_type ARCHIVE_FASTQ \
-output_file ./seq_index 2>& log


=head1 Other useful scripts

ReseqTrack/scripts/run_meta_info/dump_sequence_index_stats.pl

=cut

