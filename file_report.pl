#!/usr/bin/env perl
# ======================================================================
# NB: to view documentation run: perldoc ./file_report.pl
# ----------------------------------------------------------------------
=head1 NAME

C<file_report.pl>

=head1 DESCRIPTION

Build a file catalogue based on file properties.

=head1 TODO

=over

=item * Hidden file/directory handling

=item * Symlink handling

=item * Specific file meta-data extraction (e.g. EXIF from JPEG)

=back

=head1 FUNCTIONS

=cut

# ======================================================================
# Enable warnings
use warnings;
use strict;
# Enable UNICODE support
use utf8;
use feature 'unicode_strings';
use open ':encoding(utf8)'; # Use UTF-8 encoding for data files.

# Load modules
use Digest;                       # Message digest/checksum calculation
use Getopt::Long;                 # Command-line argument handling
use File::Basename;               # File name processing.
use File::Find;                   # 'find' utility functionallity
#use Time::HiRes qw(stat lstat);
use File::stat;                   # 'stat' function object interface
use POSIX qw(strftime);           # Time/date formatting
use DBI;                          # Database interface.
use Image::ExifTool qw(:Public);  # Metadata access
use Data::Dumper;                 # Debug output

# Find and load method to get MIME type for a file.
my $mime_method = -1;
my $file_utility = '/usr/bin/file';

if( eval('require File::MimeInfo::Magic;') ) {
    $mime_method = 2;
}
elsif( eval('require File::MimeInfo;') ) {
    $mime_method = 1;
}
elsif( eval('require File::MMagic;') ) {
    $mime_method = 3;
}
elsif( eval('require File::Type;') ) {
    $mime_method = 4;
}
elsif(-x $file_utility) {
    $mime_method = 0;
}

### Default values
my $scriptName = basename($0, ());
my $digest_method = 'MD5,SHA-256';
my $starting_dir = '.';
my ($do_usage, $dbInitFlag);
# Debug output level
my $debugLevel = 0;
# SQLite database file name.
my $DBFILENAME = 'file_catalogue.sqlite';

=head2 usage()

Output help/usage message.

  &usage();

=cut

sub usage {
    print STDERR <<EOF
Usage: $scriptName [-d starting_directory] 
 -d <starting_directory>   Directory to process [$starting_dir]

 -c <method>               Checksum/digest method [$digest_method]
                           Methods: 'MD5', 'SHA-1', 'SHA-256'
 -m <method>               MIME method [$mime_method]
                           0 - 'file' utility
                           1 - File::MimeInfo
                           2 - File::MimeInfo::Magic
                           3 - File::MMagic
                           4 - File::Type

 -dbinit                   Initialise database
 -dbfile <dbfile>          Database filename [$DBFILENAME]

 -h                        Help/usage message.
EOF
;
}

# Process command-line options.
unless (&GetOptions(
	     # Starting directory
	     'directory|d=s'  => \$starting_dir,
	     # Checksum/digest method(s)
	     'checksum|c=s'   => \$digest_method,
	     # MIME type detection method
	     'mime|m=i'       => \$mime_method,
	     # Initialise database.
	     'dbinit'         => \$dbInitFlag,
	     # SQLite database file.
	     'dbfile=s'       => \$DBFILENAME,
	     # Debug output level
	     'debug=i'        => \$debugLevel,
	     # Usage/help message
	     'help|h'         => \$do_usage
)) {
    &usage();
    exit;
}
if($do_usage) {
    &usage();
    exit(0);
}
# Initialise the database
if($dbInitFlag) {
    &init_db();
}
if($mime_method == 1) {
    require File::MimeInfo;
}
elsif($mime_method == 2) {
    require File::MimeInfo::Magic;
}
elsif($mime_method == 3) {
    require File::MMagic;
}
elsif($mime_method == 4) {
    require File::Type;
}

# Open database.
if(!-e $DBFILENAME) {
    &init_db();
}
my $dbh = &open_db();

=head2 print_debug_message()

Print a debug message at the specified debug level.

  &print_debug_message($function_name, $message, $level);

=cut

sub print_debug_message {
    my $function_name = shift;
    my $message       = shift;
    my $level         = shift;
    if ( $level <= $debugLevel ) {
	print STDERR '[', $function_name, '()] ', $message, "\n";
    }
}

=head2 frmt_time()
    
Format date/time from L<stat(1)> into a sortable string based on 
ISO6801 time formatting.

  my $time_str = &frmt_time($epoch_time);

=cut

sub frmt_time {
    my $epoch_time = shift;
    my $time_str = strftime('%Y-%m-%d %H:%M:%S', gmtime($epoch_time));
    return $time_str;
}

=head2 open_db()

Open the database read/write.

  $dbh = &open_db();

=cut

sub open_db {
    &print_debug_message('open_db', 'Begin', 1);
    my $dbh;
    $dbh = DBI->connect("dbi:SQLite:dbname=$DBFILENAME", "", "",
			{
			    RaiseError => 1,
			    AutoCommit => 1,
			});
    $dbh->sqlite_busy_timeout(60000); # 1 min.
    $dbh->do("PRAGMA foreign_keys = ON");
    &print_debug_message('open_db', 'End', 1);
    return $dbh;
}

=head2 close_db()

Close the database.

  &close_db($dbh);

=cut

sub close_db {
    &print_debug_message('close_db', 'Begin', 1);
    my $dbh = shift;
    $dbh->disconnect();
    &print_debug_message('close_db', 'End', 1);
}

=head2 init_db()

Initialise the database creating the required tables.

  &init_db();

=cut

sub init_db {
    &print_debug_message('init_db', 'Begin', 1);
    # If old database exists... delete it.
    if(-f $DBFILENAME) {
	&print_debug_message('init_db', 'Delete old database', 1);
	unlink($DBFILENAME);
    }
    # Open database, and thus create it.
    my $dbh =  &open_db();
    # Create table(s).
    $dbh->do(q{
CREATE TABLE source(
  sourceId INTEGER PRIMARY KEY AUTOINCREMENT,
  Make TEXT,
  Model TEXT,
  Software TEXT,
  FileSource TEXT,
  SerialNumber TEXT
);
});
    if($dbh->err) {
	print STDERR $dbh->errstr, "\n";
    }
    $dbh->do(q{
CREATE TABLE directories(
  dirName TEXT PRIMARY KEY,
  numFiles INT
);
});
    if($dbh->err) {
	print STDERR $dbh->errstr, "\n";
    }
    $dbh->do(q{
CREATE TABLE files(
  fullFilename TEXT PRIMARY KEY,
  mtime TEXT,
  sizeBytes INT,
  sizeBlocks INT,
  blockSize INT,
  MD5 TEXT NOT NULL,
  SHA256 TEXT NOT NULL,
  MIMEType TEXT NOT NULL,
  dirName TEXT NOT NULL,
  fileName TEXT NOT NULL,
  fileExtension TEXT,
  fileType TEXT,
  Author TEXT,
  Title TEXT,
  Comment TEXT,
  Copyright TEXT,
  sourceId INTEGER,
  FOREIGN KEY(dirName) REFERENCES directories(dirName)
  FOREIGN KEY(sourceId) REFERENCES source(sourceId)
);
});
    if($dbh->err) {
	print STDERR $dbh->errstr, "\n";
    }
    # Commit changes and close.
    &close_db($dbh);
    &print_debug_message('init_db', 'End', 1);
}

=head2 prepare_row_data()

Prepare row data for addition to database.

  ($names, $qTemplate) = &prepare_row_data($colValues_hash);

=cut

sub prepare_row_data {
    &print_debug_message('prepare_row_data', 'Begin', 21);
    my $colValues = shift;
    &print_debug_message('prepare_row_data', 'colValues: ' . Dumper($colValues), 22);
    my $names = '';
    my $qTemplate = '';
    my (@colNameList) = keys(%$colValues);
    foreach my $colName (@colNameList) {
	$names .= $colName . ',';
	$qTemplate .= '?,';
    }
    $names =~ s/,$//;
    $qTemplate =~ s/,$//;
    &print_debug_message('prepare_row_data', 'End', 21);
    return ($names, $qTemplate);
}

=head2 add_source()

Add/update database table C<source>.

  $source_id = &add_source($dbh, $colValues_hash);

=cut

sub add_source {
    &print_debug_message('add_source', 'Begin', 11);
    my $dbh = shift;
    my $colValues = shift;
    my $source_id;
    &print_debug_message('add_source', 'colValues: ' . Dumper($colValues), 2);
    # Look for existing entry.
    my $w_clause = '';
    foreach my $keyName (keys(%$colValues)) {
	$w_clause .= ' AND' if($w_clause ne '');
	$w_clause .= " $keyName IS ?"
    }
    my $sth0 = $dbh->prepare(qq{
SELECT sourceId FROM source WHERE $w_clause;
});
    $sth0->execute(values(%$colValues)) or die $dbh->errstr;
    # Found entry(s)...
    my $row_count = 0;
    while( my $row_hash = $sth0->fetchrow_hashref() ) {
	$source_id = $row_hash->{'sourceId'};
	$row_count++;
    }
    &print_debug_message('add_source', "row_count: $row_count", 13);
    # No entry... add new entry.
    unless($row_count) {
	my ($names, $qTemplate) = &prepare_row_data($colValues);
	&print_debug_message('add_source', 'names: ' . Dumper($names), 13);
	# Insert new row.
	my $tmpSth = $dbh->prepare(qq{
INSERT OR REPLACE INTO source($names) VALUES ($qTemplate);
	});
	$tmpSth->execute(values(%$colValues)) or die $dbh->errstr;
	# Get the source Id for the new entry.
	$sth0->execute(values(%$colValues)) or die $dbh->errstr;
	# Found entry(s)...
	while( my $row_hash = $sth0->fetchrow_hashref() ) {
	    $source_id = $row_hash->{'sourceId'};
	}
    }
    &print_debug_message('add_source', "source_id: $source_id", 11);
    &print_debug_message('add_source', 'End', 11);
    return $source_id;
}

=head2 add_files()

Add/update database table C<files>.

  &add_files($dbh, $colValues_hash);

=cut

sub add_files {
    &print_debug_message('add_files', 'Begin', 11);
    my $dbh = shift;
    my $colValues = shift;
    &print_debug_message('add_files', 'colValues: ' . Dumper($colValues), 2);
    my ($names, $qTemplate) = &prepare_row_data($colValues);
    &print_debug_message('add_files', 'names: ' . Dumper($names), 13);
    # Insert new row.
    my $tmpSth = $dbh->prepare(qq{
INSERT OR REPLACE INTO files($names) VALUES ($qTemplate);
});
    $tmpSth->execute(values(%$colValues)) or die $dbh->errstr;
    &print_debug_message('add_files', 'End', 11);
}

=head2 add_directories()

Add/update database table C<directories>.

  &add_directories($dbh, $colValues_hash);

=cut

sub add_directories {
    &print_debug_message('add_directories', 'Begin', 11);
    my $dbh = shift;
    my $colValues = shift;
    &print_debug_message('add_directories', 'colValues: ' . Dumper($colValues), 2);
    my ($names, $qTemplate) = &prepare_row_data($colValues);
    &print_debug_message('add_directories', 'names: ' . Dumper($names), 13);
    # Insert new row.
    my $tmpSth = $dbh->prepare(qq{
INSERT OR REPLACE INTO directories($names) VALUES ($qTemplate);
});
    $tmpSth->execute(values(%$colValues)) or die $dbh->errstr;
    &print_debug_message('add_directories', 'End', 11);
}

=head2 checksums()

Obtain checksums/digests for a file. The supported digest method 
names are those used by the L<Digest> module.

  my (%checksums) = &checksums($digest_method, $filename, $rel_filename);

=cut

sub checksums {
    &print_debug_message('checksums', 'Begin', 11);
    my $digest_method = shift;
    my $filename = shift;
    my $rel_filename = shift;
    my ($digest, $FH);
    my (%checksums) = ();

    # Deal with leading spaces as part of filename.
    my $tmp_filename = $filename;
    if($tmp_filename =~ m/^\s+/) {
	$tmp_filename = './' . $tmp_filename;
    }
    # Derive a checksum/digest value(s) for the file
    foreach my $d_method (split(/[,;]+/, $digest_method)) {
	$digest = Digest->new($d_method);
	unless( open($FH, "<$tmp_filename\0") ) {
	    warn "Unable to read $rel_filename ($!)";
	    $checksums{$d_method} = '0';
	} else {
	    binmode($FH); # Read file as binary data (i.e. bytes)
	    $digest->addfile($FH);
	    close($FH);
	    $checksums{$d_method} = $digest->hexdigest();
	}
    }
    &print_debug_message('checksums', 'End', 11);
    return %checksums;
}

=head2 file_mime_type()

Derive a MIME type from a file.

  my $mime_type = &file_mime_type($mime_method, $filename, $esc_filename, $rel_filename, $file_utility);

The specified C<$mime_method> should be one of the following:

=over

=item 0 - the UNIX L<file(1)> utility, requires C<$file_utility> to 
be specified providing path to version of the utility to use

=item 1 - L<File::MimeInfo> module

=item 2 - L<File::MimeInfo::Magic> module

=item 3 - L<File::MMagic> module

=item 4 - L<File::Type> module

=back

If an unavailable method is specified an  error is issued and the 
value "unknown" returned.

=cut

sub file_mime_type {
    &print_debug_message('file_mime_type', 'Begin', 11);
    my $mime_method = shift;
    my $filename = shift;
    my $esc_filename = shift;
    my $rel_filename = shift;
    my $file_utility = shift;
    my $file_type;
    my $mime_type = 'unknown';
    
    # Derive a file type for the file (magic number and file extension)
    if ( $mime_method == 1 ) { # File::MimeInfo
	$file_type = File::MimeInfo->new();
	$mime_type = $file_type->mimetype($filename);
    }
    elsif ( $mime_method == 2 ) { # File::MimeInfo::Magic
	$file_type = File::MimeInfo::Magic->new();
	$mime_type = $file_type->magic($filename) || $file_type->mimetype($filename);
    }
    elsif ( $mime_method == 3 ) { # File::MMagic
	$file_type = File::MMagic->new();
	$mime_type = $file_type->checktype_filename($filename);
    }
    elsif ( $mime_method == 4 ) { # File::Type
	$file_type = File::Type->new();
	$mime_type = $file_type->mime_type($filename);
    }
    # Use 'file' utility
    elsif ( $mime_method == 0 && $file_utility ne '') {
	$mime_type = `$file_utility -bi ./"$esc_filename"`;
	if($? != 0) {
	    warn "Unable to get MIME type for: $rel_filename";
	}
	chomp($mime_type);
    }
    else {
	warn "Invalid MIME method spectified: $mime_method";
    }
    &print_debug_message('file_mime_type', 'End', 11);
    return $mime_type;
}

=head2 file_info_str()

Get file information using the UNIX L<file(1)> utility.

  my $file_info_str = &file_info_str($esc_filename, $rel_filename, $file_utility);

=cut
   
sub file_info_str {
    &print_debug_message('file_info_str', 'Begin', 11);
    my $esc_filename = shift;
    my $rel_filename = shift;
    my $file_utility = shift;
    
    # Extract file-type info and core meta-data
    my $file_type_str = `$file_utility -b ./"$esc_filename"`;
    if($? != 0) {
	warn "Unable to get file type information for: $rel_filename";
    }
    chomp($file_type_str);
    $file_type_str =~ s/\s+/ /g;
    $file_type_str =~ s/\s+,/,/g;
    &print_debug_message('file_info_str', 'End', 11);
    return $file_type_str;
}

=head2 file_metadata()

Extract metadata from file (e.g. EXIF from JPEGs).

  $metadata_hash = file_metadata($filename);

Uses L<Image::ExifTool> to fetch metadata.

=cut

sub file_metadata {
    &print_debug_message('file_metadata', 'Begin', 11);
    my $filename = shift;
    my $metadata = {};
    # Create a new Image::ExifTool object
    my $exifTool = new Image::ExifTool;
    # Extract meta information from an image
    $exifTool->ExtractInfo($filename);
    # Extract common information relating to source.
    my @tagList = (
	### EXIF metadata tags (e.g. JPEG and TIFF)
	'Make',
	'Model',
	'Software',
	'Artist',
	'Copyright',
	'UserComment',
	'FileSource',
	'SerialNumber',
	'ImageDescription',
	### IPTC profile
	#'Keywords',
	#'Source',
	#'CopyrightNotice',
	#'Caption'
	### XMP (inc. Dublin Core)
	'Author',
	#'Caption',
	'Keywords',
	#'Notes',
	#'OwnerName',
	#'SerialNumber',
	#'License',
	### ICC
	'ManufacturerName',
	### Dublin Core (e.g. MS Office documents)
	'Title',
	'Creator',
	'Subject',
	'Description',
	#'Publisher',
	'Contributor',
	#'Date',
	#'Type',
	#'Format',
	#'Identifier',
	'Source',
	'Language',
	#'Relation',
	#'Coverage',
	'Rights',
	### TODO: Dublin Core Metadata Initiative (DCMI)
	);
    foreach my $tag (@tagList) {
	# Get the value of a specified tag
	my $value = $exifTool->GetValue($tag);
	if($value) {
	    # Trim any leading or trailing whitespace.
	    $value =~ s/^\s+//;
	    $value =~ s/\s+$//;
	    # The SerialNumber field is often corrupted, indicated by presence
	    # of control characters... so null the field if unprintables are
	    # present.
	    if($tag eq 'SerialNumber' && $value =~ m/\P{XPosixPrint}/) {
		$value = undef;
	    }
	    # For other fields, just attempt to remove unprintable characters.
	    else {
		$value =~ s/\P{XPosixPrint}//g;
	    }
	    # Store value in return hash.
	    $metadata->{$tag} = $value;
	}
    }
    &print_debug_message('file_metadata', Dumper($metadata), 12);
    &print_debug_message('file_metadata', 'End', 11);
    return $metadata;
}    

=head2 wanted()

Extract information for file. Invoked per-file using L<File::Find>.

  File::Find::find({wanted => \&wanted}, $starting_dir);

=cut

sub wanted {
    &print_debug_message('wanted', 'Begin', 11);
    &print_debug_message('wanted', 'File: ' . $_, 12);
    my (%checksums) = ();
    my ($file_ext, $file_type_str, $mime_type, $stat_data, $metadata);
    # Identify files
    if( ($stat_data = lstat($_)) &&
	-f $stat_data ) {
	my $filename = $_;
	my $rel_filename = $File::Find::name;
	my $esc_filename = $filename;
	$esc_filename =~ s/`/\\`/g; # Escape backticks
	my $dirname = $File::Find::dir;
	if(-r $stat_data) {
	    # Digest/checksum
	    (%checksums) = &checksums($digest_method, $filename, $rel_filename);
	    # MIME type
	    $mime_type = &file_mime_type($mime_method, $filename, $esc_filename, $rel_filename, $file_utility);
	    # File information (file utility).
	    $file_type_str = &file_info_str($esc_filename, $rel_filename, $file_utility);
	    # Extract meta-data from file (e.g. EXIF from JPEGs)
	    $metadata = &file_metadata($filename);
	}
	else {
	    warn "Unable to read $rel_filename, so no digest, MIME or file type recorded";
	    (%checksums) = (
		'MD5' => '0',
		'SHA256' => '0'
		);
	    $mime_type = 'unknown';
	    $file_type_str = '';
	}
	# Extract file extension.
	if($filename =~ m/\.([^.\/\\]+)$/) {
	    $file_ext = $1;
	}
	else {
	    $file_ext = '';
	}

	# Add to database
	# 1. Get/add source
	my $source_id = &add_source($dbh, {
	    'Make' => $metadata->{'Make'} ||
		$metadata->{'ManufacturerName'},
	    'Model' => $metadata->{'Model'},
	    'Software' => $metadata->{'Software'},
	    'FileSource' => $metadata->{'FileSource'},
	    'SerialNumber' => $metadata->{'SerialNumber'},
				    });
	# 2. Add file information.
	my $colValues = {
	    'fullFilename'  => $rel_filename,
	    'mtime'         => &frmt_time($stat_data->mtime),
	    'sizeBytes'     => $stat_data->size,
	    'sizeBlocks'    => $stat_data->blocks,
	    'blockSize'     => $stat_data->blksize,
	    'MD5'           => $checksums{'MD5'},
	    'SHA256'        => $checksums{'SHA-256'},
	    'MIMEType'      => $mime_type,
	    'dirname'       => $dirname,
	    'filename'      => $filename,
	    'fileExtension' => $file_ext,
	    'fileType'      => $file_type_str,
	    'Author'        => $metadata->{'Creator'} ||
		$metadata->{'Author'} ||
		$metadata->{'Artist'} ||
		$metadata->{'Contributor'},
	    'Title'         => $metadata->{'Title'} ||
		$metadata->{'Description'} ||
		$metadata->{'ImageDescription'},
	    'Comment'       => $metadata->{'Comment'} ||
	        $metadata->{'Subject'} ||
                $metadata->{'Keywords'},
	    'Copyright'     => $metadata->{'Copyright'} ||
	        $metadata->{'Rights'},
	    'sourceId'      => $source_id,
	    };
	&add_files($dbh, $colValues);
	
	# Print to tab-delimited format.
	#foreach my $checksum_type (sort(keys(%checksums))) {
	#    print $checksums{$checksum_type}, "\t";
	#}
	print(
	    #&frmt_time($stat_data->mtime), 
	    #"\t", $stat_data->size, "\t", $stat_data->blocks, '*', $stat_data->blksize, 
	    #"\t", $mime_type,
	    #"\t", $file_ext,
	    #"\t", $file_type_str,
	    "\t", $rel_filename, 
	    #"\t", $dirname, "\t", $filename,
	    #"\t", ($metadata->{'Author'} || $metadata->{'Artist'}),
	    #"\t", ($metadata->{'Title'} || $metadata->{'Description'} || $metadata->{'ImageDescription'}),
	    "\n");
    }
    elsif($stat_data && -d $stat_data) {
	my $rel_dirname = $File::Find::name;
	my $colValues = {
	    'dirName' => $rel_dirname,
	    #'numFiles' => 0
	};
	&add_directories($dbh, $colValues);
	print(
	    $rel_dirname,
	    "\n");
    }
    &print_debug_message('wanted', 'End', 11);
}

# Look for files.
File::Find::find({wanted => \&wanted}, $starting_dir);

# Commit database changes and close.
&close_db($dbh);

=head1 VERSION

$Id$

=head1 AUTHORS / ACKNOWLEDGEMENTS

Hamish McWilliam <h.p.mcwilliam@rgu.ac.uk>

=head1 LICENSE

Copyright 2016 Hamish McWilliam <h.p.mcwilliam@rgu.ac.uk>

This is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, Inc., 675 Mass Ave, Cambridge MA 02139,
USA; either version 2 of the License, or (at your option) any later
version; incorporated herein by reference.

=cut
