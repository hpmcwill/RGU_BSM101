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

# Load modules
use Digest;             # Message digest or checksum calculation
use Getopt::Long;       # Command-line argument handling
use File::Basename;     # File name processing.
use File::Find;         # 'find' utility functionallity
#use Time::HiRes qw(stat lstat);
use File::stat;         # 'stat' function object interface
use POSIX qw(strftime); # Time/date formatting
use DBI;                # Database interface.
use Data::Dumper;       # Debug output


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
    #$dbh->do('BEGIN IMMEDIATE TRANSACTION');
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
    #if(! $dbh->{ReadOnly}) {
	#&print_debug_message('close_db', 'Commit changes', 1);
	#$dbh->commit();
    #}
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
	unlink($DBFILENAME);
    }
    # Open database, and thus create it.
    my $dbh =  &open_db();
    # Create table(s).
    $dbh->do(q{
CREATE TABLE files(
  fullFilename TEXT PRIMARY KEY,
  mtime TEXT,
  sizeKb INT,
  sizeBlocks INT,
  blockSize INT,
  MD5 TEXT NOT NULL,
  SHA256 TEXT NOT NULL,
  MIMEType TEXT NOT NULL,
  dirName TEXT,
  fileName TEXT,
  fileExtension TEXT,
  fileType TEXT
);
});
    if($dbh->err) {
	print STDERR $dbh->errstr, "\n";
    }
    # Commit changes and close.
    &close_db($dbh);
    &print_debug_message('init_db', 'End', 1);
}

=head2 add_files()

Add/update database table C<files>.

  &add_files($dbh, %colValues);

=cut

sub add_files {
    &print_debug_message('add_files', 'Begin', 1);
    # List of columns to insert
    my $colNames = '';
    my $values = '';
    my $dbh = shift;
    my (%colValues) = @_;
    &print_debug_message('add_files', 'colValues: ' . Dumper(\%colValues), 2);
    my (@colNameList) = keys(%colValues);
    foreach my $colName (@colNameList) {
	$colNames .= $colName . ',';
	if($colValues{$colName} ne '') {
	    my $tmpValues = $colValues{$colName};
	    $tmpValues =~ s/'/''/g;
	    $values .= "'" . $tmpValues . "',";
	}
	else {
	    $values .= "'',"
	}
    }
    $colNames =~ s/,$//;
    $values =~ s/,$//;
    # Insert new row.
    my $tmpSth = $dbh->prepare(qq{
INSERT OR REPLACE INTO files($colNames) VALUES ($values);
});
    $tmpSth->execute() or die $dbh->errstr;
    &print_debug_message('add_files', 'End', 1);
}

=head2 checksums()

Obtain checksums/digests for a file. The supported digest method 
names are those used by the L<Digest> module.

  my (%checksums) = &checksums($digest_method, $filename, $rel_filename);

=cut

sub checksums {
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
	    $digest->addfile($FH);
	    close($FH);
	    $checksums{$d_method} = $digest->hexdigest();
	}
    }
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
    return $mime_type;
}

=head2 file_info_str()

Get file information using the UNIX L<file(1)> utility.

  my $file_info_str = &file_info_str($esc_filename, $rel_filename, $file_utility);

=cut
   
sub file_info_str {
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
    return $file_type_str;
}

=head2 wanted()

Extract information for file. Invoked per-file using L<File::Find>.

  File::Find::find({wanted => \&wanted}, $starting_dir);

=cut

sub wanted {
    &print_debug_message('wanted', 'Begin', 11);
    &print_debug_message('wanted', 'File: ' . $_, 12);
    my (%checksums) = ();
    my ($file_ext, $file_type_str, $mime_type, $stat_data);
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
	    # [TODO] Extract meta-data from file (e.g. EXIF from JPEGs)
	    # Image::ExifTool, Image::Magick
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
	my (%colValues) = (
	    'fullFilename'  => $rel_filename,
	    'mtime'         => &frmt_time($stat_data->mtime),
	    'sizeKb'        => $stat_data->size,
	    'sizeBlocks'    => $stat_data->blocks,
	    'blockSize'     => $stat_data->blksize,
	    'MD5'           => $checksums{'MD5'},
	    'SHA256'        => $checksums{'SHA-256'},
	    'MIMEType'      => $mime_type,
	    'dirname'       => $dirname,
	    'filename'      => $filename,
	    'fileExtension' => $file_ext,
	    'fileType'      => $file_type_str,
	    );
	&add_files($dbh, %colValues);
	
	# Print to tab-delimited format.
	foreach my $checksum_type (sort(keys(%checksums))) {
	    print $checksums{$checksum_type}, "\t";
	}
	print(
	    &frmt_time($stat_data->mtime), 
	    "\t", $stat_data->size, "\t", $stat_data->blocks, '*', $stat_data->blksize, 
	    "\t", $mime_type,
	    "\t", $file_ext,
	    "\t", $file_type_str,
	    "\t", $rel_filename, 
	    #"\t", $dirname, "\t", $filename, 
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
