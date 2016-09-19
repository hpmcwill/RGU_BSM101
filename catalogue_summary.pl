#!/usr/bin/env perl
# ======================================================================
# NB: to view documentation run: perldoc ./file_report.pl
# ----------------------------------------------------------------------
=head1 NAME

C<catalogue_summary.pl>

=head1 DESCRIPTION

Produce a summary report for a catalogue database.

=head1 FUNCTIONS

=cut

# ======================================================================
# Enable Perl warnings
use warnings;
use strict;
# Enable UNICODE support
use utf8;
use feature 'unicode_strings';
use open ':encoding(utf8)'; # Use UTF-8 encoding for data files.
use open ':std'; # Also use UTF-8 for STDIN, STDOUT and STDERR.

# Load modules
use Getopt::Long;                 # Command-line argument handling
use File::Basename;               # File name processing.
use DBI;                          # Database interface.
use Data::Dumper;                 # Debug output

### Default values
my $scriptName = basename($0, ());
my ($do_usage);
# Debug output level
my $debugLevel = 0;
# SQLite database file name for catalogue database.
my $DBFILENAME = 'file_catalogue.sqlite';

=head2 usage()

Output help/usage message.

  &usage();

=cut

sub usage {
    print STDERR <<EOF
Usage: $scriptName [-dbfile dbfile.sqlite]
 
 -dbfile <dbfile>          Database filename [$DBFILENAME]

 -h                        Help/usage message.
EOF
;
}

# Process command-line options.
unless (&GetOptions(
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

=head2 open_db_ro()

Open the database read-only.

  $dbh = &open_db_ro($db_filename);

=cut

sub open_db_ro {
    my $db_filename = shift;
    &print_debug_message('open_db_ro', 'Begin', 1);
    my $dbh;
    $dbh = DBI->connect("dbi:SQLite:dbname=$db_filename", "", "",
			{
			    RaiseError => 1,
			    ReadOnly   => 1,
			});
    $dbh->sqlite_busy_timeout(60000); # 1 min.
    $dbh->do("PRAGMA foreign_keys = ON");
    &print_debug_message('open_db_ro', 'End', 1);
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

### Database schema
#
# CREATE TABLE source(
#  sourceId INTEGER PRIMARY KEY AUTOINCREMENT,
#  Make TEXT,
#  Model TEXT,
#  Software TEXT,
#  FileSource TEXT,
#  SerialNumber TEXT
# );
# CREATE TABLE directories(
#  dirName TEXT PRIMARY KEY,
#  numFiles INT
# );
# CREATE TABLE files(
#  fullFilename TEXT PRIMARY KEY,
#  mtime TEXT,
#  sizeBytes INT,
#  sizeBlocks INT,
#  blockSize INT,
#  MD5 TEXT NOT NULL,
#  SHA256 TEXT NOT NULL,
#  MIMEType TEXT NOT NULL,
#  dirName TEXT,
#  fileName TEXT,
#  fileExtension TEXT,
#  fileType TEXT,
#  Author TEXT,
#  Title TEXT,
#  Comment TEXT,
#  Copyright TEXT,
#  sourceId INTEGER,
#  FOREIGN KEY(dirName) REFERENCES directories(dirName)
#  FOREIGN KEY(sourceId) REFERENCES source(sourceId)
#);

# Open database.
if(!-r $DBFILENAME) {
    die 'Database file not found.';
}
my $dbh = &open_db_ro($DBFILENAME);
#$dbh->{TraceLevel} = '1|SQL';

print '# Catalogue database: ', $DBFILENAME, "\n";

# Total number of files.
my $sth0 = $dbh->prepare(qq{
SELECT COUNT(fullFilename) FROM files;
});
$sth0->execute() or die $dbh->errstr;
my $ret_data = $sth0->fetchall_arrayref( [0] );
print '# Total files: ' . $ret_data->[0]->[0] . "\n";
$sth0->finish();

# Unique files by checksum
$sth0 = $dbh->prepare(qq{
SELECT COUNT(*) FROM (
  SELECT DISTINCT MD5 FROM files
) T;
});
$sth0->execute() or die $dbh->errstr;
$ret_data = $sth0->fetchall_arrayref( [0] );
print '# Total unique MD5: ' . $ret_data->[0]->[0] . "\n";
$sth0->finish();
$sth0 = $dbh->prepare(qq{
SELECT COUNT(*) FROM (
  SELECT DISTINCT SHA256 FROM files
) T;
});
$sth0->execute() or die $dbh->errstr;
$ret_data = $sth0->fetchall_arrayref( [0] );
print '# Total unique SHA256: ' . $ret_data->[0]->[0] . "\n";
$sth0->finish();

# Unique MIME types.
$sth0 = $dbh->prepare(qq{
SELECT COUNT(*) FROM (
  SELECT DISTINCT MIMEType FROM files
) T;
});
$sth0->execute() or die $dbh->errstr;
$ret_data = $sth0->fetchall_arrayref( [0] );
print '# Total unique MIME: ' . $ret_data->[0]->[0] . "\n";
$sth0->finish();

# Unique file extensions.
$sth0 = $dbh->prepare(qq{
SELECT COUNT(*) FROM (
  SELECT DISTINCT fileExtension FROM files
) T;
});
$sth0->execute() or die $dbh->errstr;
$ret_data = $sth0->fetchall_arrayref( [0] );
print '# Total unique file extensions: ' . $ret_data->[0]->[0] . "\n";
$sth0->finish();

# Total directories (use 'directories' table to include empty directories)
$sth0 = $dbh->prepare(qq{
SELECT COUNT(*) FROM (
  SELECT DISTINCT dirName FROM directories
) T;
});
$sth0->execute() or die $dbh->errstr;
$ret_data = $sth0->fetchall_arrayref( [0] );
print '# Total number of directories: ' . $ret_data->[0]->[0] . "\n";
$sth0->finish();

# Total unique file names.
$sth0 = $dbh->prepare(qq{
SELECT COUNT(*) FROM (
  SELECT DISTINCT fileName FROM files
) T;
});
$sth0->execute() or die $dbh->errstr;
$ret_data = $sth0->fetchall_arrayref( [0] );
print '# Total unique file names: ' . $ret_data->[0]->[0] . "\n";
$sth0->finish();

# File sizes
$sth0 = $dbh->prepare(qq{
SELECT MIN(sizeBytes) AS minSize, MAX(sizeBytes) AS maxSize,
  AVG(sizeBytes) AS meanSize, SUM(sizeBytes) AS sumSize FROM files;
});
$sth0->execute() or die $dbh->errstr;
$ret_data = $sth0->fetchall_arrayref();
print '# Minimum file size: ' . $ret_data->[0]->[0] . "\n";
print '# Maximum file size: ' . $ret_data->[0]->[1] . "\n";
print '# Mean file size: ' . $ret_data->[0]->[2] . "\n";
print '# Sum file size: ' . $ret_data->[0]->[3] . "\n";
$sth0->finish();

# Total data sources (from meta-data)
$sth0 = $dbh->prepare(qq{
SELECT COUNT(sourceId) FROM source;
});
$sth0->execute() or die $dbh->errstr;
$ret_data = $sth0->fetchall_arrayref( [0] );
print '# Total data sources: ' . $ret_data->[0]->[0] . "\n";
$sth0->finish();


# Close database connection and free resources.
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
