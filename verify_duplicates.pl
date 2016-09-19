#!/usr/bin/env perl
# ======================================================================
# NB: to view documentation run: perldoc ./file_report.pl
# ----------------------------------------------------------------------
=head1 NAME

C<verify_duplicates.pl>

=head1 DESCRIPTION

Verify that checksum/digest identified identical files are actually 
identical.

=head1 FUNCTIONS

=cut

# ======================================================================
# Enable Perl warnings
use warnings;
use strict;
# Enable UNICODE support
use utf8;
use feature 'unicode_strings';
use open ':encoding(utf8)';

# Load modules
use Getopt::Long;                 # Command-line argument handling
use File::Basename;               # File name processing.
use File::Compare;                # File comparision.
use DBI;                          # Database interface.
use Data::Dumper;                 # Debug output

### Default values
my $scriptName = basename($0, ());
my $digest_method = 'MD5';
my ($do_usage);
# Debug output level
my $debugLevel = 0;
# SQLite database file name.
my $DBFILENAME = 'file_catalogue.sqlite';
my $numDigests = 0;
my $numFailedDigests = 0;

=head2 usage()

Output help/usage message.

  &usage();

=cut

sub usage {
    print STDERR <<EOF
Usage: $scriptName [-d starting_directory] 
 -c <method>               Checksum/digest method [$digest_method]
                           Methods: 'MD5', 'SHA-1', 'SHA-256'
 -dbfile <dbfile>          Database filename [$DBFILENAME]

 -h                        Help/usage message.
EOF
;
}

# Process command-line options.
unless (&GetOptions(
	     # Checksum/digest method(s)
	     'checksum|c=s'   => \$digest_method,
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

  $dbh = &open_db_ro();

=cut

sub open_db_ro {
    &print_debug_message('open_db_ro', 'Begin', 1);
    my $dbh;
    $dbh = DBI->connect("dbi:SQLite:dbname=$DBFILENAME", "", "",
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
#  FOREIGN KEY(sourceId) REFERENCES source(sourceId)
#);

=head2 getChecksumsWithDuplicates()

  &getChecksumsWithDuplicates($dbh);

=cut
    
sub getChecksumsWithDuplicates {
    my $dbh = shift;
    my $checksumMethod = shift || 'MD5';
    my (@retVal) = ();
    
    &print_debug_message('getChecksumsWithDuplicates', 'Begin', 1);
    my $sth0 = $dbh->prepare(qq{
SELECT $checksumMethod, COUNT($checksumMethod) FROM files GROUP BY $checksumMethod HAVING COUNT($checksumMethod) > 1;
});
    $sth0->execute() or die $dbh->errstr;
    # Found entry(s)...
    my $row_count = 0;
    while( my $row_hash = $sth0->fetchrow_hashref() ) {
	#print Dumper($row_hash), "\n";
	push(@retVal, $row_hash->{$checksumMethod});
	$row_count++;
    }
    &print_debug_message('getChecksumsWithDuplicates', "row_count: $row_count", 13);
    &print_debug_message('getChecksumsWithDuplicates', 'End', 1);
    return @retVal;
}

=head2 getFilesForADigest()

  my @file_list = &getFilesForADigest($dbh, $digest_method, $digest_value);

=cut
    
sub getFilesForADigest {
    my $dbh = shift;
    my $digest_method = shift;
    my $digest_value = shift;
    my (@retVal) = ();
    &print_debug_message('getFilesForADigest', 'Begin', 1);
    &print_debug_message('getFilesForADigest', 'digest_method: ' . $digest_method, 11);
    &print_debug_message('getFilesForADigest', 'digest_value: ' . $digest_value, 11);
    
    my $sth0 = $dbh->prepare(qq{
SELECT fullFilename FROM files WHERE $digest_method IS ?;
});
    $sth0->execute($digest_value) or die $dbh->errstr;
    # Found entry(s)...
    my $row_count = 0;
    while( my $row_hash = $sth0->fetchrow_hashref() ) {
	#print Dumper($row_hash), "\n";
	push(@retVal, $row_hash->{'fullFilename'});
	$row_count++;
    }
    &print_debug_message('getFilesForADigest', "row_count: $row_count", 13);
    &print_debug_message('getFilesForADigest', 'End', 1);
    return @retVal;
}


# Open database.
if(!-r $DBFILENAME) {
    die 'Database file not found.';
}
my $dbh0 = &open_db_ro();
#$dbh0->{TraceLevel} = '1|SQL';

print 'For database: ', $DBFILENAME, "\n";

# For each checksum/digest with more than one corresponding file. Perform a
# byte-by-byte comparision to determine if the files are in fact identical.

# Get the duplicated checksum/digests.
my (@digest_array) = &getChecksumsWithDuplicates($dbh0, $digest_method);
foreach my $digest_value (@digest_array) {
    $numDigests++;
    my @file_list = &getFilesForADigest($dbh0, $digest_method, $digest_value);
    my $first_file = shift(@file_list);
    if(!-r $first_file) {
	die "Unable to read $first_file, so unable to compare";
    }
    foreach my $filename (@file_list) {
	unless(compare($first_file, $filename) == 0) {
	    warn "$digest_method value finds non-identical files: $digest_value";
	    $numFailedDigests++;
	    last;
	}
    }
}
print "$numFailedDigests of $numDigests digests grouped non-identical files.\n";

# Close database connection and free resources.
&close_db($dbh0);

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
