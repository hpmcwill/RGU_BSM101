#!/usr/bin/env perl
# ======================================================================
# NB: to view documentation run: perldoc ./file_report.pl
# ----------------------------------------------------------------------
=head1 NAME

C<find_nonoverlap.pl>

=head1 DESCRIPTION

Find files recorded in a catalogue database that do not appear in a 
set of databases.

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
use DBI;                          # Database interface.
use Data::Dumper;                 # Debug output

### Default values
my $scriptName = basename($0, ());
my $digest_method = 'SHA256';
my ($do_usage, $do_verbose, $outfile_name);
my $OUTFILE;
# Debug output level
my $debugLevel = 0;
# SQLite database file name for query database.
my $QUERY_DBFILENAME = 'file_catalogue.sqlite';

=head2 usage()

Output help/usage message.

  &usage();

=cut

sub usage {
    print STDERR <<EOF
Usage: $scriptName [-dbfile query_database] [-o outfile] <search_databases...>
 
 -c <method>               Checksum/digest method [$digest_method]
                           Methods: 'MD5', 'SHA-1', 'SHA-256'
 -dbfile <dbfile>          Database filename [$QUERY_DBFILENAME]
 -o <outfile>           Output file for list of files specific to the
                           query database
 -v                        Verbose output			   

 -h                        Help/usage message.
EOF
;
}

# Process command-line options.
unless (&GetOptions(
	     # Checksum/digest method(s)
	     'checksum|c=s'   => \$digest_method,
	     # SQLite database file.
	     'dbfile=s'       => \$QUERY_DBFILENAME,
	     # Output file name.
	     'output|o=s'     => \$outfile_name,
	     # Verbose output
	     'verbose|v'      => \$do_verbose,
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

=head2 getUniqueChecksums()

Get the set of unique checksum values from a database.

  $sth = &getUniqueChecksums($dbh, $checksumMethod);

=cut
    
sub getUniqueChecksums {
    my $dbh = shift;
    my $checksumMethod = shift || 'MD5';
    &print_debug_message('getUniqueChecksums', 'Begin', 1);
    my $sth0 = $dbh->prepare(qq{
SELECT DISTINCT $checksumMethod FROM files;
});
    $sth0->execute() or die $dbh->errstr;
    &print_debug_message('getUniqueChecksums', 'End', 1);
    return $sth0;
}

# Check that database files exist.
if(!-r $QUERY_DBFILENAME) {
    die 'Query database file not accessable: ' . "$QUERY_DBFILENAME ($!)";
}
my $search_db_notfound = 0;
foreach my $search_db_filename (@ARGV) {
    if(!-r $search_db_filename) {
	warn 'Search database file not accessable: ' . "$search_db_filename ($!)";
	$search_db_notfound++;
    }
}
if($search_db_notfound > 0) {
    die "Unable to access $search_db_notfound search databases [abort]";
}

# Open output file if required.
if($outfile_name) {
    open($OUTFILE, '>', $outfile_name) or
	die 'Unable to open output file ($!)';
}

# Open query database.
print '# Query database: ', $QUERY_DBFILENAME, "\n";
print $OUTFILE 'QD', "\t", $QUERY_DBFILENAME, "\n" if($OUTFILE);
my $query_dbh = &open_db_ro($QUERY_DBFILENAME);
#$query_dbh->{TraceLevel} = '1|SQL';
# Open search databases.
my (@search_dbh_list) = ();
foreach my $search_db_filename (@ARGV) {
    print '# Search database: ', $search_db_filename, "\n";
    print $OUTFILE 'SD', "\t", $search_db_filename, "\n" if($OUTFILE);
    push(@search_dbh_list, &open_db_ro($search_db_filename));
}

# Get list of unique checksum/digest values from database.
my $query_sth0 = &getUniqueChecksums($query_dbh, $digest_method);
my $query_sth1 = $query_dbh->prepare(qq{
SELECT fullFilename FROM files WHERE $digest_method IS ?;
});

# Prepare queries for search databases.
my (@search_sth_list) = ();
foreach my $search_dbh (@search_dbh_list) {
    push(@search_sth_list, $search_dbh->prepare(qq{
SELECT COUNT(fullFilename) FROM files WHERE $digest_method IS ?;
}));
}

# For each checksum/digest value...
my (%counters) = (
    'query_unique' => 0,
    'match_found' => 0,
    'match_not_found' => 0,
    'files_not_matched' => 0,
    );
while( my $query0_row_hash = $query_sth0->fetchrow_hashref() ) {
    $counters{'query_unique'}++;
    my $checksumValue = $query0_row_hash->{$digest_method};
    &print_debug_message('main', 'Checksum: ' . $checksumValue, 1);
    # For each database to search...
    my $checksum_found_count = 0;
    foreach my $search_sth (@search_sth_list) {
	$search_sth->execute($checksumValue) or die $search_sth->errstr;
	my $row_array = $search_sth->fetchrow_arrayref();
	$checksum_found_count += $row_array->[0] if($row_array);
    }
    # Report unique files.
    if($checksum_found_count < 1) {
	$counters{'match_not_found'}++;
	if($do_verbose) { # Verbose output
	    print "$digest_method $checksumValue not overlapping\n";
	}
	$query_sth1->execute($checksumValue) or die $query_dbh->errstr;
	my $file_list = $query_sth1->fetchall_arrayref([0]);
	$counters{'files_not_matched'} += scalar(@$file_list);
	if($do_verbose) { # Verbose output
	    foreach my $filename (sort(@{$file_list->[0]})) {
		print '= ' . $filename . "\n";
	    }
	}
	if($OUTFILE) { # Log file names to file.
	    print $OUTFILE 'CK', "\t", $checksumValue, "\n";
	    foreach my $filename (sort(@{$file_list->[0]})) {
		print $OUTFILE 'FI', "\t", $filename, "\n";
	    }
	}
    }
    else {
	$counters{'match_found'}++;
    }
}
print '# For ' . $counters{'query_unique'} . ' unique checksums:' . "\n" .
    '# * ' . $counters{'match_found'} . ' found matches' . "\n" .
    '# * ' . $counters{'match_not_found'} . ' checksums did not find matches' . "\n" .
    '# * ' . $counters{'files_not_matched'} . ' files did not find matches' . "\n",
    '# ========================================' , "\n";

# Close database connection and free resources.
foreach my $search_sth (@search_sth_list) {
    $search_sth->finish();
}
foreach my $search_dbh (@search_dbh_list) {
    &close_db($search_dbh);
}
$query_sth0->finish();
$query_sth1->finish();
&close_db($query_dbh);

# Close the output file
if($OUTFILE) {
    close($OUTFILE);
}

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
