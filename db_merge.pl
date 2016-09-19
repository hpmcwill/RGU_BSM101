#!/usr/bin/env perl
# ======================================================================
# NB: to view documentation run: perldoc ./file_report.pl
# ----------------------------------------------------------------------
=head1 NAME

C<db_merge.pl>

=head1 USAGE SUMMARY

  ./db_merge.pl [-o target_db.sqlite] <source_dbs...>

=head1 DESCRIPTION

Merge a set of filesystem catalogue databases into a single database.

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
use DBI;                          # Database interface.
use Data::Dumper;                 # Debug output
use File::Basename;               # File name processing.
use Getopt::Long;                 # Command-line argument handling

### Default values
my $scriptName = basename($0, ());
my ($do_usage, $do_verbose);
# Debug output level
my $debugLevel = 0;
# SQLite database file name.
my $TARGETDBFILENAME = 'merged_file_catalogue.sqlite';

=head2 usage()

Output help/usage message.

  &usage();

=cut

sub usage {
    print STDERR <<EOF
Usage: $scriptName [-o target_db] <source_dbs...>
 
 -o <target_db>            Target database filename [$TARGETDBFILENAME]

 -v                        Verbose output
 -h                        Help/usage message.
EOF
;
}

# Process command-line options.
unless (&GetOptions(
	     # SQLite database file.
	     'targetdb|o=s'   => \$TARGETDBFILENAME,
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

=head2 open_db()

Open the database read/write.

  my $dbh = &open_db($DBFILENAME);

=cut

sub open_db {
    &print_debug_message('open_db', 'Begin', 1);
    my $DBFILENAME = shift;
    my $dbh;
    &print_debug_message('open_db', 'DBFILENAME: ' . $DBFILENAME, 1);
    $dbh = DBI->connect("dbi:SQLite:dbname=$DBFILENAME", "", "",
			{
			    RaiseError => 1,
			    PrintError => 1,
			    ShowErrorStatement => 1,
			    AutoCommit => 1,
			});
    $dbh->sqlite_busy_timeout(60000); # 1 min.
    $dbh->do("PRAGMA foreign_keys = ON");
    &print_debug_message('open_db', 'End', 1);
    return $dbh;
}

=head2 open_db_ro()

Open the database read-only.

  my $dbh = &open_db_ro($DBFILENAME);

=cut

sub open_db_ro {
    &print_debug_message('open_db_ro', 'Begin', 1);
    my $DBFILENAME = shift;
    my $dbh;
    &print_debug_message('open_db_ro', 'DBFILENAME: ' . $DBFILENAME, 1);
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

=head2 init_db()

Initialise the database creating the required tables.

  &init_db($DBFILENAME);

=cut

sub init_db {
    &print_debug_message('init_db', 'Begin', 1);
    my $DBFILENAME = shift;
    # If old database exists... delete it.
    if(-f $DBFILENAME) {
	&print_debug_message('init_db', 'Delete old database', 1);
	unlink($DBFILENAME);
    }
    # Open database, and thus create it.
    my $dbh =  &open_db($DBFILENAME);
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
  fileName TEXT NULL NULL,
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

=head2 copy_directories()

Copy rows from table C<directories> in source database to target database.

  &copy_directories($source_dbh, $target_dbh);

=cut

sub copy_directories {
    &print_debug_message('copy_directories', 'Begin', 1);
    my $source_dbh = shift;
    my $target_dbh = shift;

    my $sel = $source_dbh->prepare(q{
SELECT dirName, numFiles FROM directories;
});
    $sel->execute()  or die $source_dbh->errstr;
    my $ins = $target_dbh->prepare(q{
INSERT INTO directories (dirName, numFiles) VALUES (?,?);
});
    my $fetch_tuple_sub = sub { $sel->fetchrow_arrayref };
    my @tuple_status;
    my $rc = $ins->execute_for_fetch($fetch_tuple_sub, \@tuple_status);
    unless($rc) {
	my @errors = grep { ref $_ } @tuple_status;
	print Dumper(\@errors);
    }
    &print_debug_message('copy_directories', 'End', 1);
}

=head2 copy_sources()

Copy rows from table C<sources> in source database to target database.

  my $sourceid_mapping_hash = &copy_sources($source_dbh, $target_dbh);

=cut

sub copy_sources {
    &print_debug_message('copy_sources', 'Begin', 1);
    my $source_dbh = shift;
    my $target_dbh = shift;
    my $colNames = 'Make, Model, Software, FileSource, SerialNumber';
    my @colNames = split(/, */, $colNames);
    my %mapping = ();
    my %unmapped = ();

    # Prepare statement to search for a specific source
    my $w_clause = '';
    foreach my $keyName (@colNames) {
	$w_clause .= ' AND' if($w_clause ne '');
	$w_clause .= " $keyName IS ?"
    }
    &print_debug_message('copy_sources', "w_clause: $w_clause", 13);
    my $sth0 = $target_dbh->prepare(qq{
SELECT sourceId FROM source WHERE $w_clause;
});

    # Fetch sources from source_db.
    my $sel = $source_dbh->prepare(qq{
SELECT sourceId, $colNames FROM source;
});
    $sel->execute() or die $source_dbh->errstr;
    # Generate mapping of sourceId in source_db to target_db
    while(my $source_data = $sel->fetchrow_hashref()) {
	# Check for existing mapping.
	my $source_id = $source_data->{'sourceId'};
	if(!defined($mapping{$source_id})) {
	    # No mapping, is the source in the target set?
	    my @data = ();
	    foreach my $col (@colNames) {
		if(defined($source_data->{$col}) && 
		   $source_data->{$col} =~ m/\p{XPosixCntrl}/) {
		    push(@data, undef);
		}
		else {
		    push(@data, $source_data->{$col});
		}
	    }
	    $sth0->execute(@data);
	    my $row_hash = $sth0->fetchrow_hashref();
	    if($row_hash) {
		$mapping{$source_id} = $row_hash->{'sourceId'};
	    }
	    # New source, so add to set to insert.
	    else {
		$unmapped{"$source_id"} = \@data;
	    }
	}
    }
    # Add unmapped sources to target_db, updating mapping.
    if(scalar(keys(%unmapped)) > 0) {
	my $ins = $target_dbh->prepare(qq{
INSERT INTO source ($colNames) VALUES (?,?,?,?,?);
});
	my @unmapped_list = values(%unmapped);
	my $fetch_tuple_sub = sub { shift(@unmapped_list) };
	my @tuple_status;
	my $rc = $ins->execute_for_fetch($fetch_tuple_sub, \@tuple_status);
	unless($rc) {
	    my @errors = grep { ref $_ } @tuple_status;
	    print Dumper(\@errors);
	}
	# Get new sourceId for mapping.
	foreach my $source_id (keys(%unmapped)) {
	    $sth0->execute(@{$unmapped{$source_id}});
	    my $row_hash = $sth0->fetchrow_hashref();
	    if($row_hash) {
		$mapping{$source_id} = $row_hash->{'sourceId'};
	    }
	    
	}
    }
    &print_debug_message('copy_sources', 'End', 1);
    return \%mapping;
}


=head2 copy_files()

Copy rows from table C<files> in source database to target database.

  &copy_files($source_dbh, $target_dbh, $sourceid_mapping);

=cut

sub copy_files {
    &print_debug_message('copy_files', 'Begin', 1);
    my $source_dbh = shift;
    my $target_dbh = shift;
    my $mapping = shift;
    my $colNames = 'fullFilename, mtime, sizeBytes, sizeBlocks, blockSize, MD5, SHA256, MIMEType, dirName, fileName, fileExtension, fileType, Author, Title, Comment, Copyright, sourceId';

    my $sel = $source_dbh->prepare(qq{
SELECT $colNames FROM files;
});
    &print_debug_message('copy_files', 'Query source database, begin', 1);
    $sel->execute() or die $source_dbh->errstr;
    &print_debug_message('copy_files', 'Query source database, done', 1);
    &print_debug_message('copy_files', 'Copy data to target database, begin', 1);
    my $ins = $target_dbh->prepare(qq{
INSERT INTO files ($colNames) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
});
    my $row_count = 0;
    my $fetch_tuple_sub = sub {
	# Replace sourceId from source with the correponding sourceId in
	# target.
	my $dataref = $sel->fetchrow_arrayref;
	$row_count++;
	if(defined($dataref) && defined($dataref->[-1])) {
	    $dataref->[-1] = $mapping->{$dataref->[-1]};
	}
	if($do_verbose && ($row_count % 250 == 0)) {
	    print $row_count, ': ', $dataref->[0], "\n";
	}
	return $dataref;
    };
    my @tuple_status;
    my $rc = $ins->execute_for_fetch($fetch_tuple_sub, \@tuple_status);
    unless($rc) {
	my @errors = grep { ref $_ } @tuple_status;
	print Dumper(\@errors);
    }
    print 'Rows: ', $row_count, "\n";
    &print_debug_message('copy_files', 'Copy data to target database, done', 1);
    &print_debug_message('copy_files', 'End', 1);
}


# Create target database, checking for existing file.
&init_db($TARGETDBFILENAME);
my $target_dbh = &open_db($TARGETDBFILENAME);
if($debugLevel > 9) { # Enable SQL query reporting
    $target_dbh->{TraceLevel} = '1|SQL';
}

# For each source database...
foreach my $source_db_filename (@ARGV) {
    # Open source database (read-only).
    my $source_dbh = &open_db_ro($source_db_filename);
    print $source_db_filename, "\n";
    # Copy rows from table 'directories'.
    &copy_directories($source_dbh, $target_dbh);
    # Copy rows from table 'sources', checking for existing entries.
    my $sourceid_mapping_hash = &copy_sources($source_dbh, $target_dbh);
    # Copy rows from table 'files', updating 'sources' references.
    &copy_files($source_dbh, $target_dbh, $sourceid_mapping_hash);
    # Close source database.
    &close_db($source_dbh);
}

# Close database connection and free resources.
&close_db($target_dbh);

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
