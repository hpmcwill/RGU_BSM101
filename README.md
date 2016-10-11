# RGU_BSM101

## Workflow

The typical workflow is:

1. Build a file information catalogues from the data directories/volumes using
   the 'file_report.pl' script
2. Derive summary information from the file catalogues using the
   'catalogue_summary.pl' script
3. Generate any desired agregate file catalogues from individual catalogues
   using the 'db_merge.pl' script
4. Identify files specific to a particular file catalogue using the
   'find_nonoverlap.pl' script
5. Query the file catalogue database(s) to identify particular files of
   interest

Additional optional steps:

*  Use 'verify_duplicates.pl' to ensure duplicates identified by the
   checksum/digest methods are actual duplicates and not collisions. Normally
   this should be unnecessary since the probabiliy of a collision is very,
   very low, however there are circumstances where it can be useful to check.

## File Catalogue Database

The file catalogue databases are implemented using the SQLite RDBMS. This
allows them to be handled as simple files, while providing the flexability
to query the database using SQL.

The file catalogue database schema contains information about the files, their
filesystem metadata and metadata contained within the files.

### Database schema

The database schema SQL is:
```
CREATE TABLE source(
  sourceId INTEGER PRIMARY KEY AUTOINCREMENT,
  Make TEXT,
  Model TEXT,
  Software TEXT,
  FileSource TEXT,
  SerialNumber TEXT
);
CREATE TABLE directories(
  dirName TEXT PRIMARY KEY,
  numFiles INT
);
CREATE TABLE files(
  fullFilename TEXT PRIMARY KEY,
  mtime TEXT,
  sizeBytes INT,
  sizeBlocks INT,
  blockSize INT,
  MD5 TEXT NOT NULL,
  SHA256 TEXT NOT NULL,
  MIMEType TEXT NOT NULL,
  dirName TEXT,
  fileName TEXT,
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
```

### Example Queries

MIME/media type counts:
```
SELECT MIMEType, COUNT(MIMEType) FROM files GROUP BY MIMEType;
```

Finding files by MIME type, for example looking for e-mail storage files
using the '[mbox](https://en.wikipedia.org/wiki/Mbox)' format, MIME type:
`application/mbox`:
```
SELECT fullFilename, mtime, MIMEType, fileExtension FROM files WHERE MIMEType IS 'application/mbox';
```

Additional example queries can be found in the R Markdown script
'test_file_report.Rmd'.

## Background

Based on code written for RGU Information and Library Studies module BSM101.
