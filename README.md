# filestat-bq

This repository implements a Go module
to catalogue on-prem files in BigQuery.

## Algorithm

1.  Parse command line flags, which include
    `--path` of the directory for file search,
    a file path `--regex` to match,
    and BigQuery `--project`, `--dataset` and `--table` IDs.

2.  Start walking the file tree,
    calling an asynchronous handler for each file.
    If an error occurs for a file,
    log it and continue.

3.  The file handler:

    1.  verifies that the file is regular
        (i.e. not a device or directory),
        and its path matches the regex

    2.  if the file is a symlink, determines its target

    3.  queries file or symlink target stats
        (mode, modification date, size);
        if the target is not a regular file, skip next step

    4.  send file path, mode, date, size, and target (if link)
        as a record to the output channel

4.  Concurrently with the walk, create an output stream
    corresponding to a "BigQuery load" job for the table
    (the table is auto-created if needed).

    Please note that such streaming corresponds to
    a _single_ "load" job, so the entire file listing
    will appear in BigQuery only after
    _all_ records have been written to it.
    This is in contrast to a "BigQuery streaming" job,
    which would allow to stream records in realtime,
    but provides fewer guarantees on the consistency
    of the results.

5.  Write any incoming records into the output stream.

6.  If a _fatal_ error occurs during steps 1-5,
    log it and terminate the process.
