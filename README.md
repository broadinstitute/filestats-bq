# filestats-bq

This repository implements a Go module
to catalogue on-prem files in BigQuery.

## Usage

```
go get -u github.com/broadinstitute/filestats-bq

filestats-bq --dir /path/to/dir --regex '\.txt$' \
  --key /path/to/service_account_key.json \
  --project test-project --dataset test_dataset --table test_txt
```

The Google Service Account here should be assigned
`BigQuery Data Editor` role on the associated dataset in BigQuery.

Alternatively, use `--stdout` switch to redirect results to STDOUT.

## Building

`filestats-bq` can also be distributed as a single executable
to a different system, so you don't have to have Go installed there.

To build the executable for 64-bit Linux on a Mac, install Docker and run
```
docker build -t filestats-bq .
docker run --rm --entrypoint cat filestats-bq main > filestats-bq
```
(unfortunately, regular cross-compilation won't work,
because it needs to compile with CGO, due to
[an obscure implementation of UID/GID name lookup](https://golang.org/pkg/os/user/#pkg-overview))

## Output

BigQuery table has the following fields:

| Path          | Mode       | User | Group | Size   |        Modified                | Target               | Error |
| ------------- | ---------- | ---- | ----- | ------ | ------------------------------ | -------------------- | ----- |
| /path/to/file | -rw-r--r-- | user | group | 987654 | 2019-01-31 01:02:03.456789 UTC | /path/to/linked/file | null  |

- `Path` is the absolute "source" path of a file
- `Mode` represents file mode bits
- Owner `User` and `Group` names of the file
- `Size` of the file in bytes
- `Modified` gives the timestamp of the last file modification
- if `Path` is a _symlink_, then `Target` gives the actual location of the file
- `Error` records the first error encountered during file listing

Additionally, the following holds true if `Path` is a _symlink_:
- if `Mode` starts with `L`, then `Mode`, `Modified`, and `Size` correspond to `Path` itself
- if `Mode` starts with `-`, then `Mode`, `Modified`, and `Size` correspond to the `Target` file

Here, the difference in semantics stems from the purpose of this module
to determine the attributes of the actual _files_, not _links_, where possible.
However, if a _link_ is _broken_ (i.e. its target file does not exist
or cannot be accessed), then we resort to displaying the attributes of the _link_ itself.

Additionally, you can see the cause of a failure of link resolution
in the `Error` field, such as `lstat /path/to/linked/file: no such file or directory`.

Finally, on some occasions (mostly when files or directories cannot be accessed)
`Mode`, `Modified`, and `Size` fields may be empty, which indicates that
only the `Path` could be discovered by the module.
In that case, `Error` field documents the reason for the failure, such as
`stat /path/to/file: permission denied`.

## Algorithm

The module is roughly organized as follows:

1.  Parse command line flags, which include
    `--path` of the directory for file search,
    file path `--regex` to match,
    BigQuery `--project`, `--dataset` and `--table` IDs,
    and the path to a Google Service Account `--key`,

    The key could be specified either with `--key`,
    or via Application Default Credentials.

2.  Start walking the file tree,
    calling an asynchronous handler for each file.

3.  The file handler:

    1.  Verifies that the file is regular or a symlink,
        and its path matches the regex.

    2.  If the file is a symlink,
        fully resolves its target.

    3.  Requests file or symlink target stats
        (mode, modification date, size).

    4.  If the link can't be resolved,
        it requests stats for the link itself.

    5.  If the stats don't correspond to
        a regular file or a symlink,
        skips the following steps.

    6.  Looks up user and group names
        based on `Uid` and `Gid` from the stats.

        These IDs are only available on POSIX systems.
        In addition, it caches ID -> name mappings,
        to avoid extra system lookups.

    7.  Captures any file-level errors and attempts
        to preserve as much information as possible.

    8.  Sends the file stats
        as a record to the output channel.

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

5.  Write any incoming records into the output stream,
    as a TSV file.
