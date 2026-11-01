<!--
  Project:      dfe-archiver
  File:         docs/archiving.md
  Purpose:      Operator guide to archiving: what is written, where, how long it stays and how to restore
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# Archiving

The archiver keeps a compressed copy of every record it receives, in files you can read with standard tools. This page is for the person running it. The design behind it is [internals.md](internals.md).

## What it writes

One record per line, exactly as it arrived, then a newline. The archiver parses a record only to route it and never changes it. Lines are compressed, `zstd` unless `compression.codec` says otherwise:

| Codec | File ends in |
|-------|--------------|
| zstd (default) | `.jsonl.zst` |
| lz4 | `.jsonl.lz4` |
| snappy | `.jsonl.sz` |
| gzip | `.jsonl.gz` |

Each flush appends one complete frame, so a file is a standard concatenated stream. `zstd -dc`, `lz4 -dc` or `gzip -dc` reads it whole.

A file closes when it reaches `archive.roll_size_bytes` compressed (1 GiB) or `archive.roll_interval_secs` old. Unset, the interval is 300 seconds while offsets are held and on `grpc`, otherwise 3600. Expect many small files on a quiet destination and few large ones on a busy one.

## Where it goes

`archive.destination` takes `file:///path`, `s3://bucket/prefix`, `minio://bucket/prefix`, `gs://bucket/prefix` or `az://container/prefix`.

Under it, a key is the routed destination, then the `archive.path_template` expansion, then `-<seq>-<writer id>` and the extension:

```text
s3://acme-archive/archives/events/acme/2026/10/29/03-0001-3f9a.jsonl.zst
```

The routed part is the Kafka topic then the routing fields (`events/acme`), or only the fields on `grpc`, which has no topic. By default the field is `_org_id`. A value is encoded before it becomes a path segment, so `a/b` is written `a=2Fb`. A record with no value for a field goes under `default`.

An object-store file is staged under `buffer.spool_dir/uploads` and uploaded whole once it closes. Nothing is visible in the bucket until then.

## How long it stays

The archiver never deletes an archived file, so retention belongs to the store. Set a lifecycle rule on the bucket or container, or clean the directory yourself on a `file://` path. Add a rule for incomplete multipart uploads as well. A failed attempt aborts its upload, and an abort that itself fails leaves parts behind until that rule removes them.

## Restore

There is no restore command. A file is the records, so you decompress it and put the records back where they came from.

1. List the keys for the destination and window you need, for example `aws s3 ls --recursive s3://acme-archive/archives/events/acme/2026/10/29/`.
2. Fetch the files and decompress them: `zstd -dc 03-0001-3f9a.jsonl.zst > records.jsonl`.
3. On `kafka` the first part of the key is the topic. Produce the lines back to it, for example `kcat -P -b kafka:9092 -t events -l records.jsonl`. They then take the normal path. On `grpc` the key holds no topic, so send the records to the receiver as new data.

Check two things first:

- On `kafka` with acknowledgements on, the archive is at-least-once. A record can appear in two files after a rebalance or a restart, so a replay can write it twice.
- On `grpc` the archive is at-most-once. A kill loses what the queue and the open files held, and those records are not in any file.
