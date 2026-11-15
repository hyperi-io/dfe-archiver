<!--
  Project:      dfe-archiver
  File:         docs/configuration.md
  Purpose:      Reference for the config cascade and every environment variable the archiver reads
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# Configuration reference

The archiver reads two sets of settings from two cascades: its own sections from its config file and the flat `DFE_ARCHIVER_*` variables, and scalo's runtime sections from scalo's cascade. Every key of the archiver's own sections, with its type and default, is in [config-schema.yaml](config-schema.yaml), generated from `Config`.

## The archiver's own sections

`transport`, `kafka`, `grpc`, `archive`, `buffer`, `routing`, `compression` and `dlq` resolve in this order, highest first:

1. The flat `DFE_ARCHIVER_*` variables below. An empty value reads as unset.
2. `.env` in the working directory, loaded into the environment without replacing a variable already set.
3. The config file: `--config`, else `DFE_ARCHIVER_CONFIG`, else the first of `config.yaml`, `config/settings.yaml` and `/etc/dfe-archiver/config.yaml` that exists.
4. The defaults.

These sections take the flat names only. `DFE_ARCHIVER_KAFKA__BROKERS` reaches nothing, and neither does an unprefixed `KAFKA_BROKERS` or `ARCHIVER_DESTINATION`, with no warning. The file is read as written, so `${VAR}` in it stays a literal string.

The running pipeline reads its config once, at startup. The reloader watches the config file and SIGHUP, and a change to any section but `kafka.batch_size` logs that it needs a restart. An idle archiver is the exception: the change that gives it a destination starts the pipeline ([internals.md](internals.md#idle-until-configured)).

## scalo's sections

`version_check`, `metrics`, `logger`, `self_regulation`, `scaling` and `memory` belong to scalo's runtime, which reads them from its own cascade, highest first:

1. The CLI flags.
2. `DFE_ARCHIVER_<SECTION>__<KEY>`, so `DFE_ARCHIVER_SCALING__ENABLED` sets `scaling.enabled`. The memory guard also takes the flat `DFE_ARCHIVER_MEMORY_*` names below.
3. `settings.<env>.yaml`, then `settings.yaml` with the `--config` file over it, then `defaults.yaml`, each looked for in the working directory, `config/` and `/config/`. `<env>` is `APP_ENV`, `ENVIRONMENT` or `ENV`, and `development` when none is set.
4. The defaults.

scalo sets this cascade up from `--config` before the archiver reads its own file. So a file named only by `DFE_ARCHIVER_CONFIG`, or found at one of the archiver's default paths, feeds the archiver's sections and none of scalo's. The image passes `--config /etc/dfe/archiver.yaml`, so a deployment's file reaches both.

## Environment variables

| Variable | Sets | Default |
|---|---|---|
| `DFE_ARCHIVER_TRANSPORT` | `transport`: `kafka` or `grpc` | `kafka` |
| `DFE_ARCHIVER_GRPC_LISTEN` | `grpc.listen`, the Push listener's address | `0.0.0.0:6000` |
| `DFE_ARCHIVER_KAFKA_BROKERS` | `kafka.brokers`, comma-separated | `localhost:9092` |
| `DFE_ARCHIVER_KAFKA_GROUP_ID` | `kafka.group_id` | `dfe-archiver` |
| `DFE_ARCHIVER_KAFKA_TOPICS` | `kafka.topics`, comma-separated. Empty discovers | empty |
| `DFE_ARCHIVER_KAFKA_TOPIC_INCLUDE` | `kafka.topic_include`, regexes a discovered topic must match | empty |
| `DFE_ARCHIVER_KAFKA_TOPIC_EXCLUDE` | `kafka.topic_exclude`, regexes that drop a discovered topic | scalo's list: Kafka's internal and `_dlq` topics |
| `DFE_ARCHIVER_KAFKA_TOPIC_REFRESH_SECS` | `kafka.topic_refresh_secs` | `60` |
| `DFE_ARCHIVER_KAFKA_SECURITY_PROTOCOL` | `kafka.security_protocol` | `PLAINTEXT` |
| `DFE_ARCHIVER_KAFKA_SASL_MECHANISM` | `kafka.sasl_mechanism` | none, `PLAIN` when a user and password are set |
| `DFE_ARCHIVER_KAFKA_SASL_USER` | `kafka.sasl_username` | none |
| `DFE_ARCHIVER_KAFKA_SASL_PASSWORD` | `kafka.sasl_password` | none |
| `DFE_ARCHIVER_KAFKA_SSL_CA_LOCATION` | `kafka.ssl_ca_location` | none |
| `DFE_ARCHIVER_KAFKA_ALLOW_INSECURE_TRANSPORT` | `kafka.allow_insecure_transport` (`true`, `1` or `yes`) | `false` |
| `DFE_ARCHIVER_DLQ_ENABLED` | `dlq.enabled`. Always off on `grpc` | `true` |
| `DFE_ARCHIVER_DLQ_MODE` | `dlq.mode`: `fan_out`, `file_only`, `kafka_only` or `cascade`. Any other value warns and takes `cascade` | `cascade` |
| `DFE_ARCHIVER_DLQ_TOPIC` | `dlq.kafka.common_topic`, one topic for every entry | `dfe_archiver_dlq` |
| `DFE_ARCHIVER_DESTINATION` | `archive.destination` | none, so the archiver idles |
| `DFE_ARCHIVER_PATH_TEMPLATE` | `archive.path_template` | `{year}/{month}/{day}/{hour}` |
| `DFE_ARCHIVER_COMPRESSION_CODEC` | `compression.codec` | `zstd` |
| `DFE_ARCHIVER_FLUSH_BYTES` | `buffer.flush_bytes` | `1048576` |
| `DFE_ARCHIVER_FLUSH_INTERVAL_SECS` | `buffer.flush_age_secs` | `60` |
| `DFE_ARCHIVER_MULTIPART_CHUNK_SIZE` | `archive.multipart_chunk_size` | `8388608` |
| `DFE_ARCHIVER_SPOOL_DIR` | `buffer.spool_dir` | `/var/spool/dfe/archiver` |
| `DFE_ARCHIVER_S3_BUCKET` | `archive.s3.bucket` | the destination URL's |
| `DFE_ARCHIVER_S3_REGION` | `archive.s3.region` | the AWS SDK's own |
| `DFE_ARCHIVER_S3_ACCESS_KEY_ID` | `archive.s3.access_key_id` | the AWS credential chain |
| `DFE_ARCHIVER_S3_SECRET_ACCESS_KEY` | `archive.s3.secret_access_key` | the AWS credential chain |
| `DFE_ARCHIVER_S3_ENDPOINT` | `archive.s3.endpoint` | AWS |
| `DFE_ARCHIVER_S3_ALLOW_HTTP` | `archive.s3.allow_http` (`true`, `1` or `yes`) | `false` |

The store SDKs read their own variables too, `AWS_*`, `GOOGLE_*` and `AZURE_*` ([internals.md](internals.md#storage-backends)).

## scalo runtime variables

| Variable | Sets | Default |
|---|---|---|
| `DFE_ARCHIVER_CONFIG` | the archiver's config file, when `--config` names none | none |
| `DFE_ARCHIVER_MEMORY_LIMIT_BYTES` | the memory guard's limit. `0` reads it from the cgroup | `0` |
| `DFE_ARCHIVER_MEMORY_PRESSURE_THRESHOLD` | share of the limit at which the guard reports pressure | `0.8` |
| `DFE_ARCHIVER_MEMORY_CGROUP_HEADROOM` | share of the cgroup limit the guard takes as its limit | `0.85` |
| `DFE_ARCHIVER_SELF_REGULATION__ENABLED` | `false` stops intake pausing under pressure | `true` |
| `DFE_ARCHIVER_SCALING__ENABLED` | `false` holds `scaling_pressure` at 0 | `true` |
| `DFE_ARCHIVER_SCALING__MEMORY_GATE_THRESHOLD` | scalo's memory gate, which nothing feeds in the archiver, so it changes nothing | `0.8` |
| `DFE_ARCHIVER_VERSION_CHECK__ENABLED` | `false` stops the version check against releases.hyperi.io | `true` |
| `DFE_ARCHIVER_METRICS__ADDRESS`, `METRICS_ADDR`, `--metrics-addr` | the metrics listener's address. The flag wins, then `METRICS_ADDR` | `0.0.0.0:9090` |
| `LOG_LEVEL`, `--log-level` | log level. `-v` is `debug`, `-q` is `error` | `info` |
| `LOG_FORMAT`, `--log-format` | `json`, `text` or `auto` | `auto` |
| `TOKIO_WORKER_THREADS` | Tokio worker threads, never fewer than 2 | the CPUs available |
