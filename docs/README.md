<!--
  Project:      dfe-archiver
  File:         docs/README.md
  Purpose:      Landing page for the documentation
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# dfe-archiver documentation

How dfe-archiver is built, and how to run it.

- [Archiving](archiving.md) -- what a file holds, where it goes, how long it stays and how to restore
- [How the archiver works](internals.md) -- the pipeline, buffers and writers, rolling, routing, compression and the storage backends
- [Delivery](delivery.md) -- at-least-once delivery, the DLQ, shutdown, restart and disk protection
- [Configuration reference](configuration.md) -- the config cascade and every environment variable
- [Metrics reference](metrics.md) -- every metric the archiver records itself
- [dfe-archiver architecture](architecture.md) -- the three crates, the one-way rules between them and the build graph
- [DFE Archiver build and design notes](design.md) -- how it is built and released, and the decisions behind it
- [Testing](testing.md) -- which tests run where, and the containers they start
- [config-schema.yaml](config-schema.yaml) -- the configuration as a JSON Schema (YAML)
- [config-schema.json](config-schema.json) -- the configuration as a JSON Schema (JSON)
- [capability-catalog.yaml](capability-catalog.yaml) -- what the app can do, as a catalogue (YAML)
- [capability-catalog.json](capability-catalog.json) -- what the app can do, as a catalogue (JSON)
