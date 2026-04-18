# Kestra SurrealDB Plugin

## What

- Provides plugin components under `io.kestra.plugin.surrealdb`.
- Includes classes such as `Trigger`, `SurrealDBConnection`, `Query`.

## Why

- This plugin integrates Kestra with SurrealDB.
- It provides tasks that run SurrealQL queries against SurrealDB.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `surrealdb`

Infrastructure dependencies (Docker Compose services):

- `surrealdb`

### Key Plugin Classes

- `io.kestra.plugin.surrealdb.Query`
- `io.kestra.plugin.surrealdb.Trigger`

### Project Structure

```
plugin-surrealdb/
├── src/main/java/io/kestra/plugin/surrealdb/
├── src/test/java/io/kestra/plugin/surrealdb/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
