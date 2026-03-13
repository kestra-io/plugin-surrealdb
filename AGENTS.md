# Kestra SurrealDB Plugin

## What

Utilize SurrealDB in Kestra workflows for data management. Exposes 2 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with SurrealDB, allowing orchestration of SurrealDB-based operations as part of data pipelines and automation workflows.

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

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
