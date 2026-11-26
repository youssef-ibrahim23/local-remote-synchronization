# Local Remote Synchronization

> **Database-agnostic · Backend-agnostic · Offline-First · Automatic Sync**

A modular, open-source Flutter package ecosystem that provides a production-ready
Offline-First architecture with automatic background synchronization.

---

## Vision

Modern mobile apps must work reliably without an internet connection and sync
seamlessly when connectivity is restored. **Local Remote Synchronization** gives
you the building blocks to do this correctly — without locking you into a
specific database, backend, or sync strategy.

The ecosystem is designed around strict **dependency inversion**: the Core
package defines contracts (interfaces), and every concrete technology plugs in
as an independent adapter package that depends on the Core — never the other way
around.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                        Your Flutter App                     │
│   reads from local DB only ──► writes locally first        │
└───────────────────┬─────────────────────────────────────────┘
                    │ depends on
┌───────────────────▼─────────────────────────────────────────┐
│          local_remote_synchronization  (Core)               │
│  Contracts · Models · Engine skeleton · Queue · Retry ·     │
│  Conflict resolution abstractions                           │
└──────┬─────────────────────────────────────────────┬────────┘
       │ implements                                  │ implements
┌──────▼──────────┐  ┌──────────────┐    ┌──────────▼──────────────────────┐
│ local_remote_   │  │local_remote_ │    │ local_remote_supabase           │
│ sqflite         │  │drift         │    │ local_remote_firestore          │
│ local_remote_   │  │local_remote_ │    │ local_remote_rest               │
│ hive            │  │...           │    │ local_remote_graphql            │
└─────────────────┘  └──────────────┘    └─────────────────────────────────┘
  LocalStorageAdapter                      RemoteSyncAdapter
```

**Key principle:** The UI reads from local storage only. Local writes are
persisted immediately. The sync engine processes a durable operation queue in
the background and pushes changes to the remote when connectivity is available.

---

## Package Ecosystem

| Package | Type | Responsibility | Status |
|---|---|---|---|
| `local_remote_synchronization` | Core | Contracts, models, engine skeleton, queue, retry, conflict | 🚧 In Development |
| `local_remote_sqflite` | Local adapter | `LocalStorageAdapter` via sqflite | 🗓 Planned |
| `local_remote_drift` | Local adapter | `LocalStorageAdapter` via Drift | 🗓 Planned |
| `local_remote_hive` | Local adapter | `LocalStorageAdapter` via Hive | 🗓 Planned |
| `local_remote_supabase` | Remote adapter | `RemoteSyncAdapter` via Supabase | 🗓 Planned |
| `local_remote_firestore` | Remote adapter | `RemoteSyncAdapter` via Firestore | 🗓 Planned |
| `local_remote_rest` | Remote adapter | `RemoteSyncAdapter` via REST HTTP | 🗓 Planned |
| `local_remote_graphql` | Remote adapter | `RemoteSyncAdapter` via GraphQL | 🗓 Planned |

---

## Project Structure

```
local-remote-synchronization/
├── packages/
│   ├── local_remote_synchronization/   # Core package
│   ├── local_remote_sqflite/           # sqflite local adapter
│   ├── local_remote_drift/             # Drift local adapter
│   ├── local_remote_hive/              # Hive local adapter
│   ├── local_remote_supabase/          # Supabase remote adapter
│   ├── local_remote_firestore/         # Firestore remote adapter
│   ├── local_remote_rest/              # REST remote adapter
│   └── local_remote_graphql/           # GraphQL remote adapter
├── examples/
│   └── basic_offline_first_app/        # Reference Flutter app
├── docs/
│   ├── architecture.md
│   ├── package_structure.md
│   └── adapter_development.md
├── scripts/                            # Utility scripts
├── melos.yaml                          # Monorepo management
└── analysis_options.yaml               # Shared lint rules
```

---

## Development Setup

### Prerequisites

- Flutter SDK ≥ 3.19.0 (Dart ≥ 3.3.0)
- [Melos](https://melos.invertase.dev/) — monorepo manager

### Install Melos

```bash
dart pub global activate melos
```

### Bootstrap the monorepo

```bash
melos bootstrap
```

This resolves dependencies across all packages and links local path overrides.

### Run analysis

```bash
melos analyze
```

### Run tests

```bash
melos test
```

### Format code

```bash
melos format:fix
```

---

## Contributing

Contributions are welcome! Please read the [Architecture Documentation](docs/architecture.md)
and the [Adapter Development Guide](docs/adapter_development.md) before opening a pull request.

---

## License

MIT — see [LICENSE](LICENSE).
