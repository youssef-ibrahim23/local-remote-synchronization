# Package Structure — Local Remote Synchronization

## Monorepo Layout

```
local-remote-synchronization/
│
├── packages/
│   ├── local_remote_synchronization/          Core package
│   │   ├── lib/
│   │   │   ├── local_remote_synchronization.dart   Public barrel export
│   │   │   └── src/
│   │   │       ├── contracts/     Abstract interfaces (LocalStorageAdapter, OutboxWriter, RemoteSyncAdapter, ConnectivityProvider)
│   │   │       ├── models/        Immutable data classes (SyncOperation, LocalMutation, SyncResult, SyncError)
│   │   │       ├── enums/         Domain enumerations (SyncOperationType, SyncOperationStatus, SyncErrorCode)
│   │   │       ├── engine/        SyncEngine, SyncEngineConfig, RetryPolicy, ExponentialBackoffRetryPolicy, SyncCycleReport, SyncConflictEvent
│   │   │       └── utils/         Deep-freeze immutability helpers
│   │   ├── test/
│   │   └── example/
│   │
│   ├── local_remote_sqflite/      LocalStorageAdapter — sqflite
│   │   ├── lib/src/
│   │   └── test/
│   │
│   ├── local_remote_drift/        LocalStorageAdapter — Drift
│   │   ├── lib/src/
│   │   └── test/
│   │
│   ├── local_remote_hive/         LocalStorageAdapter — Hive
│   │   ├── lib/src/
│   │   └── test/
│   │
│   ├── local_remote_supabase/     RemoteSyncAdapter — Supabase
│   │   ├── lib/src/
│   │   └── test/
│   │
│   ├── local_remote_firestore/    RemoteSyncAdapter — Firestore
│   │   ├── lib/src/
│   │   └── test/
│   │
│   ├── local_remote_rest/         RemoteSyncAdapter — REST HTTP
│   │   ├── lib/src/
│   │   └── test/
│   │
│   └── local_remote_graphql/      RemoteSyncAdapter — GraphQL
│       ├── lib/src/
│       └── test/
│
├── examples/
│   └── basic_offline_first_app/   Reference Flutter app
│
├── docs/
│   ├── architecture.md            Dependency direction and data flow
│   ├── package_structure.md       This file
│   └── adapter_development.md     Guide to building custom adapters
│
├── scripts/                       Utility shell scripts
│
├── melos.yaml                     Monorepo management
├── analysis_options.yaml          Shared strict lint rules
├── .gitignore
├── README.md
├── CHANGELOG.md
└── LICENSE
```

---

## Core Package Module Responsibilities

| Module | Folder | Responsibility |
|---|---|---|
| Contracts | `src/contracts/` | `LocalStorageAdapter`, `OutboxWriter`, `RemoteSyncAdapter`, `ConnectivityProvider` |
| Models | `src/models/` | `SyncOperation`, `LocalMutation`, `SyncResult` (`SyncSuccess`, `SyncConflict`, `SyncFailure`), `SyncError` |
| Enums | `src/enums/` | `SyncOperationType`, `SyncOperationStatus`, `SyncErrorCode` |
| Engine | `src/engine/` | `SyncEngine`, `SyncEngineConfig`, `SyncCycleReport`, `SyncConflictEvent`, `RetryPolicy`, `ExponentialBackoffRetryPolicy` |
| Utils | `src/utils/` | Deep-freeze immutability helpers |

---

## Adapter Package Responsibilities

| Package | Contract Implemented | Technology |
|---|---|---|
| `local_remote_sqflite` | `LocalStorageAdapter` | sqflite |
| `local_remote_drift` | `LocalStorageAdapter` | Drift ORM |
| `local_remote_hive` | `LocalStorageAdapter` | Hive key-value |
| `local_remote_supabase` | `RemoteSyncAdapter` | Supabase PostgREST |
| `local_remote_firestore` | `RemoteSyncAdapter` | Cloud Firestore |
| `local_remote_rest` | `RemoteSyncAdapter` | Generic HTTP REST |
| `local_remote_graphql` | `RemoteSyncAdapter` | Generic GraphQL |

---

## Pub.dev Publication Plan

| Phase | Packages | Prerequisite |
|---|---|---|
| 1 | `local_remote_synchronization` | Contracts + models stabilised |
| 2 | `local_remote_sqflite` | Core published |
| 3 | `local_remote_rest` | Core published |
| 4 | `local_remote_drift`, `local_remote_hive` | Core published |
| 5 | `local_remote_supabase`, `local_remote_firestore`, `local_remote_graphql` | Core published |
