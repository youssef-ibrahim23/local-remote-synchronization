# Architecture — Local Remote Synchronization

## Overview

The Local Remote Synchronization ecosystem is built on a strict
**Dependency Inversion** architecture. The Core package defines abstract
contracts (interfaces); every concrete technology plugs in as an independent
adapter package that depends on the Core — never the other way around.

---

## Dependency Direction

```
 ┌─────────────────────────────────────────────────┐
 │             Application (Flutter / Dart)        │
 │  Owns local domain tables & typed DAOs.         │
 │  Persists domain writes + outbox atomically.    │
 └───────────────────┬─────────────────────────────┘
                     │  depends on
 ┌───────────────────▼─────────────────────────────┐
 │       local_remote_synchronization (Core)       │
 │                 (Pure Dart)                     │
 │                                                 │
 │  Implemented (Core):                            │
 │    Engine:    SyncEngine, SyncEngineConfig,     │
 │               RetryPolicy,                      │
 │               ExponentialBackoffRetryPolicy     │
 │    Contracts: LocalStorageAdapter, OutboxWriter,│
 │               RemoteSyncAdapter,                │
 │               ConnectivityProvider              │
 │    Models:    SyncOperation, LocalMutation,     │
 │               SyncResult, SyncError             │
 │                                                 │
 │  Out of Core Scope:                             │
 │    Pull sync, ConflictResolver, background jobs │
 └──────┬──────────────────────────────┬───────────┘
        │  implements                  │  implements
 ┌──────▼──────────┐         ┌─────────▼──────────────────────┐
 │  Local Adapters │         │  Remote Adapters               │
 │  (Independent)  │         │  (Independent)                 │
 │                 │         │                                │
 │ local_remote_   │         │ local_remote_supabase          │
 │   sqflite       │         │ local_remote_firestore         │
 │ local_remote_   │         │ local_remote_rest              │
 │   drift         │         │ local_remote_graphql           │
 │ local_remote_   │         │                                │
 │   hive          │         └────────────────────────────────┘
 └─────────────────┘            RemoteSyncAdapter
   LocalStorageAdapter
   (Each adapter persists its own Outbox in its chosen DB)
```

**Rule:** Arrows flow from dependent → dependency.
Adapters depend on Core; Core depends on nothing domain-specific.
Each local adapter (`local_remote_sqflite`, `local_remote_drift`,
`local_remote_hive`) is independent and persists its own durable Outbox using
the same database technology selected by the application.

---

## Package Boundaries

### Core (`local_remote_synchronization`)

**Allowed dependencies (Pure Dart):**
- `meta` (annotations)
- `collection` (deep collection equality and utilities)

**Forbidden dependencies (forever):**
- Any database package (sqflite, Drift, Hive, Realm, …)
- Any backend SDK (Firebase, Supabase, AWS, …)
- Any HTTP/GraphQL client
- Any connectivity detection library
- Platform-specific plugins

### Local Adapter Packages

**Allowed:**
- Core package
- Their specific database package
- Utility packages needed for that database

**Forbidden:**
- Remote backend SDKs
- Other adapter packages
- Cross-adapter shared code outside the Core

### Remote Adapter Packages

**Allowed:**
- Core package
- Their specific backend/HTTP SDK

**Forbidden:**
- Database packages
- Other adapter packages
- Cross-adapter shared code outside the Core

---

## Data Flow (Push-Only Engine Orchestration)

```
User Action
    │
    ▼
Atomic Local Write + Outbox Enqueue (via LocalStorageAdapter / typed txn API)
    │
    ▼
SyncEngine observes ConnectivityProvider (or manual syncNow())
    │
    ▼
Apply eligible operation via RemoteSyncAdapter
    │
    ├─── SyncSuccess  ──► markSucceeded (removes from active outbox)
    │
    ├─── SyncConflict ──► markDeadLetter + emit SyncConflictEvent on engine.conflicts
    │
    └─── SyncFailure  ──► RetryPolicy (markFailed with nextRetryAt, or markDeadLetter)
```

---

## Idempotency

Every `SyncOperation` carries a stable `idempotencyKey` (defaulting to
`operationId`). Remote adapters send this key on every push attempt, but
retrying a request provides **at-least-once** delivery — not guaranteed
exactly-once remote execution. Effective duplicate protection requires the
remote backend to persist and deduplicate on `idempotencyKey`.

---

## Background Execution Caveats

> **Important:** Android and iOS impose strict background execution limits.

The sync engine makes **no promises** about background execution timing.
Apps must handle the case where sync only runs when the app is in the
foreground or immediately after returning from background. Use platform-specific
background processing solutions (WorkManager on Android, BGTaskScheduler on iOS)
as a separate optional layer — not built into this Core.

---

## Atomicity Requirement

A local mutation and its corresponding `SyncOperation` **must** be persisted
atomically. If the app crashes between writing the mutation and enqueuing the
operation, data will be lost.

Because the Core `LocalStorageAdapter.runInTransaction` contract is
database-agnostic and does not expose native transaction handles, adapters whose
databases require explicit transaction objects (such as Sqflite) **must**
provide an adapter-specific typed transaction API alongside `runInTransaction`
so that both the application's domain writes and the `OutboxWriter` share the
exact same underlying native database transaction.
