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
 │                  Flutter App                    │
 │  Reads from local storage only.                 │
 │  Writes locally first, then sync fires.         │
 └───────────────────┬─────────────────────────────┘
                     │  depends on
 ┌───────────────────▼─────────────────────────────┐
 │       local_remote_synchronization (Core)        │
 │                                                 │
 │  Contracts: LocalStorageAdapter                 │
 │             RemoteSyncAdapter                   │
 │             ConnectivityProvider                │
 │             ConflictResolver                    │
 │             RetryPolicy                         │
 │                                                 │
 │  Models:    SyncOperation, LocalMutation,       │
 │             SyncResult, SyncStatus,             │
 │             SyncConfiguration, SyncError        │
 │                                                 │
 │  Engine:    SyncEngine (skeleton)               │
 │  Queue:     SyncQueue (abstraction)             │
 │  Scheduler: SyncScheduler (skeleton)            │
 └──────┬──────────────────────────────┬───────────┘
        │  implements                  │  implements
 ┌──────▼──────────┐         ┌─────────▼──────────────────────┐
 │  Local Adapters │         │  Remote Adapters               │
 │                 │         │                                │
 │ local_remote_   │         │ local_remote_supabase          │
 │   sqflite       │         │ local_remote_firestore         │
 │ local_remote_   │         │ local_remote_rest              │
 │   drift         │         │ local_remote_graphql           │
 │ local_remote_   │         │                                │
 │   hive          │         └────────────────────────────────┘
 └─────────────────┘
   LocalStorageAdapter          RemoteSyncAdapter
```

**Rule:** Arrows flow from dependent → dependency.
Adapters depend on Core; Core depends on nothing domain-specific.

---

## Package Boundaries

### Core (`local_remote_synchronization`)

**Allowed dependencies:**
- `flutter` SDK
- `meta` (annotations)
- `collection` (utilities)
- `equatable` (value equality)

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

## Data Flow

```
User Action
    │
    ▼
Write to Local DB (via LocalStorageAdapter)
    │
    ▼
Enqueue SyncOperation in SyncQueue (durable, survives restarts)
    │
    ▼
SyncEngine observes queue + ConnectivityProvider
    │
    ▼
SyncProcessor picks next operation
    │
    ▼
Apply operation via RemoteSyncAdapter
    │
    ├─── Success ──► Dequeue + mark SyncResult.success
    │
    └─── Failure ──► RetryPolicy decides backoff / abandon
                         │
                         └── ConflictResolver if conflict detected
```

---

## Idempotency

Every `SyncOperation` carries a stable `operationId` (UUID). Remote adapters
must use this ID to deduplicate retried operations (e.g., as an idempotency
key in HTTP headers or a Firestore document ID).

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
operation, data will be lost. Each `LocalStorageAdapter` implementation must
guarantee this, typically by using a database transaction that writes both
the domain record and the queue entry.
