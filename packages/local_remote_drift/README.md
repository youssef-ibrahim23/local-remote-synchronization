# local_remote_drift

[![pub.dev](https://img.shields.io/pub/v/local_remote_drift.svg)](https://pub.dev/packages/local_remote_drift)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](../../LICENSE)

**Drift** implementation of `LocalStorageAdapter` for the
[local_remote_synchronization](../local_remote_synchronization) ecosystem.

---

## Responsibility

This package bridges the Core contract `LocalStorageAdapter` with the
[Drift](https://drift.simonbinder.eu/) ORM. It provides fully type-safe,
reactive offline-first local storage for Android, iOS, macOS, and desktop.

Drift's code-generation approach means schema changes are caught at compile
time, making it an excellent choice for teams that prefer strong typing over
raw SQL.

**It does not know about remote backends.**

---

## Package Layout

```
lib/src/
└── (implementation files will go here)
```

---

## Status

> 🗓 **Planned.** Not yet implemented.

---

## Implementation Checklist

- [ ] Define Drift database with sync queue tables.
- [ ] Implement `LocalStorageAdapter` interface.
- [ ] Support reactive streams via Drift's `watch()` API.
- [ ] Handle schema migrations with `MigrationStrategy`.
- [ ] Write unit tests using `drift_testbed`.

---

## Installation

```yaml
dependencies:
  local_remote_drift:
    path: ../local_remote_drift   # monorepo path
```

---

## License

MIT — see [LICENSE](../../LICENSE).
