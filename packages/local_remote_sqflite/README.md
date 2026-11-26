# local_remote_sqflite

[![pub.dev](https://img.shields.io/pub/v/local_remote_sqflite.svg)](https://pub.dev/packages/local_remote_sqflite)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](../../LICENSE)

**sqflite** implementation of `LocalStorageAdapter` for the
[local_remote_synchronization](../local_remote_synchronization) ecosystem.

---

## Responsibility

This package bridges the Core contract `LocalStorageAdapter` with the
[sqflite](https://pub.dev/packages/sqflite) package. It provides durable
offline-first local storage for Android and iOS using SQLite.

**It does not know about remote backends.** The sync engine from the Core
package coordinates how and when local data is pushed to or pulled from the
remote.

---

## Package Layout

```
lib/src/
└── (implementation files will go here)
```

---

## Status

> 🗓 **Planned.** Not yet implemented.

This scaffold exists to reserve the package boundary and document
the intended responsibility. Implementation will follow once the Core
contracts are stabilised.

---

## Implementation Checklist

- [ ] Implement `LocalStorageAdapter` interface.
- [ ] Manage `SyncQueue` persistence via sqflite tables.
- [ ] Handle schema migrations.
- [ ] Write unit and integration tests with `sqflite_common_ffi`.

---

## Installation

```yaml
dependencies:
  local_remote_sqflite:
    path: ../local_remote_sqflite   # monorepo path
```

---

## License

MIT — see [LICENSE](../../LICENSE).
