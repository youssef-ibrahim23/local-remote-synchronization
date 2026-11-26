# local_remote_hive

[![pub.dev](https://img.shields.io/pub/v/local_remote_hive.svg)](https://pub.dev/packages/local_remote_hive)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](../../LICENSE)

**Hive** implementation of `LocalStorageAdapter` for the
[local_remote_synchronization](../local_remote_synchronization) ecosystem.

---

## Responsibility

This package bridges the Core contract `LocalStorageAdapter` with the
[Hive](https://pub.dev/packages/hive_flutter) key-value store. It is well-suited
for apps that prefer a lightweight, schema-free storage solution that works
across all Flutter platforms including Web.

**It does not know about remote backends.**

---

## Status

> 🗓 **Planned.** Not yet implemented.

---

## Implementation Checklist

- [ ] Implement `LocalStorageAdapter` using Hive boxes.
- [ ] Persist `SyncQueue` entries in a dedicated Hive box.
- [ ] Handle Hive TypeAdapters for all Core models.
- [ ] Write unit tests using `hive_test`.

---

## License

MIT — see [LICENSE](../../LICENSE).
