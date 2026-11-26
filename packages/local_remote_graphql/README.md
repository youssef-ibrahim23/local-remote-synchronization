# local_remote_graphql

[![pub.dev](https://img.shields.io/pub/v/local_remote_graphql.svg)](https://pub.dev/packages/local_remote_graphql)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](../../LICENSE)

**GraphQL** implementation of `RemoteSyncAdapter` for the
[local_remote_synchronization](../local_remote_synchronization) ecosystem.

---

## Responsibility

This package bridges the Core contract `RemoteSyncAdapter` with any GraphQL
backend. It translates `SyncOperation` payloads into GraphQL mutations and
maps responses back to `SyncResult`.

**It does not know which local database is being used.**

---

## Status

> 🗓 **Planned.** Not yet implemented.

---

## Implementation Checklist

- [ ] Implement `RemoteSyncAdapter` interface.
- [ ] Support configurable GraphQL endpoint and auth link.
- [ ] Map `SyncOperationType` to GraphQL mutation documents.
- [ ] Handle GraphQL errors and map to `SyncError`.
- [ ] Implement conflict-detection via mutation response fields.
- [ ] Write unit tests with mocked GraphQL client.

---

## License

MIT — see [LICENSE](../../LICENSE).
