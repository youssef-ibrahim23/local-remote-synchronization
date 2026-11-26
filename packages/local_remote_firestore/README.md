# local_remote_firestore

[![pub.dev](https://img.shields.io/pub/v/local_remote_firestore.svg)](https://pub.dev/packages/local_remote_firestore)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](../../LICENSE)

**Cloud Firestore** implementation of `RemoteSyncAdapter` for the
[local_remote_synchronization](../local_remote_synchronization) ecosystem.

---

## Responsibility

This package bridges the Core contract `RemoteSyncAdapter` with the
[cloud_firestore](https://pub.dev/packages/cloud_firestore) SDK. It handles
applying local mutations to Firestore collections and reading remote changes.

**It does not know which local database is being used.**

---

## Status

> 🗓 **Planned.** Not yet implemented.

---

## Implementation Checklist

- [ ] Implement `RemoteSyncAdapter` interface.
- [ ] Support Firestore batch writes for atomic multi-document operations.
- [ ] Handle Firestore Security Rules token injection via Firebase Auth.
- [ ] Implement conflict-detection using Firestore document `updateTime`.
- [ ] Write unit tests with `fake_cloud_firestore`.

---

## License

MIT — see [LICENSE](../../LICENSE).
