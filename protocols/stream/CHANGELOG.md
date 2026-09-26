## 0.5.0-alpha

- Add `Behaviour::with_max_negotiating_outbound_streams` so a connection can negotiate several
  outbound streams at once and an `open_stream` need not wait a round trip behind every request
  queued ahead of it. The default stays one at a time.
- Skip an outbound stream request whose `open_stream` caller has stopped waiting, instead of
  negotiating a stream nobody will use.
- Deliver streams requested while a connection is still being established to that connection,
  instead of parking them behind a dial the swarm skips and leaving them hanging. A skipped dial to
  an already-connected peer now fails its parked requests rather than stranding them.
- Fix memory leak: remove the per-connection `Sender` from `Shared::senders`
  when a connection closes. Previously every established connection leaked one
  sender entry forever, growing memory unboundedly under connection churn.
  See [PR 6638](https://github.com/libp2p/rust-libp2p/pull/6638).
- Raise MSRV to 1.88.0.
  See [PR 6273](https://github.com/libp2p/rust-libp2p/pull/6273).

## 0.4.0-alpha

- Garbage-collect deregistered streams when accepting new streams.
  See [PR 5999](https://github.com/libp2p/rust-libp2p/pull/5999).

<!-- Update to libp2p-swarm v0.47.0 -->

## 0.3.0-alpha

- Deprecate `void` crate.
  See [PR 5676](https://github.com/libp2p/rust-libp2p/pull/5676).

<!-- Update to libp2p-core v0.43.0 -->

## 0.2.0-alpha

<!-- Update to libp2p-swarm v0.45.0 -->

## 0.1.0-alpha.1
- Implement Error for `OpenStreamError`.
  See [PR 5169](https://github.com/libp2p/rust-libp2p/pull/5169).

## 0.1.0-alpha

Initial release.
