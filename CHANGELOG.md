# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
[Unreleased]: https://github.com/lerna-stack/akka-entity-replication/compare/v2.0.0...master

### Added
- Efficient recovery of commit log store, which is on the query side [#112](https://github.com/lerna-stack/akka-entity-replication/issues/112)

  This change will improve the performance of the recovery on the query side.
  You should migrate settings described at [Migration Guide](docs/migration_guide.md#210-from-200).

- Raft actors start automatically after an initialization of `ClusterReplication` [#118](https://github.com/lerna-stack/akka-entity-replication/issues/118)

  This feature is enabled only by using `typed.ClusterReplication`.
  It is highly recommended that you switch using the typed API since the classic API was deprecated.

- Raft actors track the progress of the event sourcing [#136](https://github.com/lerna-stack/akka-entity-replication/issues/136).

  This feature ensures that
  - Event Sourcing won't halt even if the event-sourcing store is unavailable for a long period.
    After the event-sourcing store recovers, Event Sourcing will work again automatically.
  - Compaction won't delete committed events that are not persisted to the event-sourcing store yet.

  It adds new following settings (for more details, please see `reference.conf`):
  - `lerna.akka.entityreplication.raft.eventsourced.committed-log-entries-check-interval`
  - `lerna.akka.entityreplication.raft.eventsourced.max-append-committed-entries-size`
  - `lerna.akka.entityreplication.raft.eventsourced.max-append-committed-entries-batch-size`

  It deletes the following settings:
  - `lerna.akka.entityreplication.raft.eventsourced.commit-log-store.retry.attempts`
  - `lerna.akka.entityreplication.raft.eventsourced.commit-log-store.retry.delay`

  It requires that
  `lerna.akka.entityreplication.raft.compaction.preserve-log-size` is less than
  `lerna.akka.entityreplication.raft.compaction.log-size-threshold`. 


### Changed
- Bump up Akka version to 2.6.17 [PR#98](https://github.com/lerna-stack/akka-entity-replication/pull/98)

  This change will show you deserialization warnings during the rolling update, it's safe to ignore. 
  For more details, see [Akka 2.6.16 release note](https://akka.io/blog/news/2021/08/19/akka-2.6.16-released#rolling-upgrades)

### Fixed
- TestKit throws "Shard received unexpected message" exception after the entity passivated [PR#100](https://github.com/lerna-stack/akka-entity-replication/pull/100)
- `ReplicatedEntity` can produce illegal snapshot if compaction and receiving new event occur same time [#111](https://github.com/lerna-stack/akka-entity-replication/issues/111)
- Starting a follower member later than leader completes a compaction may break ReplicatedLog of the follower [#105](https://github.com/lerna-stack/akka-entity-replication/issues/105)
- The Raft leader uses the same previous `LogEntryIndex` and `Term` to all batched `AppendEntries` messages [#123](https://github.com/lerna-stack/akka-entity-replication/issues/123)
- Raft Actors doesn't accept a `RequestVote(lastLogIndex < log.lastLogIndex, lastLogTerm > log.lastLogTerm)` message [#125](https://github.com/lerna-stack/akka-entity-replication/issues/125)
- A new event is created even though all past events have not been applied [#130](https://github.com/lerna-stack/akka-entity-replication/issues/130)
- `InstallSnapshot` can miss snapshots to copy [PR#128](https://github.com/lerna-stack/akka-entity-replication/pull/128)

  ⚠️ This change adds a new persistence event. This might don't allow downgrading after upgrading.
- Moving a leader during snapshot synchronization can delete committed log entries [#133](https://github.com/lerna-stack/akka-entity-replication/issues/133)

  ⚠️ This change adds a new persistence event. This might don't allow downgrading after upgrading.

## [v2.0.0] - 2021-07-16
[v2.0.0]: https://github.com/lerna-stack/akka-entity-replication/compare/v1.0.0...v2.0.0

### Breaking Change

- Change the shard-distribution-strategy to distribute shard (`RaftActor`) more evenly [PR#82](https://github.com/lerna-stack/akka-entity-replication/pull/82)

  ⚠️ This change does not allow rolling updates. You have to update your system by stopping the whole cluster.

- Made internal APIs private

  If you are only using the APIs using in the implementation guide, this change does not affect your application.
  Otherwise, some APIs may be unavailable.
  Please see [PR#47](https://github.com/lerna-stack/akka-entity-replication/pull/47) to check APIs that will no longer be available. 
  
### Added
- Java11 support
- Add new typed API based on Akka Typed [PR#79](https://github.com/lerna-stack/akka-entity-replication/pull/79)
  - This API reduces runtime errors and increases productivity.

### Deprecated

- Untyped (classic) API has been deprecated [PR#96](https://github.com/lerna-stack/akka-entity-replication/pull/96)

  ⚠️ This API will be removed in the next major version release.

## [v1.0.0] - 2021-03-29
[v1.0.0]: https://github.com/lerna-stack/akka-entity-replication/compare/v0.1.1...v1.0.0

- GA release 🚀

## [v0.1.0] - 2021-01-12
[v0.1.0]: https://github.com/lerna-stack/akka-entity-replication/tree/v0.1.1

- Initial release (under development)
