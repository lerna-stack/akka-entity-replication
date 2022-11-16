# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
[Unreleased]: https://github.com/lerna-stack/akka-entity-replication/compare/v2.1.0...master

### Added
- Add diagnostic logs
  [PR#164](https://github.com/lerna-stack/akka-entity-replication/pull/164),
  [PR#176](https://github.com/lerna-stack/akka-entity-replication/pull/176),
  [PR#177](https://github.com/lerna-stack/akka-entity-replication/pull/177)
- Add function extracting shard id from entity id to lerna.akka.entityreplication.typed.ClusterReplication
  [PR#172](https://github.com/lerna-stack/akka-entity-replication/pull/172)
- Add function of Disabling raft actor
  [PR#173](https://github.com/lerna-stack/akka-entity-replication/pull/173),
  [PR#188](https://github.com/lerna-stack/akka-entity-replication/pull/188)

### Changed
- Enhance leader's replication response handling [PR#160](https://github.com/lerna-stack/akka-entity-replication/pull/160)
- Change event sourcing log level to debug
  [PR#163](https://github.com/lerna-stack/akka-entity-replication/pull/163)
- ReplicationRegionRaftActorStarter uses its FQCN as its logger name
  [PR178](https://github.com/lerna-stack/akka-entity-replication/pull/178)
- Add diagnostic info to logs of sending replication results
  [PR#179](https://github.com/lerna-stack/akka-entity-replication/pull/179)

### Fixed
- RaftActor might delete committed entries
  [#152](https://github.com/lerna-stack/akka-entity-replication/issues/152)
  [#165](https://github.com/lerna-stack/akka-entity-replication/issues/165)
  [PR#151](https://github.com/lerna-stack/akka-entity-replication/pull/151)
  [PR#166](https://github.com/lerna-stack/akka-entity-replication/pull/166)  
  ⚠️ This fix adds a new persistent event type. It doesn't allow downgrading after being updated.
- An entity on a follower could stick at `WaitForReplication` if the entity has a `ProcessCommand` in its mailbox
  [#157](https://github.com/lerna-stack/akka-entity-replication/issues/157),
  [PR#158](https://github.com/lerna-stack/akka-entity-replication/pull/158)
- Leader cannot reply to an entity with a `ReplicationFailed` message in some cases
  [#153](https://github.com/lerna-stack/akka-entity-replication/issues/153),
  [PR#161](https://github.com/lerna-stack/akka-entity-replication/pull/161),
  [#170](https://github.com/lerna-stack/akka-entity-replication/issues/170),
  [PR#171](https://github.com/lerna-stack/akka-entity-replication/pull/171)
- An entity could stick at WaitForReplication when a Raft log entry is truncated by conflict
  [#155](https://github.com/lerna-stack/akka-entity-replication/issues/155),
  [#PR162](https://github.com/lerna-stack/akka-entity-replication/pull/162)
- A RaftAcotor(Leader) could mis-deliver a ReplicationSucceeded message to a different entity
  [156](https://github.com/lerna-stack/akka-entity-replication/issues/156),
  [#PR162](https://github.com/lerna-stack/akka-entity-replication/pull/162)
- Snapshot synchronization could remove committed log entries that not be included in snapshots
  [#167](https://github.com/lerna-stack/akka-entity-replication/issues/167)
  [#PR168](https://github.com/lerna-stack/akka-entity-replication/pull/168)
- SnapshotStore doesn't reply with SnapshotNotFound sometimes
  [#182](https://github.com/lerna-stack/akka-entity-replication/issues/182),
  [#PR183](https://github.com/lerna-stack/akka-entity-replication/pull/183)

## [v2.1.0] - 2022-03-24
[v2.1.0]: https://github.com/lerna-stack/akka-entity-replication/compare/v2.0.0...v2.1.0

### Added
- Efficient recovery of commit log store, which is on the query side [#112](https://github.com/lerna-stack/akka-entity-replication/issues/112)

  This change will improve the performance of the recovery on the query side.
  You should migrate settings described at [Migration Guide](docs/migration_guide.md#210-from-200).

- Raft actors start automatically after an initialization of `ClusterReplication` [#118](https://github.com/lerna-stack/akka-entity-replication/issues/118)

  This feature is enabled only by using `typed.ClusterReplication`.
  It is highly recommended that you switch using the typed API since the classic API was deprecated.

- Raft actors track the progress of the event sourcing
  [#136](https://github.com/lerna-stack/akka-entity-replication/issues/136),
  [PR#137](https://github.com/lerna-stack/akka-entity-replication/pull/137),
  [PR#142](https://github.com/lerna-stack/akka-entity-replication/pull/142).

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

- Compaction warns if it might not delete enough entries [PR#142](https://github.com/lerna-stack/akka-entity-replication/pull/142)

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
