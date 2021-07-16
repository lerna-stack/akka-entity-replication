# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
[Unreleased]: https://github.com/lerna-stack/akka-entity-replication/compare/v2.0.0...master


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
