akka-entity-replication
===

[![CI](https://github.com/lerna-stack/akka-entity-replication/workflows/CI/badge.svg?branch=master)](https://github.com/lerna-stack/akka-entity-replication/actions?query=workflow%3ACI+branch%3Amaster)

## Introduction

If a node failure or network failure occurs, some entities in Cluster Sharding become unavailable and it will take more than 10 seconds to recover. akka-entity-replication provides fast recovery by creating replicas of entities in multiple locations and always synchronizing their status. 

Each replicated entity is distributed across multiple nodes in the cluster like Cluster Sharding. This provides high scalability in addition to high availability.

akka-entity-replication helps to implement *Event Sourcing* and *Command Query Responsibility Segregation* (CQRS). Entity state updates are represented as events, and based on the events, a read model is updated for queries.

![](docs/images/demo.apng)

*Requests recover (become green) immediately even if failure (kill a node) occurred*

## Technical background

Entity status synchronization is archived by Raft consensus algorithm. This algorithm ensures that the replica states of the entities are synchronized, so that if some failure occurs and a replica becomes unavailable, the other replicas can immediately continue processing.

The replicas of each entity are not started on every node in the Cluster but are placed in such a way that the load is distributed by sharding with `akka-cluster-sharding`. As nodes are added or removed, they are automatically rebalanced.

Akka ensures that the order of arrival of messages between source and destination is maintained. Note that if you send multiple messages, it is possible for entities to receive messages in a different order than they were sent, since the replicas of the entities in akka-entity-replication may be replaced in the course of sending messages.

## Getting Started

To use this library, you must add a dependency into your sbt project, add the following lines to your `build.sbt` file:

**Stable Release**

[![Maven Central](https://img.shields.io/maven-central/v/com.lerna-stack/akka-entity-replication_2.13?color=%23005cb2&label=stable)](https://mvnrepository.com/artifact/com.lerna-stack/akka-entity-replication) 

⚠️ `v2.0.0` contains breaking changes. For more details see [CHANGELOG](./CHANGELOG.md).

```scala
libraryDependencies += "com.lerna-stack" %% "akka-entity-replication" % "X.X.X"
```

**Unstable Release (SNAPSHOT)**

[![Sonatype Nexus (Snapshots)](https://img.shields.io/nexus/s/com.lerna-stack/akka-entity-replication_2.13?color=%237B1FA2&label=unstable&server=https%3A%2F%2Foss.sonatype.org)](https://oss.sonatype.org/index.html#nexus-search;gav~com.lerna-stack~akka-entity-replication_*~~~)

```scala
resolvers += Resolver.sonatypeRepo("snapshots")  // If you use SNAPSHOT, you need to refer to Sonatype

libraryDependencies += "com.lerna-stack" %% "akka-entity-replication" % "X.X.X-SNAPSHOT"
```

These versions of akka-entity-replication depends on **Akka 2.6.x**. It has been published for Scala 2.13.
akka-entity-replication is tested with OpenJDK8 and OpenJDK11.

**NOTE**: akka-entity-replication uses Akka's internal API.
It is recommended that you use the same version of Akka as akka-entity-replication for your application to avoid compatibility issues.
Please see [build.sbt](./build.sbt) for the version of Akka that akka-entity-replication depends on.

For more information on how to implement an application using this library, please refer to the following documents.

- [Implementation Guide](docs/typed/implementation_guide.md)
- [Testing Guide](docs/typed/testing_guide.md)

## For Contributors

[CONTRIBUTING](CONTRIBUTING.md) may help us.

## Examples

You can see a sample application using this extension in the following project.

[lerna-stack/akka-entity-replication-sample](https://github.com/lerna-stack/akka-entity-replication-sample)

## Changelog

You can see all the notable changes in [CHANGELOG](CHANGELOG.md).

## Migration Guide

[Migration Guide](docs/migration_guide.md) describes how to migrate code and settings from previous versions.

## License

akka-entity-replication is released under the terms of the [Apache License Version 2.0](./LICENSE).

<!-- Escape to set blank lines and use "*" -->
\
\
\* The names of the companies and products described in this site are trademarks or registered trademarks of the respective companies.  
\* Akka is a trademark of Lightbend, Inc.

© 2020 TIS Inc.
