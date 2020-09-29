akka-entity-replication
===

## Introduction

If a node failure or network failure occurs, some entities in Cluster Sharding become unavailable and it will take more than 10 seconds to recover. akka-entity-replication provides fast recovery by creating replicas of entities in multiple locations and always synchronizing their status. 

Each replicated entities are distributed across multiple nodes in the cluster like Cluster Sharding. This provides high scalability in addition to high availablity.

akka-entity-replication helps to implement *Event Sourcing* and *Command Query Responsibility Segregation* (CQRS). Entity state updates are represented as events, and based on the events, a read model is updated for queries.

## Project Status

⚠️⚠️⚠️

**akka-entity-replication is currently under development. It is NOT recommended to be used in production. Use at your own risk.**

⚠️⚠️⚠️

We have decided to publish this library as early preview to be able to discuss improvements about this.

## Technical background

Entity status synchronization is archived by Raft consensus algorithm. This algorithm ensures that the replica states of the entities are synchronized, so that if some failure occurs and a replica becomes unavailable, the other replicas can immediately continue processing.

The replicas of each entity are not started on every node in the Cluster, but are placed in such a way that the load is distributed by sharding based on Consistent Hash. As nodes are added or removed, they are automatically rebalanced.

Akka ensures that the order of arrival of messages between source and destination is maintained. Note that if you send multiple messages, it is possible for entities to receive messages in a different order than they were sent, since the replicas of the entities in akka-entity-replicaiton may be replaced in the course of sending messages.

## Getting Started

To use this library, you must add a dependency into your sbt project, add the following lines to your `build.sbt` file:

```scala
resolvers += Resolver.sonatypeRepo("snapshots")  // If you use SNAPSHOT, you need to refer to Sonatype

libraryDependencies += "io.github.lerna-stack" %% "akka-entity-replication" % "0.1.0-SNAPSHOT"
```

This version of akka-entity-replication depends on **Akka 2.6.x**. It has been published for Scala 2.12.

akka-entity-replication currently uses Apache Cassandra to store entity events. Other data stores may be available in future releases.

For more information on how to implement an application using this libray, please refer to [this Implementation Guide](docs/implementation_guide.md).

## License

akka-entity-replication is released under the terms of the [Apache License Version 2.0](./LICENSE).

\
\
\* The names of the companies and products described in this site are trademarks or registered trademarks of the respective companies.  
\* Akka is a trademark of Lightbend, Inc.

© 2020 TIS Inc.
