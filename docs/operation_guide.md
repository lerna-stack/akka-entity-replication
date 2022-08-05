# Operation Guide

**Principle: Don't terminate all Raft members at the same time.**

Reasons behind the principle:
When all Raft members (who have the same ID but different roles) stop, their event-sourcing also stops. This stop
continues until the cluster receives a request for the Raft members. If at least one of the Raft members is running,
messages such as a heartbeat from the running member will trigger other Raft members to start.

Tips: Raft members are running as entities of Akka Cluster Sharding. By the nature of re-balancing shards, adding or
removing nodes to the cluster stops some Raft members. There is no guarantee that at least one of the Raft members is
still running if cluster membership changes for all roles.

To achieve this principle, you carefully do cluster operations.
This document supposes that you will use three `multi-raft-roles` (`replica-group-1`, `replica-group-2`, `replica-group-3`).


## Start a new cluster
The important point is to start only one node in at least one role.

A possible full cluster start operation is the following:
* Start a node (or multiple nodes) with role `replica-group-1`.
* Start a node (or multiple nodes) with role `replica-group-2`.
* Start exactly one node with `replica-group-3`.

You can do the above operations simultaneously.
You have to wait for all Raft members to be running
before adding more nodes with `replica-group-3`.
The waiting time depends on your settings, such as the following:
* `lerna.akka.entityreplication.raft.election-timeout`
* `lerna.akka.entityreplication.raft.heartbeat-interval`
* `lerna.akka.entityreplication.raft.number-of-shards`
* `lerna.akka.entityreplication.raft.raft-actor-auto-start`


## Add nodes
Don't add nodes with different roles at the same time.

Any of the following operations is possible:
* Add a node (or multiple nodes) with `replica-group-1`.
* Add a node (or multiple nodes) with `replica-group-2`.
* Add a node (or multiple nodes) with `replica-group-3`.

You **should not** do the above operations simultaneously.
You have to wait for all Raft members to be running
before adding more nodes or removing nodes.
The waiting time depends on your settings, such as the following:
* `lerna.akka.entityreplication.raft.election-timeout`
* `lerna.akka.entityreplication.raft.heartbeat-interval`

Any of the following operations is possible but **not recommended**.
It is because you cannot ensure that any of nodes with untouched roles won't crash.
* Add nodes with `replica-group-1` and nodes with `replica-group-2`.
* Add nodes with `replica-group-2` and nodes with `replica-group-3`.
* Add nodes with `replica-group-3` and nodes with `replica-group-1`.


## Remove nodes
Don't remove nodes with different roles at the same time.

Any of the following operations is possible:
* Remove a node (or multiple nodes) with `replica-group-1`.
* Remove a node (or multiple nodes) with `replica-group-2`.
* Remove a node (or multiple nodes) with `replica-group-3`.

You **should not** do the above operations simultaneously.
You have to wait for all Raft members to be running
before adding nodes or removing more nodes.
Waiting time depends on your settings, such as the following:
* `lerna.akka.entityreplication.raft.election-timeout`
* `lerna.akka.entityreplication.raft.heartbeat-interval`

Any of the following operations is possible but **not recommended**.
It is because you cannot ensure that any of nodes with untouched roles won't crash.
* Remove nodes with `replica-group-1` and nodes with `replica-group-2`.
* Remove nodes with `replica-group-2` and nodes with `replica-group-3`.
* Remove nodes with `replica-group-3` and nodes with `replica-group-1`.
