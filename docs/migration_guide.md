# Migration Guide

## 2.1.0 from 2.0.0

### Configure a snapshot store on the query side
*akka-entity-replication 2.1.0* introduces an efficient recovery on the query side.
We've achieved this efficient recovery by using a snapshot feature of Akka persistence.
This efficient recovery requires you to configure a snapshot store like the following:
```hocon
lerna.akka.entityreplication.raft.eventsourced.persistence {
  snapshot-store.plugin = "Specify your snapshot store plugin ID to use"
}
```
Note that this snapshot store is mandatory.
You have to configure the snapshot store.

This efficient recovery also introduces new settings named `lerna.akka.entityreplication.raft.eventsourced.persistence.snapshot-every`.
*akka-entity-replication 2.1.0* saves a snapshot every `snapshot-every` events.
The default value of `snapshot-every` is 1000.
You can override this setting according to your requirements.
