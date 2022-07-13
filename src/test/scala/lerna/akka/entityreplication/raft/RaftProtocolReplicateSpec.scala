package lerna.akka.entityreplication.raft

import akka.actor.ActorSystem
import akka.testkit.{ TestKit, TestProbe }
import lerna.akka.entityreplication.model.{ EntityInstanceId, NormalizedEntityId }
import lerna.akka.entityreplication.raft.RaftProtocol.Replicate
import lerna.akka.entityreplication.raft.model.LogEntryIndex
import org.scalatest.Inside

final class RaftProtocolReplicateSpec
    extends TestKit(ActorSystem("RaftProtocolReplicateSpec"))
    with ActorSpec
    with Inside {

  "ReplicateForInternal.entityId should be None" in {
    val replicateForInternal = Replicate.ReplicateForInternal("event-1", TestProbe().ref)
    replicateForInternal.entityId should be(None)
  }

  "ReplicateForInternal.instanceId should be None" in {
    val replicateForInternal = Replicate.ReplicateForInternal("event-1", TestProbe().ref)
    replicateForInternal.instanceId should be(None)
  }

  "ReplicateForInternal.entityLastAppliedIndex should be None" in {
    val replicateForInternal = Replicate.ReplicateForInternal("event-1", TestProbe().ref)
    replicateForInternal.entityLastAppliedIndex should be(None)
  }

  "ReplicateForInternal.originSender should be None" in {
    val replicateForInternal = Replicate.ReplicateForInternal("event-1", TestProbe().ref)
    replicateForInternal.originSender should be(None)
  }

  "ReplicateForEntity.entityId should be an Option containing the given entityId" in {
    val replicateForInternal = Replicate.ReplicateForEntity(
      "event-1",
      TestProbe().ref,
      NormalizedEntityId("entity-1"),
      EntityInstanceId(1),
      LogEntryIndex(2),
      TestProbe().ref,
    )
    replicateForInternal.entityId should be(Option(NormalizedEntityId("entity-1")))
  }

  "ReplicateForEntity.instanceId should be an Option containing the given instanceId" in {
    val replicateForInternal = Replicate.ReplicateForEntity(
      "event-1",
      TestProbe().ref,
      NormalizedEntityId("entity-1"),
      EntityInstanceId(1),
      LogEntryIndex(2),
      TestProbe().ref,
    )
    replicateForInternal.instanceId should be(Option(EntityInstanceId(1)))
  }

  "ReplicateForEntity.entityLastAppliedIndex should be an Option containing the given entityLastAppliedIndex" in {
    val replicateForInternal = Replicate.ReplicateForEntity(
      "event-1",
      TestProbe().ref,
      NormalizedEntityId("entity-1"),
      EntityInstanceId(1),
      LogEntryIndex(2),
      TestProbe().ref,
    )
    replicateForInternal.entityLastAppliedIndex should be(Option(LogEntryIndex(2)))
  }

  "ReplicateForEntity.originSender should be an Option containing the given originSender" in {
    val originSender = TestProbe().ref
    val replicateForInternal = Replicate.ReplicateForEntity(
      "event-1",
      TestProbe().ref,
      NormalizedEntityId("entity-1"),
      EntityInstanceId(1),
      LogEntryIndex(2),
      originSender,
    )
    replicateForInternal.originSender should be(Option(originSender))
  }

  "Replicate.apply should create a ReplicateForEntity instance with the given parameters" in {

    val replyTo      = TestProbe().ref
    val originSender = TestProbe().ref
    val replicate =
      Replicate("event-1", replyTo, NormalizedEntityId("entity-1"), EntityInstanceId(1), LogEntryIndex(2), originSender)
    replicate should be(
      Replicate.ReplicateForEntity(
        "event-1",
        replyTo,
        NormalizedEntityId("entity-1"),
        EntityInstanceId(1),
        LogEntryIndex(2),
        originSender,
      ),
    )

  }

  "Replicate.internal should create a ReplicateForInternal instance with the given parameters" in {
    val replyTo   = TestProbe().ref
    val replicate = Replicate.internal("event-1", replyTo)
    replicate should be(
      Replicate.ReplicateForInternal("event-1", replyTo),
    )
  }

}
