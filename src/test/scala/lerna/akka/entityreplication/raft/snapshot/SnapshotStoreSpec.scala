package lerna.akka.entityreplication.raft.snapshot

import akka.actor.ActorSystem
import akka.testkit.TestKit
import lerna.akka.entityreplication.model.{ NormalizedEntityId, TypeName }
import lerna.akka.entityreplication.raft.ActorSpec
import lerna.akka.entityreplication.raft.routing.MemberIndex

final class SnapshotStoreSpec extends TestKit(ActorSystem("SnapshotStoreSpec")) with ActorSpec {

  "SnapshotStore.persistenceId" should {

    "return a persistence ID for the given type name, entity ID, and member index" in {
      val persistenceId = SnapshotStore.persistenceId(
        TypeName.from("test-type-name"),
        NormalizedEntityId.from("test-entity-id"),
        MemberIndex("test-member-index"),
      )
      assert(persistenceId === "SnapshotStore:test-type-name:test-entity-id:test-member-index")
    }

  }

}
