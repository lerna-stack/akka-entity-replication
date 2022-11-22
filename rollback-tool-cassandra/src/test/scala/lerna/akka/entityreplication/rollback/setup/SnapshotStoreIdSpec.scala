package lerna.akka.entityreplication.rollback.setup

import lerna.akka.entityreplication.model.{ NormalizedEntityId, TypeName }
import lerna.akka.entityreplication.raft.routing.MemberIndex
import org.scalatest.{ Matchers, WordSpec }

final class SnapshotStoreIdSpec extends WordSpec with Matchers {

  "SnapshotStoreId.persistenceId" should {

    "return the persistence ID for the SnapshotStore" in {
      val id = SnapshotStoreId(TypeName.from("example"), MemberIndex("member1"), NormalizedEntityId("entity1"))
      id.persistenceId should be("SnapshotStore:example:entity1:member1")
    }

  }

}
