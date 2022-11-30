package lerna.akka.entityreplication.rollback.setup

import lerna.akka.entityreplication.model.{ NormalizedShardId, TypeName }
import lerna.akka.entityreplication.raft.routing.MemberIndex
import org.scalatest.{ Matchers, WordSpec }

final class SnapshotSyncManagerIdSpec extends WordSpec with Matchers {

  "SnapshotSyncManagerId.persistenceId" should {

    "return the persistence ID for the SnapshotSyncManager" in {
      val id = SnapshotSyncManagerId(
        TypeName.from("example"),
        NormalizedShardId.from("shard1"),
        sourceMemberIndex = MemberIndex("member1"),
        destinationMemberIndex = MemberIndex("member2"),
      )
      id.persistenceId should be("SnapshotSyncManager:example:member1:member2:shard1")
    }

  }

}
