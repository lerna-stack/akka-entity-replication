package lerna.akka.entityreplication.rollback.setup

import lerna.akka.entityreplication.model.{ NormalizedShardId, TypeName }
import lerna.akka.entityreplication.raft.routing.MemberIndex
import org.scalatest.{ Matchers, WordSpec }

final class RaftActorIdSpec extends WordSpec with Matchers {

  "RaftActorId.persistenceId" should {

    "return the persistence ID for the RaftActor" in {
      val id = RaftActorId(TypeName.from("example"), NormalizedShardId.from("shard1"), MemberIndex("member1"))
      id.persistenceId should be("raft:example:shard1:member1")
    }

  }

}
