package lerna.akka.entityreplication.rollback.setup

import lerna.akka.entityreplication.model.{ NormalizedShardId, TypeName }
import org.scalatest.{ Matchers, WordSpec }

final class CommitLogStoreActorIdSpec extends WordSpec with Matchers {

  "CommitLogStoreActorId.persistenceId" should {

    "return the persistence ID for the CommitLogStoreActor" in {
      val id = CommitLogStoreActorId(TypeName.from("example"), NormalizedShardId.from("shard1"))
      id.persistenceId should be("CommitLogStore:example:shard1")
    }

  }

}
