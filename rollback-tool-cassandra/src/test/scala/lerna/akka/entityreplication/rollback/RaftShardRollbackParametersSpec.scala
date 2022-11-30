package lerna.akka.entityreplication.rollback

import lerna.akka.entityreplication.model.{ NormalizedShardId, TypeName }
import lerna.akka.entityreplication.raft.routing.MemberIndex
import org.scalatest.{ Matchers, WordSpec }

import java.time.Instant

final class RaftShardRollbackParametersSpec extends WordSpec with Matchers {

  "RaftShardRollbackParameters" should {

    "throw an IllegalArgumentException if the given allMemberIndices doesn't contain the leader member index" in {
      val exception = intercept[IllegalArgumentException] {
        RaftShardRollbackParameters(
          TypeName.from("example"),
          NormalizedShardId.from("1"),
          Set(
            MemberIndex("replica-group-1"),
            MemberIndex("replica-group-2"),
            MemberIndex("replica-group-3"),
          ),
          MemberIndex("replica-group-4"),
          Instant.now(),
        )
      }
      exception.getMessage should be(
        "requirement failed: allMemberIndices [Set(replica-group-1, replica-group-2, replica-group-3)] " +
        "should contain the leader member index [replica-group-4]",
      )
    }

  }

}
