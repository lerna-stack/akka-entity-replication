package lerna.akka.entityreplication.testkit

import akka.actor.Props
import lerna.akka.entityreplication.ReplicationActor

object TestReplicationActorProps {

  type ReplicationActorProvider = (=> ReplicationActor[_])

  def apply(replicationActorProvider: ReplicationActorProvider): Props = {
    Props(new TestReplicationActor(replicationActorProvider))
  }
}
