package lerna.akka.entityreplication.testkit

import akka.actor.Props
import lerna.akka.entityreplication.ReplicationActor

/**
  * The [[TestReplicationActorProps]] allows to test [[ReplicationActor]] like a normal Actor.
  */
@deprecated(message = "Use typed.testkit.ReplicatedEntityBehaviorTestKit instead", since = "2.0.0")
object TestReplicationActorProps {

  def apply(replicationActorProps: Props): Props = {
    if (classOf[ReplicationActor[_]].isAssignableFrom(replicationActorProps.actorClass())) {
      Props(new TestReplicationActor(replicationActorProps))
    } else {
      throw new IllegalArgumentException(
        s"The Props for [${replicationActorProps.actorClass()}] doesn't provide ReplicationActor",
      )
    }
  }
}
