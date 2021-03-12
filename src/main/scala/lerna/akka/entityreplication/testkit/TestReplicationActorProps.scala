package lerna.akka.entityreplication.testkit

import akka.actor.Props

object TestReplicationActorProps {

  def apply(replicationActorProps: Props): Props = {
    // TODO: check Type
    // if (replicationActorProps.actorClass().isInstance(classOf[ReplicationActor[_]])) {
    Props(new TestReplicationActor(replicationActorProps))
  }
}
