package lerna.akka.entityreplication.serialization

import akka.actor.ExtendedActorSystem

class ClusterReplicationSerializer(system: ExtendedActorSystem)
    extends io.altoo.akka.serialization.kryo.KryoSerializer(system) {
  // To avoid an exception bellow. 1602579212 is an magic number which was created from timestamp.
  // -----------------------------------------------------------------------------------------------------------------------------
  // java.lang.IllegalArgumentException:
  // Serializer identifier [123454323] of [lerna.akka.entityreplication.serialization.ClusterReplicationSerializer] is not unique.
  // It is also used by [io.altoo.akka.serialization.kryo.KryoSerializer]
  override def identifier: Int = 1602579212
}
