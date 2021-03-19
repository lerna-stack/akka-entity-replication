package lerna.akka.entityreplication.protobuf

import akka.actor.ActorSystem
import akka.serialization.SerializationExtension
import lerna.akka.entityreplication.ClusterReplicationSerializable

final class ClusterReplicationSerializerBindingSpec
    extends SerializerSpecBase(ActorSystem("ClusterReplicationSerializerBingingSpec")) {

  private val serialization = SerializationExtension(system)

  "ClusterReplicationSerializer " should {

    "be bound to ClusterReplicationSerializable" in {
      val serializer = serialization.serializerFor(classOf[ClusterReplicationSerializable])
      serializer shouldBe a[ClusterReplicationSerializer]
    }

  }

}
