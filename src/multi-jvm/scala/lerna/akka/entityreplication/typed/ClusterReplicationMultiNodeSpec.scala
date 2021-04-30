package lerna.akka.entityreplication.typed

import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.Behaviors
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{ MultiNodeConfig, MultiNodeSpec }
import lerna.akka.entityreplication.{ STMultiNodeSerializable, STMultiNodeSpec }
import com.typesafe.config.{ ConfigFactory, ConfigValueFactory }
import lerna.akka.entityreplication.raft.routing.MemberIndex

import scala.jdk.CollectionConverters._

object ClusterReplicationMultiNodeSpecConfig extends MultiNodeConfig {
  val node1: RoleName = role("node1")
  val node2: RoleName = role("node2")
  val node3: RoleName = role("node3")

  val memberIndexes: Map[RoleName, MemberIndex] = Map(
    node1 -> MemberIndex("member-1"),
    node2 -> MemberIndex("member-2"),
    node3 -> MemberIndex("member-3"),
  )

  commonConfig(
    debugConfig(false)
      .withFallback(ConfigFactory.parseString(s"""
      akka.actor.provider = cluster
      akka.test.single-expect-default = 15s
      """))
      .withValue(
        "lerna.akka.entityreplication.raft.multi-raft-roles",
        ConfigValueFactory.fromIterable(memberIndexes.values.map(_.role).toSet.asJava),
      )
      .withFallback(ConfigFactory.parseResources("multi-jvm-testing.conf")),
  )
  nodeConfig(node1)(ConfigFactory.parseString(s"""
    akka.cluster.roles = ["${memberIndexes(node1)}"]
  """))
  nodeConfig(node2)(ConfigFactory.parseString(s"""
    akka.cluster.roles = ["${memberIndexes(node2)}"]
  """))
  nodeConfig(node3)(ConfigFactory.parseString(s"""
    akka.cluster.roles = ["${memberIndexes(node3)}"]
  """))
}

class ClusterReplicationMultiNodeSpecMultiJvmNode1 extends ClusterReplicationMultiNodeSpec
class ClusterReplicationMultiNodeSpecMultiJvmNode2 extends ClusterReplicationMultiNodeSpec
class ClusterReplicationMultiNodeSpecMultiJvmNode3 extends ClusterReplicationMultiNodeSpec

abstract class ClusterReplicationMultiNodeSpec
    extends MultiNodeSpec(ClusterReplicationMultiNodeSpecConfig)
    with STMultiNodeSpec {

  import ClusterReplicationMultiNodeSpec._
  import ClusterReplicationMultiNodeSpecConfig._

  import akka.actor.typed.scaladsl.adapter._

  private[this] val clusterReplication = ClusterReplication(system.toTyped)

  "ClusterReplication" should {

    "wait for all nodes to join the cluster" in {
      joinCluster(node1, node2, node3)
    }

    "provide a ReplicatedEntityContext to the entity behavior" in {
      import GetEntityContextEntity._
      clusterReplication.init(GetEntityContextEntity())
      val entityId      = "test-entity"
      val entityRef     = clusterReplication.entityRefFor(GetEntityContextEntity.typeKey, entityId = entityId)
      val entityContext = (entityRef ask GetEntityContext).await.context

      entityContext.entityId should be(entityId)
      entityContext.entityTypeKey should be(typeKey)
      entityContext.shard shouldBe a[ActorRef[_]]
    }
  }
}

object ClusterReplicationMultiNodeSpec {

  object GetEntityContextEntity {

    val typeKey: ReplicatedEntityTypeKey[GetEntityContext] = ReplicatedEntityTypeKey("GetEntityContext")

    final case class GetEntityContext(replyTo: ActorRef[Reply])                extends STMultiNodeSerializable
    final case class Reply(context: ReplicatedEntityContext[GetEntityContext]) extends STMultiNodeSerializable

    def apply(): ReplicatedEntity[GetEntityContext, ReplicationEnvelope[GetEntityContext]] = {
      ReplicatedEntity(typeKey)(context =>
        Behaviors.receiveMessage { msg =>
          msg.replyTo ! Reply(context)
          Behaviors.same
        },
      )
    }
  }

}
