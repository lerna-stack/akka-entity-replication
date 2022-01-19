package lerna.akka.entityreplication.typed

import akka.NotUsed
import akka.actor.typed.ActorRef
import akka.cluster.sharding.{ ClusterSharding, ShardRegion }
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{ MultiNodeConfig, MultiNodeSpec }
import com.typesafe.config.{ ConfigFactory, ConfigValueFactory }
import lerna.akka.entityreplication.raft.routing.MemberIndex
import lerna.akka.entityreplication.{ ReplicationRegion, STMultiNodeSerializable, STMultiNodeSpec }

import scala.concurrent.duration.{ DurationInt, FiniteDuration }
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

  private val settings                 = ClusterReplicationSettings(system.toTyped)
  private[this] val clusterReplication = ClusterReplication(system.toTyped)

  "ClusterReplication" should {

    "wait for all nodes to join the cluster" in {
      joinCluster(node1, node2, node3)
    }

    "start ClusterReplication" in {
      clusterReplication.init(GetEntityContextEntity())
      enterBarrier("ClusterReplication started.")
    }

    "start all Raft actors automatically" in {
      // All Raft actors should start within this timeout.
      val autoStartTimeout: FiniteDuration = 10.seconds
      val expectedShardRegionState = {
        val allRaftActorIds = (0 until settings.raftSettings.numberOfShards).map(_.toString).toSet
        ShardRegion.CurrentShardRegionState(allRaftActorIds.map(raftActorId => {
          // Only one Raft actor runs on each shard.
          // Both Shard ID and Entity ID is the same as Raft actor ID.
          ShardRegion.ShardState(raftActorId, Set(raftActorId))
        }))
      }
      Set(node1, node2, node3).foreach { node =>
        runOn(node) {
          val shardingTypeName = {
            val clusterReplicationTypeName = GetEntityContextEntity.typeKey.name
            ReplicationRegion.raftShardingTypeName(clusterReplicationTypeName, memberIndexes(node))
          }
          awaitAssert(
            {
              // We have to get the shard region in this awaitAssert assertion
              // since the getting shard region would also succeed eventually.
              ClusterSharding(system).shardRegion(shardingTypeName) ! ShardRegion.GetShardRegionState
              expectMsg(expectedShardRegionState)
            },
            max = autoStartTimeout,
          )
        }
      }
      enterBarrier("All Raft actors are running.")
    }

    "provide a ReplicatedEntityContext to the entity behavior" in {
      import GetEntityContextEntity._
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
        ReplicatedEntityBehavior[GetEntityContext, NotUsed.type, NotUsed.type](
          context,
          emptyState = NotUsed,
          commandHandler = { (_, msg) =>
            Effect.reply(msg.replyTo)(Reply(context))
          },
          eventHandler = (_, _) => NotUsed,
        ),
      )
    }
  }

}
