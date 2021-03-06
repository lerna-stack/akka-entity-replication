package lerna.akka.entityreplication.typed

import akka.NotUsed
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.scaladsl.Behaviors
import org.scalatest.{ BeforeAndAfterAll, FlatSpec, Matchers }
import org.scalatest.concurrent.ScalaFutures

class ClusterReplicationSpec extends FlatSpec with Matchers with ScalaFutures with BeforeAndAfterAll {

  private[this] val actorTestKit = ActorTestKit()

  private[this] val clusterReplication = ClusterReplication(actorTestKit.system)

  override def afterAll(): Unit = {
    actorTestKit.shutdownTestKit()
    super.afterAll()
  }

  behavior of "ClusterReplication.init"

  it should "provide a same ActorRef instance even if it was called multiple time" in {
    val typeKey = ReplicatedEntityTypeKey[NotUsed]("InitMultipleTime")
    val entity  = ReplicatedEntity(typeKey)(_ => Behaviors.empty)

    val region1 = clusterReplication.init(entity)
    val region2 = clusterReplication.init(entity)

    region1 should be theSameInstanceAs region2
  }

  behavior of "ClusterReplication.entityRefFor"

  it should "throw an exception if the typeKey has not initialized" in {
    val typeKey = ReplicatedEntityTypeKey[NotUsed]("NotInitialized")

    val exception =
      intercept[IllegalStateException] {
        clusterReplication.entityRefFor(typeKey, "dummy")
      }
    exception.getMessage should be(
      "The type [ReplicatedEntityTypeKey[akka.NotUsed](NotInitialized)] must be init first",
    )
  }

  it should "provide ReplicatedEntityRef after the region was initialized" in {
    val typeKey = ReplicatedEntityTypeKey[NotUsed]("ProvideReplicatedEntityRef")
    val entity  = ReplicatedEntity(typeKey)(_ => Behaviors.empty)

    clusterReplication.init(entity)

    clusterReplication.entityRefFor(typeKey, "test") shouldBe a[ReplicatedEntityRef[_]]
  }
}
