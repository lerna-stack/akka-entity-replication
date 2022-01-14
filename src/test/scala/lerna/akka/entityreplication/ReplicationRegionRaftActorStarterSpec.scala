package lerna.akka.entityreplication

import akka.actor.testkit.typed.scaladsl.ManualTime
import akka.actor.typed.scaladsl.adapter.{ ClassicActorSystemOps, TypedActorRefOps }
import akka.actor.{ ActorRef, ActorSystem }
import akka.cluster.sharding.ShardRegion
import akka.testkit.{ TestKit, TestProbe }
import com.typesafe.config.{ Config, ConfigFactory }
import lerna.akka.entityreplication.raft.{ ActorSpec, RaftSettings }

import scala.concurrent.duration.DurationInt

object ReplicationRegionRaftActorStarterSpec {
  private val config: Config = {
    ManualTime.config.withFallback(ConfigFactory.load())
  }
}

final class ReplicationRegionRaftActorStarterSpec
    extends TestKit(ActorSystem("ReplicationRegionRaftActorStarterSpec", ReplicationRegionRaftActorStarterSpec.config))
    with ActorSpec {

  private val manualTime = ManualTime()(system.toTyped)

  private def spawnRaftActorStarter(
      shardRegion: ActorRef,
      ids: Set[ShardRegion.EntityId],
      settings: RaftSettings,
  ): ActorRef = {
    val starter =
      system.spawnAnonymous[Nothing](ReplicationRegionRaftActorStarter(shardRegion, ids, settings)).toClassic
    planAutoKill(starter)
  }

  private def expectStartEntityAndThenAck(shardRegionProbe: TestProbe): ShardRegion.EntityId = {
    val startMessage = shardRegionProbe.expectMsgType[ShardRegion.StartEntity]
    shardRegionProbe.reply(ShardRegion.StartEntityAck(startMessage.entityId, "unused"))
    startMessage.entityId
  }

  private def expectStartEntityAndThenNoAck(shardRegionProbe: TestProbe): ShardRegion.EntityId = {
    val startMessage = shardRegionProbe.expectMsgType[ShardRegion.StartEntity]
    startMessage.entityId
  }

  "ReplicationRegionRaftActorStarter" should {

    "trigger all actor starts" in {
      val shardRegionProbe = TestProbe()

      val defaultSettings = RaftSettings(system.settings.config)
      val raftActorStarter =
        spawnRaftActorStarter(shardRegionProbe.ref, Set("1", "2", "3"), defaultSettings)

      assume(defaultSettings.raftActorAutoStartNumberOfActors >= 3)

      // The starter will stop at the end of this test.
      watch(raftActorStarter)

      // The starter should trigger all actor starts.
      val startedRaftActorIds = (1 to 3).map { _ =>
        expectStartEntityAndThenAck(shardRegionProbe)
      }.toSet
      startedRaftActorIds shouldBe Set("1", "2", "3")

      // The starter should stop itself.
      expectTerminated(raftActorStarter)

    }

    "retry all actor starts with no ACK" in {

      val shardRegionProbe = TestProbe()

      val defaultSettings = RaftSettings(system.settings.config)
      val raftActorStarter =
        spawnRaftActorStarter(shardRegionProbe.ref, Set("1", "2", "3"), defaultSettings)

      assume(defaultSettings.raftActorAutoStartNumberOfActors >= 3)

      // The starter will stop at the end of this test.
      watch(raftActorStarter)

      // First round (all starts will fail)
      // The shard region should not reply ACK
      // since this test verifies that the starter should retry such starts.
      (1 to 3).map { _ =>
        expectStartEntityAndThenNoAck(shardRegionProbe)
      }.toSet

      // Advance the clock by `retry-interval`.
      // Then the starter will try again.
      manualTime.expectNoMessageFor(defaultSettings.raftActorAutoStartRetryInterval - 1.milli)
      manualTime.timePasses(1.milli)

      // Second round (all starts will succeed)
      val startedRaftActorIdsOnSecondTry = (1 to 3).map { _ =>
        expectStartEntityAndThenAck(shardRegionProbe)
      }.toSet
      startedRaftActorIdsOnSecondTry shouldBe Set("1", "2", "3")

      // The starter should stop itself.
      expectTerminated(raftActorStarter)

    }

    "trigger all actor starts in multiple rounds" in {
      val shardRegionProbe = TestProbe()

      val defaultSettings = RaftSettings(system.settings.config)
      val raftActorStarter =
        spawnRaftActorStarter(
          shardRegionProbe.ref,
          Set("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"),
          defaultSettings,
        )

      assume(defaultSettings.raftActorAutoStartNumberOfActors == 5)

      // The starter will stop at the end of this test.
      watch(raftActorStarter)

      // First round
      val startedRaftActorIdsOnFirstTry = (1 to 5).map { _ =>
        expectStartEntityAndThenAck(shardRegionProbe)
      }.toSet

      // Advance the clock by `frequency`.
      // The starter will trigger starts on the second round.
      manualTime.expectNoMessageFor(defaultSettings.raftActorAutoStartFrequency - 1.milli)
      manualTime.timePasses(1.milli)

      // Second round
      val startedRaftActorIdsOnSecondTry = (1 to 5).map { _ =>
        expectStartEntityAndThenAck(shardRegionProbe)
      }.toSet

      // Advance the clock by `frequency`.
      // The starter will trigger starts on the third round.
      manualTime.expectNoMessageFor(defaultSettings.raftActorAutoStartFrequency - 1.milli)
      manualTime.timePasses(1.milli)

      // Third round
      val startedRaftActorIdsOnThirdTry = (1 to 2).map { _ =>
        expectStartEntityAndThenAck(shardRegionProbe)
      }.toSet

      val startedRaftActorIds =
        startedRaftActorIdsOnFirstTry
          .union(startedRaftActorIdsOnSecondTry)
          .union(startedRaftActorIdsOnThirdTry)
      startedRaftActorIds shouldBe Set("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")

      // The starter should stop itself.
      expectTerminated(raftActorStarter)

    }

  }

}
