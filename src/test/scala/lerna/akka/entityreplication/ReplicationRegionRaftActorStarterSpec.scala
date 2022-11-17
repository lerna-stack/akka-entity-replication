package lerna.akka.entityreplication

import akka.actor.testkit.typed.scaladsl.ManualTime
import akka.actor.typed.scaladsl.adapter.{ ClassicActorSystemOps, TypedActorRefOps }
import akka.actor.{ ActorRef, ActorSystem }
import akka.cluster.sharding.ShardRegion
import akka.testkit.{ TestKit, TestProbe }
import com.typesafe.config.{ Config, ConfigFactory }
import lerna.akka.entityreplication.raft.{ ActorSpec, RaftSettings }

import scala.concurrent.duration.{ Duration, DurationInt, FiniteDuration }

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

  /** Classic version of [[ManualTime.expectNoMessageFor]] */
  private def expectNoMessageFor(duration: FiniteDuration, probe: TestProbe): Unit = {
    manualTime.timePasses(duration)
    probe.expectNoMessage(Duration.Zero)
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

    "trigger all actor starts without disabled actor" in {
      val shardRegionProbe = TestProbe()

      val customSettings = RaftSettings(system.settings.config)
        .withDisabledShards(Set("2"))
      val raftActorStarter =
        spawnRaftActorStarter(shardRegionProbe.ref, Set("1", "2", "3"), customSettings)

      assume(customSettings.raftActorAutoStartNumberOfActors >= 3)

      // The starter will stop at the end of this test.
      watch(raftActorStarter)

      // The starter should trigger all actor starts without disabled shards.
      val startedRaftActorIds = (1 to 2).map { _ =>
        expectStartEntityAndThenAck(shardRegionProbe)
      }.toSet
      startedRaftActorIds shouldBe Set("1", "3")

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
      expectNoMessageFor(defaultSettings.raftActorAutoStartRetryInterval - 1.milli, shardRegionProbe)
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

      val customSettings = RaftSettings(
        ConfigFactory
          .parseString(
            """
            |lerna.akka.entityreplication.raft.raft-actor-auto-start {
            |  frequency = 200ms
            |  number-of-actors = 5
            |  retry-interval = 1s
            |}
            |""".stripMargin,
          ).withFallback(system.settings.config),
      )
      val raftActorStarter =
        spawnRaftActorStarter(
          shardRegionProbe.ref,
          Set("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"),
          customSettings,
        )

      assert(customSettings.raftActorAutoStartNumberOfActors == 5)
      assert(
        customSettings.raftActorAutoStartRetryInterval > customSettings.raftActorAutoStartFrequency * 3, // 3 rounds
        "The starter should never send retries to simplify this test.",
      )

      // The starter will stop at the end of this test.
      watch(raftActorStarter)

      // First round
      val startedRaftActorIdsOnFirstTry = (1 to 5).map { _ =>
        expectStartEntityAndThenAck(shardRegionProbe)
      }.toSet
      startedRaftActorIdsOnFirstTry.size shouldBe 5

      // Advance the clock by `frequency`.
      // The starter will trigger starts on the second round.
      expectNoMessageFor(customSettings.raftActorAutoStartFrequency - 1.milli, shardRegionProbe)
      manualTime.timePasses(1.milli)

      // Second round
      val startedRaftActorIdsOnSecondTry = (1 to 5).map { _ =>
        expectStartEntityAndThenAck(shardRegionProbe)
      }.toSet
      startedRaftActorIdsOnSecondTry.size shouldBe 5

      // Advance the clock by `frequency`.
      // The starter will trigger starts on the third round.
      expectNoMessageFor(customSettings.raftActorAutoStartFrequency - 1.milli, shardRegionProbe)
      manualTime.timePasses(1.milli)

      // Third round
      val startedRaftActorIdsOnThirdTry = (1 to 2).map { _ =>
        expectStartEntityAndThenAck(shardRegionProbe)
      }.toSet
      startedRaftActorIdsOnThirdTry.size shouldBe 2

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
