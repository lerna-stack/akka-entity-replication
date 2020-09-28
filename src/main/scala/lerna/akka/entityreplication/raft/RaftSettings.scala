package lerna.akka.entityreplication.raft

import java.util.concurrent.TimeUnit.NANOSECONDS

import com.typesafe.config.Config
import lerna.akka.entityreplication.util.JavaDurationConverters._

import scala.collection.JavaConverters._
import scala.concurrent.duration.{ Duration, FiniteDuration }
import scala.util.Random

object RaftSettings {
  def apply(root: Config) = new RaftSettings(root)
}

class RaftSettings(root: Config) {

  val config: Config = root.getConfig("lerna.akka.entityreplication.raft")

  val electionTimeout: FiniteDuration = config.getDuration("election-timeout").asScala

  def randomizedElectionTimeout(): FiniteDuration = randomized(electionTimeout)

  val heartbeatInterval: FiniteDuration = config.getDuration("heartbeat-interval").asScala

  private[this] val randomizedMinFactor = 0.75

  private[this] val randomizedMaxFactor = 0.75

  /**
    * 75% - 150% of duration
    */
  def randomized(duration: FiniteDuration): FiniteDuration = {
    val randomizedDuration = duration * (randomizedMinFactor + randomizedMaxFactor * Random.nextDouble())
    FiniteDuration(randomizedDuration.toNanos, NANOSECONDS)
  }

  val electionTimeoutMin: Duration = electionTimeout * randomizedMinFactor

  // heartbeatInterval < electionTimeout < MTBF が満たせないと可用性が損なわれるため
  require(
    electionTimeoutMin > heartbeatInterval,
    s"electionTimeout (${electionTimeout.toMillis} ms) * $randomizedMinFactor must be larger than heartbeatInterval (${heartbeatInterval.toMillis} ms)",
  )

  val multiRaftRoles: Set[String] = config.getStringList("multi-raft-roles").asScala.toSet

  require(
    multiRaftRoles.size >= 3,
    s"multi-raft-roles should have size 3 or more for availability",
  )

  val replicationFactor: Int = multiRaftRoles.size

  val quorumSize: Int = (replicationFactor / 2) + 1

  val compactionSnapshotCacheTimeToLive: FiniteDuration =
    config.getDuration("compaction.snapshot-cache-time-to-live").asScala

  val compactionLogSizeThreshold: Int = config.getInt("compaction.log-size-threshold")

  require(
    0 < compactionLogSizeThreshold,
    s"log-size-threshold ($compactionLogSizeThreshold) should be larger than 0",
  )

  val compactionLogSizeCheckInterval: FiniteDuration = config.getDuration("compaction.log-size-check-interval").asScala

  def randomizedCompactionLogSizeCheckInterval(): FiniteDuration = randomized(compactionLogSizeCheckInterval)

  val journalPluginId: String = config.getString("persistence.journal.plugin")

  val snapshotStorePluginId: String = config.getString("persistence.snapshot-store.plugin")
}
