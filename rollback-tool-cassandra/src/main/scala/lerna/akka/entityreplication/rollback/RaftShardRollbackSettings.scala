package lerna.akka.entityreplication.rollback

import akka.actor.ActorSystem
import com.typesafe.config.Config

import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters._

private object RaftShardRollbackSettings {

  /** Creates a [[RaftShardRollbackSettings]] from the given system
    *
    * The root config instance for the settings is extracted from the config of the given system.
    * Config path `lerna.akka.entityreplication.rollback` is used for the extraction.
    *
    * @throws java.lang.IllegalArgumentException if the config contains an invalid setting value
    */
  def apply(system: ActorSystem): RaftShardRollbackSettings = {
    RaftShardRollbackSettings(system.settings.config.getConfig("lerna.akka.entityreplication.rollback"))
  }

  /** Creates a [[RaftShardRollbackSettings]] from the given config
    *
    * @throws java.lang.IllegalArgumentException if the config contains an invalid setting value
    */
  def apply(config: Config): RaftShardRollbackSettings = {
    val dryRun =
      config.getBoolean("dry-run")
    val logProgressEvery =
      config.getInt("log-progress-every")
    val clockOutOfSyncTolerance =
      config.getDuration("clock-out-of-sync-tolerance").toScala
    val readParallelism =
      config.getInt("read-parallelism")
    val writeParallelism =
      config.getInt("write-parallelism")
    new RaftShardRollbackSettings(
      dryRun,
      logProgressEvery,
      clockOutOfSyncTolerance,
      readParallelism,
      writeParallelism,
    )
  }

}

final class RaftShardRollbackSettings private (
    val dryRun: Boolean,
    val logProgressEvery: Int,
    val clockOutOfSyncTolerance: FiniteDuration,
    val readParallelism: Int,
    val writeParallelism: Int,
) {
  require(logProgressEvery > 0, s"log-progress-every [$logProgressEvery] should be greater than 0")
  require(readParallelism > 0, s"read-parallelism [$readParallelism] should be greater than 0")
  require(writeParallelism > 0, s"write-parallelism [$writeParallelism] should be greater than 0")
}
