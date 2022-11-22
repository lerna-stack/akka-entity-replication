package lerna.akka.entityreplication.rollback.cassandra

import com.typesafe.config.Config

private object CassandraQuerySettings {

  /** Creates a [[CassandraQuerySettings]] from the given config
    *
    * The given config should have the same structure as the one of Akka Persistence Cassandra plugin
    * (`akka.persistence.cassandra`).
    *
    * @throws java.lang.IllegalArgumentException if the given config contains an invalid setting value
    */
  def apply(pluginConfig: Config): CassandraQuerySettings = {
    val readProfile =
      pluginConfig.getString("query.read-profile")
    val maxBufferSize =
      pluginConfig.getInt("query.max-buffer-size")
    val deserializationParallelism =
      pluginConfig.getInt("query.deserialization-parallelism")
    new CassandraQuerySettings(
      readProfile,
      maxBufferSize,
      deserializationParallelism,
    )
  }

}

private final class CassandraQuerySettings private (
    val readProfile: String,
    val maxBufferSize: Int,
    val deserializationParallelism: Int,
) {
  require(
    maxBufferSize > 0,
    s"query.max-buffer-size [$maxBufferSize] should be greater than 0",
  )
  require(
    deserializationParallelism > 0,
    s"query.deserialization-parallelism [$deserializationParallelism] should be greater than 0",
  )
}
