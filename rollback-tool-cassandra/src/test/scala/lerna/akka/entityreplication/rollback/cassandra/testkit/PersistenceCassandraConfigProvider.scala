package lerna.akka.entityreplication.rollback.cassandra.testkit

import com.typesafe.config.{ Config, ConfigFactory }

import java.time.ZonedDateTime

trait PersistenceCassandraConfigProvider {

  private def firstTimeBucket: String = {
    val time = ZonedDateTime.now().minusMinutes(1)
    FirstTimeBucket.format(time)
  }

  def cassandraPort: Int = 9042

  def persistenceCassandraConfig(
      journalKeyspace: String,
      snapshotKeyspace: String,
      autoCreate: Boolean = false,
  ): Config = {
    require(
      journalKeyspace.length <= 48,
      s"journalKeyspace.length [${journalKeyspace.length}] should be less than or equal to 48. See https://docs.datastax.com/en/cql-oss/3.3/cql/cql_reference/refLimits.html.",
    )
    require(
      snapshotKeyspace.length <= 48,
      s"snapshotKeyspace.length [${snapshotKeyspace.length}] should be less than or equal to 48. See https://docs.datastax.com/en/cql-oss/3.3/cql/cql_reference/refLimits.html.",
    )
    ConfigFactory.parseString(s"""
      |akka.persistence.journal.plugin = akka.persistence.cassandra.journal
      |akka.persistence.snapshot-store.plugin = akka.persistence.cassandra.snapshot
      |
      |akka.persistence.cassandra.journal.keyspace = "$journalKeyspace"
      |akka.persistence.cassandra.journal.keyspace-autocreate = $autoCreate
      |akka.persistence.cassandra.journal.tables-autocreate = $autoCreate
      |
      |akka.persistence.cassandra.snapshot.keyspace = "$snapshotKeyspace"
      |akka.persistence.cassandra.snapshot.keyspace-autocreate = $autoCreate
      |akka.persistence.cassandra.snapshot.tables-autocreate = $autoCreate
      |
      |akka.persistence.cassandra.events-by-tag.eventual-consistency-delay = 1000ms
      |akka.persistence.cassandra.events-by-tag.first-time-bucket = "$firstTimeBucket"
      |
      |datastax-java-driver {
      |  advanced.reconnect-on-init = true
      |  basic.contact-points = ["127.0.0.1:$cassandraPort"]
      |  basic.load-balancing-policy.local-datacenter = "datacenter1"
      |}
      |""".stripMargin)
  }

}
