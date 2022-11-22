package lerna.akka.entityreplication.rollback.testkit

import akka.persistence.query.TimeBasedUUID
import com.datastax.oss.driver.api.core.uuid.Uuids

import java.time.Instant

final class TimeBasedUuids(val baseTimestamp: Instant) {

  def create(deltaMillis: Long): TimeBasedUUID = {
    val timestamp = baseTimestamp.plusMillis(deltaMillis)
    val uuid      = Uuids.startOf(timestamp.toEpochMilli)
    TimeBasedUUID(uuid)
  }

}
