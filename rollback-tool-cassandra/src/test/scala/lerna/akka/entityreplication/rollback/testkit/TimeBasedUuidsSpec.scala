package lerna.akka.entityreplication.rollback.testkit

import com.datastax.oss.driver.api.core.uuid.Uuids
import org.scalatest.{ Matchers, WordSpec }

import java.time.{ ZoneOffset, ZonedDateTime }

final class TimeBasedUuidsSpec extends WordSpec with Matchers {

  "TimeBasedUuids.create" should {

    "return a TimeBasedUUID containing the timestamp calculated from the base timestamp and the given delta" in {
      val baseTimestamp =
        ZonedDateTime.of(2021, 7, 12, 11, 7, 0, 0, ZoneOffset.UTC).toInstant
      val timeBasedUuids =
        new TimeBasedUuids(baseTimestamp)

      // Test:
      Uuids.unixTimestamp(timeBasedUuids.create(-2).value) should be(baseTimestamp.plusMillis(-2).toEpochMilli)
      Uuids.unixTimestamp(timeBasedUuids.create(-1).value) should be(baseTimestamp.plusMillis(-1).toEpochMilli)
      Uuids.unixTimestamp(timeBasedUuids.create(0).value) should be(baseTimestamp.toEpochMilli)
      Uuids.unixTimestamp(timeBasedUuids.create(+1).value) should be(baseTimestamp.plusMillis(1).toEpochMilli)
      Uuids.unixTimestamp(timeBasedUuids.create(+2).value) should be(baseTimestamp.plusMillis(2).toEpochMilli)
    }

  }

}
