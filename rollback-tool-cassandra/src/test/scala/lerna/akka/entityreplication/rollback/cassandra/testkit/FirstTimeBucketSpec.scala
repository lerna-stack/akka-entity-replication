package lerna.akka.entityreplication.rollback.cassandra.testkit

import org.scalatest.{ Matchers, WordSpec }

import java.time.{ ZoneOffset, ZonedDateTime }

final class FirstTimeBucketSpec extends WordSpec with Matchers {

  "FirstTimeBucket" should {

    "return a formatted string for first-time-bucket of Akka Persistence Cassandra" in {

      val dateTime1 = ZonedDateTime.of(2022, 11, 3, 14, 58, 0, 0, ZoneOffset.ofHours(2))
      FirstTimeBucket.format(dateTime1) should be("20221103T12:58")

      val dateTime2 = ZonedDateTime.of(2021, 4, 1, 14, 7, 0, 0, ZoneOffset.ofHours(5))
      FirstTimeBucket.format(dateTime2) should be("20210401T09:07")

    }

  }

}
