package lerna.akka.entityreplication.rollback.cassandra.testkit

import java.time.{ LocalDateTime, ZoneOffset, ZonedDateTime }
import java.time.format.DateTimeFormatter

object FirstTimeBucket {

  // see https://docs.oracle.com/javase/jp/8/docs/api/java/time/format/DateTimeFormatter.html
  private val formatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyyMMdd'T'HH:mm")

  /** Returns a formatted string for first-time-bucket of Akka Persistence Cassandra
    *
    * The given time is converted to UTC timezone and then will be formatted.
    *
    * @see https://doc.akka.io/docs/akka-persistence-cassandra/1.0.5/events-by-tag.html#first-time-bucket
    */
  def format(time: ZonedDateTime): String = {
    val utcTime = LocalDateTime.ofInstant(time.toInstant, ZoneOffset.UTC)
    utcTime.format(formatter)
  }

}
