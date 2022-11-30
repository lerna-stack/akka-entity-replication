package lerna.akka.entityreplication.rollback

import akka.actor.{ ActorSystem, ClassicActorSystemProvider }
import akka.persistence.query.TimeBasedUUID
import akka.stream.scaladsl.{ Sink, Source }
import com.datastax.oss.driver.api.core.uuid.Uuids
import lerna.akka.entityreplication.rollback.PersistenceQueries.TaggedEventEnvelope

import java.time.Instant
import scala.concurrent.Future

/** @inheritdoc
  *
  * This strategy searches for the highest sequence number by iterating events in the descending order of the sequence
  * number. While this strategy can work if there is a clock out-of-sync, it requires a linear search. It is suitable
  * for searching the sequence number for near-past timestamps.
  */
private final class LinearSequenceNrSearchStrategy(
    systemProvider: ClassicActorSystemProvider,
    queries: PersistenceQueries,
) extends SequenceNrSearchStrategy {

  private implicit val system: ActorSystem =
    systemProvider.classicSystem

  import system.dispatcher

  /** @inheritdoc */
  override def findUpperBound(persistenceId: String, timestamp: Instant): Future[Option[SequenceNr]] = {
    val targetTimestampMillis = timestamp.toEpochMilli
    val currentEventsBefore = Source.futureSource(for {
      highestSequenceNr <- queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(1))
    } yield {
      highestSequenceNr.fold(Source.empty[TaggedEventEnvelope])(queries.currentEventsBefore(persistenceId, _))
    })
    currentEventsBefore
      .dropWhile { envelope =>
        envelope.offset match {
          case TimeBasedUUID(uuid) =>
            val eventTimestampInMillis = Uuids.unixTimestamp(uuid)
            eventTimestampInMillis > targetTimestampMillis
          case _ =>
            throw new IllegalStateException("event offset should always be TimeBasedUUID")
        }
      }
      .map(_.sequenceNr)
      .runWith(Sink.headOption)
  }

}
