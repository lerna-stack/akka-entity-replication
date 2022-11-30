package akka.persistence.cassandra.lerna

import akka.actor.ActorSystem
import akka.persistence.PersistentRepr
import akka.persistence.cassandra.Extractors
import akka.persistence.cassandra.journal.CassandraJournal
import akka.persistence.cassandra.lerna.Extractor.TaggedPersistentRepr
import akka.persistence.query.TimeBasedUUID
import akka.serialization.{ Serialization, SerializationExtension }
import com.datastax.oss.driver.api.core.cql.Row

import scala.concurrent.{ ExecutionContext, Future }

object Extractor {
  final case class TaggedPersistentRepr(repr: PersistentRepr, offset: TimeBasedUUID, tags: Set[String])
}

/** Provides extractor from rows to event envelopes
  *
  * Since it depends on some internal APIs of Akka Persistence Cassandra, this class is under namespace
  * `akka.persistence.cassandra`.
  */
final class Extractor(system: ActorSystem) {

  private val eventDeserializer: CassandraJournal.EventDeserializer =
    new CassandraJournal.EventDeserializer(system)
  private val serialization: Serialization =
    SerializationExtension(system)
  private val extractor: Extractors.Extractor[Extractors.TaggedPersistentRepr] =
    Extractors.taggedPersistentRepr(eventDeserializer, serialization)

  /** Returns `TaggedPersistentRepr` extracted from the given row
    *
    * If `async` is true, internal deserialization is executed asynchronously.
    */
  def extract(row: Row, async: Boolean)(implicit executionContext: ExecutionContext): Future[TaggedPersistentRepr] = {
    extractor
      .extract(row, async)
      .map { repr =>
        TaggedPersistentRepr(repr.pr, TimeBasedUUID(repr.offset), repr.tags)
      }
  }

}
