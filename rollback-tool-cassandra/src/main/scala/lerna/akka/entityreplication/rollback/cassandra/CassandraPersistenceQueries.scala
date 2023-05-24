package lerna.akka.entityreplication.rollback.cassandra

import akka.NotUsed
import akka.actor.{ ActorSystem, ClassicActorSystemProvider }
import akka.persistence.cassandra.lerna.Extractor
import akka.stream.alpakka.cassandra.scaladsl.{ CassandraSession, CassandraSessionRegistry, CassandraSource }
import akka.stream.scaladsl.Source
import com.datastax.oss.driver.api.core.cql.{ PreparedStatement, Row, Statement }
import lerna.akka.entityreplication.rollback.PersistenceQueries.TaggedEventEnvelope
import lerna.akka.entityreplication.rollback.{ PersistenceQueries, SequenceNr }

import scala.concurrent.Future

/** @inheritdoc */
private final class CassandraPersistenceQueries(
    systemProvider: ClassicActorSystemProvider,
    val settings: CassandraPersistenceQueriesSettings,
) extends PersistenceQueries {

  private implicit val system: ActorSystem =
    systemProvider.classicSystem

  private val journalSettings: CassandraJournalSettings =
    settings.resolveJournalSettings(system)
  private val querySettings: CassandraQuerySettings =
    settings.resolveQuerySettings(system)

  private val extractor: Extractor =
    new Extractor(system)

  private lazy val session: CassandraSession =
    CassandraSessionRegistry(system).sessionFor(settings.pluginLocation)
  private lazy val statements: CassandraPersistenceQueriesStatements =
    new CassandraPersistenceQueriesStatements(system, settings)
  private lazy val selectHighestSequenceNrPreparedStatement: Future[PreparedStatement] =
    session.prepare(statements.selectHighestSequenceNr)
  private lazy val selectMessagesFromAscPreparedStatement: Future[PreparedStatement] =
    session.prepare(statements.selectMessagesFromAsc)
  private lazy val selectMessagesFromDescPreparedStatement: Future[PreparedStatement] =
    session.prepare(statements.selectMessagesFromDesc)
  private lazy val selectDeletedToPreparedStatement: Future[PreparedStatement] =
    session.prepare(statements.selectDeletedTo)

  import system.dispatcher

  /** @inheritdoc
    *
    * Since `akka.persistence.AtomicWrite` can skip at most one entire partition, the partition gap (an empty partition
    * exists between non-empty partitions) could occur. This method can handle such a partition gap.
    */
  override def findHighestSequenceNrAfter(
      persistenceId: String,
      from: SequenceNr,
  ): Future[Option[SequenceNr]] = {
    val fromPartitionNr =
      PartitionNr.fromSequenceNr(from, journalSettings.targetPartitionSize)
    findHighestSequenceNr(persistenceId, fromPartitionNr)
      .map(_.filter(_ >= from))
  }

  /** Finds the highest sequence number from the given partition or above
    *
    * If there are no events whose partition numbers are greater than or equal to the given partition, this method
    * returns `Future.successful(None)`.
    *
    * Since `akka.persistence.AtomicWrite` can skip at most one entire partition, the partition gap (an empty partition
    * exists between non-empty partitions) could occur. This method can handle such a partition gap.
    *
    * @see [[findHighestPartitionNr]]
    */
  def findHighestSequenceNr(
      persistenceId: String,
      from: PartitionNr,
  ): Future[Option[SequenceNr]] = {
    val allowableConsecutiveEmptyPartitionCount: Int = 1
    def find(
        currentPartitionNr: PartitionNr,
        lastHighestSequenceNr: Option[SequenceNr],
        consecutiveEmptyPartitionCount: Int,
    ): Future[Option[SequenceNr]] = {
      selectHighestSequenceNr(persistenceId, currentPartitionNr)
        .flatMap {
          case Some(highestSequenceNr) =>
            assert(highestSequenceNr.value > 0)
            find(currentPartitionNr + 1, Some(highestSequenceNr), 0)
          case None =>
            if (consecutiveEmptyPartitionCount < allowableConsecutiveEmptyPartitionCount) {
              find(currentPartitionNr + 1, lastHighestSequenceNr, consecutiveEmptyPartitionCount + 1)
            } else {
              Future.successful(lastHighestSequenceNr)
            }
        }
    }
    selectDeletedToSequenceNr(persistenceId).flatMap {
      case Some(deletedToSequenceNr) =>
        val possibleFromPartitionNr =
          PartitionNr.fromSequenceNr(deletedToSequenceNr + 1, journalSettings.targetPartitionSize)
        if (from <= possibleFromPartitionNr) {
          find(possibleFromPartitionNr, Some(deletedToSequenceNr), 0)
        } else {
          find(from, None, 0)
        }
      case None =>
        find(from, None, 0)
    }
  }

  /** Returns the highest sequence number of the given partition
    *
    * If the given partition is empty (there are no events on the partition), this method returns
    * `Future.successful(None)`.
    */
  def selectHighestSequenceNr(
      persistenceId: String,
      partitionNr: PartitionNr,
  ): Future[Option[SequenceNr]] = {
    selectHighestSequenceNrPreparedStatement.flatMap { ps =>
      val bs = ps.bind(persistenceId, partitionNr.value)
      selectOne(bs)
        .map { rowOption =>
          rowOption
            .map(_.getLong("sequence_nr"))
        }.flatMap {
          case None | Some(0) =>
            Future.successful(None)
          case Some(highestSequenceNr) =>
            assert(highestSequenceNr > 0)
            Future.successful(Some(SequenceNr(highestSequenceNr)))
        }
    }
  }

  /** Finds the highest partition number from the given partition or above
    *
    * If there are no events whose partition numbers are greater than or equal to the given partition, this method
    * returns `Future.successful(None)`.
    *
    * Since `akka.persistence.AtomicWrite` can skip at most one entire partition, the partition gap (an empty partition
    * exists between non-empty partitions) could occur. This method can handle such a partition gap.
    *
    * @see [[findHighestSequenceNr]]
    */
  def findHighestPartitionNr(
      persistenceId: String,
      from: PartitionNr,
  ): Future[Option[PartitionNr]] = {
    findHighestSequenceNr(persistenceId, from)
      .map { sequenceNrOption =>
        sequenceNrOption.map { sequenceNr =>
          PartitionNr.fromSequenceNr(sequenceNr, journalSettings.targetPartitionSize)
        }
      }
  }

  /** @inheritdoc */
  override def currentEventsAfter(
      persistenceId: String,
      from: SequenceNr,
  ): Source[PersistenceQueries.TaggedEventEnvelope, NotUsed] = {
    val fromPartitionNr = PartitionNr.fromSequenceNr(from, journalSettings.targetPartitionSize)
    val futureSource = for {
      toPartitionNr <- findHighestPartitionNr(persistenceId, fromPartitionNr).map(_.getOrElse(fromPartitionNr))
    } yield {
      val sources =
        for (partitionNr <- fromPartitionNr.value to toPartitionNr.value) yield {
          currentEventsAfterOnPartition(persistenceId, from, PartitionNr(partitionNr))
        }
      sources.fold(Source.empty)(_.concat(_))
    }
    Source.futureSource(futureSource).mapMaterializedValue(_ => NotUsed)
  }

  /** Returns a `Source` that emits current events (after the given sequence number inclusive) of the given partition
    *
    * The source will emit events in ascending order of sequence number.
    */
  def currentEventsAfterOnPartition(
      persistenceId: String,
      from: SequenceNr,
      partitionNr: PartitionNr,
  ): Source[PersistenceQueries.TaggedEventEnvelope, NotUsed] = {
    val futureSource = selectMessagesFromAscPreparedStatement.map { ps =>
      val bs = ps.bind(persistenceId, partitionNr.value, from.value)
      source(bs)
    }
    Source.futureSource(futureSource).mapMaterializedValue(_ => NotUsed)
  }

  /** @inheritdoc */
  override def currentEventsBefore(
      persistenceId: String,
      from: SequenceNr,
  ): Source[PersistenceQueries.TaggedEventEnvelope, NotUsed] = {
    val fromPartitionNr = PartitionNr.fromSequenceNr(from, journalSettings.targetPartitionSize)
    val futureSource = selectDeletedToSequenceNr(persistenceId).map { deletedToOption =>
      val toPartitionNr = deletedToOption.fold(PartitionNr(0)) { deletedTo =>
        PartitionNr.fromSequenceNr(deletedTo + 1, journalSettings.targetPartitionSize)
      }
      val sources =
        for (partitionNr <- fromPartitionNr.value + 1 to toPartitionNr.value by -1) yield {
          currentEventsBeforeOnPartition(persistenceId, from, PartitionNr(partitionNr))
        }
      sources
        .fold(Source.empty)(_.concat(_))
        .mapMaterializedValue(_ => NotUsed)
    }
    Source.futureSource(futureSource).mapMaterializedValue(_ => NotUsed)
  }

  /** Returns a `Source` that emits current events (before the given sequence number inclusive) of the given partition
    *
    * The source will emit events in descending order of sequence number.
    */
  def currentEventsBeforeOnPartition(
      persistenceId: String,
      from: SequenceNr,
      partitionNr: PartitionNr,
  ): Source[PersistenceQueries.TaggedEventEnvelope, NotUsed] = {
    val futureSource = selectMessagesFromDescPreparedStatement.map { ps =>
      val bs = ps.bind(persistenceId, partitionNr.value, from.value)
      source(bs)
    }
    Source.futureSource(futureSource).mapMaterializedValue(_ => NotUsed)
  }

  /** Returns the highest deleted sequence number (called `deleted_to`)
    *
    * All events with sequence sequence numbers less than or equal to `deleted_to` have been deleted.
    *
    * If no events have been deleted, this method returns `Future.successful(None)`.
    */
  def selectDeletedToSequenceNr(persistenceId: String): Future[Option[SequenceNr]] = {
    selectDeletedToPreparedStatement.flatMap { ps =>
      val bs = ps.bind(persistenceId)
      selectOne(bs)
        .map { rowOption =>
          rowOption.map(_.getLong("deleted_to")) match {
            case None | Some(0) =>
              None
            case Some(deletedToSequenceNr) =>
              assert(deletedToSequenceNr > 0)
              Some(SequenceNr(deletedToSequenceNr))
          }
        }
    }
  }

  private def selectOne[T <: Statement[T]](statement: Statement[T]): Future[Option[Row]] = {
    session.selectOne(statement.setExecutionProfileName(journalSettings.readProfile))
  }

  private def source[T <: Statement[T]](statement: Statement[T]): Source[TaggedEventEnvelope, NotUsed] = {
    CassandraSource(
      statement
        .setExecutionProfileName(querySettings.readProfile)
        .setPageSize(querySettings.maxBufferSize),
    )(session)
      .mapAsync(querySettings.deserializationParallelism) { row =>
        val async = querySettings.deserializationParallelism > 1
        extractor.extract(row, async)
      }.map { repr =>
        TaggedEventEnvelope(
          repr.repr.persistenceId,
          SequenceNr(repr.repr.sequenceNr),
          repr.repr.payload,
          repr.offset,
          repr.tags,
          repr.repr.writerUuid,
        )
      }
  }

}
