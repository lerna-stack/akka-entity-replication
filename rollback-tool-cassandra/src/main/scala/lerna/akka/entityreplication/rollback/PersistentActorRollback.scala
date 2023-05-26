package lerna.akka.entityreplication.rollback

import akka.Done

import scala.concurrent.Future

/** Provides rolling back the persistent actor to the specific sequence number
  *
  * Note that actors related to the persistence ID should not run simultaneously while this tool is running. If there
  * is an event subscriber, compensation might be needed depending on the content of the events. While the tools doesn't
  * provide such compensation, tool users might have to conduct such compensation.
  */
private trait PersistentActorRollback {
  import PersistentActorRollback._

  /** Returns `true` if this rollback is running in dry-run mode, `false` otherwise */
  def isDryRun: Boolean

  /** Returns [[PersistenceQueries]] this rollback uses */
  def persistenceQueries: PersistenceQueries

  /** Finds rollback requirements for the persistent actor
    *
    * If any rollback is impossible, this method returns a failed `Future` containing a [[RollbackRequirementsNotFound]].
    */
  def findRollbackRequirements(persistenceId: String): Future[RollbackRequirements]

  /** Rolls back the persistent actor to the given sequence number
    *
    * This method doesn't verify that the rollback is actually possible. Use [[findRollbackRequirements]] to confirm that.
    *
    * Since restrictions depends on concrete implementations, see documents of concrete implementation to use.
    */
  def rollbackTo(persistenceId: String, to: SequenceNr): Future[Done]

  /** Delete all data for the persistent Actor
    *
    * Since restrictions depends on concrete implementations, see documents of concrete implementation to use.
    */
  def deleteAll(persistenceId: String): Future[Done]

}

private object PersistentActorRollback {

  /** Rollback requirements for the persistent actor with `persistenceId`
    *
    * The persistent actor can be rolled back to a sequence number greater than or equal to `lowestSequenceNr`.
    */
  final case class RollbackRequirements(
      persistenceId: String,
      lowestSequenceNr: SequenceNr,
  )

}
