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

  /** Returns `true` if this rollback is running in dry-run mode, `false` otherwise */
  def isDryRun: Boolean

  /** Returns [[PersistenceQueries]] this rollback uses */
  def persistenceQueries: PersistenceQueries

  /** Rolls back the persistent actor to the given sequence number
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
