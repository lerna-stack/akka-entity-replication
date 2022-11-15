package lerna.akka.entityreplication.rollback.setup

/** Rollback setup for the persistent actor
  *
  * The persistent actor (id = `persistenceId`) will be rolled back to the sequence number (`toSequenceNr`).
  * If `toSequenceNr` is equal to `0`, all data for the persistent actor will be deleted.
  */
final case class RollbackSetup(persistenceId: String, toSequenceNr: Long) {
  require(toSequenceNr >= 0, s"toSequenceNr [$toSequenceNr] should be greater than or equal to 0")
}
