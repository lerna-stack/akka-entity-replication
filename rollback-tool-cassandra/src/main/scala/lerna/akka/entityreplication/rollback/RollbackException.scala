package lerna.akka.entityreplication.rollback

/** Exception thrown when a rollback-related operation fails */
class RollbackException private[rollback] (message: String) extends RuntimeException(message)

/** Exception thrown when rollback requirements are not found */
private class RollbackRequirementsNotFound(message: String)
    extends RollbackException(s"Rollback requirements not found: $message")

/** Exception thrown when a rollback request doesn't fulfill rollback requirements */
private class RollbackRequirementsNotFulfilled(message: String)
    extends RollbackException(s"Rollback requirements not fulfilled: $message")

/** Exception thrown when a rollback timestamp hint is not found */
private class RollbackTimestampHintNotFound(message: String)
    extends RollbackException(s"Rollback timestamp hint not found: $message")
