package lerna.akka.entityreplication.raft.model.exception

import lerna.akka.entityreplication.raft.model.LogEntryIndex

final case class SeqIndexOutOfBoundsException(self: LogEntryIndex, offset: LogEntryIndex)
    extends RuntimeException(s"The Seq index of $self from $offset is out of bounds")
