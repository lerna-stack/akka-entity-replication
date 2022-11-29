package lerna.akka.entityreplication.raft.model

private[entityreplication] object LogEntry {

  def apply(index: LogEntryIndex, event: EntityEvent, term: Term) =
    new LogEntry(index, event, term)
}

private[entityreplication] class LogEntry(val index: LogEntryIndex, val event: EntityEvent, val term: Term)
    extends Serializable {
  require(index > LogEntryIndex.initial())

  def canEqual(other: Any): Boolean = other.isInstanceOf[LogEntry]

  override def equals(other: Any): Boolean =
    other match {
      case that: LogEntry =>
        (that canEqual this) &&
        index == that.index &&
        term == that.term
      case _ => false
    }

  override def hashCode(): Int = {
    val state = Seq(index, term)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"LogEntry($index, $event, $term)"
}
