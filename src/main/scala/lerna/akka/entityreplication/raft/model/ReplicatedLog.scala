package lerna.akka.entityreplication.raft.model

object ReplicatedLog {

  def apply(): ReplicatedLog = ReplicatedLog(Seq.empty)

  private def apply(entries: Seq[LogEntry]) = new ReplicatedLog(entries)
}

case class ReplicatedLog private[model] (
    entries: Seq[LogEntry],
    ancestorLastTerm: Term = Term.initial(),
    ancestorLastIndex: LogEntryIndex = LogEntryIndex.initial(),
) {
  def get(index: LogEntryIndex): Option[LogEntry] = {
    val logCollectionIndex = toSeqIndex(index)
    if (entries.size > logCollectionIndex && logCollectionIndex >= 0) Some(entries(logCollectionIndex))
    else None
  }

  def getFrom(nextIndex: LogEntryIndex, maxCount: Int): Seq[LogEntry] =
    sliceEntries(from = nextIndex, nextIndex.plus(maxCount - 1))

  def sliceEntriesFromHead(to: LogEntryIndex): Seq[LogEntry] = {
    headIndexOption match {
      case Some(headIndex) =>
        sliceEntries(from = headIndex, to)
      case None =>
        Seq()
    }
  }

  def sliceEntries(from: LogEntryIndex, to: LogEntryIndex): Seq[LogEntry] = {
    entries.slice(toSeqIndex(from), until = toSeqIndex(to.next()))
  }

  def nonEmpty: Boolean = entries.nonEmpty

  def append(event: EntityEvent, term: Term): ReplicatedLog = {
    val entryIndex = lastLogIndex.next()
    copy(entries :+ LogEntry(entryIndex, event, term))
  }

  def headIndexOption: Option[LogEntryIndex] = entries.headOption.map(_.index)

  def lastIndexOption: Option[LogEntryIndex] = entries.lastOption.map(_.index)

  def last: LogEntry = entries.last

  def lastOption: Option[LogEntry] = entries.lastOption

  def lastLogIndex: LogEntryIndex = lastOption.map(_.index).getOrElse(ancestorLastIndex)

  def lastLogTerm: Term = lastOption.map(_.term).getOrElse(ancestorLastTerm)

  def termAt(logEntryIndex: LogEntryIndex): Option[Term] =
    logEntryIndex match {
      case `ancestorLastIndex`                                           => Option(ancestorLastTerm)
      case initialLogIndex if initialLogIndex == LogEntryIndex.initial() => Option(Term.initial())
      case logEntryIndex                                                 => get(logEntryIndex).map(_.term)
    }

  def merge(thatEntries: Seq[LogEntry], prevLogIndex: LogEntryIndex): ReplicatedLog = {
    val newEntries = this.entries.takeWhile(_.index <= prevLogIndex) ++ thatEntries
    copy(newEntries)
  }

  def deleteOldEntries(to: LogEntryIndex, preserveLogSize: Int): ReplicatedLog = {
    require(
      preserveLogSize > 0,
      s"preserveLogSize ($preserveLogSize) must be greater than 0 because ReplicatedLog must keep at least one log entry after add an entry",
    )
    val toLogIndex             = toSeqIndex(to.next())
    val preservedLogIndex      = if (entries.size > preserveLogSize) entries.size - preserveLogSize else 0
    val from                   = Math.min(toLogIndex, preservedLogIndex)
    val maybeAncestorLastEntry = entries.lift(from - 1)
    val newAncestorLastTerm    = maybeAncestorLastEntry.map(_.term).getOrElse(ancestorLastTerm)
    val newAncestorLastIndex   = maybeAncestorLastEntry.map(_.index).getOrElse(ancestorLastIndex)

    copy(entries = entries.slice(from, entries.size), newAncestorLastTerm, newAncestorLastIndex)
  }

  /**
    * Clear all log entries in memory and update [[ReplicatedLog.lastLogTerm]] and [[ReplicatedLog.lastLogIndex]]
    *
    * @param ancestorLastTerm [[ReplicatedLog.lastLogTerm]] of reset ReplicatedLog
    * @param ancestorLastIndex [[ReplicatedLog.lastLogIndex]] of reset ReplicatedLog
    * @return updated ReplicatedLog
    */
  def reset(ancestorLastTerm: Term, ancestorLastIndex: LogEntryIndex): ReplicatedLog = {
    copy(entries = Seq(), ancestorLastTerm, ancestorLastIndex)
  }

  private[this] def toSeqIndex(index: LogEntryIndex): Int = {
    index.toSeqIndex(offset = ancestorLastIndex)
  }
}
