package lerna.akka.entityreplication.raft.model

object ReplicatedLog {

  def apply(): ReplicatedLog = ReplicatedLog(Seq.empty)

  private def apply(entries: Seq[LogEntry]) = new ReplicatedLog(entries)
}

case class ReplicatedLog private[model] (entries: Seq[LogEntry]) {
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

  def lastLogIndex: LogEntryIndex = lastOption.map(_.index).getOrElse(LogEntryIndex.initial())

  def lastLogTerm: Term = lastOption.map(_.term).getOrElse(Term.initial())

  def merge(thatEntries: Seq[LogEntry], prevLogIndex: LogEntryIndex): ReplicatedLog = {
    val newEntries = this.entries.takeWhile(_.index <= prevLogIndex) ++ thatEntries
    copy(newEntries)
  }

  def deleteOldEntries(to: LogEntryIndex, preserveLogSize: Int): ReplicatedLog = {
    require(
      preserveLogSize > 0,
      s"preserveLogSize ($preserveLogSize) must be greater than 0 because ReplicatedLog must keep at least one log entry after add an entry",
    )
    val toLogIndex        = toSeqIndex(to.next())
    val preservedLogIndex = if (entries.size > preserveLogSize) entries.size - preserveLogSize else 0
    val from              = Math.min(toLogIndex, preservedLogIndex)
    copy(entries = entries.slice(from, entries.size))
  }

  private[this] def toSeqIndex(index: LogEntryIndex): Int = {
    val ancestorLastIndex = headIndexOption.map(_.prev()).getOrElse(LogEntryIndex.initial())
    index.underlying - ancestorLastIndex.underlying - 1
  }
}
