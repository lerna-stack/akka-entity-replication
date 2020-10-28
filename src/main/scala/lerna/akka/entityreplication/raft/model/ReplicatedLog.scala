package lerna.akka.entityreplication.raft.model

object ReplicatedLog {

  def apply(): ReplicatedLog = ReplicatedLog(Seq.empty)

  private def apply(entries: Seq[LogEntry]) = new ReplicatedLog(entries)
}

case class ReplicatedLog private[model] (
    entries: Seq[LogEntry],
    ancestorLastIndex: LogEntryIndex = LogEntryIndex.initial(),
    ancestorLastTerm: Term = Term.initial(),
) {
  def get(index: LogEntryIndex): Option[LogEntry] = {
    val logCollectionIndex = toSeqIndex(index)
    if (entries.size > logCollectionIndex && logCollectionIndex >= 0) Some(entries(logCollectionIndex))
    else None
  }

  def getAllFrom(nextIndex: LogEntryIndex): Seq[LogEntry] =
    entries.takeRight(entries.size - toSeqIndex(nextIndex))

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
    val entryIndex = lastIndexOption.getOrElse(ancestorLastIndex).next()
    copy(entries :+ LogEntry(entryIndex, event, term))
  }

  def headIndexOption: Option[LogEntryIndex] = entries.headOption.map(_.index)

  def lastIndexOption: Option[LogEntryIndex] = entries.lastOption.map(_.index)

  def last: LogEntry = entries.last

  def lastOption: Option[LogEntry] = entries.lastOption

  def lastLogIndex: LogEntryIndex = lastOption.map(_.index).getOrElse(ancestorLastIndex)

  def lastLogTerm: Term = lastOption.map(_.term).getOrElse(ancestorLastTerm)

  def merge(thatEntries: Seq[LogEntry], prevLogIndex: LogEntryIndex): ReplicatedLog = {
    val newEntries = this.entries.takeWhile(_.index <= prevLogIndex) ++ thatEntries
    copy(newEntries)
  }

  def deleteOldEntries(to: LogEntryIndex, preserveLogSize: Int): ReplicatedLog = {
    val toLogIndex        = toSeqIndex(to.next())
    val preservedLogIndex = if (entries.size > preserveLogSize) entries.size - preserveLogSize else 0
    val from              = Math.min(toLogIndex, preservedLogIndex)
    val newEntries        = entries.slice(from, entries.size)
    val headEntryOption   = newEntries.headOption
    val headLogIndex      = headEntryOption.map(_.index.prev()).getOrElse(ancestorLastIndex)
    val headLogTerm       = headEntryOption.map(_.term.prev()).getOrElse(ancestorLastTerm)
    copy(entries = newEntries, ancestorLastIndex = headLogIndex, ancestorLastTerm = headLogTerm)
  }

  private[this] def toSeqIndex(index: LogEntryIndex): Int = {
    index.underlying - ancestorLastIndex.underlying - 1
  }
}
