package lerna.akka.entityreplication.raft.model

private[entityreplication] object ReplicatedLog {

  def apply(): ReplicatedLog = ReplicatedLog(Seq.empty)

  private def apply(entries: Seq[LogEntry]) = new ReplicatedLog(entries)

  sealed trait FindConflictResult extends Serializable with Product
  object FindConflictResult {

    /** Indicates no conflict found */
    case object NoConflict extends FindConflictResult

    /** Indicates a conflict found on the index */
    final case class ConflictFound(conflictIndex: LogEntryIndex, conflictTerm: Term) extends FindConflictResult
  }

}

private[entityreplication] final case class ReplicatedLog private[model] (
    entries: Seq[LogEntry],
    ancestorLastTerm: Term = Term.initial(),
    ancestorLastIndex: LogEntryIndex = LogEntryIndex.initial(),
) {
  import ReplicatedLog._

  def get(index: LogEntryIndex): Option[LogEntry] = {
    val logCollectionIndex = toSeqIndex(index)
    if (entries.size > logCollectionIndex && logCollectionIndex >= 0) Some(entries(logCollectionIndex))
    else None
  }

  def getFrom(nextIndex: LogEntryIndex, maxEntryCount: Int, maxBatchCount: Int): Seq[Seq[LogEntry]] = {
    sliceEntries(from = nextIndex, nextIndex.plus(maxEntryCount * maxBatchCount - 1))
      .sliding(maxEntryCount, maxEntryCount).toSeq
  }

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

  def entriesAfter(index: LogEntryIndex): Iterator[LogEntry] =
    entries.iterator.drop(n = toSeqIndex(index) + 1)

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
      case `ancestorLastIndex` => Option(ancestorLastTerm)
      case logEntryIndex       => get(logEntryIndex).map(_.term)
    }

  /** Return true if the given log with the term and index is at least as up-to-date as this log.
    *
    * Determines which log is more up-to-date by comparing the term and index of the last entry.
    * If two logs have last entries with different terms, the greater term is more up-to-date.
    * If two logs have last entries with the same term, greater index is more up-to-date.
    *
    * Note: Returns true if two logs have last entries with the same term and the same index.
    *
    * @see [[https://github.com/ongardie/dissertation Raft thesis]] section 3.6.1
    */
  def isGivenLogUpToDate(term: Term, index: LogEntryIndex): Boolean = {
    term > lastLogTerm ||
    (term == lastLogTerm && index >= lastLogIndex)
  }

  @deprecated("Use ReplicatedLog.truncatedAndAppend instead", "2.1.1")
  def merge(thatEntries: Seq[LogEntry], prevLogIndex: LogEntryIndex): ReplicatedLog = {
    val newEntries = this.entries.takeWhile(_.index <= prevLogIndex) ++ thatEntries
    copy(newEntries)
  }

  /** Finds the index of the conflict
    *
    * This method returns the first index of conflicting entries between existing entries and the given entries.
    * If there is no conflict, this returns [[FindConflictResult.NoConflict]].
    * An entry is considered to be conflicting if it has the same index but a different term.
    *
    * For example:
    * <pre>
    *  raft log:         [(index=1,term=1), (index=2,term=1), (index=3,term=1), (index=4,term=1)]
    *  given entries:                      [(index=2,term=1), (index=3,term=2), (index=4,term=3)]
    *  conflict: index=3 and term =2
    * </pre>
    *
    * If there is no overlapping index between exising entries and the given entries,
    * this method returns [[FindConflictResult.NoConflict]].
    *
    * @throws IllegalArgumentException
    *   - if the given entries contains the already compacted entries, excluding the last one.
    *   - if the given entries conflict with the last compacted entry.
    *
    * @note
    *  - The index of the given entries MUST be continuously increasing (not checked on this method).
    *  - Returned conflict index should always meet all of the following conditions:
    *     - greater than [[ancestorLastIndex]]
    *     - less than or equal to [[lastLogIndex]]
    */
  def findConflict(thatEntries: Seq[LogEntry]): FindConflictResult = {
    if (thatEntries.isEmpty) {
      FindConflictResult.NoConflict
    } else {
      require(
        thatEntries.head.index >= ancestorLastIndex,
        s"The given entries shouldn't contain compacted entries, excluding the last one " +
        s"(ancestorLastIndex: [$ancestorLastIndex], ancestorLastTerm: [${ancestorLastTerm.term}]), " +
        s"but got entries (indices: [${thatEntries.head.index}..${thatEntries.last.index}]).",
      )
      require(
        thatEntries.head.index != ancestorLastIndex || thatEntries.head.term == ancestorLastTerm,
        s"The given first entry (index: [${thatEntries.head.index}], term: [${thatEntries.head.term.term}]) " +
        s"shouldn't conflict with the last compacted entry (ancestorLastIndex: [$ancestorLastIndex], ancestorLastTerm: [${ancestorLastTerm.term}]).",
      )
      val conflictEntryOption = thatEntries.find(entry => {
        termAt(entry.index).exists(_ != entry.term)
      })
      conflictEntryOption match {
        case Some(conflictEntry) =>
          assert(
            conflictEntry.index > ancestorLastIndex && conflictEntry.index <= lastLogIndex,
            "The given entries should always conflict with the existing entries " +
            s"(ancestorLastIndex: [$ancestorLastIndex], lastLogIndex: [$lastLogIndex]). " +
            s"conflict index: [${conflictEntry.index}], conflict term: [${conflictEntry.term.term}], " +
            s"given entries with indices [${thatEntries.head.index}..${thatEntries.last.index}])",
          )
          FindConflictResult.ConflictFound(conflictEntry.index, conflictEntry.term)
        case None =>
          FindConflictResult.NoConflict
      }
    }
  }

  /** Truncates the exising entries and appends the given entries
    *
    * This method truncates the existing entries with an index greater than or equal to the first index of the given entries.
    * If the given entries are empty, this method truncates no entry.
    *
    * The given entries should start with an index less than or equal to the last index of exising entries plus one.
    * If this requirement breaks, this method throws an [[IllegalArgumentException]] since it will miss some entries.
    *
    * @note The index of the given entries MUST be continuously increasing (not checked on this method).
    */
  def truncateAndAppend(thatEntries: Seq[LogEntry]): ReplicatedLog = {
    if (thatEntries.isEmpty) {
      this
    } else {
      val headIndex = thatEntries.head.index
      require(
        headIndex >= ancestorLastIndex.plus(1) && headIndex <= lastLogIndex.plus(1),
        "Replicated log should not contain a missing entry." +
        s" The head index [$headIndex] of the given entries with indices [${thatEntries.head.index}..${thatEntries.last.index}]" +
        s" should be between ancestorLastIndex([$ancestorLastIndex])+1 and lastLogIndex([$lastLogIndex])+1.",
      )
      val truncatedEntries = sliceEntriesFromHead(headIndex.prev())
      val newEntries       = truncatedEntries ++ thatEntries
      copy(newEntries)
    }
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
