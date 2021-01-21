package lerna.akka.entityreplication.raft.model

import lerna.akka.entityreplication.raft.model.exception.SeqIndexOutOfBoundsException

object LogEntryIndex {

  def initial(): LogEntryIndex = LogEntryIndex(0)

  def min(a: LogEntryIndex, b: LogEntryIndex): LogEntryIndex = {
    if (a <= b) a else b
  }
}

case class LogEntryIndex(private[model] val underlying: Long) extends Ordered[LogEntryIndex] {
  require(underlying >= 0)

  def next(): LogEntryIndex = copy(underlying + 1)

  def plus(count: Int): LogEntryIndex = copy(underlying + count)

  def prev(): LogEntryIndex =
    if (underlying > 0) copy(underlying - 1)
    else LogEntryIndex.initial()

  override def compare(that: LogEntryIndex): Int =
    underlying.compareTo(that.underlying)

  override def toString: String = underlying.toString

  def toSeqIndex(offset: LogEntryIndex): Int = {
    val maybeSeqIndex = underlying - offset.underlying - 1
    if (maybeSeqIndex > Int.MaxValue) {
      throw SeqIndexOutOfBoundsException(this, offset)
    } else {
      maybeSeqIndex.toInt
    }
  }
}
