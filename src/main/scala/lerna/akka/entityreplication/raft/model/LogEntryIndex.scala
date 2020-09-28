package lerna.akka.entityreplication.raft.model

object LogEntryIndex {

  def initial() = LogEntryIndex(0)

  def min(a: LogEntryIndex, b: LogEntryIndex): LogEntryIndex = {
    if (a <= b) a else b
  }
}

case class LogEntryIndex(underlying: Int) extends Ordered[LogEntryIndex] {
  require(underlying >= 0)

  def next(): LogEntryIndex = copy(underlying + 1)

  def prev(): LogEntryIndex =
    if (underlying > 0) copy(underlying - 1)
    else LogEntryIndex.initial()

  override def compare(that: LogEntryIndex): Int =
    underlying.compareTo(that.underlying)
}
