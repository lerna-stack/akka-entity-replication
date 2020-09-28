package lerna.akka.entityreplication.raft.model

object Term {
  def initial() = new Term(0)
}

case class Term(term: Long) extends Ordered[Term] {
  def prev(): Term                      = this.copy(term = term - 1)
  def next(): Term                      = this.copy(term = term + 1)
  def isOlderThan(other: Term): Boolean = this.term < other.term
  def isNewerThan(other: Term): Boolean = this.term > other.term

  override def compare(that: Term): Int =
    term.compareTo(that.term)
}
