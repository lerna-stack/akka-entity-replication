package lerna.akka.entityreplication.raft.model

private[entityreplication] object Term {
  def initial() = new Term(0)
}

private[entityreplication] final case class Term(term: Long) extends Ordered[Term] {
  def next(): Term                      = this.copy(term = term + 1)
  def isOlderThan(other: Term): Boolean = this.term < other.term
  def isNewerThan(other: Term): Boolean = this.term > other.term

  override def compare(that: Term): Int =
    term.compareTo(that.term)
}
