package lerna.akka.entityreplication.rollback

/** The sequence number in Akka Persistence */
private final case class SequenceNr(value: Long) extends Ordered[SequenceNr] {
  require(value > 0, s"value [$value] should be greater than 0")

  /** Returns a SequenceNr whose value is incremented by the given delta
    *
    * @throws java.lang.IllegalArgumentException if the new value will be zero or negative
    */
  def +(delta: Long): SequenceNr = SequenceNr(value + delta)

  /** @inheritdoc */
  override def compare(that: SequenceNr): Int =
    this.value.compare(that.value)

}
