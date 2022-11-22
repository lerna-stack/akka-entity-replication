package lerna.akka.entityreplication.rollback.cassandra

import lerna.akka.entityreplication.rollback.SequenceNr

private object PartitionNr {

  /** Returns a PartitionNr that the given sequence number belongs to
    *
    * @throws java.lang.IllegalArgumentException if the given partition size is less than 1
    */
  def fromSequenceNr(sequenceNr: SequenceNr, partitionSize: Long): PartitionNr = {
    require(partitionSize > 0, s"partitionSize [$partitionSize] should be greater than 0")
    PartitionNr((sequenceNr.value - 1L) / partitionSize)
  }

}

/** The partition number in Akka Persistence Cassandra */
private final case class PartitionNr(value: Long) extends Ordered[PartitionNr] {
  require(value >= 0, s"value [$value] should be greater than or equal to 0")

  /** Returns a PartitionNr whose value is incremented by the given delta
    *
    * @throws java.lang.IllegalArgumentException if the new value will be negative
    */
  def +(delta: Long): PartitionNr = PartitionNr(value + delta)

  /** @inheritdoc */
  override def compare(that: PartitionNr): Int =
    this.value.compare(that.value)

}
