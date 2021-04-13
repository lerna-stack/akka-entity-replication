package lerna.akka.entityreplication.typed.internal.behavior

private[entityreplication] object InstanceId {
  def initial(): InstanceId = InstanceId(1)
}

private[entityreplication] case class InstanceId(underlying: Int) extends AnyVal {
  def next(): InstanceId = copy(underlying + 1)
}
