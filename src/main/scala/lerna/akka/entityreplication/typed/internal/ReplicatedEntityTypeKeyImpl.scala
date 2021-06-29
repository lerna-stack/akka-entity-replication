package lerna.akka.entityreplication.typed.internal

import lerna.akka.entityreplication.typed.ReplicatedEntityTypeKey

private[entityreplication] final case class ReplicatedEntityTypeKeyImpl[Command](name: String, messageClassName: String)
    extends ReplicatedEntityTypeKey[Command] {
  override def toString: String = s"ReplicatedEntityTypeKey[$messageClassName]($name)"
}
