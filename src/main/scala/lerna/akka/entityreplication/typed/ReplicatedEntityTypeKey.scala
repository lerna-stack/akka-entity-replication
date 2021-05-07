package lerna.akka.entityreplication.typed

import lerna.akka.entityreplication.typed.internal.ReplicatedEntityTypeKeyImpl

import scala.reflect.ClassTag

object ReplicatedEntityTypeKey {

  /**
    * Creates an [[ReplicatedEntityTypeKey]]. The `name` must be unique.
    */
  def apply[M](name: String)(implicit mTag: ClassTag[M]): ReplicatedEntityTypeKey[M] =
    ReplicatedEntityTypeKeyImpl(name, mTag.runtimeClass.getName)
}

/**
  * The key of an entity type, the `name` must be unique.
  */
trait ReplicatedEntityTypeKey[-M] {

  def name: String
}
