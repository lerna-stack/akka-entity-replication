package lerna.akka.entityreplication.typed

import scala.reflect.ClassTag

object ReplicatedEntityTypeKey {

  /**
    * Creates an [[ReplicatedEntityTypeKey]]. The `name` must be unique.
    */
  def apply[M](name: String)(implicit mTag: ClassTag[M]): ReplicatedEntityTypeKey[M] = ???
}

/**
  * The key of an entity type, the `name` must be unique.
  */
trait ReplicatedEntityTypeKey[-M] {

  def name: String
}
