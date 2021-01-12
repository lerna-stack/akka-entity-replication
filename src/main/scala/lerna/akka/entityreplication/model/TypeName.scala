package lerna.akka.entityreplication.model

import java.net.URLEncoder

object TypeName {
  def from(typeName: String): TypeName = new TypeName(URLEncoder.encode(typeName, "utf-8"))
}

final case class TypeName private (underlying: String) extends AnyVal {
  override def toString: String = underlying
}
