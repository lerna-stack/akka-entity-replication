package lerna.akka.entityreplication.model

import java.net.URLEncoder

private[entityreplication] object TypeName {
  def from(typeName: String): TypeName = new TypeName(URLEncoder.encode(typeName, "utf-8"))
}

private[entityreplication] final class TypeName private (val underlying: String) extends AnyVal {
  override def toString: String = underlying
}
