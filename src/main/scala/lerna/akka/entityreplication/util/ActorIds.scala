package lerna.akka.entityreplication.util

private[entityreplication] object ActorIds {

  private[this] val delimiter = ":"

  def actorName(urlEncodedElements: String*): String = validateAndCreate(urlEncodedElements: _*)

  def persistenceId(urlEncodedElements: String*): String = validateAndCreate(urlEncodedElements: _*)

  private[this] def validateAndCreate(urlEncodedElements: String*): String = {
    val invalidElements = urlEncodedElements.zipWithIndex.filter { case (e, _) => e.contains(delimiter) }
    // Not URL encoded values induce ID duplication
    require(
      invalidElements.isEmpty,
      s"Not URL encoded value found: ${invalidElements.map { case (e, i) => s"(${i + 1}: $e)" }.mkString(", ")}",
    )
    String.join(delimiter, urlEncodedElements: _*)
  }
}
