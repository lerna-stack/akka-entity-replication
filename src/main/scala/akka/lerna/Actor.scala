package akka.lerna

/** Proxy for using `def aroundPreStart` and `def aroundReceive` from [[lerna]] package
  */
trait Actor extends akka.actor.Actor {
  override def aroundPreStart(): Unit = super.aroundPreStart()

  override def aroundReceive(receive: Receive, msg: Any): Unit = super.aroundReceive(receive, msg)
}
