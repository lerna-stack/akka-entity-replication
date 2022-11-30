package lerna.akka.entityreplication.rollback.testkit

import akka.actor.{ ActorSystem, ClassicActorSystemProvider, PoisonPill }
import akka.testkit.TestProbe

import java.util.UUID
import scala.concurrent.duration.{ DurationInt, FiniteDuration }

object PersistenceInitializationAwaiter {
  def apply(systemProvider: ClassicActorSystemProvider): PersistenceInitializationAwaiter = {
    new PersistenceInitializationAwaiter(systemProvider)
  }
}

final class PersistenceInitializationAwaiter private (systemProvider: ClassicActorSystemProvider) {
  implicit val system: ActorSystem = systemProvider.classicSystem

  /** Waits for the initialization completions of the default journal plugin and snapshot plugin */
  def awaitInit(
      max: FiniteDuration = 60.seconds,
      interval: FiniteDuration = 3.seconds,
  ): Unit = {
    val probe = TestProbe()
    probe.within(max) {
      probe.awaitAssert(
        {
          val persistenceId = getClass.getSimpleName + UUID.randomUUID().toString
          val persistentActor =
            system.actorOf(TestPersistentActor.props(persistenceId))
          probe.watch(persistentActor)
          persistentActor ! TestPersistentActor.PersistEvent(probe.ref)
          persistentActor ! TestPersistentActor.SaveSnapshot(probe.ref)
          try {
            probe.expectMsgType[TestPersistentActor.Ack]
            probe.expectMsgType[TestPersistentActor.Ack]
          } finally {
            persistentActor ! PoisonPill
            probe.expectTerminated(persistentActor)
          }
        },
        probe.remainingOrDefault,
        interval,
      )
    }
  }

}
