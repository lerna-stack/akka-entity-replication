import akka.Done
import akka.actor.typed.{ ActorRef, ActorSystem }
import akka.cluster.typed.{ ClusterSingleton, SingletonActor }
import akka.persistence.query.Offset
import akka.projection.eventsourced.EventEnvelope
import akka.projection.scaladsl.ExactlyOnceProjection
import akka.projection.slick.{ SlickHandler, SlickProjection }
import akka.projection.{ ProjectionBehavior, ProjectionId }
import slick.basic.DatabaseConfig
import slick.dbio.DBIO
import slick.jdbc.JdbcProfile

sealed trait Event
final case class Deposited(amount: Int)  extends Event
final case class Withdrawed(amount: Int) extends Event

trait StatisticsActions {
  def insertWithdrawalRecord(amount: Int): DBIO[Done]
  def insertDepositRecord(amount: Int): DBIO[Done]
}

class EventHandler(actions: StatisticsActions) extends SlickHandler[EventEnvelope[Event]] {
  override def process(envelope: EventEnvelope[Event]): DBIO[Done] = {
    envelope.event match {
      case Deposited(amount) =>
        actions.insertDepositRecord(amount)
      case Withdrawed(amount) =>
        actions.insertWithdrawalRecord(amount)
    }
  }
}

import lerna.akka.entityreplication.raft.eventsourced.EntityReplicationEventSource

object EventHandler {
  def start(
      actions: StatisticsActions,
      databaseConfig: DatabaseConfig[JdbcProfile],
  )(implicit
      system: ActorSystem[_],
  ): ActorRef[ProjectionBehavior.Command] = {
    def generateProjection(): ExactlyOnceProjection[Offset, EventEnvelope[Event]] =
      SlickProjection.exactlyOnce(
        projectionId = ProjectionId(name = "BankAccount", key = "aggregate"),
        sourceProvider = EntityReplicationEventSource.sourceProvider,
        databaseConfig = databaseConfig,
        handler = () => new EventHandler(actions),
      )

    val projection = generateProjection()
    ClusterSingleton(system).init(SingletonActor(ProjectionBehavior(projection), projection.projectionId.id))
  }
}
