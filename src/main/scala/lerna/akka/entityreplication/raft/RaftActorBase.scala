package lerna.akka.entityreplication.raft

import akka.actor.SupervisorStrategy.Stop
import akka.actor._
import akka.persistence.{ PersistentActor, RecoveryCompleted, SnapshotOffer }
import lerna.akka.entityreplication.raft.PersistentStateData.PersistentState
import lerna.akka.entityreplication.raft.RaftActor._

private[raft] object RaftActorBase {

  object `->` {
    def unapply(in: (State, State)) = Some(in)
  }
}

private[raft] trait RaftActorBase extends PersistentActor with ActorLogging {

  type TransitionHandler = PartialFunction[(State, State), Unit]

  type StateBehaviors = State => Receive

  protected def settings: RaftSettings

  private[this] var _currentState: State = Recovering

  protected def currentState: State = _currentState

  private[this] var _currentData: RaftMemberData = RaftMemberData()

  protected def currentData: RaftMemberData = _currentData

  protected def updateState(domainEvent: DomainEvent): RaftMemberData

  protected val stateBehaviors: StateBehaviors

  protected val onTransition: TransitionHandler

  protected def onRecoveryCompleted(): Unit

  override def supervisorStrategy: SupervisorStrategy =
    OneForOneStrategy()({
      case ex: ActorInitializationException => super.supervisorStrategy.decider(ex)
      case ex: ActorKilledException         => super.supervisorStrategy.decider(ex)
      case ex: DeathPactException           => super.supervisorStrategy.decider(ex)
      case ex: Exception                    => Stop // Entity termination is handled by RaftActor
    })

  final override def receiveRecover: Receive = {
    case SnapshotOffer(_, snapshot: PersistentState) =>
      _currentData = RaftMemberData(snapshot)
    case domainEvent: PersistEvent =>
      _currentData = updateState(domainEvent)
    case RecoveryCompleted =>
      onRecoveryCompleted()
  }

  final override def receiveCommand: Receive = stateBehaviors(currentState)

  // Avoid false positive
  // DomainEvent has only two subtypes: PersistEvent and NonPersistEvent
  @annotation.nowarn("msg=match may not be exhaustive")
  protected def applyDomainEvent[T <: DomainEvent](domainEvent: T)(f: T => Unit): Unit =
    domainEvent match {
      case _: PersistEvent =>
        val startNanoTime = System.nanoTime()
        persist(domainEvent) { event =>
          try {
            val endNanoTime              = System.nanoTime()
            val persistingTimeMillis     = (endNanoTime - startNanoTime) / 1000000
            val electionTimeoutMinMillis = settings.electionTimeoutMin.toMillis
            if (persistingTimeMillis > settings.electionTimeoutMin.toMillis) {
              if (log.isWarningEnabled)
                log.warning(
                  "[{}] persisting time ({} ms) is grater than minimum of election-timeout ({} ms)",
                  currentState,
                  persistingTimeMillis,
                  electionTimeoutMinMillis,
                )
            } else {
              if (log.isDebugEnabled)
                log.debug("=== [{}] persisting time: {} ms ===", currentState, persistingTimeMillis)
            }
            _currentData = updateState(event)
            f(domainEvent)
          } catch {
            case e: Exception =>
              if (log.isErrorEnabled) log.error(e, "persisted event handling failed")
              throw e
          }
        }
      case _: NonPersistEvent =>
        _currentData = updateState(domainEvent)
        f(domainEvent)
    }

  protected def become(state: State): Unit = {
    if (log.isDebugEnabled) log.debug("=== Transition: {} -> {} ===", currentState, state)
    if (onTransition.isDefinedAt((currentState, state))) {
      onTransition((currentState, state))
    }
    _currentState = state
    context.become(stateBehaviors(state))
  }

  protected val `->` : RaftActorBase.`->`.type = RaftActorBase.`->`

}
