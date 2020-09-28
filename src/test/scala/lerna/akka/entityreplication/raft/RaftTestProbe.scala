package lerna.akka.entityreplication.raft

import lerna.akka.entityreplication.raft.RaftActor._

// テスト用
// FIXME: このクラスに依存しないテストにする
object RaftTestProbe {

  case class SetState(stateName: State, stateData: RaftMemberData)
  case object StateChanged

  case object GetState
  case class RaftState(stateName: State, stateData: RaftMemberData)

  case class Using(data: RaftMemberData) extends NonPersistEventLike

  trait RaftTestProbeSupport extends RaftActorBase {

    override def unhandled(message: Any): Unit =
      message match {
        case SetState(stateName, stateData) =>
          applyDomainEvent(Using(stateData)) { _ =>
            become(stateName)
            sender() ! StateChanged
          }
        case GetState =>
          sender() ! RaftState(currentState, currentData)
        case msg =>
          super.unhandled(msg)
      }

    abstract override def updateState(domainEvent: DomainEvent): RaftMemberData =
      domainEvent match {
        case Using(data) => data
        case _           => super.updateState(domainEvent)
      }
  }
}
