package lerna.akka.entityreplication.typed.internal.behavior

import akka.actor.PoisonPill
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{ ActorContext, Behaviors, StashBuffer }
import lerna.akka.entityreplication.ReplicationRegion.Passivate
import lerna.akka.entityreplication.raft.RaftProtocol.{ EntityCommand, Snapshot, TakeSnapshot }
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol.EntityState
import lerna.akka.entityreplication.typed.internal.effect._

import scala.collection.immutable

private[entityreplication] trait ReplicationOperations[Command, Event, State] {

  protected def setup: BehaviorSetup[Command, Event, State]

  protected def context: ActorContext[EntityCommand] = setup.context

  def receiveTakeSnapshot(command: TakeSnapshot, entityState: State): Behavior[EntityCommand] = {
    val TakeSnapshot(metadata, replyTo) = command
    replyTo ! Snapshot(metadata, EntityState(entityState))
    Behaviors.same
  }

  def applySideEffects(
      effects: immutable.Seq[SideEffect[State]],
      stashBuffer: StashBuffer[EntityCommand],
      entityState: State,
      behavior: Behavior[EntityCommand],
  ): Behavior[EntityCommand] = {
    val appliedBehavior =
      effects.foldLeft(behavior)((b, effect) => applySideEffect(effect, stashBuffer, entityState, b))
    appliedBehavior
  }

  def applySideEffect(
      effect: SideEffect[State],
      stashBuffer: StashBuffer[EntityCommand],
      entityState: State,
      behavior: Behavior[EntityCommand],
  ): Behavior[EntityCommand] =
    effect match {
      case _: PassivateEffect[_] =>
        setup.shard ! Passivate(context.self.path, setup.stopMessage.getOrElse(PoisonPill))
        behavior

      case _: StopLocallyEffect[_] =>
        Behaviors.stopped

      case _: UnstashAllEffect[_] =>
        stashBuffer.unstashAll(behavior)
        behavior

      case callback: Callback[State] =>
        callback.sideEffect(entityState)
        behavior

      case _ =>
        throw new IllegalArgumentException(s"Unsupported side effect found: $effect")
    }
}
