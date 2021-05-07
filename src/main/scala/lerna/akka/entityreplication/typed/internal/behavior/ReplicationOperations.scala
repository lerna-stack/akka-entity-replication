package lerna.akka.entityreplication.typed.internal.behavior

import akka.actor.{ PoisonPill, UnhandledMessage }
import akka.actor.typed.Behavior
import akka.actor.typed.eventstream.EventStream
import akka.actor.typed.scaladsl.{ ActorContext, Behaviors, StashBuffer }
import lerna.akka.entityreplication.ReplicationRegion.Passivate
import lerna.akka.entityreplication.raft.RaftProtocol.{ EntityCommand, Snapshot, TakeSnapshot }
import lerna.akka.entityreplication.raft.snapshot.SnapshotProtocol.EntityState
import lerna.akka.entityreplication.typed.internal.effect._
import akka.actor.typed.scaladsl.adapter._
import lerna.akka.entityreplication.raft.RaftProtocol

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
      command: RaftProtocol.ProcessCommand,
      effects: immutable.Seq[SideEffect[State]],
      stashBuffer: StashBuffer[EntityCommand],
      entityState: State,
      behavior: Behavior[EntityCommand],
  ): Behavior[EntityCommand] = {
    val appliedBehavior =
      effects.foldLeft(behavior)((b, effect) => applySideEffect(command, effect, stashBuffer, entityState, b))
    appliedBehavior
  }

  def applySideEffect(
      command: RaftProtocol.ProcessCommand,
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

      case _: UnhandledEffect[_] =>
        val unhandledMessage =
          UnhandledMessage(command.command, setup.context.system.toClassic.deadLetters, setup.context.self.toClassic)
        setup.context.system.eventStream ! EventStream.Publish(unhandledMessage)
        behavior

      case _ =>
        throw new IllegalArgumentException(s"Unsupported side effect found: $effect")
    }
}
