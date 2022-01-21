package lerna.akka.entityreplication

import akka.actor.NoSerializationVerificationNeeded
import akka.actor.typed.scaladsl.adapter.TypedActorRefOps
import akka.actor.typed.scaladsl.{ ActorContext, Behaviors }
import akka.actor.typed.{ ActorRef, Behavior }
import akka.cluster.sharding.ShardRegion
import akka.{ actor => classic }
import lerna.akka.entityreplication.raft.RaftSettings

import scala.concurrent.duration.DurationInt

/** Triggers all specified Raft actor starts.
  *
  * This actor trigger a start at a constant rate.
  * After this actor triggers all starts, it will stop itself.
  */
private[entityreplication] object ReplicationRegionRaftActorStarter {

  private sealed trait Command extends NoSerializationVerificationNeeded

  /** Triggers a batch of Raft actor starts. */
  private case object StartBatch extends Command

  /** Re-sends starts that hasn't ACKed yet. */
  private case object ResendUnAckedStartEntity extends Command

  /** Informs that starting the entity with the given EntityId triggered. */
  private final case class ClassicStartEntityAck(entityId: ShardRegion.EntityId) extends Command

  def apply(
      shardRegion: classic.ActorRef,
      ids: Set[ShardRegion.EntityId],
      settings: RaftSettings,
  ): Behavior[Nothing] = {
    Behaviors
      .setup[Command] { context =>
        val startEntityAckAdapter =
          context.messageAdapter[ShardRegion.StartEntityAck](ack => ClassicStartEntityAck(ack.entityId))
        Behaviors.withTimers { timers =>
          timers.startTimerWithFixedDelay(StartBatch, 0.milli, settings.raftActorAutoStartFrequency)
          timers.startTimerWithFixedDelay(ResendUnAckedStartEntity, settings.raftActorAutoStartRetryInterval)
          val starter = new ReplicationRegionRaftActorStarter(
            context,
            shardRegion,
            startEntityAckAdapter,
            settings.raftActorAutoStartNumberOfActors,
          )
          starter.behavior(ids, Set.empty)
        }
      }.narrow[Nothing]
  }

}

private[entityreplication] final class ReplicationRegionRaftActorStarter private (
    context: ActorContext[ReplicationRegionRaftActorStarter.Command],
    shardRegion: classic.ActorRef,
    startEntityAckAdapter: ActorRef[ShardRegion.StartEntityAck],
    batchSize: Int,
) {
  import ReplicationRegionRaftActorStarter._

  private def behavior(
      remainingIds: Set[ShardRegion.EntityId],
      ackWaitingIds: Set[ShardRegion.EntityId],
  ): Behavior[Command] = {
    val isDone = remainingIds.isEmpty && ackWaitingIds.isEmpty
    if (isDone) {
      Behaviors.stopped { () =>
        context.log.info("Triggered starting all Raft actors on Shard Region [{}]", shardRegion)
        context.log.info("Stops itself [{}]", context.self)
      }
    } else {
      Behaviors.receiveMessage {
        case StartBatch =>
          val (newBatch, newRemainingIds) = remainingIds.splitAt(batchSize)
          val newAckWaitingIds            = ackWaitingIds.union(newBatch)
          if (newBatch.nonEmpty) {
            newBatch.foreach(sendStartEntity)
            context.log.info("Sent StartEntity(s) to trigger [{}] Raft actors starting.", newBatch.size)
          }
          behavior(newRemainingIds, newAckWaitingIds)
        case ClassicStartEntityAck(entityId) =>
          context.log.debug("Received StartEntityAck [{}].", entityId)
          val newAckWaitingIds = ackWaitingIds - entityId
          behavior(remainingIds, newAckWaitingIds)
        case ResendUnAckedStartEntity =>
          if (ackWaitingIds.nonEmpty) {
            context.log.info(
              "Found [{}] Raft actors waiting for StartEntityAck. Resends StartEntity for those actors.",
              ackWaitingIds.size,
            )
            ackWaitingIds.foreach(sendStartEntity)
          }
          Behaviors.same
      }
    }
  }

  /** Sends StartEntity to the Shard Region.
    *
    * This actor will receives StartEntityAck from the Shard Region.
    */
  private def sendStartEntity(entityId: ShardRegion.EntityId): Unit = {
    val message = ShardRegion.StartEntity(entityId)
    shardRegion.tell(message, startEntityAckAdapter.toClassic)
  }

}
