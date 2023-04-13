package lerna.akka.entityreplication.rollback.cassandra

import akka.Done
import akka.actor.ActorRef
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.{ Offset, PersistenceQuery }
import akka.stream.scaladsl.Sink
import akka.testkit.TestProbe
import com.typesafe.config.{ Config, ConfigFactory }
import lerna.akka.entityreplication.rollback.SequenceNr
import lerna.akka.entityreplication.rollback.testkit.TestPersistentActor

object CassandraPersistentActorRollbackSpec {

  private val config: Config = ConfigFactory.parseString("""
      |akka.persistence.cassandra.journal.target-partition-size = 10
      |""".stripMargin)

}

final class CassandraPersistentActorRollbackSpec
    extends CassandraSpecBase("CassandraPersistentActorRollbackSpec", CassandraPersistentActorRollbackSpec.config) {

  private val settings =
    CassandraPersistentActorRollbackSettings(system, "akka.persistence.cassandra", dryRun = false)
  private val settingsForDryRun =
    CassandraPersistentActorRollbackSettings(system, "akka.persistence.cassandra", dryRun = true)

  private val queries =
    PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

  "CassandraPersistentActorRollback" should {

    "log an info message if it is in dry-run mode" in {
      implicit val typedSystem: ActorSystem[Nothing] = system.toTyped
      LoggingTestKit
        .info(
          "CassandraPersistentActorRollback (plugin-location = [akka.persistence.cassandra]) is in dry-run mode",
        ).expect {
          new CassandraPersistentActorRollback(system, settingsForDryRun)
        }
    }

  }

  "CassandraPersistentActorRollback.isDryRun" should {

    "return true if this rollback is in dry-run mode" in {
      val rollback = new CassandraPersistentActorRollback(system, settingsForDryRun)
      rollback.isDryRun should be(true)
    }

    "return false if this rollback is not in dry-run mode" in {
      val rollback = new CassandraPersistentActorRollback(system, settings)
      rollback.isDryRun should be(false)
    }

  }

  "CassandraPersistentActorRollback.rollbackTo" should {

    "roll back to the given sequence number" in {
      val rollback      = new CassandraPersistentActorRollback(system, settings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()
      val tagA          = nextUniqueTag()
      val tagB          = nextUniqueTag()

      // Prepare:
      locally {
        val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))

        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(1))
        actor ! TestPersistentActor.PersistTaggedEvent(tagA, probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(2))
        actor ! TestPersistentActor.PersistTaggedEvent(tagB, probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(3))

        actor ! TestPersistentActor.SaveSnapshot(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(3))

        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(4))
        actor ! TestPersistentActor.PersistTaggedEvent(tagA, probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(5))
        actor ! TestPersistentActor.PersistTaggedEvent(tagB, probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(6))

        actor ! TestPersistentActor.SaveSnapshot(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(6))

        actor ! TestPersistentActor.PersistTaggedEvent(tagA, probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(7))
        actor ! TestPersistentActor.PersistTaggedEvent(tagB, probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(8))

        // Prepare: stop the actor for recovery
        probe.watch(actor)
        system.stop(actor)
        probe.expectTerminated(actor)
      }

      // Test prerequisites:
      locally {
        val actor = system.actorOf(TestPersistentActor.props(persistenceId))
        actor ! TestPersistentActor.GetRecoveryTracking(probe.ref)
        probe.expectMsg(
          TestPersistentActor.RecoveryTracking(
            offeredSnapshot = Some(TestPersistentActor.Snapshot(6)),
            replayedEvents = (7 to 8).map(sequenceNr => TestPersistentActor.Event(sequenceNr)).toVector,
          ),
        )
        eventually {
          val eventsOfTagA =
            queries.currentEventsByTag(tagA, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
          eventsOfTagA should be(
            Seq(TestPersistentActor.Event(2), TestPersistentActor.Event(5), TestPersistentActor.Event(7)),
          )
          val eventsOfTagB =
            queries.currentEventsByTag(tagB, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
          eventsOfTagB should be(
            Seq(TestPersistentActor.Event(3), TestPersistentActor.Event(6), TestPersistentActor.Event(8)),
          )
        }

        // Prepare: stop the actor before deletes
        probe.watch(actor)
        system.stop(actor)
        probe.expectTerminated(actor)
      }

      // Test:
      rollback.rollbackTo(persistenceId, SequenceNr(5)).futureValue should be(Done)
      val actor = system.actorOf(TestPersistentActor.props(persistenceId))
      actor ! TestPersistentActor.GetRecoveryTracking(probe.ref)
      probe.expectMsg(
        TestPersistentActor.RecoveryTracking(
          offeredSnapshot = Some(TestPersistentActor.Snapshot(3)),
          replayedEvents = (4 to 5).map(sequenceNr => TestPersistentActor.Event(sequenceNr)).toVector,
        ),
      )
      eventually {
        val eventsOfTagA =
          queries.currentEventsByTag(tagA, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
        eventsOfTagA should be(Seq(TestPersistentActor.Event(2), TestPersistentActor.Event(5)))
        val eventsOfTagB =
          queries.currentEventsByTag(tagB, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
        eventsOfTagB should be(Seq(TestPersistentActor.Event(3)))
      }
    }

    "not roll back to the given sequence number if it is in dry-run mode" in {
      val rollback      = new CassandraPersistentActorRollback(system, settingsForDryRun)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()
      val tagA          = nextUniqueTag()
      val tagB          = nextUniqueTag()

      // Prepare:
      locally {
        val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))

        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(1))
        actor ! TestPersistentActor.PersistTaggedEvent(tagA, probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(2))
        actor ! TestPersistentActor.PersistTaggedEvent(tagB, probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(3))

        actor ! TestPersistentActor.SaveSnapshot(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(3))

        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(4))
        actor ! TestPersistentActor.PersistTaggedEvent(tagA, probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(5))
        actor ! TestPersistentActor.PersistTaggedEvent(tagB, probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(6))

        actor ! TestPersistentActor.SaveSnapshot(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(6))

        actor ! TestPersistentActor.PersistTaggedEvent(tagA, probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(7))
        actor ! TestPersistentActor.PersistTaggedEvent(tagB, probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(8))

        // Prepare: stop the actor for recovery
        probe.watch(actor)
        system.stop(actor)
        probe.expectTerminated(actor)
      }

      // Test prerequisites:
      locally {
        val actor = system.actorOf(TestPersistentActor.props(persistenceId))
        actor ! TestPersistentActor.GetRecoveryTracking(probe.ref)
        probe.expectMsg(
          TestPersistentActor.RecoveryTracking(
            offeredSnapshot = Some(TestPersistentActor.Snapshot(6)),
            replayedEvents = (7 to 8).map(sequenceNr => TestPersistentActor.Event(sequenceNr)).toVector,
          ),
        )
        eventually {
          val eventsOfTagA =
            queries.currentEventsByTag(tagA, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
          eventsOfTagA should be(
            Seq(TestPersistentActor.Event(2), TestPersistentActor.Event(5), TestPersistentActor.Event(7)),
          )
          val eventsOfTagB =
            queries.currentEventsByTag(tagB, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
          eventsOfTagB should be(
            Seq(TestPersistentActor.Event(3), TestPersistentActor.Event(6), TestPersistentActor.Event(8)),
          )
        }

        // Prepare: stop the actor before deletes
        probe.watch(actor)
        system.stop(actor)
        probe.expectTerminated(actor)
      }

      // Test:
      locally {
        implicit val typedSystem: ActorSystem[Nothing] = system.toTyped
        LoggingTestKit
          .info(
            s"dry-run: roll back to sequence_nr [5] for persistenceId [$persistenceId]",
          ).expect {
            rollback.rollbackTo(persistenceId, SequenceNr(5)).futureValue should be(Done)
          }
      }

      // Test: not to roll back (= delete nothing)
      val actor = system.actorOf(TestPersistentActor.props(persistenceId))
      actor ! TestPersistentActor.GetRecoveryTracking(probe.ref)
      probe.expectMsg(
        TestPersistentActor.RecoveryTracking(
          offeredSnapshot = Some(TestPersistentActor.Snapshot(6)),
          replayedEvents = (7 to 8).map(sequenceNr => TestPersistentActor.Event(sequenceNr)).toVector,
        ),
      )
      val eventsOfTagA =
        queries.currentEventsByTag(tagA, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
      eventsOfTagA should be(
        Seq(TestPersistentActor.Event(2), TestPersistentActor.Event(5), TestPersistentActor.Event(7)),
      )
      val eventsOfTagB =
        queries.currentEventsByTag(tagB, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
      eventsOfTagB should be(
        Seq(TestPersistentActor.Event(3), TestPersistentActor.Event(6), TestPersistentActor.Event(8)),
      )
    }

  }

  "CassandraPersistentActorRollback.deleteAll" should {

    "delete all data for the given persistence ID" in {
      val rollback      = new CassandraPersistentActorRollback(system, settings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()
      val tagA          = nextUniqueTag()
      val tagB          = nextUniqueTag()

      // Prepare:
      locally {
        val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))

        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(1))
        actor ! TestPersistentActor.PersistTaggedEvent(tagA, probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(2))
        actor ! TestPersistentActor.PersistTaggedEvent(tagB, probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(3))

        actor ! TestPersistentActor.SaveSnapshot(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(3))

        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(4))
        actor ! TestPersistentActor.PersistTaggedEvent(tagA, probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(5))
        actor ! TestPersistentActor.PersistTaggedEvent(tagB, probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(6))

        actor ! TestPersistentActor.SaveSnapshot(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(6))

        actor ! TestPersistentActor.PersistTaggedEvent(tagA, probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(7))
        actor ! TestPersistentActor.PersistTaggedEvent(tagB, probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(8))

        // Prepare: stop the actor for recovery
        probe.watch(actor)
        system.stop(actor)
        probe.expectTerminated(actor)
      }

      // Test prerequisites:
      locally {
        val actor = system.actorOf(TestPersistentActor.props(persistenceId))
        actor ! TestPersistentActor.GetRecoveryTracking(probe.ref)
        probe.expectMsg(
          TestPersistentActor.RecoveryTracking(
            offeredSnapshot = Some(TestPersistentActor.Snapshot(6)),
            replayedEvents = (7 to 8).map(sequenceNr => TestPersistentActor.Event(sequenceNr)).toVector,
          ),
        )
        eventually {
          val eventsOfTagA =
            queries.currentEventsByTag(tagA, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
          eventsOfTagA should be(
            Seq(TestPersistentActor.Event(2), TestPersistentActor.Event(5), TestPersistentActor.Event(7)),
          )
          val eventsOfTagB =
            queries.currentEventsByTag(tagB, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
          eventsOfTagB should be(
            Seq(TestPersistentActor.Event(3), TestPersistentActor.Event(6), TestPersistentActor.Event(8)),
          )
        }

        // Prepare: stop the actor before deletes
        probe.watch(actor)
        system.stop(actor)
        probe.expectTerminated(actor)
      }

      // Test:
      rollback.deleteAll(persistenceId).futureValue should be(Done)
      val actor = system.actorOf(TestPersistentActor.props(persistenceId))
      actor ! TestPersistentActor.GetRecoveryTracking(probe.ref)
      probe.expectMsg(
        TestPersistentActor.RecoveryTracking(
          offeredSnapshot = None,
          replayedEvents = Vector.empty,
        ),
      )
      queries.currentEventsByTag(tagA, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue should be(empty)
      queries.currentEventsByTag(tagB, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue should be(empty)
    }

    "delete no data for the given persistence ID if it is in dry-run mode" in {
      val rollback      = new CassandraPersistentActorRollback(system, settingsForDryRun)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()
      val tagA          = nextUniqueTag()
      val tagB          = nextUniqueTag()

      // Prepare:
      locally {
        val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))

        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(1))
        actor ! TestPersistentActor.PersistTaggedEvent(tagA, probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(2))
        actor ! TestPersistentActor.PersistTaggedEvent(tagB, probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(3))

        actor ! TestPersistentActor.SaveSnapshot(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(3))

        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(4))
        actor ! TestPersistentActor.PersistTaggedEvent(tagA, probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(5))
        actor ! TestPersistentActor.PersistTaggedEvent(tagB, probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(6))

        actor ! TestPersistentActor.SaveSnapshot(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(6))

        actor ! TestPersistentActor.PersistTaggedEvent(tagA, probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(7))
        actor ! TestPersistentActor.PersistTaggedEvent(tagB, probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(8))

        // Prepare: stop the actor for recovery
        probe.watch(actor)
        system.stop(actor)
        probe.expectTerminated(actor)
      }

      // Test prerequisites:
      locally {
        val actor = system.actorOf(TestPersistentActor.props(persistenceId))
        actor ! TestPersistentActor.GetRecoveryTracking(probe.ref)
        probe.expectMsg(
          TestPersistentActor.RecoveryTracking(
            offeredSnapshot = Some(TestPersistentActor.Snapshot(6)),
            replayedEvents = (7 to 8).map(sequenceNr => TestPersistentActor.Event(sequenceNr)).toVector,
          ),
        )
        eventually {
          val eventsOfTagA =
            queries.currentEventsByTag(tagA, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
          eventsOfTagA should be(
            Seq(TestPersistentActor.Event(2), TestPersistentActor.Event(5), TestPersistentActor.Event(7)),
          )
          val eventsOfTagB =
            queries.currentEventsByTag(tagB, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
          eventsOfTagB should be(
            Seq(TestPersistentActor.Event(3), TestPersistentActor.Event(6), TestPersistentActor.Event(8)),
          )
        }

        // Prepare: stop the actor before deletes
        probe.watch(actor)
        system.stop(actor)
        probe.expectTerminated(actor)
      }

      // Test:
      locally {
        implicit val typedSystem: ActorSystem[Nothing] = system.toTyped
        LoggingTestKit
          .info(
            s"dry-run: delete for persistence_id [$persistenceId]",
          ).expect {
            rollback.deleteAll(persistenceId).futureValue should be(Done)
          }
      }

      // Test: delete nothing
      val actor = system.actorOf(TestPersistentActor.props(persistenceId))
      actor ! TestPersistentActor.GetRecoveryTracking(probe.ref)
      probe.expectMsg(
        TestPersistentActor.RecoveryTracking(
          offeredSnapshot = Some(TestPersistentActor.Snapshot(6)),
          replayedEvents = (7 to 8).map(sequenceNr => TestPersistentActor.Event(sequenceNr)).toVector,
        ),
      )
      val eventsOfTagA =
        queries.currentEventsByTag(tagA, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
      eventsOfTagA should be(
        Seq(TestPersistentActor.Event(2), TestPersistentActor.Event(5), TestPersistentActor.Event(7)),
      )
      val eventsOfTagB =
        queries.currentEventsByTag(tagB, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
      eventsOfTagB should be(
        Seq(TestPersistentActor.Event(3), TestPersistentActor.Event(6), TestPersistentActor.Event(8)),
      )
    }

  }

  "CassandraPersistentActorRollback.deleteEventsFrom" should {

    "delete events whose sequence numbers are greater than or equal to the given sequence number " in {
      val rollback      = new CassandraPersistentActorRollback(system, settings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      locally {
        val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
        for (sequenceNr <- 1 to 15) {
          actor ! TestPersistentActor.PersistEvent(probe.ref)
          probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
        }

        // Test prerequisites:
        rollback.queries.selectHighestSequenceNr(persistenceId, PartitionNr(0)).futureValue should be(
          Some(SequenceNr(10)),
        )
        rollback.queries.selectHighestSequenceNr(persistenceId, PartitionNr(1)).futureValue should be(
          Some(SequenceNr(15)),
        )

        // Prepare: stop the actor before deletes
        probe.watch(actor)
        system.stop(actor)
        probe.expectTerminated(actor)
      }

      // Test:
      rollback.deleteEventsFrom(persistenceId, SequenceNr(12)).futureValue should be(Done)
      val actor = system.actorOf(TestPersistentActor.props(persistenceId))
      actor ! TestPersistentActor.GetRecoveryTracking(probe.ref)
      probe.expectMsg(
        TestPersistentActor.RecoveryTracking(
          offeredSnapshot = None,
          replayedEvents = (1 until 12).map(sequenceNr => TestPersistentActor.Event(sequenceNr)).toVector,
        ),
      )
    }

    "delete events from multiple partitions" in {
      val rollback      = new CassandraPersistentActorRollback(system, settings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      locally {
        val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
        for (sequenceNr <- 1 to 15) {
          actor ! TestPersistentActor.PersistEvent(probe.ref)
          probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
        }

        // Test prerequisites:
        rollback.queries.selectHighestSequenceNr(persistenceId, PartitionNr(0)).futureValue should be(
          Some(SequenceNr(10)),
        )
        rollback.queries.selectHighestSequenceNr(persistenceId, PartitionNr(1)).futureValue should be(
          Some(SequenceNr(15)),
        )

        // Prepare: stop the actor before deletes
        probe.watch(actor)
        system.stop(actor)
        probe.expectTerminated(actor)
      }

      // Test:
      rollback.deleteEventsFrom(persistenceId, SequenceNr(7)).futureValue should be(Done)
      val actor = system.actorOf(TestPersistentActor.props(persistenceId))
      actor ! TestPersistentActor.GetRecoveryTracking(probe.ref)
      probe.expectMsg(
        TestPersistentActor.RecoveryTracking(
          offeredSnapshot = None,
          replayedEvents = (1 until 7).map(sequenceNr => TestPersistentActor.Event(sequenceNr)).toVector,
        ),
      )
    }

    "delete events from multiple partitions if an empty partition exists" in {
      val rollback      = new CassandraPersistentActorRollback(system, settings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      locally {
        val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
        for (sequenceNr <- 1 to 10) {
          actor ! TestPersistentActor.PersistEvent(probe.ref)
          probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
        }
        actor ! TestPersistentActor.PersistEventsAtomically(11, probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(21))

        // Test prerequisites:
        rollback.queries.selectHighestSequenceNr(persistenceId, PartitionNr(0)).futureValue should be(
          Some(SequenceNr(10)),
        )
        rollback.queries.selectHighestSequenceNr(persistenceId, PartitionNr(1)).futureValue should be(None)
        rollback.queries.selectHighestSequenceNr(persistenceId, PartitionNr(2)).futureValue should be(
          Some(SequenceNr(21)),
        )
        rollback.queries.selectHighestSequenceNr(persistenceId, PartitionNr(3)).futureValue should be(None)
        rollback.queries.selectHighestSequenceNr(persistenceId, PartitionNr(4)).futureValue should be(None)

        // Prepare: stop the actor before deletes
        probe.watch(actor)
        system.stop(actor)
        probe.expectTerminated(actor)
      }

      // Test:
      rollback.deleteEventsFrom(persistenceId, SequenceNr(8)).futureValue should be(Done)
      val actor = system.actorOf(TestPersistentActor.props(persistenceId))
      actor ! TestPersistentActor.GetRecoveryTracking(probe.ref)
      probe.expectMsg(
        TestPersistentActor.RecoveryTracking(
          offeredSnapshot = None,
          replayedEvents = (1 until 8).map(sequenceNr => TestPersistentActor.Event(sequenceNr)).toVector,
        ),
      )
    }

    "delete no events if the given sequence number is greater than the highest sequence number" in {
      val rollback      = new CassandraPersistentActorRollback(system, settings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      locally {
        val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
        for (sequenceNr <- 1 to 15) {
          actor ! TestPersistentActor.PersistEvent(probe.ref)
          probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
        }

        // Test prerequisites:
        rollback.queries.selectHighestSequenceNr(persistenceId, PartitionNr(0)).futureValue should be(
          Some(SequenceNr(10)),
        )
        rollback.queries.selectHighestSequenceNr(persistenceId, PartitionNr(1)).futureValue should be(
          Some(SequenceNr(15)),
        )

        // Prepare: stop the actor before deletes
        probe.watch(actor)
        system.stop(actor)
        probe.expectTerminated(actor)
      }

      // Test:
      rollback.deleteEventsFrom(persistenceId, SequenceNr(16)).futureValue should be(Done)
      val actor = system.actorOf(TestPersistentActor.props(persistenceId))
      actor ! TestPersistentActor.GetRecoveryTracking(probe.ref)
      probe.expectMsg(
        TestPersistentActor.RecoveryTracking(
          offeredSnapshot = None,
          replayedEvents = (1 to 15).map(sequenceNr => TestPersistentActor.Event(sequenceNr)).toVector,
        ),
      )
    }

    "throw an IllegalArgumentException if the given sequence number is equal to 1" in {
      val rollback      = new CassandraPersistentActorRollback(system, settings)
      val persistenceId = nextPersistenceId()
      val exception = intercept[IllegalArgumentException] {
        rollback.deleteEventsFrom(persistenceId, SequenceNr(1))
      }
      exception.getMessage should be("requirement failed: from [1] should be greater than 1")
    }

    "delete no events and log an info message if it is in dry-run mode" in {
      val rollback      = new CassandraPersistentActorRollback(system, settingsForDryRun)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      locally {
        val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
        for (sequenceNr <- 1 to 15) {
          actor ! TestPersistentActor.PersistEvent(probe.ref)
          probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
        }

        // Test prerequisites:
        rollback.queries.selectHighestSequenceNr(persistenceId, PartitionNr(0)).futureValue should be(
          Some(SequenceNr(10)),
        )
        rollback.queries.selectHighestSequenceNr(persistenceId, PartitionNr(1)).futureValue should be(
          Some(SequenceNr(15)),
        )

        // Prepare: stop the actor before deletes
        probe.watch(actor)
        system.stop(actor)
        probe.expectTerminated(actor)
      }

      // Test:
      implicit val typedSystem: ActorSystem[Nothing] = system.toTyped
      LoggingTestKit
        .info(
          s"dry-run: delete events (partition_nr=[1], sequence_nr >= [13]) for persistenceId [$persistenceId]",
        ).expect {
          rollback.deleteEventsFrom(persistenceId, SequenceNr(13)).futureValue should be(Done)
        }

      // Test: delete no events
      val actor = system.actorOf(TestPersistentActor.props(persistenceId))
      actor ! TestPersistentActor.GetRecoveryTracking(probe.ref)
      probe.expectMsg(
        TestPersistentActor.RecoveryTracking(
          offeredSnapshot = None,
          replayedEvents = (1 to 15).map(sequenceNr => TestPersistentActor.Event(sequenceNr)).toVector,
        ),
      )
    }

  }

  "CassandraPersistentActorRollback.deleteSnapshotsFrom" should {

    "delete snapshots whose sequence numbers are greater than or equal to the given sequence number" in {
      val rollback      = new CassandraPersistentActorRollback(system, settings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      locally {
        val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
        for (sequenceNr <- 1 to 5) {
          actor ! TestPersistentActor.PersistEvent(probe.ref)
          probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
        }
        actor ! TestPersistentActor.SaveSnapshot(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(5))
        for (sequenceNr <- 6 to 10) {
          actor ! TestPersistentActor.PersistEvent(probe.ref)
          probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
        }
        actor ! TestPersistentActor.SaveSnapshot(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(10))
        for (sequenceNr <- 11 to 15) {
          actor ! TestPersistentActor.PersistEvent(probe.ref)
          probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
        }
        actor ! TestPersistentActor.SaveSnapshot(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(15))
        for (sequenceNr <- 16 to 19) {
          actor ! TestPersistentActor.PersistEvent(probe.ref)
          probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
        }

        // Prepare: stop the actor for recovery
        probe.watch(actor)
        system.stop(actor)
        probe.expectTerminated(actor)
      }

      // Test prerequisites:
      locally {
        val actor = system.actorOf(TestPersistentActor.props(persistenceId))
        actor ! TestPersistentActor.GetRecoveryTracking(probe.ref)
        probe.expectMsg(
          TestPersistentActor.RecoveryTracking(
            offeredSnapshot = Some(TestPersistentActor.Snapshot(15)),
            replayedEvents = (16 to 19).map(sequenceNr => TestPersistentActor.Event(sequenceNr)).toVector,
          ),
        )

        // Prepare: stop the actor before deletes
        probe.watch(actor)
        system.stop(actor)
        probe.expectTerminated(actor)
      }

      // Test:
      rollback.deleteSnapshotsFrom(persistenceId, SequenceNr(10)).futureValue should be(Done)
      val actor = system.actorOf(TestPersistentActor.props(persistenceId))
      actor ! TestPersistentActor.GetRecoveryTracking(probe.ref)
      probe.expectMsg(
        TestPersistentActor.RecoveryTracking(
          offeredSnapshot = Some(TestPersistentActor.Snapshot(5)),
          replayedEvents = (6 to 19).map(sequenceNr => TestPersistentActor.Event(sequenceNr)).toVector,
        ),
      )
    }

    "delete no snapshots if the given sequence number is greater than the sequence number of the latest snapshot" in {
      val rollback      = new CassandraPersistentActorRollback(system, settings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      locally {
        val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
        for (sequenceNr <- 1 to 5) {
          actor ! TestPersistentActor.PersistEvent(probe.ref)
          probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
        }
        actor ! TestPersistentActor.SaveSnapshot(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(5))
        for (sequenceNr <- 6 to 10) {
          actor ! TestPersistentActor.PersistEvent(probe.ref)
          probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
        }
        actor ! TestPersistentActor.SaveSnapshot(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(10))
        for (sequenceNr <- 11 to 14) {
          actor ! TestPersistentActor.PersistEvent(probe.ref)
          probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
        }

        // Prepare: stop the actor for deletes
        probe.watch(actor)
        system.stop(actor)
        probe.expectTerminated(actor)
      }

      // Test:
      rollback.deleteSnapshotsFrom(persistenceId, SequenceNr(11)).futureValue should be(Done)
      val actor = system.actorOf(TestPersistentActor.props(persistenceId))
      actor ! TestPersistentActor.GetRecoveryTracking(probe.ref)
      probe.expectMsg(
        TestPersistentActor.RecoveryTracking(
          offeredSnapshot = Some(TestPersistentActor.Snapshot(10)),
          replayedEvents = (11 to 14).map(sequenceNr => TestPersistentActor.Event(sequenceNr)).toVector,
        ),
      )
    }

    "throw an IllegalArgumentException if the given sequence number is equal to 1" in {
      val rollback      = new CassandraPersistentActorRollback(system, settings)
      val persistenceId = nextPersistenceId()
      val exception = intercept[IllegalArgumentException] {
        rollback.deleteSnapshotsFrom(persistenceId, SequenceNr(1))
      }
      exception.getMessage should be("requirement failed: from [1] should be greater than 1")
    }

    "delete no snapshots and log an info message if it is in dry-run mode" in {
      val rollback      = new CassandraPersistentActorRollback(system, settingsForDryRun)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      locally {
        val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
        for (sequenceNr <- 1 to 5) {
          actor ! TestPersistentActor.PersistEvent(probe.ref)
          probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
        }
        actor ! TestPersistentActor.SaveSnapshot(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(5))
        for (sequenceNr <- 6 to 10) {
          actor ! TestPersistentActor.PersistEvent(probe.ref)
          probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
        }
        actor ! TestPersistentActor.SaveSnapshot(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(10))
        for (sequenceNr <- 11 to 14) {
          actor ! TestPersistentActor.PersistEvent(probe.ref)
          probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
        }

        // Prepare: stop the actor for deletes
        probe.watch(actor)
        system.stop(actor)
        probe.expectTerminated(actor)
      }

      // Test:
      implicit val typedSystem: ActorSystem[Nothing] = system.toTyped
      LoggingTestKit
        .info(
          s"dry-run: delete snapshots (sequence_nr >= [7]) for persistence_id [$persistenceId]",
        ).expect {
          rollback.deleteSnapshotsFrom(persistenceId, SequenceNr(7)).futureValue should be(Done)
        }

      // Test: delete no snapshots
      val actor = system.actorOf(TestPersistentActor.props(persistenceId))
      actor ! TestPersistentActor.GetRecoveryTracking(probe.ref)
      probe.expectMsg(
        TestPersistentActor.RecoveryTracking(
          offeredSnapshot = Some(TestPersistentActor.Snapshot(10)),
          replayedEvents = (11 to 14).map(sequenceNr => TestPersistentActor.Event(sequenceNr)).toVector,
        ),
      )
    }

  }

  "CassandraPersistentActorRollback.deleteTagView" should {

    "delete tag views for the given persistence ID" in {
      val rollback      = new CassandraPersistentActorRollback(system, settings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()
      val tagA          = nextUniqueTag()
      val tagB          = nextUniqueTag()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      actor ! TestPersistentActor.PersistEvent(probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(1))
      actor ! TestPersistentActor.PersistTaggedEvent(tagA, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(2))
      actor ! TestPersistentActor.PersistTaggedEvent(tagB, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(3))
      actor ! TestPersistentActor.PersistTaggedEvent(tagA, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(4))
      actor ! TestPersistentActor.PersistTaggedEvent(tagB, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(5))

      // Test prerequisites:
      eventually {
        val eventsOfTagA =
          queries.currentEventsByTag(tagA, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
        eventsOfTagA should be(Seq(TestPersistentActor.Event(2), TestPersistentActor.Event(4)))
        val eventsOfTagB =
          queries.currentEventsByTag(tagB, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
        eventsOfTagB should be(Seq(TestPersistentActor.Event(3), TestPersistentActor.Event(5)))
      }

      // Prepare: stop the actor before deletes
      probe.watch(actor)
      system.stop(actor)
      probe.expectTerminated(actor)

      // Test:
      rollback.deleteTagView(persistenceId, SequenceNr(3)).futureValue should be(Done)
      val eventsOfTagA =
        queries.currentEventsByTag(tagA, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
      eventsOfTagA should be(Seq(TestPersistentActor.Event(2)))
      val eventsOfTagB =
        queries.currentEventsByTag(tagB, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
      eventsOfTagB should be(empty)
    }

    "delete no tag views and log an info message if it is in dry-run mode" in {
      val rollback      = new CassandraPersistentActorRollback(system, settingsForDryRun)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()
      val tagA          = nextUniqueTag()
      val tagB          = nextUniqueTag()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      actor ! TestPersistentActor.PersistEvent(probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(1))
      actor ! TestPersistentActor.PersistTaggedEvent(tagA, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(2))
      actor ! TestPersistentActor.PersistTaggedEvent(tagB, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(3))
      actor ! TestPersistentActor.PersistTaggedEvent(tagA, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(4))
      actor ! TestPersistentActor.PersistTaggedEvent(tagB, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(5))

      // Test prerequisites:
      eventually {
        val eventsOfTagA =
          queries.currentEventsByTag(tagA, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
        eventsOfTagA should be(Seq(TestPersistentActor.Event(2), TestPersistentActor.Event(4)))
        val eventsOfTagB =
          queries.currentEventsByTag(tagB, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
        eventsOfTagB should be(Seq(TestPersistentActor.Event(3), TestPersistentActor.Event(5)))
      }

      // Prepare: stop the actor before deletes
      probe.watch(actor)
      system.stop(actor)
      probe.expectTerminated(actor)

      // Test:
      locally {
        implicit val typedSystem: ActorSystem[Nothing] = system.toTyped
        LoggingTestKit
          .info(
            s"dry-run: delete tag_view(s) [Set($tagA, $tagB)] after sequence_nr [3] for persistence_id [$persistenceId]",
          ).expect {
            rollback.deleteTagView(persistenceId, SequenceNr(3)).futureValue should be(Done)
          }
      }

      // Test: delete no tag views
      val eventsOfTagA =
        queries.currentEventsByTag(tagA, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
      eventsOfTagA should be(Seq(TestPersistentActor.Event(2), TestPersistentActor.Event(4)))
      val eventsOfTagB =
        queries.currentEventsByTag(tagB, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
      eventsOfTagB should be(Seq(TestPersistentActor.Event(3), TestPersistentActor.Event(5)))
    }

    "delete no tag views for persistence IDs other than the given" in {
      val rollback            = new CassandraPersistentActorRollback(system, settings)
      val probe               = TestProbe()
      val targetPersistenceId = nextPersistenceId()
      val otherPersistenceId  = nextPersistenceId()
      val tagA                = nextUniqueTag()
      val tagB                = nextUniqueTag()

      def currentEventsByTagAndPersistenceId(tag: String, persistenceId: String): Seq[Any] = {
        queries
          .currentEventsByTag(tag, Offset.noOffset)
          .filter(_.persistenceId == persistenceId)
          .map(_.event)
          .runWith(Sink.seq)
          .futureValue
      }

      // Prepare:
      val targetActor: ActorRef = system.actorOf(TestPersistentActor.props(targetPersistenceId))
      targetActor ! TestPersistentActor.PersistEvent(probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(1))
      targetActor ! TestPersistentActor.PersistTaggedEvent(tagA, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(2))
      targetActor ! TestPersistentActor.PersistTaggedEvent(tagB, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(3))
      targetActor ! TestPersistentActor.PersistTaggedEvent(tagA, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(4))
      targetActor ! TestPersistentActor.PersistTaggedEvent(tagB, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(5))

      val otherActor: ActorRef = system.actorOf(TestPersistentActor.props(otherPersistenceId))
      otherActor ! TestPersistentActor.PersistEvent(probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(1))
      otherActor ! TestPersistentActor.PersistTaggedEvent(tagB, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(2))
      otherActor ! TestPersistentActor.PersistTaggedEvent(tagA, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(3))
      otherActor ! TestPersistentActor.PersistTaggedEvent(tagB, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(4))
      otherActor ! TestPersistentActor.PersistTaggedEvent(tagA, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(5))

      // Test prerequisites:
      eventually {
        currentEventsByTagAndPersistenceId(tagA, targetPersistenceId) should be(
          Seq(TestPersistentActor.Event(2), TestPersistentActor.Event(4)),
        )
        currentEventsByTagAndPersistenceId(tagB, targetPersistenceId) should be(
          Seq(TestPersistentActor.Event(3), TestPersistentActor.Event(5)),
        )
        currentEventsByTagAndPersistenceId(tagA, otherPersistenceId) should be(
          Seq(TestPersistentActor.Event(3), TestPersistentActor.Event(5)),
        )
        currentEventsByTagAndPersistenceId(tagB, otherPersistenceId) should be(
          Seq(TestPersistentActor.Event(2), TestPersistentActor.Event(4)),
        )
      }

      // Prepare: stop the actors before deletes
      probe.watch(targetActor)
      system.stop(targetActor)
      probe.expectTerminated(targetActor)

      probe.watch(otherActor)
      system.stop(otherActor)
      probe.expectTerminated(otherActor)

      // Test:
      rollback.deleteTagView(targetPersistenceId, SequenceNr(3)).futureValue should be(Done)

      // Test: Tagged events with the target persistence ID is deleted.
      currentEventsByTagAndPersistenceId(tagA, targetPersistenceId) should be(Seq(TestPersistentActor.Event(2)))
      currentEventsByTagAndPersistenceId(tagB, targetPersistenceId) should be(empty)
      // Test: Tagged events with the other persistence ID is not deleted.
      currentEventsByTagAndPersistenceId(tagA, otherPersistenceId) should be(
        Seq(TestPersistentActor.Event(3), TestPersistentActor.Event(5)),
      )
      currentEventsByTagAndPersistenceId(tagB, otherPersistenceId) should be(
        Seq(TestPersistentActor.Event(2), TestPersistentActor.Event(4)),
      )
    }

  }

  "CassandraPersistentActorRollback.rebuildTagView" should {

    "rebuild tag views for the given persistence ID" in {
      val rollback      = new CassandraPersistentActorRollback(system, settings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()
      val tagA          = nextUniqueTag()
      val tagB          = nextUniqueTag()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      actor ! TestPersistentActor.PersistEvent(probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(1))
      actor ! TestPersistentActor.PersistTaggedEvent(tagA, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(2))
      actor ! TestPersistentActor.PersistTaggedEvent(tagB, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(3))
      actor ! TestPersistentActor.PersistTaggedEvent(tagA, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(4))
      actor ! TestPersistentActor.PersistTaggedEvent(tagB, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(5))

      // Test prerequisites:
      eventually {
        val eventsOfTagA =
          queries.currentEventsByTag(tagA, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
        eventsOfTagA should be(Seq(TestPersistentActor.Event(2), TestPersistentActor.Event(4)))
        val eventsOfTagB =
          queries.currentEventsByTag(tagB, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
        eventsOfTagB should be(Seq(TestPersistentActor.Event(3), TestPersistentActor.Event(5)))
      }

      // Prepare: stop the actor before deletes
      probe.watch(actor)
      system.stop(actor)
      probe.expectTerminated(actor)

      // Prepare: delete tag views
      rollback.deleteTagView(persistenceId, SequenceNr(3)).futureValue should be(Done)

      // Test prerequisites:
      locally {
        val eventsOfTagA =
          queries.currentEventsByTag(tagA, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
        eventsOfTagA should be(Seq(TestPersistentActor.Event(2)))
        val eventsOfTagB =
          queries.currentEventsByTag(tagB, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
        eventsOfTagB should be(empty)
      }

      // Test:
      rollback.rebuildTagView(persistenceId).futureValue should be(Done)
      val eventsOfTagA =
        queries.currentEventsByTag(tagA, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
      eventsOfTagA should be(Seq(TestPersistentActor.Event(2), TestPersistentActor.Event(4)))
      val eventsOfTagB =
        queries.currentEventsByTag(tagB, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
      eventsOfTagB should be(Seq(TestPersistentActor.Event(3), TestPersistentActor.Event(5)))
    }

    "rebuild no tag views and log an info message if it is in dry-run mode" in {
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()
      val tagA          = nextUniqueTag()
      val tagB          = nextUniqueTag()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      actor ! TestPersistentActor.PersistEvent(probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(1))
      actor ! TestPersistentActor.PersistTaggedEvent(tagA, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(2))
      actor ! TestPersistentActor.PersistTaggedEvent(tagB, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(3))
      actor ! TestPersistentActor.PersistTaggedEvent(tagA, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(4))
      actor ! TestPersistentActor.PersistTaggedEvent(tagB, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(5))

      // Test prerequisites:
      eventually {
        val eventsOfTagA =
          queries.currentEventsByTag(tagA, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
        eventsOfTagA should be(Seq(TestPersistentActor.Event(2), TestPersistentActor.Event(4)))
        val eventsOfTagB =
          queries.currentEventsByTag(tagB, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
        eventsOfTagB should be(Seq(TestPersistentActor.Event(3), TestPersistentActor.Event(5)))
      }

      // Prepare: stop the actor before deletes
      probe.watch(actor)
      system.stop(actor)
      probe.expectTerminated(actor)

      // Prepare: delete tag views
      locally {
        val rollback = new CassandraPersistentActorRollback(system, settings)
        rollback.deleteTagView(persistenceId, SequenceNr(3)).futureValue should be(Done)

        // Test prerequisites:
        val eventsOfTagA =
          queries.currentEventsByTag(tagA, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
        eventsOfTagA should be(Seq(TestPersistentActor.Event(2)))
        val eventsOfTagB =
          queries.currentEventsByTag(tagB, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
        eventsOfTagB should be(empty)
      }

      // Test:
      val rollback = new CassandraPersistentActorRollback(system, settingsForDryRun)
      locally {
        implicit val typedSystem: ActorSystem[Nothing] = system.toTyped
        LoggingTestKit
          .info(
            s"dry-run: rebuild tag_view for persistence_id [$persistenceId]",
          ).expect {
            rollback.rebuildTagView(persistenceId).futureValue should be(Done)
          }
      }

      // Test: rebuild no tag views
      val eventsOfTagA =
        queries.currentEventsByTag(tagA, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
      eventsOfTagA should be(Seq(TestPersistentActor.Event(2)))
      val eventsOfTagB =
        queries.currentEventsByTag(tagB, Offset.noOffset).map(_.event).runWith(Sink.seq).futureValue
      eventsOfTagB should be(empty)
    }

  }

}
