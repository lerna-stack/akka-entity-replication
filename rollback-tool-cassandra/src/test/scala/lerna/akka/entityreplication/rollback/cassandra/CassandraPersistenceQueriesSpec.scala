package lerna.akka.entityreplication.rollback.cassandra

import akka.actor.ActorRef
import akka.persistence.query.TimeBasedUUID
import akka.stream.scaladsl.Sink
import akka.testkit.TestProbe
import com.typesafe.config.{ Config, ConfigFactory }
import lerna.akka.entityreplication.rollback.SequenceNr
import lerna.akka.entityreplication.rollback.testkit.TestPersistentActor

object CassandraPersistenceQueriesSpec {

  private val config: Config = ConfigFactory.parseString("""
      |akka.persistence.cassandra.journal.target-partition-size = 10
      |""".stripMargin)

}

final class CassandraPersistenceQueriesSpec
    extends CassandraSpecBase("CassandraPersistenceQueriesSpec", CassandraPersistenceQueriesSpec.config) {

  private val defaultSettings: CassandraPersistenceQueriesSettings =
    new CassandraPersistenceQueriesSettings("akka.persistence.cassandra")

  "CassandraPersistenceQueries.findHighestSequenceNrAfter" should {

    "find the highest sequence number after the given sequence number inclusive" in {
      val queries       = new CassandraPersistenceQueries(system, defaultSettings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      for (sequenceNr <- 1 to 15) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }

      // Test prerequisites:
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(0)).futureValue should be(Some(SequenceNr(10)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(1)).futureValue should be(Some(SequenceNr(15)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(2)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(3)).futureValue should be(None)

      // Test:
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(1)).futureValue should be(Some(SequenceNr(15)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(2)).futureValue should be(Some(SequenceNr(15)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(3)).futureValue should be(Some(SequenceNr(15)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(9)).futureValue should be(Some(SequenceNr(15)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(10)).futureValue should be(Some(SequenceNr(15)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(11)).futureValue should be(Some(SequenceNr(15)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(14)).futureValue should be(Some(SequenceNr(15)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(15)).futureValue should be(Some(SequenceNr(15)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(16)).futureValue should be(None)
    }

    "find the highest sequence number after the given sequence number inclusive if an empty partition exists" in {
      val queries       = new CassandraPersistenceQueries(system, defaultSettings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      for (sequenceNr <- 1 to 10) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }
      actor ! TestPersistentActor.PersistEventsAtomically(11, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(21))

      // Test prerequisites:
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(0)).futureValue should be(Some(SequenceNr(10)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(1)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(2)).futureValue should be(Some(SequenceNr(21)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(3)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(4)).futureValue should be(None)

      // Test:
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(1)).futureValue should be(Some(SequenceNr(21)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(2)).futureValue should be(Some(SequenceNr(21)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(3)).futureValue should be(Some(SequenceNr(21)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(9)).futureValue should be(Some(SequenceNr(21)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(10)).futureValue should be(Some(SequenceNr(21)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(11)).futureValue should be(Some(SequenceNr(21)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(19)).futureValue should be(Some(SequenceNr(21)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(20)).futureValue should be(Some(SequenceNr(21)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(21)).futureValue should be(Some(SequenceNr(21)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(22)).futureValue should be(None)
    }

    "find the highest sequence number after the given sequence number if all events are deleted" in {
      val queries       = new CassandraPersistenceQueries(system, defaultSettings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      for (sequenceNr <- 1 to 15) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }
      actor ! TestPersistentActor.SaveSnapshot(probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(15))
      actor ! TestPersistentActor.DeleteEventsTo(15, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(15))

      // Test prerequisites:
      queries.selectDeletedToSequenceNr(persistenceId).futureValue should be(Some(SequenceNr(15)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(0)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(1)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(2)).futureValue should be(None)

      // Test:
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(1)).futureValue should be(Some(SequenceNr(15)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(10)).futureValue should be(Some(SequenceNr(15)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(11)).futureValue should be(Some(SequenceNr(15)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(14)).futureValue should be(Some(SequenceNr(15)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(15)).futureValue should be(Some(SequenceNr(15)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(16)).futureValue should be(None)
    }

    "find the highest sequence number after the given sequence number if old events are deleted" in {
      val queries       = new CassandraPersistenceQueries(system, defaultSettings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      for (sequenceNr <- 1 to 25) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }
      actor ! TestPersistentActor.SaveSnapshot(probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(25))
      actor ! TestPersistentActor.DeleteEventsTo(22, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(22))

      // Test prerequisites:
      queries.selectDeletedToSequenceNr(persistenceId).futureValue should be(Some(SequenceNr(22)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(0)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(1)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(2)).futureValue should be(Some(SequenceNr(25)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(3)).futureValue should be(None)

      // Test:
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(1)).futureValue should be(Some(SequenceNr(25)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(10)).futureValue should be(Some(SequenceNr(25)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(11)).futureValue should be(Some(SequenceNr(25)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(20)).futureValue should be(Some(SequenceNr(25)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(21)).futureValue should be(Some(SequenceNr(25)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(22)).futureValue should be(Some(SequenceNr(25)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(23)).futureValue should be(Some(SequenceNr(25)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(24)).futureValue should be(Some(SequenceNr(25)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(25)).futureValue should be(Some(SequenceNr(25)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(26)).futureValue should be(None)
    }

    "find the highest sequence number after the given sequence number " +
    "if old events are deleted, and an empty partition exists" in {
      val queries       = new CassandraPersistenceQueries(system, defaultSettings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      for (sequenceNr <- 1 to 30) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }
      actor ! TestPersistentActor.SaveSnapshot(probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(30))
      actor ! TestPersistentActor.DeleteEventsTo(30, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(30))
      actor ! TestPersistentActor.PersistEventsAtomically(numOfEvents = 11, replyTo = probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(41))

      // Test prerequisites:
      queries.selectDeletedToSequenceNr(persistenceId).futureValue should be(Some(SequenceNr(30)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(0)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(1)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(2)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(3)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(4)).futureValue should be(Some(SequenceNr(41)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(5)).futureValue should be(None)

      // Test:
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(1)).futureValue should be(Some(SequenceNr(41)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(10)).futureValue should be(Some(SequenceNr(41)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(11)).futureValue should be(Some(SequenceNr(41)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(20)).futureValue should be(Some(SequenceNr(41)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(21)).futureValue should be(Some(SequenceNr(41)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(30)).futureValue should be(Some(SequenceNr(41)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(31)).futureValue should be(Some(SequenceNr(41)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(40)).futureValue should be(Some(SequenceNr(41)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(41)).futureValue should be(Some(SequenceNr(41)))
      queries.findHighestSequenceNrAfter(persistenceId, SequenceNr(42)).futureValue should be(None)
    }

  }

  "CassandraPersistenceQueries.findHighestSequenceNr" should {

    "find the highest sequence number from the given partition or above" in {
      val queries       = new CassandraPersistenceQueries(system, defaultSettings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      for (sequenceNr <- 1 to 15) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }

      // Test prerequisites:
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(0)).futureValue should be(Some(SequenceNr(10)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(1)).futureValue should be(Some(SequenceNr(15)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(2)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(3)).futureValue should be(None)

      // Test:
      queries.findHighestSequenceNr(persistenceId, PartitionNr(0)).futureValue should be(Some(SequenceNr(15)))
      queries.findHighestSequenceNr(persistenceId, PartitionNr(1)).futureValue should be(Some(SequenceNr(15)))
      queries.findHighestSequenceNr(persistenceId, PartitionNr(2)).futureValue should be(None)
    }

    "find the highest sequence number from the given partition or above if an empty partition exists" in {
      val queries       = new CassandraPersistenceQueries(system, defaultSettings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      for (sequenceNr <- 1 to 10) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }
      actor ! TestPersistentActor.PersistEventsAtomically(11, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(21))

      // Test prerequisites:
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(0)).futureValue should be(Some(SequenceNr(10)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(1)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(2)).futureValue should be(Some(SequenceNr(21)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(3)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(4)).futureValue should be(None)

      // Test:
      queries.findHighestSequenceNr(persistenceId, PartitionNr(0)).futureValue should be(Some(SequenceNr(21)))
      queries.findHighestSequenceNr(persistenceId, PartitionNr(1)).futureValue should be(Some(SequenceNr(21)))
      queries.findHighestSequenceNr(persistenceId, PartitionNr(2)).futureValue should be(Some(SequenceNr(21)))
      queries.findHighestSequenceNr(persistenceId, PartitionNr(3)).futureValue should be(None)
    }

    "find the highest sequence number from the given partition or above if all events are deleted" in {
      val queries       = new CassandraPersistenceQueries(system, defaultSettings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      for (sequenceNr <- 1 to 15) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }
      actor ! TestPersistentActor.SaveSnapshot(probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(15))
      actor ! TestPersistentActor.DeleteEventsTo(15, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(15))

      // Test prerequisites:
      queries.selectDeletedToSequenceNr(persistenceId).futureValue should be(Some(SequenceNr(15)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(0)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(1)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(2)).futureValue should be(None)

      // Test:
      queries.findHighestSequenceNr(persistenceId, PartitionNr(0)).futureValue should be(Some(SequenceNr(15)))
      queries.findHighestSequenceNr(persistenceId, PartitionNr(1)).futureValue should be(Some(SequenceNr(15)))
      queries.findHighestSequenceNr(persistenceId, PartitionNr(2)).futureValue should be(None)
    }

    "find the highest sequence number from the given partition or above if old events are deleted" in {
      val queries       = new CassandraPersistenceQueries(system, defaultSettings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      for (sequenceNr <- 1 to 25) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }
      actor ! TestPersistentActor.SaveSnapshot(probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(25))
      actor ! TestPersistentActor.DeleteEventsTo(22, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(22))

      // Test prerequisites:
      queries.selectDeletedToSequenceNr(persistenceId).futureValue should be(Some(SequenceNr(22)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(0)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(1)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(2)).futureValue should be(Some(SequenceNr(25)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(3)).futureValue should be(None)

      // Test:
      queries.findHighestSequenceNr(persistenceId, PartitionNr(0)).futureValue should be(Some(SequenceNr(25)))
      queries.findHighestSequenceNr(persistenceId, PartitionNr(1)).futureValue should be(Some(SequenceNr(25)))
      queries.findHighestSequenceNr(persistenceId, PartitionNr(2)).futureValue should be(Some(SequenceNr(25)))
      queries.findHighestSequenceNr(persistenceId, PartitionNr(3)).futureValue should be(None)
    }

    "find the highest sequence number from the given partition or above " +
    "if old events are deleted, and an empty partition exists" in {
      val queries       = new CassandraPersistenceQueries(system, defaultSettings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      for (sequenceNr <- 1 to 30) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }
      actor ! TestPersistentActor.SaveSnapshot(probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(30))
      actor ! TestPersistentActor.DeleteEventsTo(30, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(30))
      actor ! TestPersistentActor.PersistEventsAtomically(numOfEvents = 11, replyTo = probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(41))

      // Test prerequisites:
      queries.selectDeletedToSequenceNr(persistenceId).futureValue should be(Some(SequenceNr(30)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(0)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(1)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(2)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(3)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(4)).futureValue should be(Some(SequenceNr(41)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(5)).futureValue should be(None)

      // Test:
      queries.findHighestSequenceNr(persistenceId, PartitionNr(0)).futureValue should be(Some(SequenceNr(41)))
      queries.findHighestSequenceNr(persistenceId, PartitionNr(1)).futureValue should be(Some(SequenceNr(41)))
      queries.findHighestSequenceNr(persistenceId, PartitionNr(2)).futureValue should be(Some(SequenceNr(41)))
      queries.findHighestSequenceNr(persistenceId, PartitionNr(3)).futureValue should be(Some(SequenceNr(41)))
      queries.findHighestSequenceNr(persistenceId, PartitionNr(4)).futureValue should be(Some(SequenceNr(41)))
      queries.findHighestSequenceNr(persistenceId, PartitionNr(5)).futureValue should be(None)
    }

  }

  "CassandraPersistenceQueries.selectHighestSequenceNr" should {

    "return the highest sequence number of the given partition" in {
      val queries       = new CassandraPersistenceQueries(system, defaultSettings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      for (sequenceNr <- 1 to 15) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }

      // Test:
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(0)).futureValue should be(Some(SequenceNr(10)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(1)).futureValue should be(Some(SequenceNr(15)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(2)).futureValue should be(None)
    }

  }

  "CassandraPersistenceQueries.findHighestPartitionNr" should {

    "find the highest partition number from the given partition or above" in {
      val queries       = new CassandraPersistenceQueries(system, defaultSettings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      for (sequenceNr <- 1 to 15) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }

      // Test prerequisites:
      queries.findHighestSequenceNr(persistenceId, PartitionNr(0)).futureValue should be(Some(SequenceNr(15)))
      queries.findHighestSequenceNr(persistenceId, PartitionNr(1)).futureValue should be(Some(SequenceNr(15)))
      queries.findHighestSequenceNr(persistenceId, PartitionNr(2)).futureValue should be(None)

      // Test:
      queries.findHighestPartitionNr(persistenceId, PartitionNr(0)).futureValue should be(Some(PartitionNr(1)))
      queries.findHighestPartitionNr(persistenceId, PartitionNr(1)).futureValue should be(Some(PartitionNr(1)))
      queries.findHighestPartitionNr(persistenceId, PartitionNr(2)).futureValue should be(None)
    }

    "find the highest partition number from the given partition or above if an empty partition exists" in {
      val queries       = new CassandraPersistenceQueries(system, defaultSettings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      for (sequenceNr <- 1 to 10) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }
      actor ! TestPersistentActor.PersistEventsAtomically(11, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(21))

      // Test prerequisites:
      queries.findHighestSequenceNr(persistenceId, PartitionNr(0)).futureValue should be(Some(SequenceNr(21)))
      queries.findHighestSequenceNr(persistenceId, PartitionNr(1)).futureValue should be(Some(SequenceNr(21)))
      queries.findHighestSequenceNr(persistenceId, PartitionNr(2)).futureValue should be(Some(SequenceNr(21)))
      queries.findHighestSequenceNr(persistenceId, PartitionNr(3)).futureValue should be(None)

      // Test:
      queries.findHighestPartitionNr(persistenceId, PartitionNr(0)).futureValue should be(Some(PartitionNr(2)))
      queries.findHighestPartitionNr(persistenceId, PartitionNr(1)).futureValue should be(Some(PartitionNr(2)))
      queries.findHighestPartitionNr(persistenceId, PartitionNr(2)).futureValue should be(Some(PartitionNr(2)))
      queries.findHighestPartitionNr(persistenceId, PartitionNr(3)).futureValue should be(None)
    }

    "find the highest partition number after the given partition if all events are deleted" in {
      val queries       = new CassandraPersistenceQueries(system, defaultSettings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      for (sequenceNr <- 1 to 15) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }
      actor ! TestPersistentActor.SaveSnapshot(probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(15))
      actor ! TestPersistentActor.DeleteEventsTo(15, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(15))

      // Test prerequisites:
      queries.selectDeletedToSequenceNr(persistenceId).futureValue should be(Some(SequenceNr(15)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(0)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(1)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(2)).futureValue should be(None)

      // Test:
      queries.findHighestPartitionNr(persistenceId, PartitionNr(0)).futureValue should be(Some(PartitionNr(1)))
      queries.findHighestPartitionNr(persistenceId, PartitionNr(1)).futureValue should be(Some(PartitionNr(1)))
      queries.findHighestPartitionNr(persistenceId, PartitionNr(2)).futureValue should be(None)
    }

    "find the highest partition number after the given partition if old events are deleted" in {
      val queries       = new CassandraPersistenceQueries(system, defaultSettings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      for (sequenceNr <- 1 to 25) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }
      actor ! TestPersistentActor.SaveSnapshot(probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(25))
      actor ! TestPersistentActor.DeleteEventsTo(22, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(22))

      // Test prerequisites:
      queries.selectDeletedToSequenceNr(persistenceId).futureValue should be(Some(SequenceNr(22)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(0)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(1)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(2)).futureValue should be(Some(SequenceNr(25)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(3)).futureValue should be(None)

      // Test:
      queries.findHighestPartitionNr(persistenceId, PartitionNr(0)).futureValue should be(Some(PartitionNr(2)))
      queries.findHighestPartitionNr(persistenceId, PartitionNr(1)).futureValue should be(Some(PartitionNr(2)))
      queries.findHighestPartitionNr(persistenceId, PartitionNr(2)).futureValue should be(Some(PartitionNr(2)))
      queries.findHighestPartitionNr(persistenceId, PartitionNr(3)).futureValue should be(None)
    }

    "find the highest partition number after the given partition " +
    "if old events are deleted, and an empty partition exists" in {
      val queries       = new CassandraPersistenceQueries(system, defaultSettings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      for (sequenceNr <- 1 to 30) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }
      actor ! TestPersistentActor.SaveSnapshot(probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(30))
      actor ! TestPersistentActor.DeleteEventsTo(30, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(30))
      actor ! TestPersistentActor.PersistEventsAtomically(numOfEvents = 11, replyTo = probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(41))

      // Test prerequisites:
      queries.selectDeletedToSequenceNr(persistenceId).futureValue should be(Some(SequenceNr(30)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(0)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(1)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(2)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(3)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(4)).futureValue should be(Some(SequenceNr(41)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(5)).futureValue should be(None)

      // Test:
      queries.findHighestPartitionNr(persistenceId, PartitionNr(0)).futureValue should be(Some(PartitionNr(4)))
      queries.findHighestPartitionNr(persistenceId, PartitionNr(1)).futureValue should be(Some(PartitionNr(4)))
      queries.findHighestPartitionNr(persistenceId, PartitionNr(2)).futureValue should be(Some(PartitionNr(4)))
      queries.findHighestPartitionNr(persistenceId, PartitionNr(3)).futureValue should be(Some(PartitionNr(4)))
      queries.findHighestPartitionNr(persistenceId, PartitionNr(4)).futureValue should be(Some(PartitionNr(4)))
      queries.findHighestPartitionNr(persistenceId, PartitionNr(5)).futureValue should be(None)
    }

  }

  "CassandraPersistenceQueries.currentEventsAfter" should {

    "return a source that emits current events after the given sequence number inclusive" in {
      val queries       = new CassandraPersistenceQueries(system, defaultSettings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()
      val tagA          = nextUniqueTag()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      for (sequenceNr <- 1 to 5) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }
      for (sequenceNr <- 6 to 10) {
        actor ! TestPersistentActor.PersistTaggedEvent(tagA, probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }
      for (sequenceNr <- 11 to 15) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }

      // Test:
      val events =
        queries.currentEventsAfter(persistenceId, SequenceNr(7)).runWith(Sink.seq).futureValue
      all(events.map(_.persistenceId)) should be(persistenceId)
      events.map(_.sequenceNr) should be((7 to 15).map(SequenceNr(_)))
      events.map(_.event) should be((7 to 15).map(TestPersistentActor.Event(_)))
      all(events.map(_.offset)) shouldBe a[TimeBasedUUID]
      locally {
        val (events7to10, others) = events.partition(event => (7 to 10).contains(event.sequenceNr.value))
        all(events7to10.map(_.tags)) should be(Set(tagA))
        all(others.map(_.tags)) should be(empty)
      }
    }

    "return a source that emits current events after the given sequence number inclusive if an empty partition exists" in {
      val queries       = new CassandraPersistenceQueries(system, defaultSettings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      for (sequenceNr <- 1 to 10) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }
      actor ! TestPersistentActor.PersistEventsAtomically(11, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(21))

      // Test prerequisites:
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(0)).futureValue should be(Some(SequenceNr(10)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(1)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(2)).futureValue should be(Some(SequenceNr(21)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(3)).futureValue should be(None)

      // Test:
      val events =
        queries.currentEventsAfter(persistenceId, SequenceNr(9)).runWith(Sink.seq).futureValue
      all(events.map(_.persistenceId)) should be(persistenceId)
      events.map(_.sequenceNr) should be((9 to 21).map(SequenceNr(_)))
      events.map(_.event) should be((9 to 21).map(TestPersistentActor.Event(_)))
      all(events.map(_.offset)) shouldBe a[TimeBasedUUID]
      all(events.map(_.tags)) should be(empty)
    }

    "return an empty source if all events are deleted" in {
      val queries       = new CassandraPersistenceQueries(system, defaultSettings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      for (sequenceNr <- 1 to 15) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }
      actor ! TestPersistentActor.SaveSnapshot(probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(15))
      actor ! TestPersistentActor.DeleteEventsTo(15, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(15))

      // Test prerequisites:
      queries.selectDeletedToSequenceNr(persistenceId).futureValue should be(Some(SequenceNr(15)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(0)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(1)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(2)).futureValue should be(None)

      // Test:
      queries.currentEventsAfter(persistenceId, SequenceNr(9)).runWith(Sink.seq).futureValue should be(empty)
    }

    "return a source that emits current events after th given sequence number inclusive " +
    "if old events are deleted" in {
      val queries       = new CassandraPersistenceQueries(system, defaultSettings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      for (sequenceNr <- 1 to 25) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }
      actor ! TestPersistentActor.SaveSnapshot(probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(25))
      actor ! TestPersistentActor.DeleteEventsTo(22, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(22))

      // Test prerequisites:
      queries.selectDeletedToSequenceNr(persistenceId).futureValue should be(Some(SequenceNr(22)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(0)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(1)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(2)).futureValue should be(Some(SequenceNr(25)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(3)).futureValue should be(None)

      // Test:
      val events =
        queries.currentEventsAfter(persistenceId, SequenceNr(9)).runWith(Sink.seq).futureValue
      all(events.map(_.persistenceId)) should be(persistenceId)
      events.map(_.sequenceNr) should be((23 to 25).map(SequenceNr(_)))
      events.map(_.event) should be((23 to 25).map(TestPersistentActor.Event(_)))
      all(events.map(_.offset)) shouldBe a[TimeBasedUUID]
      all(events.map(_.tags)) should be(empty)
    }

    "return a source that emits current events after th given sequence number inclusive " +
    "if old events are deleted, and an empty partition exists" in {
      val queries       = new CassandraPersistenceQueries(system, defaultSettings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      for (sequenceNr <- 1 to 30) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }
      actor ! TestPersistentActor.SaveSnapshot(probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(30))
      actor ! TestPersistentActor.DeleteEventsTo(30, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(30))
      actor ! TestPersistentActor.PersistEventsAtomically(numOfEvents = 11, replyTo = probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(41))

      // Test prerequisites:
      queries.selectDeletedToSequenceNr(persistenceId).futureValue should be(Some(SequenceNr(30)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(0)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(1)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(2)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(3)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(4)).futureValue should be(Some(SequenceNr(41)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(5)).futureValue should be(None)

      // Test:
      val events =
        queries.currentEventsAfter(persistenceId, SequenceNr(9)).runWith(Sink.seq).futureValue
      all(events.map(_.persistenceId)) should be(persistenceId)
      events.map(_.sequenceNr) should be((31 to 41).map(SequenceNr(_)))
      events.map(_.event) should be((31 to 41).map(TestPersistentActor.Event(_)))
      all(events.map(_.offset)) shouldBe a[TimeBasedUUID]
      all(events.map(_.tags)) should be(empty)
    }

  }

  "CassandraPersistenceQueries.currentEventsAfterOnPartition" should {

    "return a source that emits current events of the given partition" in {
      val queries       = new CassandraPersistenceQueries(system, defaultSettings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()
      val tagA          = nextUniqueTag()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      for (sequenceNr <- 1 to 5) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }
      for (sequenceNr <- 6 to 10) {
        actor ! TestPersistentActor.PersistTaggedEvent(tagA, probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }
      for (sequenceNr <- 11 to 15) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }

      // Test:
      val eventsOfPartition0 = queries
        .currentEventsAfterOnPartition(persistenceId, SequenceNr(7), PartitionNr(0)).runWith(Sink.seq).futureValue
      all(eventsOfPartition0.map(_.persistenceId)) should be(persistenceId)
      eventsOfPartition0.map(_.sequenceNr) should be((7 to 10).map(SequenceNr(_)))
      eventsOfPartition0.map(_.event) should be((7 to 10).map(TestPersistentActor.Event(_)))
      all(eventsOfPartition0.map(_.offset)) shouldBe a[TimeBasedUUID]
      all(eventsOfPartition0.map(_.tags)) should be(Set(tagA))

      val eventsOfPartition1 = queries
        .currentEventsAfterOnPartition(persistenceId, SequenceNr(7), PartitionNr(1)).runWith(Sink.seq).futureValue
      all(eventsOfPartition1.map(_.persistenceId)) should be(persistenceId)
      eventsOfPartition1.map(_.sequenceNr) should be((11 to 15).map(SequenceNr(_)))
      eventsOfPartition1.map(_.event) should be((11 to 15).map(TestPersistentActor.Event(_)))
      all(eventsOfPartition1.map(_.offset)) shouldBe a[TimeBasedUUID]
      all(eventsOfPartition1.map(_.tags)) should be(empty)

      val eventsOfPartition2 = queries
        .currentEventsAfterOnPartition(persistenceId, SequenceNr(7), PartitionNr(2)).runWith(Sink.seq).futureValue
      eventsOfPartition2 should be(empty)
    }

  }

  "CassandraPersistenceQueries.currentEventsBefore" should {

    "return a source that emits current events before the given sequence number inclusive in the descending order of the sequence number" in {
      val queries       = new CassandraPersistenceQueries(system, defaultSettings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()
      val tagA          = nextUniqueTag()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      for (sequenceNr <- 1 to 5) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }
      for (sequenceNr <- 6 to 10) {
        actor ! TestPersistentActor.PersistTaggedEvent(tagA, probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }
      for (sequenceNr <- 11 to 15) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }

      // Test:
      val events =
        queries.currentEventsBefore(persistenceId, SequenceNr(7)).runWith(Sink.seq).futureValue
      all(events.map(_.persistenceId)) should be(persistenceId)
      events.map(_.sequenceNr) should be((7 to 1 by -1).map(SequenceNr(_)))
      events.map(_.event) should be((7 to 1 by -1).map(TestPersistentActor.Event(_)))
      all(events.map(_.offset)) shouldBe a[TimeBasedUUID]
      locally {
        val (events6to7, others) = events.partition(event => (6 to 7).contains(event.sequenceNr.value))
        all(events6to7.map(_.tags)) should be(Set(tagA))
        all(others.map(_.tags)) should be(empty)
      }
    }

    "return a source that emits current events before the given sequence number inclusive if an empty partition exists" in {
      val queries       = new CassandraPersistenceQueries(system, defaultSettings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      for (sequenceNr <- 1 to 10) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }
      actor ! TestPersistentActor.PersistEventsAtomically(11, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(21))

      // Test prerequisites:
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(0)).futureValue should be(Some(SequenceNr(10)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(1)).futureValue should be(None)
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(2)).futureValue should be(Some(SequenceNr(21)))
      queries.selectHighestSequenceNr(persistenceId, PartitionNr(3)).futureValue should be(None)

      // Test:
      val events =
        queries.currentEventsBefore(persistenceId, SequenceNr(14)).runWith(Sink.seq).futureValue
      all(events.map(_.persistenceId)) should be(persistenceId)
      events.map(_.sequenceNr) should be((14 to 1 by -1).map(SequenceNr(_)))
      events.map(_.event) should be((14 to 1 by -1).map(TestPersistentActor.Event(_)))
      all(events.map(_.offset)) shouldBe a[TimeBasedUUID]
      all(events.map(_.tags)) should be(empty)
    }

    "return a source that emits current events before the given sequence number inclusive if old events are deleted" in {
      val queries       = new CassandraPersistenceQueries(system, defaultSettings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      for (sequenceNr <- 1 to 25) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }
      actor ! TestPersistentActor.SaveSnapshot(probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(25))
      actor ! TestPersistentActor.DeleteEventsTo(15, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(15))

      // Test:
      val events =
        queries.currentEventsBefore(persistenceId, SequenceNr(22)).runWith(Sink.seq).futureValue
      all(events.map(_.persistenceId)) should be(persistenceId)
      events.map(_.sequenceNr) should be((22 to 16 by -1).map(SequenceNr(_)))
      events.map(_.event) should be((22 to 16 by -1).map(TestPersistentActor.Event(_)))
      all(events.map(_.offset)) shouldBe a[TimeBasedUUID]
    }

  }

  "CassandraPersistenceQueries.currentEventsBeforeOnPartition" should {

    "return a source that emits current events of the given partition in the descending order of the sequence number" in {
      val queries       = new CassandraPersistenceQueries(system, defaultSettings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()
      val tagA          = nextUniqueTag()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      for (sequenceNr <- 1 to 5) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }
      for (sequenceNr <- 6 to 10) {
        actor ! TestPersistentActor.PersistTaggedEvent(tagA, probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }
      for (sequenceNr <- 11 to 15) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }

      // Test:
      val eventsOfPartition0 = queries
        .currentEventsBeforeOnPartition(persistenceId, SequenceNr(12), PartitionNr(0)).runWith(Sink.seq).futureValue
      all(eventsOfPartition0.map(_.persistenceId)) should be(persistenceId)
      eventsOfPartition0.map(_.sequenceNr) should be((10 to 1 by -1).map(SequenceNr(_)))
      eventsOfPartition0.map(_.event) should be((10 to 1 by -1).map(TestPersistentActor.Event(_)))
      all(eventsOfPartition0.map(_.offset)) shouldBe a[TimeBasedUUID]
      locally {
        val (events6to10, others) = eventsOfPartition0.partition(event => (6 to 10).contains(event.sequenceNr.value))
        all(events6to10.map(_.tags)) should be(Set(tagA))
        all(others.map(_.tags)) should be(empty)
      }

      val eventsOfPartition1 = queries
        .currentEventsBeforeOnPartition(persistenceId, SequenceNr(12), PartitionNr(1)).runWith(Sink.seq).futureValue
      all(eventsOfPartition1.map(_.persistenceId)) should be(persistenceId)
      eventsOfPartition1.map(_.sequenceNr) should be((12 to 11 by -1).map(SequenceNr(_)))
      eventsOfPartition1.map(_.event) should be((12 to 11 by -1).map(TestPersistentActor.Event(_)))
      all(eventsOfPartition1.map(_.offset)) shouldBe a[TimeBasedUUID]
      all(eventsOfPartition1.map(_.tags)) should be(empty)

      val eventsOfPartition2 = queries
        .currentEventsBeforeOnPartition(persistenceId, SequenceNr(12), PartitionNr(2)).runWith(Sink.seq).futureValue
      eventsOfPartition2 should be(empty)
    }

  }

  "CassandraPersistenceQueries.selectDeletedToSequenceNr" should {

    "return None if no events are persisted" in {
      val queries       = new CassandraPersistenceQueries(system, defaultSettings)
      val persistenceId = nextPersistenceId()

      // Prepare:
      val _: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))

      // Test:
      queries.selectDeletedToSequenceNr(persistenceId).futureValue should be(None)
    }

    "return None if no events are deleted" in {
      val queries       = new CassandraPersistenceQueries(system, defaultSettings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      actor ! TestPersistentActor.PersistEvent(probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(1))

      // Test:
      queries.selectDeletedToSequenceNr(persistenceId).futureValue should be(None)
    }

    "return the highest deleted sequence number (`deleted_to`) if events are deleted" in {
      val queries       = new CassandraPersistenceQueries(system, defaultSettings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      for (sequenceNr <- 1 to 5) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }
      actor ! TestPersistentActor.SaveSnapshot(probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(5))

      // Test:
      actor ! TestPersistentActor.DeleteEventsTo(3, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(3))
      queries.selectDeletedToSequenceNr(persistenceId).futureValue should be(Some(SequenceNr(3)))
    }

  }

  "CassandraPersistenceQueries.selectLowestSnapshotSequenceNr" should {

    "return None if no snapshots are persisted" in {
      val queries       = new CassandraPersistenceQueries(system, defaultSettings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      for (sequenceNr <- 1 to 3) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }

      // Test:
      queries.selectLowestSnapshotSequenceNr(persistenceId, SequenceNr(1)).futureValue should be(None)
      queries.selectLowestSnapshotSequenceNr(persistenceId, SequenceNr(2)).futureValue should be(None)
      queries.selectLowestSnapshotSequenceNr(persistenceId, SequenceNr(3)).futureValue should be(None)
    }

    "return the lowest snapshot sequence number greater than or equal to the lower sequence number" in {
      val queries       = new CassandraPersistenceQueries(system, defaultSettings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      for (sequenceNr <- 1 to 3) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }
      actor ! TestPersistentActor.SaveSnapshot(probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(3))
      for (sequenceNr <- 4 to 6) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }
      actor ! TestPersistentActor.SaveSnapshot(probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(6))

      // Test:
      queries.selectLowestSnapshotSequenceNr(persistenceId, SequenceNr(1)).futureValue should be(Some(SequenceNr(3)))
      queries.selectLowestSnapshotSequenceNr(persistenceId, SequenceNr(2)).futureValue should be(Some(SequenceNr(3)))
      queries.selectLowestSnapshotSequenceNr(persistenceId, SequenceNr(3)).futureValue should be(Some(SequenceNr(3)))
      queries.selectLowestSnapshotSequenceNr(persistenceId, SequenceNr(4)).futureValue should be(Some(SequenceNr(6)))
      queries.selectLowestSnapshotSequenceNr(persistenceId, SequenceNr(5)).futureValue should be(Some(SequenceNr(6)))
      queries.selectLowestSnapshotSequenceNr(persistenceId, SequenceNr(6)).futureValue should be(Some(SequenceNr(6)))
      queries.selectLowestSnapshotSequenceNr(persistenceId, SequenceNr(7)).futureValue should be(None)
    }

    "return the lowest snapshot sequence number greater than or equal to the lower sequence number " +
    "if old snapshots are deleted" in {
      val queries       = new CassandraPersistenceQueries(system, defaultSettings)
      val probe         = TestProbe()
      val persistenceId = nextPersistenceId()

      // Prepare:
      val actor: ActorRef = system.actorOf(TestPersistentActor.props(persistenceId))
      for (sequenceNr <- 1 to 3) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }
      actor ! TestPersistentActor.SaveSnapshot(probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(3))
      for (sequenceNr <- 4 to 6) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }
      actor ! TestPersistentActor.SaveSnapshot(probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(6))
      for (sequenceNr <- 7 to 9) {
        actor ! TestPersistentActor.PersistEvent(probe.ref)
        probe.expectMsg(TestPersistentActor.Ack(sequenceNr))
      }
      actor ! TestPersistentActor.SaveSnapshot(probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(9))
      actor ! TestPersistentActor.DeleteSnapshotsTo(4, probe.ref)
      probe.expectMsg(TestPersistentActor.Ack(4))

      // Test:
      queries.selectLowestSnapshotSequenceNr(persistenceId, SequenceNr(1)).futureValue should be(Some(SequenceNr(6)))
      queries.selectLowestSnapshotSequenceNr(persistenceId, SequenceNr(2)).futureValue should be(Some(SequenceNr(6)))
      queries.selectLowestSnapshotSequenceNr(persistenceId, SequenceNr(3)).futureValue should be(Some(SequenceNr(6)))
      queries.selectLowestSnapshotSequenceNr(persistenceId, SequenceNr(4)).futureValue should be(Some(SequenceNr(6)))
      queries.selectLowestSnapshotSequenceNr(persistenceId, SequenceNr(5)).futureValue should be(Some(SequenceNr(6)))
      queries.selectLowestSnapshotSequenceNr(persistenceId, SequenceNr(6)).futureValue should be(Some(SequenceNr(6)))
      queries.selectLowestSnapshotSequenceNr(persistenceId, SequenceNr(7)).futureValue should be(Some(SequenceNr(9)))
      queries.selectLowestSnapshotSequenceNr(persistenceId, SequenceNr(8)).futureValue should be(Some(SequenceNr(9)))
      queries.selectLowestSnapshotSequenceNr(persistenceId, SequenceNr(9)).futureValue should be(Some(SequenceNr(9)))
      queries.selectLowestSnapshotSequenceNr(persistenceId, SequenceNr(10)).futureValue should be(None)
    }

  }

}
