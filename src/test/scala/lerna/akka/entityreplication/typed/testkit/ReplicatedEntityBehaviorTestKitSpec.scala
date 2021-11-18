package lerna.akka.entityreplication.typed.testkit

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.scaladsl.{ ActorContext, Behaviors }
import lerna.akka.entityreplication.testkit.KryoSerializable
import lerna.akka.entityreplication.typed._
import lerna.akka.entityreplication.typed.testkit.ReplicatedEntityBehaviorTestKit._
import org.scalatest.{ BeforeAndAfterEach, FlatSpec, Matchers }

import scala.reflect.ClassTag

class ReplicatedEntityBehaviorTestKitSpec extends FlatSpec with Matchers with BeforeAndAfterEach {

  import ReplicatedEntityBehaviorTestKitSpec._

  private[this] val testKit = ActorTestKit()

  private[this] val replicatedEntityTestKit =
    ReplicatedEntityBehaviorTestKit[TargetBehavior.Command, TargetBehavior.Event, TargetBehavior.State](
      testKit.system,
      TargetBehavior.typeKey,
      entityId = "test-entity",
      behavior = context => TargetBehavior(context),
    )

  override def afterEach(): Unit = {
    super.afterEach()
    replicatedEntityTestKit.clear()
  }

  import TargetBehavior._

  behavior of "A result from ReplicatedEntityBehaviorTestKit.runCommand with tell messages"

  it should behave like commandResultWithAEvent {
    replicatedEntityTestKit.runCommand(TellAndReplicateAEvent(inc = 10))
  }(
    expectedCommand = classOf[TellAndReplicateAEvent],
    expectedEvent = classOf[SerializableEvent],
    invalidEvent = classOf[UnserializableEvent],
    expectedState = classOf[SerializableState],
    invalidState = classOf[UnserializableState],
  )
  it should behave like commandResultWithNoEvents {
    replicatedEntityTestKit.runCommand(TellAndNoEvents())
  }

  behavior of "A result from ReplicatedEntityBehaviorTestKit.runCommand with ask messages"

  it should behave like commandResultWithAEvent {
    replicatedEntityTestKit.runCommand(AskAndReplicateAEvent(inc = 10, _))
  }(
    expectedCommand = classOf[AskAndReplicateAEvent],
    expectedEvent = classOf[SerializableEvent],
    invalidEvent = classOf[UnserializableEvent],
    expectedState = classOf[SerializableState],
    invalidState = classOf[UnserializableState],
  )
  it should behave like commandResultWithNoEvents {
    replicatedEntityTestKit.runCommand(AskAndNoEvents)
  }
  it should behave like commandResultWithReply {
    replicatedEntityTestKit.runCommand(AskAndReplicateAEvent(inc = 10, _))
  }(
    expectedReply = classOf[SerializableReply],
    invalidReply = classOf[UnserializableReply],
  )
  it should behave like commandResultWithNoReply {
    // Withdraw handler has a bug
    replicatedEntityTestKit.runCommand(AskAndNoReply)
  }

  behavior of "ReplicatedEntityBehaviorTestKit"

  it should "still work after passivation which is triggered by handling message with 'tell' pattern" in {
    // check no exception raised
    replicatedEntityTestKit.runCommand(TellAndPassivate())
    replicatedEntityTestKit.runCommand(TellAndReplicateAEventAndPassivate(inc = 10))
    replicatedEntityTestKit.runCommand(TellAndReplicateAEvent(inc = 10))
  }
  it should "still work after passivation which is triggered by handling message with 'ask' pattern" in {
    // check no exception raised
    replicatedEntityTestKit.runCommand(AskAndReplicateAEventAndPassivate(inc = 10, _))
    replicatedEntityTestKit.runCommand(AskAndReplicateAEvent(inc = 10, _))
  }
  it should "report the command that could not be serialized" in {
    val ex = intercept[AssertionError] {
      replicatedEntityTestKit.runCommand(FailCommandSerialization())
    }
    ex.getMessage should be("Command [FailCommandSerialization()] isn't serializable")
  }
  it should "report the reply that could not be serialized" in {
    val ex = intercept[AssertionError] {
      replicatedEntityTestKit.runCommand(FailReplySerialization)
    }
    ex.getMessage should be("Reply [UnserializableReply()] isn't serializable")
  }
  it should "report the state that could not be serialized" in {
    val ex = intercept[AssertionError] {
      replicatedEntityTestKit.runCommand(FailStateSerialization())
    }
    ex.getMessage should be("State [UnserializableState()] isn't serializable")
  }
  it should "report the event that could not be serialized" in {
    val ex = intercept[AssertionError] {
      replicatedEntityTestKit.runCommand(FailEventSerialization())
    }
    ex.getMessage should be("Event [UnserializableEvent()] isn't serializable")
  }
  it should "recover the entity state with 'restart()'" in {
    replicatedEntityTestKit.runCommand(AskAndReplicateAEvent(inc = 100, _))
    val stateBefore = replicatedEntityTestKit.state
    stateBefore should be(SerializableState(100))
    replicatedEntityTestKit.restart()
    val stateAfter = replicatedEntityTestKit.state
    stateAfter should be(SerializableState(100))
    // They have the same value but different instances
    stateBefore should not be theSameInstanceAs(stateAfter)
  }
  it should "reset the entity with 'clear()'" in {
    replicatedEntityTestKit.runCommand(AskAndReplicateAEvent(inc = 100, _))
    replicatedEntityTestKit.state should be(SerializableState(100))
    replicatedEntityTestKit.clear()
    replicatedEntityTestKit.state should be(SerializableState(0))
  }

  def commandResultWithReply[ExpectedReply <: Reply: ClassTag, InvalidReply <: Reply: ClassTag](
      commandResult: => CommandResultWithReply[Command, Event, State, Reply],
  )(expectedReply: Class[ExpectedReply], invalidReply: Class[InvalidReply]): Unit = {

    it should "provide the reply" in {
      commandResult.reply shouldBe a[Reply]
    }

    it should "provide the reply as the specified type" in {
      commandResult.replyOfType[ExpectedReply]
    }

    it should "throw AssertionError if the entity reply the another type message" in {
      intercept[AssertionError] {
        commandResult.replyOfType[InvalidReply]
      }.getMessage should be(s"Expected reply class [${invalidReply.getName}], but was [${expectedReply.getName}]")
    }
  }

  def commandResultWithNoReply(
      commandResult: => CommandResultWithReply[Command, Event, State, Reply],
  ): Unit = {

    it should "throw AssertionError if the entity no reply any messages" in {
      val expectedMessage = "No reply"
      intercept[AssertionError] {
        commandResult.reply
      }.getMessage should be(expectedMessage)

      intercept[AssertionError] {
        commandResult.replyOfType[Reply]
      }.getMessage should be(expectedMessage)
    }
  }

  def commandResultWithAEvent[
      ExpectedCommand <: Command: ClassTag,
      ExpectedEvent <: Event: ClassTag,
      InvalidEvent <: Event: ClassTag,
      ExpectedState <: State: ClassTag,
      InvalidState <: State: ClassTag,
  ](
      commandResult: => CommandResult[Command, Event, State],
  )(
      expectedCommand: Class[ExpectedCommand],
      expectedEvent: Class[ExpectedEvent],
      invalidEvent: Class[InvalidEvent],
      expectedState: Class[ExpectedState],
      invalidState: Class[InvalidState],
  ): Unit = {

    it should "provide the sent command" in {
      commandResult.command shouldBe a[ExpectedCommand]
    }

    it should "provide 'false' if the entity replicated a event" in {
      commandResult.hasNoEvents should be(false)
    }

    it should "provide the replicated event" in {
      commandResult.event shouldBe a[ExpectedEvent]
    }

    it should "provide the replicated event as the specified type" in {
      commandResult.eventOfType[ExpectedEvent]
    }

    it should "throw AssertionError if the entity replicated the another type event" in {
      intercept[AssertionError] {
        commandResult.eventOfType[InvalidEvent]
      }.getMessage should be(
        s"Expected event class [${invalidEvent.getName}], but was [${expectedEvent.getName}]",
      )
    }

    it should "provide the state of the entity" in {
      commandResult.state shouldBe a[ExpectedState]
    }

    it should "provide the state of the entity as the specified type" in {
      commandResult.stateOfType[ExpectedState]
    }

    it should "throw AssertionError if the entity has the another type state" in {
      intercept[AssertionError] {
        commandResult.stateOfType[InvalidState]
      }.getMessage should be(
        s"Expected state class [${invalidState.getName}], but was [${expectedState.getName}]",
      )
    }
  }

  def commandResultWithNoEvents(
      commandResult: => CommandResult[Command, Event, State],
  ): Unit = {

    it should "provide 'true' if the entity didn't replicate any events" in {
      commandResult.hasNoEvents should be(true)
    }

    it should "throw AssertionError when user try to get event" in {
      val expectedMessage = "No event"
      intercept[AssertionError] {
        commandResult.event
      }.getMessage should be(expectedMessage)

      intercept[AssertionError] {
        commandResult.eventOfType[Event]
      }.getMessage should be(expectedMessage)
    }
  }
}

object ReplicatedEntityBehaviorTestKitSpec {

  import akka.actor.typed.ActorRef
  import akka.actor.typed.Behavior

  object TargetBehavior {

    val typeKey: ReplicatedEntityTypeKey[Command] = ReplicatedEntityTypeKey("Target")

    sealed trait Command
    final case class AskAndReplicateAEvent(inc: Int, replyTo: ActorRef[Reply]) extends Command with KryoSerializable
    final case class AskAndReplicateAEventAndPassivate(inc: Int, replyTo: ActorRef[Reply])
        extends Command
        with KryoSerializable
    final case class AskAndNoReply(replyTo: ActorRef[Reply])          extends Command with KryoSerializable
    final case class AskAndNoEvents(replyTo: ActorRef[Reply])         extends Command with KryoSerializable
    final case class TellAndNoEvents()                                extends Command with KryoSerializable
    final case class TellAndPassivate()                               extends Command with KryoSerializable
    final case class TellAndReplicateAEvent(inc: Int)                 extends Command with KryoSerializable
    final case class TellAndReplicateAEventAndPassivate(inc: Int)     extends Command with KryoSerializable
    final case class FailCommandSerialization()                       extends Command
    final case class FailReplySerialization(replyTo: ActorRef[Reply]) extends Command with KryoSerializable
    final case class FailStateSerialization()                         extends Command with KryoSerializable
    final case class FailEventSerialization()                         extends Command with KryoSerializable

    trait Reply
    final case class SerializableReply(inc: Int, total: Int) extends Reply with KryoSerializable
    final case class UnserializableReply()                   extends Reply

    sealed trait Event
    final case class SerializableEvent(inc: Int) extends Event with KryoSerializable
    final case class BecomeUnserializableState() extends Event with KryoSerializable
    final case class UnserializableEvent()       extends Event

    type Effect = lerna.akka.entityreplication.typed.Effect[Event, State]

    sealed trait State {
      def total: Int
      def applyCommand(command: Command, context: ActorContext[Command]): Effect
      def applyEvent(event: Event, context: ActorContext[Command]): State
    }
    final case class SerializableState(total: Int) extends State with KryoSerializable {

      def add(inc: Int): SerializableState = copy(total = total + inc)

      override def applyCommand(command: Command, context: ActorContext[Command]): Effect =
        command match {

          case AskAndReplicateAEvent(inc, replyTo) =>
            Effect
              .replicate(SerializableEvent(inc))
              .thenRun { _: State =>
                context.log.info("complete depositing")
              }
              .thenReply(replyTo)(state => SerializableReply(inc, state.total))

          case AskAndReplicateAEventAndPassivate(inc, replyTo) =>
            Effect
              .replicate(SerializableEvent(inc))
              .thenRun { _: State =>
                context.log.info("complete depositing")
              }
              .thenPassivate()
              .thenReply(replyTo)(state => SerializableReply(inc, state.total))

          case AskAndNoReply(_) =>
            Effect.noReply

          case AskAndNoEvents(replyTo) =>
            Effect.none.thenReply(replyTo)(state => SerializableReply(inc = 0, state.total))

          case TellAndNoEvents() =>
            Effect.none
              .thenRun { _: State =>
                context.log.info("receive a command")
              }.thenNoReply()

          case TellAndPassivate() =>
            Effect.passivate().thenNoReply()

          case TellAndReplicateAEvent(inc: Int) =>
            Effect.replicate(SerializableEvent(inc)).thenNoReply()

          case TellAndReplicateAEventAndPassivate(inc: Int) =>
            Effect.replicate(SerializableEvent(inc)).thenPassivate().thenNoReply()

          case FailCommandSerialization() =>
            Effect.noReply

          case FailStateSerialization() =>
            Effect.replicate(BecomeUnserializableState()).thenNoReply()

          case FailReplySerialization(replyTo) =>
            Effect.reply(replyTo)(UnserializableReply())

          case FailEventSerialization() =>
            Effect.replicate(UnserializableEvent()).thenNoReply()
        }

      override def applyEvent(event: Event, context: ActorContext[Command]): State =
        event match {
          case SerializableEvent(inc)      => this.add(inc)
          case UnserializableEvent()       => this
          case BecomeUnserializableState() => UnserializableState()
        }
    }
    final case class UnserializableState() extends State {

      val total = 0

      override def applyCommand(command: Command, context: ActorContext[Command]): Effect =
        command match {
          case _ => Effect.unhandled.thenNoReply()
        }

      override def applyEvent(event: Event, context: ActorContext[Command]): State =
        event match {
          case _ => this
        }
    }

    def apply(entityContext: ReplicatedEntityContext[Command]): Behavior[Command] = {
      Behaviors.setup { context =>
        ReplicatedEntityBehavior[Command, Event, State](
          entityContext,
          emptyState = SerializableState(0),
          commandHandler = (state, command) => state.applyCommand(command, context),
          eventHandler = (state, event) => state.applyEvent(event, context),
        )
      }
    }
  }
}
