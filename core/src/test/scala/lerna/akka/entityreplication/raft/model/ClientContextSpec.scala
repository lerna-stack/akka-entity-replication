package lerna.akka.entityreplication.raft.model

import akka.actor.ActorSystem
import akka.testkit.{ TestKit, TestProbe }
import lerna.akka.entityreplication.raft.ActorSpec

final class ClientContextSpec extends TestKit(ActorSystem("ClientContextSpec")) with ActorSpec {

  "ClientContext.forward should send the given message to the actor, including no sender, if the context doesn't have an original sender" in {
    val probe         = TestProbe()
    val clientContext = ClientContext(probe.ref, instanceId = None, originSender = None)
    clientContext.forward("message-1")
    probe.expectMsg("message-1")
    probe.sender() should be(system.deadLetters)
  }

  "ClientContext.forward should send the given message to the actor, including an original sender, if the context has the original sender" in {
    val probe          = TestProbe()
    val originalSender = TestProbe().ref
    val clientContext  = ClientContext(probe.ref, instanceId = None, originSender = Some(originalSender))
    clientContext.forward("message-1")
    probe.expectMsg("message-1")
    probe.sender() should be(originalSender)
  }

}
