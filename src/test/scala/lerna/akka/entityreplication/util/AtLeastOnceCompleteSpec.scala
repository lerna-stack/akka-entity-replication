package lerna.akka.entityreplication.util

import akka.actor.{ ActorSystem, NoSerializationVerificationNeeded }
import akka.testkit.TestKit
import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.Inside
import org.scalatest.concurrent.ScalaFutures

object AtLeastOnceCompleteSpec {
  private val config: Config = ConfigFactory
    .parseString(s"""
                    | akka.actor {
                    |   provider = local
                    | }
       """.stripMargin)
    .withFallback(ConfigFactory.load())

  final case class RequestMessage(message: String)  extends NoSerializationVerificationNeeded
  final case class ResponseMessage(message: String) extends NoSerializationVerificationNeeded
}

class AtLeastOnceCompleteSpec
    extends TestKit(ActorSystem("AtLeastOnceCompleteSpec", AtLeastOnceCompleteSpec.config))
    with ScalaFutures
    with Inside {

  // TODO: test
}
