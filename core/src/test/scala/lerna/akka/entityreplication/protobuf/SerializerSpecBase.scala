package lerna.akka.entityreplication.protobuf

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.scalatest.{ Matchers, WordSpecLike }

abstract class SerializerSpecBase(system: ActorSystem) extends TestKit(system) with WordSpecLike with Matchers
