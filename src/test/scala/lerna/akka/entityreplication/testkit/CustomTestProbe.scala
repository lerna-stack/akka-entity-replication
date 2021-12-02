package lerna.akka.entityreplication.testkit

import akka.testkit.TestProbe

object CustomTestProbe {

  implicit class CustomTestProbe(testProbe: TestProbe) {
    def fishMatchMessagesWhile(messages: Int)(f: PartialFunction[Any, Unit]): Unit = {
      var count = 0
      testProbe.fishForMessage() {
        case msg if f.isDefinedAt(msg) =>
          f(msg)
          count = count + 1
          count >= messages
        case _ => false // ignore
      }
    }
  }
}
