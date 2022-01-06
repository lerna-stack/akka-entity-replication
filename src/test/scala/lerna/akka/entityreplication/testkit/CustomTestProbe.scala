package lerna.akka.entityreplication.testkit

import akka.testkit.TestProbe

object CustomTestProbe {

  implicit class CustomTestProbe(testProbe: TestProbe) {
    def fishForMessageN[T](messages: Int)(f: PartialFunction[Any, T]): Seq[T] = {
      var fishedMessages = Seq.empty[T]
      testProbe.fishForMessage() {
        case msg if f.isDefinedAt(msg) =>
          fishedMessages :+= f(msg)
          fishedMessages.sizeIs >= messages
        case _ => false // ignore
      }
      fishedMessages
    }
  }
}
