package lerna.akka.entityreplication.util

import java.time.{ Duration => JDuration }

import scala.concurrent.duration.{ Duration, FiniteDuration }

/**
  * INTERNAL API
  */
object JavaDurationConverters {

  final implicit class JavaDurationOps(val self: JDuration) extends AnyVal {
    def asScala: FiniteDuration = Duration.fromNanos(self.toNanos)
  }

  final implicit class ScalaDurationOps(val self: Duration) extends AnyVal {
    def asJava: JDuration = JDuration.ofNanos(self.toNanos)
  }
}
