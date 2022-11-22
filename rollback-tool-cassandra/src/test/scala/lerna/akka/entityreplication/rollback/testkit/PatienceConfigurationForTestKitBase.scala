package lerna.akka.entityreplication.rollback.testkit

import akka.testkit.TestKitBase
import org.scalatest.TestSuite
import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.time.{ Millis, Span }

trait PatienceConfigurationForTestKitBase { this: TestKitBase with TestSuite with PatienceConfiguration =>

  lazy val customPatienceConfig: PatienceConfig = PatienceConfig(
    timeout = testKitSettings.DefaultTimeout.duration,
    interval = Span(100, Millis),
  )
  override implicit def patienceConfig: PatienceConfig = customPatienceConfig

}
