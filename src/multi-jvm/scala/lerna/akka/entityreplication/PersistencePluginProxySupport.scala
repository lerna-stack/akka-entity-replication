package lerna.akka.entityreplication

import akka.persistence.journal.PersistencePluginProxy
import akka.remote.testkit.MultiNodeSpec

trait PersistencePluginProxySupport { self: MultiNodeSpec =>

  PersistencePluginProxy.setTargetLocation(system, node(roles.head).address)
}
