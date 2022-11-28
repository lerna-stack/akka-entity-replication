package lerna.akka.entityreplication

import akka.persistence.journal.PersistencePluginProxy
import akka.remote.testkit.MultiNodeSpec
import lerna.akka.entityreplication.util.persistence.query.proxy.scaladsl.ReadJournalPluginProxy

trait PersistencePluginProxySupport { self: MultiNodeSpec =>

  PersistencePluginProxy.setTargetLocation(system, node(roles.head).address)
  ReadJournalPluginProxy.setTargetLocation(system, node(roles.head).address)
}
