package lerna.akka.entityreplication.util.persistence.query.proxy

import akka.actor.ExtendedActorSystem
import akka.persistence.query.ReadJournalProvider
import com.typesafe.config.Config

class ReadJournalPluginProxyProvider(system: ExtendedActorSystem, config: Config) extends ReadJournalProvider {

  override val scaladslReadJournal: scaladsl.ReadJournalPluginProxy =
    new scaladsl.ReadJournalPluginProxy(config)(system)

  override val javadslReadJournal: javadsl.ReadJournalPluginProxy =
    new javadsl.ReadJournalPluginProxy(scaladslReadJournal)
}
