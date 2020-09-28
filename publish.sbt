ThisBuild / organization := "io.github.lerna-stack"
ThisBuild / organizationName := "Lerna Project"
ThisBuild / organizationHomepage := Some(url("https://lerna-stack.github.io/"))

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/lerna-stack/akka-entity-replication"),
    "scm:git@github.com:lerna-stack/akka-entity-replication.git",
  ),
)
ThisBuild / developers := List(
  Developer(
    id = "lerna",
    name = "Lerna Team",
    email = "go-reactive@tis.co.jp",
    url = url("https://lerna-stack.github.io/"),
  ),
)

ThisBuild / description := "Replicate Entity on multiple nodes in Akka Cluster for fast failover in case of node failure."
ThisBuild / licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage := Some(url("https://github.com/lerna-stack/akka-entity-replication"))

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
ThisBuild / publishMavenStyle := true
