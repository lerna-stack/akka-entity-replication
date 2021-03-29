ThisBuild / organization := "com.lerna-stack"
ThisBuild / organizationName := "Lerna Project"
ThisBuild / organizationHomepage := Some(url("https://lerna-stack.github.io/"))

ThisBuild / developers := List(
  Developer(
    id = "lerna",
    name = "Lerna Team",
    email = "go-reactive@tis.co.jp",
    url = url("https://lerna-stack.github.io/"),
  ),
)

ThisBuild / description := "Akka extension for fast recovery from failure with replicating stateful entity on multiple nodes in Cluster."
ThisBuild / licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage := Some(url("https://github.com/lerna-stack/akka-entity-replication"))

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }
