import org.scalafmt.sbt.ScalafmtPlugin.scalafmtConfigSettings

resolvers += "dnvriend" at "https://dl.bintray.com/dnvriend/maven"

lazy val akkaVersion           = "2.6.9"
lazy val akkaProjectionVersion = "1.0.0"

lazy val lerna = (project in file("."))
  .enablePlugins(MultiJvmPlugin)
  .configs(MultiJvm)
  .settings(
    inThisBuild(
      List(
        scalaVersion := "2.12.12",
        scalacOptions ++= Seq(
            "-deprecation",
            "-feature",
            "-unchecked",
            "-Xlint",
          ),
        scalacOptions ++= sys.props.get("lerna.enable.discipline").map(_ => "-Xfatal-warnings").toSeq,
      ),
    ),
    name := "akka-entity-replication",
    fork in Test := true,
    parallelExecution in Test := false,
    libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-stream" % akkaVersion,
        // akka-projection-core requires akka-cluster-typed:
        // akka-projection-core depends on actor-typed, and actor-typed requires akka-cluster-typed when the cluster is enabled
        // see: https://github.com/akka/akka/blob/v2.6.9/akka-actor-typed/src/main/scala/akka/actor/typed/internal/receptionist/ReceptionistImpl.scala#L21-L32
        "com.typesafe.akka" %% "akka-cluster-typed"    % akkaVersion,
        "com.typesafe.akka" %% "akka-cluster"          % akkaVersion,
        "com.typesafe.akka" %% "akka-cluster-sharding" % akkaVersion,
        // persistence-query 2.6.x を明示的に指定しないとエラーになる。
        // 恐らく akka-persistence-inmemory の影響である。
        "com.typesafe.akka"  %% "akka-persistence-query"       % akkaVersion,
        "com.lightbend.akka" %% "akka-projection-core"         % akkaProjectionVersion,
        "com.lightbend.akka" %% "akka-projection-eventsourced" % akkaProjectionVersion,
        "io.altoo"           %% "akka-kryo-serialization"      % "1.1.5",
        "com.typesafe.akka"  %% "akka-slf4j"                   % akkaVersion % Test,
        "ch.qos.logback"      % "logback-classic"              % "1.2.3"     % Test,
        "org.scalatest"      %% "scalatest"                    % "3.0.5"     % Test,
        "com.typesafe.akka"  %% "akka-multi-node-testkit"      % akkaVersion % Test,
        // akka-persistence-inmemory が 2.6.x 系に対応していない。
        // TODO 2.6.x 系に対応できる方法に変更する。
        "com.github.dnvriend" %% "akka-persistence-inmemory" % "2.5.15.2" % Test,
      ),
    // multi-jvm ディレクトリをフォーマットするために必要
    inConfig(MultiJvm)(scalafmtConfigSettings),
  )
