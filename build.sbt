import org.scalafmt.sbt.ScalafmtPlugin.scalafmtConfigSettings

resolvers += "dnvriend" at "https://dl.bintray.com/dnvriend/maven"

lazy val akkaVersion = "2.6.12"

lazy val lerna = (project in file("."))
  .enablePlugins(
    MultiJvmPlugin,
    SiteScaladocPlugin,
    GhpagesPlugin,
  )
  .configs(MultiJvm)
  .settings(
    inThisBuild(
      List(
        scalaVersion := "2.13.4",
        scalacOptions ++= Seq(
            "-feature",
            "-unchecked",
            "-Xlint",
            "-Yrangepos",
            "-Ywarn-unused:imports",
          ),
        scalacOptions ++= sys.props.get("lerna.enable.discipline").map(_ => "-Xfatal-warnings").toSeq,
        scalafixScalaBinaryVersion := CrossVersion.binaryScalaVersion(scalaVersion.value),
        // https://scalacenter.github.io/scalafix/docs/users/installation.html#sbt
        semanticdbEnabled := true,
        semanticdbVersion := scalafixSemanticdb.revision,
      ),
    ),
    name := "akka-entity-replication",
    fork in Test := true,
    parallelExecution in Test := false,
    libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-cluster-typed"    % akkaVersion,
        "com.typesafe.akka" %% "akka-stream"           % akkaVersion,
        "com.typesafe.akka" %% "akka-cluster"          % akkaVersion,
        "com.typesafe.akka" %% "akka-cluster-sharding" % akkaVersion,
        "com.typesafe.akka" %% "akka-persistence"      % akkaVersion,
        // persistence-query 2.6.x を明示的に指定しないとエラーになる。
        // 恐らく akka-persistence-inmemory の影響である。
        "com.typesafe.akka" %% "akka-persistence-query"  % akkaVersion,
        "io.altoo"          %% "akka-kryo-serialization" % "1.1.5"     % Test,
        "com.typesafe.akka" %% "akka-slf4j"              % akkaVersion % Test,
        "ch.qos.logback"     % "logback-classic"         % "1.2.3"     % Test,
        "org.scalatest"     %% "scalatest"               % "3.0.9"     % Test,
        "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion % Test,
        // akka-persistence-inmemory が 2.6.x 系に対応していない。
        // TODO 2.6.x 系に対応できる方法に変更する。
        "com.github.dnvriend" %% "akka-persistence-inmemory" % "2.5.15.2" % Test,
      ),
    inConfig(MultiJvm)(
      // multi-jvm ディレクトリをフォーマットするために必要
      scalafmtConfigSettings
      ++ scalafixConfigSettings(MultiJvm)
      ++ Seq(
        scalatestOptions ++= Seq(
            "-u",
            "target/multi-jvm-test-reports",
          ),
      ),
    ),
    // doc
    Compile / doc / autoAPIMappings := true,
    git.remoteRepo := "git@github.com:lerna-stack/akka-entity-replication.git",
    // test-coverage
    coverageMinimum := 80,
    coverageFailOnMinimum := true,
    coverageExcludedPackages := Seq(
        "lerna\\.akka\\.entityreplication\\.protobuf\\.msg\\..*",
      ).mkString(";"),
    // scalapb
    Compile / PB.targets := Seq(
        scalapb.gen(flatPackage = true, lenses = false, grpc = false) -> (sourceManaged in Compile).value / "scalapb",
      ),
    // mima
    mimaPreviousArtifacts := previousStableVersion.value.map(organization.value %% moduleName.value % _).toSet,
    mimaReportSignatureProblems := true, // check also generic parameters
  )

addCommandAlias(
  "testCoverage",
  Seq(
    "clean",
    "coverage",
    "test",
    "multi-jvm:test",
    "coverageReport",
  ).mkString(";"),
)
