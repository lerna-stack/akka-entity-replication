# Contributing

## Requirements
- [Java 8](https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html)
- [SBT](https://www.scala-sbt.org/index.html)

## Workflow
1. Create your fork of this repository
2. Create a local branch based on `master`  
3. Work in the branch
4. Push the branch into your repository
5. Create a Pull Request to the `master` branch of this repository

## Code Style
We use [Scalafmt](https://scalameta.org/scalafmt/) to format the source code.  
We recommend you set up your editor as documented [here](https://scalameta.org/scalafmt/docs/installation.html).

## Run Tests
```shell
sbt test
```

## Run Integration Tests
```shell
sbt multi-jvm:test
```

If we want to dig in failed test cases in integration tests,
we can look at integration test reports `target/multi-jvm-test-reports/*.xml` using [xunit-viewer](https://www.npmjs.com/package/xunit-viewer).

Tips: Integration tests is not stable for now.

## Take test coverage
```shell
sbt testCoverage
```

A test coverage is generated in the directory `target/scala-2.13/scoverage-report`.

## Build Scaladoc
```shell
sbt doc
```

A Scaladoc is generated in the directory `target/scala-2.13/api`.