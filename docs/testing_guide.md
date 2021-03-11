# Testing Guide

## Testing ReplicationActors

`akka-entity-replication` requires implementing your Entities with special trait `ReplicationActor`.

`TestReplicationActorProps` allows testing behavior of an entity with `akka-testkit`.
First, you need to add a dependency to your project.

For more details for `akka-testkit`, see the following page.

[Testing Classic Actors • Akka Documentation](https://doc.akka.io/docs/akka/2.6/testing.html)

The `TestReplicationActorProps` allows to test `ReplicationActor` like a normal Actors.
For more information on the behavior of `TestReplicationActorProps`, please see [TestReplicationActorPropsSpec](/src/test/scala/lerna/akka/entityreplication/testkit/TestReplicationActorPropsSpec.scala).

```scala
import akka.actor.ActorSystem
import akka.testkit.{ ImplicitSender, TestActors, TestKit }
import org.scalatest.{ Matchers, WordSpecLike, BeforeAndAfterAll }
import lerna.akka.entityreplication.testkit.TestReplicationActorProps

class WordCountReplicationActorSpec 
  extends TestKit(ActorSystem("WordCountReplicationActorSpec")) 
    with ImplicitSender 
    with WordSpecLike 
    with Matchers 
    with BeforeAndAfterAll {
  
  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  import WordCountReplicationActor._
  
  "A WordCountReplicationActorSpec" should {
    
    "send back a Counted event after sending a CountWord command" in {
      
      val actor = system.actorOf(TestReplicationActorProps(WordCountReplicationActor.props))
      
      actor ! CountWord("hello")
      expectMsg(Counted(wordCount = "hello".length))
    }
  }
}
```
