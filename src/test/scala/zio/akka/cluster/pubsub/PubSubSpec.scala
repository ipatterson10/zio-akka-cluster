package zio.akka.cluster.pubsub

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import com.typesafe.config.{ Config, ConfigFactory }
import zio.test.Assertion._
import zio.test._
import zio.test.environment.TestEnvironment
import zio.{ ExecutionStrategy, Has, Managed, Task, ZLayer }

object PubSubSpec extends DefaultRunnableSpec {

  val config: Config = ConfigFactory.parseString(s"""
                                                    |akka {
                                                    |  actor {
                                                    |    provider = "cluster"
                                                    |  }
                                                    |  remote {
                                                    |    artery.canonical {
                                                    |      hostname = "127.0.0.1"
                                                    |      port = 2551
                                                    |    }
                                                    |  }
                                                    |  cluster {
                                                    |    seed-nodes = ["akka://Test@127.0.0.1:2551"]
                                                    |  }
                                                    |}
           """.stripMargin)

  val actorSystem: ZLayer[Any, Throwable, Has[ActorSystem[_]]] =
    ZLayer.fromManaged(
      Managed.make(Task(ActorSystem[Any](Behaviors.ignore, "Test", config)))(sys =>
        Task.fromFuture(_ ⇒ sys.whenTerminated).unit.either
      )
    )

  val topic = "topic"
  val msg   = "yo"

  def spec: ZSpec[TestEnvironment, Any] =
    suite("PubSubSpec")(
      testM("send and receive a single message") {
        assertM(
          for {
            pubSub <- PubSub.createPubSub[String]
            queue  <- pubSub.listen(topic)
            _      <- pubSub.publish(topic, msg)
            item   <- queue.take
          } yield item
        )(equalTo(msg)).provideLayer(actorSystem)
      },
      testM("support multiple subscribers") {
        assertM(
          for {
            pubSub <- PubSub.createPubSub[String]
            queue1 <- pubSub.listen(topic)
            queue2 <- pubSub.listen(topic)
            _      <- pubSub.publish(topic, msg)
            item1  <- queue1.take
            item2  <- queue2.take
          } yield (item1, item2)
        )(equalTo((msg, msg))).provideLayer(actorSystem)
      },
      testM("support multiple publishers") {
        val msg2 = "what's up"
        assertM(
          for {
            pubSub <- PubSub.createPubSub[String]
            queue  <- pubSub.listen(topic)
            _      <- pubSub.publish(topic, msg)
            _      <- pubSub.publish(topic, msg2)
            item1  <- queue.take
            item2  <- queue.take
          } yield (item1, item2)
        )(equalTo((msg, msg2))).provideLayer(actorSystem)
      },
      testM("send only one message to a single group") {
        val group = "group"
        assertM(
          for {
            pubSub <- PubSub.createPubSub[String]
            queue1 <- pubSub.listen(topic, Some(group))
            queue2 <- pubSub.listen(topic, Some(group))
            _      <- pubSub.publish(topic, msg)
            item   <- queue1.take race queue2.take
            sizes  <- queue1.size zip queue2.size
          } yield (item, sizes)
        )(equalTo((msg, (0, 0)))).provideLayer(actorSystem)
      },
      testM("send one message to each group") {
        val group1 = "group1"
        val group2 = "group2"
        assertM(
          for {
            pubSub <- PubSub.createPubSub[String]
            queue1 <- pubSub.listen(topic, Some(group1))
            queue2 <- pubSub.listen(topic, Some(group2))
            _      <- pubSub.publish(topic, msg)
            item1  <- queue1.take
            item2  <- queue2.take
          } yield List(item1, item2)
        )(equalTo(List(msg, msg))).provideLayer(actorSystem)
      }
    )

  override def aspects: List[TestAspect[Nothing, TestEnvironment, Nothing, Any]] =
    List(TestAspect.executionStrategy(ExecutionStrategy.Sequential))
}
