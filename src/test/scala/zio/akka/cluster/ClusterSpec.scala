package zio.akka.cluster

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.ClusterEvent.MemberLeft
import akka.cluster.typed.Leave
import com.typesafe.config.{ Config, ConfigFactory }
import zio.test.Assertion._
import zio.test._
import zio.test.environment.TestEnvironment
import zio.{ Managed, Task, ZLayer }

object ClusterSpec extends DefaultRunnableSpec {

  def spec: ZSpec[TestEnvironment, Any] =
    suite("ClusterSpec")(
      testM("receive cluster events") {
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

        val actorSystem: Managed[Throwable, ActorSystem[_]] =
          Managed.make(Task(ActorSystem[Any](Behaviors.ignore, "Test", config)))(sys =>
            Task.fromFuture(_ ⇒ sys.whenTerminated).unit.either
          )

        assertM(
          for {
            queue <- Cluster.clusterEvents
            _      ← Cluster.leave
            item  <- queue.filterOutput {
                       case _: MemberLeft ⇒ true
                       case _            => false
                     }.take
          } yield item
        )(isSubtype[MemberLeft](anything)).provideLayer(ZLayer.fromManaged(actorSystem))
      }
    )
}
