package zio.akka.cluster

import akka.actor.Address
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ ActorSystem, Behavior }
import akka.cluster.ClusterEvent._
import akka.cluster.typed.{ JoinSeedNodes, Leave, Subscribe, Unsubscribe }
import zio.Exit.{ Failure, Success }
import zio.{ Has, Queue, Runtime, Task, ZIO }

object Cluster {

  private val cluster: ZIO[Has[ActorSystem[_]], Throwable, akka.cluster.typed.Cluster] =
    for {
      actorSystem <- ZIO.access[Has[ActorSystem[_]]](_.get)
      cluster     <- Task(akka.cluster.typed.Cluster(actorSystem))
    } yield cluster

  val selfMember: ZIO[Has[ActorSystem[_]], Throwable, akka.cluster.Member] = cluster.map(_.selfMember)

  /**
   *  Returns the current state of the cluster.
   */
  val clusterState: ZIO[Has[ActorSystem[_]], Throwable, CurrentClusterState] =
    for {
      cluster <- cluster
      state   <- Task(cluster.state)
    } yield state

  /**
   *  Joins a cluster using the provided seed nodes.
   */
  def join(seedNodes: List[Address]): ZIO[Has[ActorSystem[_]], Throwable, Unit] =
    for {
      cluster <- cluster
      _       <- Task(cluster.manager ! JoinSeedNodes(seedNodes))
    } yield ()

  /**
   *  Leaves the current cluster.
   */
  val leave: ZIO[Has[ActorSystem[_]], Throwable, Unit] =
    for {
      cluster <- cluster
      _        ← Task(cluster.manager ! Leave(cluster.selfMember.address))
    } yield ()

  /**
   *  Subscribes to the current cluster events. It returns an unbounded queue that will be fed with cluster events.
   *  `initialStateAsEvents` indicates if you want to receive previous cluster events leading to the current state, or only future events.
   *  To unsubscribe, use `queue.shutdown`.
   *  To use a bounded queue, see `clusterEventsWith`.
   */
  def clusterEvents: ZIO[Has[ActorSystem[_]], Throwable, Queue[ClusterDomainEvent]] =
    Queue.unbounded[ClusterDomainEvent].tap(clusterEventsWith)

  /**
   *  Subscribes to the current cluster events, using the provided queue to push the events.
   *  `initialStateAsEvents` indicates if you want to receive previous cluster events leading to the current state, or only future events.
   *  To unsubscribe, use `queue.shutdown`.
   */
  def clusterEventsWith(
    queue: Queue[ClusterDomainEvent]
  ): ZIO[Has[ActorSystem[_]], Throwable, Unit] =
    for {
      rts         <- Task.runtime
      actorSystem <- ZIO.access[Has[ActorSystem[_]]](_.get)
      _            ← Task(
                       actorSystem.systemActorOf(SubscriberBehavior(rts, queue), "cluster-event-subscriber")
                     )
    } yield ()

  object SubscriberBehavior {

    sealed trait Message

    case class ClusterEvent(event: ClusterDomainEvent) extends Message

    case object Stop extends Message
    type Stop = Stop.type

    def apply(
      rts: Runtime[Any],
      queue: Queue[ClusterDomainEvent]
    ): Behavior[Message] =
      Behaviors.setup { ctx =>
        val eventAdapter = ctx.messageAdapter[ClusterDomainEvent](ClusterEvent.apply)
        val cluster      = akka.cluster.typed.Cluster(ctx.system)
        cluster.subscriptions ! Subscribe(eventAdapter, classOf[ClusterDomainEvent])
        Behaviors.receiveMessage {
          case ClusterEvent(event) ⇒
            rts.unsafeRunAsync(queue.offer(event)) {
              case Success(_)     ⇒ ()
              case Failure(cause) ⇒ if (cause.interrupted) ctx.self ! Stop else ()
            }
            Behaviors.same
          case Stop                ⇒
            cluster.subscriptions ! Unsubscribe(eventAdapter)
            Behaviors.stopped
        }
      }
  }
}
