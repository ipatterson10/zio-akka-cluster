package zio.akka.cluster.pubsub.impl

import akka.actor.typed.pubsub.Topic
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ ActorRef, ActorSystem, Behavior, PostStop }
import zio.Exit.{ Failure, Success }
import zio.akka.cluster.pubsub.Subscriber
import zio.{ Promise, Queue, Runtime, Task }

import scala.reflect.ClassTag

private[pubsub] trait SubscriberImpl[A] extends Subscriber[A] {
  val actorSystem: ActorSystem[_]
  implicit val tag: ClassTag[A]

  override def listenWith(topic: String, queue: Queue[A], group: Option[String] = None): Task[Unit] =
    for {
      rts        <- Task.runtime
      subscribed <- Promise.make[Nothing, Unit]
      _          <- Task(
                      actorSystem.systemActorOf(SubscriberImpl(topic, queue, rts, subscribed), s"$topic-subscriber")
                    )
      _          <- subscribed.await
    } yield ()
}

private[impl] object SubscriberImpl {

  sealed trait Message

  case class Event[M](message: M) extends Message

  case object Stop extends Message
  type Stop = Stop.type

  def apply[A: ClassTag](
    topic: String,
    queue: Queue[A],
    rts: Runtime[Any],
    subscribed: Promise[Nothing, Unit]
  ): Behavior[Message] =
    Behaviors.setup { ctx ⇒
      val adapter: ActorRef[A]                 = ctx.messageAdapter[A](Event.apply)
      val topicRef: ActorRef[Topic.Command[A]] = ctx.spawnAnonymous(Topic[A](topic))
      topicRef ! Topic.subscribe(adapter)
      rts.unsafeRun(subscribed.succeed(()))
      Behaviors
        .receiveMessage[Message] {
          case Event(msg) ⇒
            rts.unsafeRunAsync(queue.offer(msg.asInstanceOf[A])) {
              case Success(_)     ⇒ ()
              case Failure(cause) ⇒ if (cause.interrupted) ctx.self ! Stop else ()
            }
            Behaviors.same
          case Stop       ⇒
            Behaviors.stopped { () ⇒
              topicRef ! Topic.unsubscribe(adapter)
            }
        }
        .receiveSignal {
          case (_, PostStop) =>
            topicRef ! Topic.unsubscribe(adapter)
            Behaviors.stopped[Message]
        }
    }
}
