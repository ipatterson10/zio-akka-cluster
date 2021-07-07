package zio.akka.cluster.pubsub

import akka.actor.typed.pubsub.Topic
import akka.actor.typed.{ ActorRef, ActorSystem }
import zio.akka.cluster.pubsub.impl.{ PublisherImpl, SubscriberImpl }
import zio.{ Has, Queue, Ref, Tag, Task, ZIO }

import scala.reflect.ClassTag

/**
 *  A `Publisher[A]` is able to send messages of type `A` through Akka PubSub.
 */
trait Publisher[A] {
  def publish(topic: String, data: A): Task[Unit]
}

/**
 *  A `Subscriber[A]` is able to receive messages of type `A` through Akka PubSub.
 */
trait Subscriber[A] {

  def listen(topic: String, group: Option[String] = None): Task[Queue[A]] =
    Queue.unbounded[A].tap(listenWith(topic, _, group))

  def listenWith(topic: String, queue: Queue[A], group: Option[String] = None): Task[Unit]
}

/**
 *  A `PubSub[A]` is able to both send and receive messages of type `A` through Akka PubSub.
 */
trait PubSub[A] extends Publisher[A] with Subscriber[A]

object PubSub {

  /**
   *  Creates a new `Publisher[A]`.
   */
  def createPublisher[A: Tag]: ZIO[Has[ActorSystem[_]], Throwable, Publisher[A]] =
    for {
      sys <- ZIO.access[Has[ActorSystem[_]]](_.get)
      ref  ← Ref.make[Map[String, ActorRef[Topic.Command[A]]]](Map.empty)
    } yield new Publisher[A] with PublisherImpl[A] {
      val actorSystem: ActorSystem[_]        = sys
      val topics: Ref[Map[String, TopicRef]] = ref
      implicit val tag: ClassTag[A]          = ClassTag(implicitly[Tag[A]].getClass)
    }

  /**
   *  Creates a new `Subscriber[A]`.
   */
  def createSubscriber[A: Tag]: ZIO[Has[ActorSystem[_]], Throwable, Subscriber[A]] =
    ZIO.access[Has[ActorSystem[_]]](_.get).map { sys ⇒
      new Subscriber[A] with SubscriberImpl[A] {
        val actorSystem: ActorSystem[_] = sys
        implicit val tag: ClassTag[A]   = ClassTag(implicitly[Tag[A]].getClass)
      }
    }

  /**
   *  Creates a new `PubSub[A]`.
   */
  def createPubSub[A: Tag]: ZIO[Has[ActorSystem[_]], Throwable, PubSub[A]] =
    for {
      sys <- ZIO.access[Has[ActorSystem[_]]](_.get)
      ref  ← Ref.make[Map[String, ActorRef[Topic.Command[A]]]](Map.empty)
    } yield new PubSub[A] with PublisherImpl[A] with SubscriberImpl[A] {
      val actorSystem: ActorSystem[_] = sys

      val topics: Ref[Map[String, TopicRef]] = ref

      implicit val tag: ClassTag[A] = ClassTag(implicitly[Tag[A]].getClass)
    }

}
