package zio.akka.cluster.pubsub.impl

import akka.actor.typed.pubsub.Topic
import akka.actor.typed.{ ActorRef, ActorSystem }
import zio.akka.cluster.pubsub.Publisher
import zio.{ Ref, Task }

import scala.reflect.ClassTag

private[pubsub] trait PublisherImpl[A] extends Publisher[A] {
  val actorSystem: ActorSystem[_]
  type TopicRef = ActorRef[Topic.Command[A]]
  val topics: Ref[Map[String, TopicRef]]
  implicit val tag: ClassTag[A]

  private def getOrCreateTopic(topic: String): Task[TopicRef] =
    for {
      ts  ← topics.get
      opt = ts.get(topic)
      ref ← Task(opt.getOrElse(actorSystem.systemActorOf(Topic[A](topic), s"$topic-publisher")))
      _   ← topics.update(_.updated(topic, ref))
    } yield ref

  override def publish(topic: String, data: A): Task[Unit] =
    getOrCreateTopic(topic).map(ref ⇒ ref ! Topic.publish(data))

}
