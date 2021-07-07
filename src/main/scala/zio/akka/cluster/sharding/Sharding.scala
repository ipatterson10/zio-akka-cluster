package zio.akka.cluster.sharding

import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.{ ActorContext, Behaviors }
import akka.actor.typed.{ ActorRef, ActorSystem, Behavior }
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.scaladsl.{ ClusterSharding, EntityContext, EntityTypeKey, Entity ⇒ ShardEntity }
import akka.util.Timeout
import zio.{ =!=, Has, Ref, Runtime, Tag, Task, UIO, ZIO, ZLayer }

import scala.concurrent.duration._
import scala.reflect.ClassTag

/**
 *  A `Sharding[M]` is able to send messages of type `M` to a sharded entity or to stop one.
 */
trait Sharding[M] {

  def send(entityId: String, data: M): Task[Unit]

  def stop(entityId: String): Task[Unit]

  def passivate(entityId: String): Task[Unit]

  def ask[R <: M](entityId: String, data: M)(implicit tag: ClassTag[R], proof: R =!= Nothing): Task[R]

}

object Sharding {

  def start[R <: Has[_], Msg, State: Tag](
    name: String,
    onMessage: Msg => ZIO[Entity[State] with R, Nothing, Unit],
    askTimeout: FiniteDuration = 10.seconds
  ): ZIO[Has[ActorSystem[_]] with R, Throwable, Sharding[Msg]] =
    for {
      rts         ← ZIO.runtime[Has[ActorSystem[_]] with R]
      actorSystem = rts.environment.get[ActorSystem[_]]
      typeKey     ← Task(EntityTypeKey[MessageEnvelope.Payload](name))
      sharding    ← Task(ClusterSharding(actorSystem))
      entity      ← Task(
                      sharding.init[MessageEnvelope.Payload, ShardingEnvelope[MessageEnvelope.Payload]](
                        ShardEntity[MessageEnvelope.Payload](typeKey)(ec ⇒ new ShardEntity(rts, ec)(onMessage).behavior)
                      )
                    )
    } yield new Sharding[Msg] {
      def send(entityId: String, data: Msg): Task[Unit] =
        Task(entity ! ShardingEnvelope(entityId = entityId, message = MessageEnvelope.MessagePayload(data)))

      def stop(entityId: String): Task[Unit] =
        Task(entity ! ShardingEnvelope(entityId = entityId, message = MessageEnvelope.PoisonPillPayload))

      def passivate(entityId: String): Task[Unit] =
        Task(entity ! ShardingEnvelope(entityId = entityId, message = MessageEnvelope.PassivatePayload))

      def ask[R0 <: Msg](entityId: String, data: Msg)(implicit tag: ClassTag[R0], proof: R0 =!= Nothing): Task[R0] =
        ZIO.fromFuture { _ =>
          implicit val scheduler        = actorSystem.scheduler
          implicit val timeout: Timeout = Timeout(askTimeout)
          entity.ask[R0](ref ⇒
            ShardingEnvelope(entityId = entityId, message = MessageEnvelope.MessagePayload(data, Some(ref)))
          )
        }
    }

  class ShardEntity[R <: Has[_], Msg, State: Tag](rts: Runtime[R], ec: EntityContext[MessageEnvelope.Payload])(
    onMessage: Msg => ZIO[Entity[State] with R, Nothing, Unit]
  ) {
    val behavior: Behavior[MessageEnvelope.Payload] =
      Behaviors.setup[MessageEnvelope.Payload] { ctx ⇒
        val ref: Ref[Option[State]]        = rts.unsafeRun(Ref.make[Option[State]](None))
        val service: Entity.Service[State] = new Entity.Service[State] {
          def context: ActorContext[_]                                   = ctx
          def replyToSender[M](msg: M, replyTo: ActorRef[M]): Task[Unit] = Task(replyTo ! msg)
          def id: String                                                 = ctx.self.path.name
          def state: Ref[Option[State]]                                  = ref
          def stop: UIO[Unit]                                            = UIO(ctx.stop(ctx.self))
          def passivate: UIO[Unit]                                       = UIO(ec.shard ! ClusterSharding.Passivate(ctx.self))
          def passivateAfter(duration: Duration): UIO[Unit]              =
            UIO(ctx.setReceiveTimeout(FiniteDuration(duration.length, duration.unit), MessageEnvelope.PassivatePayload))
        }

        val entity: ZLayer[Any, Nothing, Entity[State]] = ZLayer.succeed(service)

        Behaviors.receiveMessage {
          case MessageEnvelope.PoisonPillPayload   ⇒
            Behaviors.stopped
          case MessageEnvelope.PassivatePayload    ⇒
            ec.shard ! ClusterSharding.Passivate(ctx.self)
            Behaviors.same
          case MessageEnvelope.MessagePayload(msg) ⇒
            rts.unsafeRunSync(onMessage(msg.asInstanceOf[Msg]).provideSomeLayer[R](entity))
            Behaviors.same
        }

      }
  }

}
