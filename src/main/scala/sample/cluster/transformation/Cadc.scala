package sample.cluster.transformation

import akka.actor._
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberUp}
import akka.cluster.{Cluster, Member, MemberStatus}
import com.typesafe.config.ConfigFactory

import scala.language.postfixOps

class Cadc extends Actor {

  val cluster = Cluster(context.system)
  var transformers = IndexedSeq.empty[ActorRef]
  var jobCounter = 0

  override def preStart(): Unit = cluster.subscribe(self, classOf[MemberUp])
  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive = {
    case job: TransformationResult =>
      println(job.text + "ack")

    case TransformersRegistration if !transformers.contains(sender()) =>
      context watch sender()
      transformers = transformers :+ sender()

    case Terminated(a) =>
      transformers = transformers.filterNot(_ == a)

    case state: CurrentClusterState =>
      state.members.filter(_.status == MemberStatus.Up) foreach register
    case MemberUp(m) => register(m)
  }

  def register(member: Member): Unit =
    if (member.hasRole("transformers"))
      context.actorSelection(RootActorPath(member.address) / "user" / "transformers") ! CadcRegistration

}

object Cadc {
  def main(args: Array[String]): Unit = {
    val port = if (args.isEmpty) "0" else args(0)
    val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port").
      withFallback(ConfigFactory.parseString("akka.cluster.roles = [cadc]")).
      withFallback(ConfigFactory.load())

    val system = ActorSystem("ClusterSystem", config)
    system.actorOf(Props[Cadc], name = "cadc")
  }
}
