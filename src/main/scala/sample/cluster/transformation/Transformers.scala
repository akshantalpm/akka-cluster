package sample.cluster.transformation

import akka.actor._
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberUp}
import akka.cluster.{Cluster, Member, MemberStatus}
import com.typesafe.config.ConfigFactory

import scala.language.postfixOps

class Transformers extends Actor {
  var pims = IndexedSeq.empty[ActorRef]
  var cadcs = IndexedSeq.empty[ActorRef]
  var jobCounter = 0
  val cluster = Cluster(context.system)

  override def preStart(): Unit = cluster.subscribe(self, classOf[MemberUp])
  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive = {
    case PimRegistration if !pims.contains(sender()) =>
      context watch sender()
      pims = pims :+ sender()
    case CadcRegistration if !cadcs.contains(sender()) =>
      context watch sender()
      cadcs = cadcs :+ sender()
    case job: TransformationJob if cadcs.isEmpty =>
      sender() ! JobFailed("Service cadc is unavailable, try again later", job)
    case TransformationJob(text) =>
      cadcs(jobCounter % cadcs.size) ! TransformationResult(text.toUpperCase)
    case state: CurrentClusterState =>
      state.members.filter(_.status == MemberStatus.Up) foreach register
    case MemberUp(m) => register(m)
  }

  def register(member: Member): Unit =
    if (member.hasRole("pim")) {
      context.actorSelection(RootActorPath(member.address) / "user" / "pim") ! TransformersRegistration
      context.actorSelection(RootActorPath(member.address) / "user" / "cadc") ! TransformersRegistration
    }
}

object Transformers {
  def main(args: Array[String]): Unit = {
    val port = if (args.isEmpty) "0" else args(0)
    val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port").
      withFallback(ConfigFactory.parseString("akka.cluster.roles = [transformers]")).
      withFallback(ConfigFactory.load())

    val system = ActorSystem("ClusterSystem", config)
    system.actorOf(Props[Transformers], name = "transformers")
  }
}