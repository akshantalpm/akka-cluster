package sample.cluster.transformation

import java.util.concurrent.atomic.AtomicInteger

import akka.actor._
import akka.cluster.ClusterEvent.{MemberUp, CurrentClusterState}
import akka.cluster.{Member, MemberStatus}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.language.postfixOps

class Pim extends Actor {

  var transformers = IndexedSeq.empty[ActorRef]
  var jobCounter = 0

  def receive = {
    case job: TransformationJob if transformers.isEmpty =>
      sender() ! JobFailed("Service transformers is unavailable, try again later", job)

    case job: TransformationJob =>
      jobCounter += 1
      transformers(jobCounter % transformers.size) forward job

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
      context.actorSelection(RootActorPath(member.address) / "user" / "transformers") !
        PimRegistration

}

object Pim {
  def main(args: Array[String]): Unit = {
    val port = if (args.isEmpty) "0" else args(0)
    val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port").
      withFallback(ConfigFactory.parseString("akka.cluster.roles = [pim]")).
      withFallback(ConfigFactory.load())

    val system = ActorSystem("ClusterSystem", config)
    val frontend = system.actorOf(Props[Pim], name = "pim")

    val counter = new AtomicInteger
    import system.dispatcher
    system.scheduler.schedule(2.seconds, 2.seconds) {
      implicit val timeout = Timeout(5 seconds)
      (frontend ? TransformationJob("hello-" + counter.incrementAndGet())) onSuccess {
        case result => println(result)
      }
    }
  }
}