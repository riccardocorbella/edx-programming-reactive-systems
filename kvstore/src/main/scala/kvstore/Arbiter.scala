package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef}

object Arbiter {
  case object Join

  case object JoinedPrimary
  case object JoinedSecondary

  /**
    * This message contains all replicas currently known to the arbiter, including the primary.
    */
  case class Replicas(replicas: Set[ActorRef])
}

class Arbiter extends Actor with ActorLogging {
  import Arbiter._
  var leader: Option[ActorRef] = None
  var replicas = Set.empty[ActorRef]

  def receive: PartialFunction[Any, Unit] = {
    case Join =>
      if (leader.isEmpty) {
        log.debug("{} joined as primary", sender)
        leader = Some(sender)
        replicas += sender
        sender ! JoinedPrimary
      } else {
        /*replicas += context.actorOf(Unreliable.props(sender))
        sender ! JoinedSecondary*/
        replicas += sender
        sender ! JoinedSecondary
      }
      leader foreach (_ ! Replicas(replicas))
  }

}
