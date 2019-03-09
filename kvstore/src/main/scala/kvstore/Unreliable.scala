package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, Props}

import scala.util.Random

object Unreliable {
  def props(childProps: Props) = Props(classOf[Unreliable], childProps)
}

class Unreliable(childProps: Props) extends Actor with ActorLogging {
  val child: ActorRef = context.actorOf(childProps)

  override def receive: Receive = {
    case msg if Random.nextDouble() < 0.8 =>
      log.warning("{} not forwarded to {}", msg, child)
    case msg =>
      log.info("forward {} to {}", msg, child)
      child forward msg
  }
}
