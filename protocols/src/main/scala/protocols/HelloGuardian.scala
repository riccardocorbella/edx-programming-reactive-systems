package protocols

import akka.Done
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.util.Timeout

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

// This object illustrates how to implement the guardian pattern
// as shown in the lecture
object HelloGuardian {
    sealed trait Command
    final case class Greet(whom: String, replyTo: ActorRef[Done]) extends Command
    final case object Stop extends Command

    val greeter: Behavior[Command] =
        Behaviors.receiveMessage[Command] {
            case Greet(whom, replyTo) =>
                println(s"Hello $whom!")
                replyTo ! Done
                Behaviors.same
            case Stop =>
                println("shutting down ...")
                Behaviors.stopped
        }

    sealed trait Guardian
    final case class NewGreeter(replyTo: ActorRef[ActorRef[Command]]) extends Guardian
    final case object Shutdown extends Guardian

    val guardian = Behaviors.receive[Guardian] {
        case (ctx, NewGreeter(replyTo)) =>
            val ref = ctx.spawnAnonymous(greeter)
            replyTo ! ref
            Behavior.same
        case (_, Shutdown) =>
            Behavior.stopped
    }

    def main(args: Array[String]): Unit = {
        val system: ActorSystem[Guardian] = ActorSystem(guardian, "helloworld")
    
        import akka.actor.typed.scaladsl.AskPattern._
        implicit val ec = ExecutionContext.global
        implicit val timeout = Timeout(3.seconds)
        implicit val scheduler = system.scheduler
    
        for {
            greeter <- system ? NewGreeter
            _ = greeter ! Greet("world", system.deadLetters)
            another <- system ? NewGreeter
            _ = another ! Greet("there", system.deadLetters)
            _ = another ! Stop
            _ = greeter ! Greet("again", system.deadLetters)
        } system ! Shutdown
    }
}
