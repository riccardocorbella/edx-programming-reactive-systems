package kvstore

import akka.actor.{
  Actor,
  ActorLogging,
  ActorRef,
  Cancellable,
  OneForOneStrategy,
  PoisonPill,
  Props,
  SupervisorStrategy,
  Terminated
}
import kvstore.Arbiter._

import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart

import scala.annotation.tailrec
import akka.pattern.{ask, pipe}

import scala.concurrent.duration._
import akka.util.Timeout

import scala.language.postfixOps

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long)
      extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props =
    Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props)
    extends Actor
    with ActorLogging {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var persistence: ActorRef = _

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  // a map from operation id to key, requester and missing acks
  var replicationAcks = Map.empty[Long, (String, ActorRef, Int)]
  // a set with the operation ids that have to be acknowledged by the persistence service
  var persistenceAcks = Map.empty[Long, (ActorRef, Cancellable)]

  def replicate(op: Operation,
                replicators: Set[ActorRef],
                currentReplicationAcks: Map[Long, (String, ActorRef, Int)])
    : Map[Long, (String, ActorRef, Int)] = {
    val replicationMsg = op match {
      case Insert(key, value, id) => Replicate(key, Some(value), id)
      case Remove(key, id)        => Replicate(key, None, id)
    }
    replicators foreach (_ ! replicationMsg)
    currentReplicationAcks + ((op.id, (op.key, sender, replicators.size)))
  }

  override def preStart(): Unit = {
    arbiter ! Join
    log.debug("start persistence service")
    persistence = context actorOf persistenceProps
  }

  def receive: PartialFunction[Any, Unit] = {
    case JoinedPrimary => context.become(leader)
//    case JoinedSecondary => context.become(replica)
    case JoinedSecondary => context.become(replica(0))
  }

  def leader: Receive = {
    case Insert(key, value, id) =>
      log.info("upsert {} -> {}", key, value)
      kv += (key -> value)
      log.debug("replicate insert {} on secondary nodes", id)
      val updatedReplicationAcks =
        replicate(Insert(key, value, id), replicators, replicationAcks)
      replicationAcks = updatedReplicationAcks
      log.debug("persist insert")
      persistence ! Persist(key, Some(value), id)
      val scheduledPersists =
        context.system.scheduler.schedule(0 milliseconds, 500 milliseconds) {
          persistence ! Persist(key, Some(value), id)
        }
      persistenceAcks += ((id, (sender, scheduledPersists)))
    /*val operationReply = OperationAck(id)
      sender ! operationReply*/
    case Remove(key, id) =>
      log.info("remove {}", key)
      kv -= key
      log.debug("replicate remove {} on secondary nodes", id)
      val updatedReplicationAcks =
        replicate(Remove(key, id), replicators, replicationAcks)
      replicationAcks = updatedReplicationAcks
      log.debug("persist remove")
      persistence ! Persist(key, None, id)
      val scheduledPersists =
        context.system.scheduler.schedule(0 milliseconds, 500 milliseconds) {
          persistence ! Persist(key, None, id)
        }
      persistenceAcks += ((id, (sender, scheduledPersists)))
    /*val operationReply = OperationAck(id)
      sender ! operationReply*/
    case Get(key, id) =>
      log.info("get {}", key)
      val valueOption = kv.get(key).orElse(None)
      val operationReply = GetResult(key, valueOption, id)
      sender ! operationReply
    case Replicas(replicas) =>
      log.debug("compute the new replicas")
      val newReplicas = replicas &~ secondaries.keySet
      log.debug("compute the dead replicas")
      val deadReplicas = secondaries.keySet &~ replicas
      log.debug("identify the replicators to kill")
      val replicatorsToKill = for {
        replica <- deadReplicas
        replicator <- secondaries.get(replica)
      } yield replicator
      log.debug("kill replicators that belong to dead replicas")
      replicatorsToKill foreach context.stop //TODO is it better to kill the replicator with a poison pill?
      for {
        (id, (key, _, _)) <- replicationAcks
        _ <- replicatorsToKill
      } self ! Replicated(key, id)
      replicators --= replicatorsToKill
      log.debug("update secondaries")
      val newReplicators = for (replica <- newReplicas) yield {
        val replicator = context.actorOf(Replicator.props(replica)) //TODO add name
        for (((key, value), id) <- kv.zipWithIndex)
          //TODO check if is correct to generate ids in this way
          replicator ! Replicate(key, Some(value), id)
        replicator
      }
      secondaries ++= newReplicas zip newReplicators
      secondaries --= deadReplicas
    case Replicated(_, id) =>
      val (key, requester, missingAcks) = replicationAcks(id)
      missingAcks match {
        case 0 =>
          requester ! OperationAck(id)
          replicationAcks -= id
          log.debug("completed request {}", id)
        case _ =>
          replicationAcks += ((id, (key, requester, missingAcks - 1)))
      }
    case Persisted(_, id) =>
      log.debug("operation {} has been persisted", id)
      val (_, scheduledPersists) = persistenceAcks(id)
      scheduledPersists.cancel()
      persistenceAcks -= id
    //TODO maybe send ack to client
  }

  /* TODO Behavior for the replica role. */
  def replica(expectedSeq: Long): Receive = {
    case Insert(_, _, id) =>
      log.error("insert not permitted on replica")
      sender ! OperationFailed(id)
    case Remove(_, id) =>
      log.error("remove not permitted on replica")
      sender ! OperationFailed(id)
    case Get(key, id) =>
      log.info("get {}", key)
      val valueOption = kv get key orElse None
      val operationReply = GetResult(key, valueOption, id)
      sender ! operationReply
    case Snapshot(key, _, seq) if seq < expectedSeq =>
      log.debug("seq num lower than expected")
      sender ! SnapshotAck(key, seq)
    case Snapshot(key, Some(value), seq) if seq == expectedSeq =>
      log.debug("insert {} -> {}", key, value)
      kv += key -> value
      log.debug("persist insert")
      persistence ! Persist(key, Some(value), seq)
      val scheduledPersists =
        context.system.scheduler.schedule(0 milliseconds, 100 milliseconds) {
          persistence ! Persist(key, Some(value), seq)
        }
      persistenceAcks += ((seq, (sender, scheduledPersists)))
//      sender ! SnapshotAck(key, seq)
//      context.become(replica(expectedSeq + 1))
    case Snapshot(key, None, seq) if seq == expectedSeq =>
      log.debug("remove {}", key)
      kv -= key
      persistence ! Persist(key, None, seq)
      val scheduledPersists =
        context.system.scheduler.schedule(0 milliseconds, 100 milliseconds) {
          persistence ! Persist(key, None, seq)
        }
      persistenceAcks += ((seq, (sender, scheduledPersists)))
//      sender ! SnapshotAck(key, seq)
//      context.become(replica(expectedSeq + 1))
    case Persisted(key, seq) =>
      log.debug("operation {} has been persisted", seq)
      val (replicator, scheduledPersists) = persistenceAcks(seq)
      log.debug("send snapshot ack to {}", context.parent)
      replicator ! SnapshotAck(key, seq)
      scheduledPersists.cancel()
      persistenceAcks -= seq
      context.become(replica(expectedSeq + 1))
  }
}
