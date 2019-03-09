package kvstore

import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor.{Actor, ActorLogging, ActorRef, OneForOneStrategy, Props}
import kvstore.Arbiter._

import scala.concurrent.duration._
import scala.language.postfixOps

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  case class OperationStatus(id: Long, client: ActorRef)

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
  import Persistence._
  import Replica._
  import Replicator._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var persistence: ActorRef = _

  //TODO manage failure of persistence service (customize supervision strategy)

  var kv = Map.empty[String, (String, Long)]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  // a map from operation id to key, requester and missing acks
  var replicationAcks = Map.empty[Long, (String, Option[ActorRef], Int)]
  // a map from seq to requester
  var persistenceAcks = Map.empty[Long, ActorRef]
  // a map from key to the last operation
  //var lastOperation = Map.empty[String, Long]

  val firstSeqNum = 0

  var _replicatorId = 0L
  def nextReplicatorId(): Long = {
    val ret = _replicatorId
    _replicatorId += 1
    ret
  }

  def replicate(
      op: Operation,
      replicators: Set[ActorRef],
      currentReplicationAcks: Map[Long, (String, Option[ActorRef], Int)])
    : Map[Long, (String, Option[ActorRef], Int)] = {
//    log.info("replicators {}", replicators)
    val replicationMsg = op match {
      case Insert(key, value, id) => Replicate(key, Some(value), id)
      case Remove(key, id)        => Replicate(key, None, id)
    }
    replicators foreach { x =>
      log.info("replicate {} on {}", replicationMsg, x)
      x ! replicationMsg
    }
    if (replicators.nonEmpty) {
      /*context.system.scheduler.scheduleOnce(50 milliseconds) {
        self ! ReplicateRetry(replicationMsg.key, replicationMsg.valueOption, replicationMsg.id)
      }*/
      currentReplicationAcks + ((op.id,
                                 (op.key, Some(sender), replicators.size)))
    } else {
      currentReplicationAcks
    }
  }

  override val supervisorStrategy: OneForOneStrategy = OneForOneStrategy() {
    case _: PersistenceException => Restart
    case t =>
      super.supervisorStrategy.decider.applyOrElse(t, (_: Any) => Escalate)
  }

  override def preStart(): Unit = {
    log.debug("join the replica set")
    arbiter ! Join
    log.debug("start persistence service")
    persistence = context.actorOf(persistenceProps, "persistence")
  }

  def receive: PartialFunction[Any, Unit] = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica(firstSeqNum))
  }

  /* Behavior for the leader role. */
  def leader: Receive = {
    case Insert(key, value, id) =>
      log.info("upsert {}: {} -> {}", id, key, value)
      kv += ((key, (value, id)))
      log.debug("replicate upsert {} on {}", id, secondaries.keys)
      val updatedReplicationAcks =
        replicate(Insert(key, value, id), replicators, replicationAcks)
      replicationAcks = updatedReplicationAcks
      log.debug("persist insert")
      persistence ! Persist(key, Some(value), id)
      context.system.scheduler.scheduleOnce(50 milliseconds) {
        self ! PersistRetry(key, Some(value), id)
      }
      persistenceAcks += id -> sender
      val client = sender
      context.system.scheduler.scheduleOnce(1 second) {
        log.info("check status of operation {}", id)
        self ! OperationStatus(id, client)
      }

    case Remove(key, id) =>
      log.info("remove {}: {}", id, key)
      kv -= key
      log.debug("replicate remove {} on secondary nodes", id)
      val updatedReplicationAcks =
        replicate(Remove(key, id), replicators, replicationAcks)
      replicationAcks = updatedReplicationAcks
      log.debug("persist remove")
      persistence ! Persist(key, None, id)
      context.system.scheduler.scheduleOnce(50 milliseconds) {
        self ! PersistRetry(key, None, id)
      }
      persistenceAcks += id -> sender
      val client = sender
      context.system.scheduler.scheduleOnce(1 second) {
        self ! OperationStatus(id, client)
      }

    case Get(key, id) =>
      log.info("get {}", key)
      val operationReply = kv get key match {
        case Some((value, _)) => GetResult(key, Some(value), id)
        case None             => GetResult(key, None, id)
      }
      sender ! operationReply

    case Replicas(replicas) =>
      val newReplicas = (replicas &~ secondaries.keySet) - self
//      log.debug("new replicas: {}", newReplicas)
      val deadReplicas = secondaries.keySet &~ replicas
//      log.debug("dead replicas: {}", deadReplicas)
      val replicatorsToKill = for {
        replica <- deadReplicas
        replicator <- secondaries.get(replica)
      } yield replicator
//      log.debug("kill replicators that belong to dead replicas: {}", replicatorsToKill)
      replicatorsToKill foreach context.stop //TODO is it better to kill the replicator with a poison pill?
      for {
        (id, (key, _, _)) <- replicationAcks
        _ <- replicatorsToKill
      } self ! Replicated(key, id)
      val newReplicators = for (replica <- newReplicas) yield {
        val replicator = context.actorOf(Replicator.props(replica),
                                         "replicator-" + nextReplicatorId())
        val newReplicationAcks = for ((key, (value, id)) <- kv) yield {
          val newValue = replicationAcks get id match {
            case Some((_, requester, missingAcks)) =>
              (id, (key, requester, missingAcks + 1))
            case None if persistenceAcks get id nonEmpty =>
              (id, (key, Some(persistenceAcks(id)), 1))
            case None if persistenceAcks get id isEmpty =>
              (id, (key, None, 1))
          }
          replicator ! Replicate(key, Some(value), id)
          //TODO add retries
          newValue
        }
        replicationAcks ++= newReplicationAcks
//        log.debug("updated replication acks {}", replicationAcks)
        replicator
      }
      replicators --= replicatorsToKill
      replicators ++= newReplicators
      secondaries --= deadReplicas
      secondaries ++= newReplicas zip newReplicators
//      log.info("updated secondaries {}", secondaries)
      log.debug("replication acks after replicas message: {}", replicationAcks)

    case Replicated(_, id) if persistenceAcks get id isEmpty =>
      replicationAcks(id) match {
        case (_, Some(requester), 1) =>
          log.debug("complete request {}", id)
          requester ! OperationAck(id)
          replicationAcks -= id
        case (key, Some(requester), missingAcks) =>
          replicationAcks += ((id, (key, Some(requester), missingAcks - 1)))
        case (_, None, 1) =>
          replicationAcks -= id
        case (key, None, missingAcks) =>
          replicationAcks += ((id, (key, None, missingAcks - 1)))
      }

    case Replicated(_, id) if persistenceAcks get id nonEmpty =>
      replicationAcks(id) match {
        case (_, _, 1) => replicationAcks -= id
        case (key, requester, missingAcks) =>
          replicationAcks += ((id, (key, requester, missingAcks - 1)))
      }
      log.debug("operation {} replicated, replication acks: {}",
                replicationAcks)

    case PersistRetry(key, valueOption, id)
        if persistenceAcks get id nonEmpty =>
      log.debug("retry to persist operation {}", id)
      persistence ! Persist(key, valueOption, id)
      context.system.scheduler.scheduleOnce(100 milliseconds) {
        self ! PersistRetry(key, valueOption, id)
      }

    case Persisted(_, id) if replicationAcks get id isEmpty =>
//      log.info("operation {} persisted and all replication acks received", id)
      log.debug("complete request {}", id)
      persistenceAcks(id) ! OperationAck(id)
      persistenceAcks -= id

    case Persisted(_, id) if replicationAcks get id nonEmpty =>
      log.info("persisted operation {}", id)
      persistenceAcks -= id

    case OperationStatus(id, client)
        if (replicationAcks get id nonEmpty) || (persistenceAcks get id nonEmpty) =>
      log.error("operation {} failed", id)
      client ! OperationFailed(id)
  }

  /* Behavior for the replica role. */
  def replica(expectedSeq: Long): Receive = {
    case Insert(_, _, id) =>
      log.error("insert not permitted on replica")
      sender ! OperationFailed(id)

    case Remove(_, id) =>
      log.error("remove not permitted on replica")
      sender ! OperationFailed(id)

    case Get(key, id) =>
      log.info("get {}", key)
      val operationReply = kv get key match {
        case Some((value, _)) => GetResult(key, Some(value), id)
        case None             => GetResult(key, None, id)
      }
      sender ! operationReply

    case Snapshot(key, _, seq) if seq < expectedSeq =>
      log.debug("seq num lower than expected")
      sender ! SnapshotAck(key, seq)

    case Snapshot(key, Some(value), seq) if seq == expectedSeq =>
      kv += ((key, (value, seq)))
      log.debug("persist insert {}", seq)
      persistence ! Persist(key, Some(value), seq)
      context.system.scheduler.scheduleOnce(100 milliseconds) {
        self ! PersistRetry(key, Some(value), seq)
      }
      persistenceAcks += seq -> sender

    case Snapshot(key, None, seq) if seq == expectedSeq =>
      log.debug("remove {}", key)
      kv -= key
      log.debug("persist remove")
      persistence ! Persist(key, None, seq)
      context.system.scheduler.scheduleOnce(100 milliseconds) {
        self ! PersistRetry(key, None, seq)
      }
      persistenceAcks += seq -> sender

    case PersistRetry(key, valueOption, seq)
        if persistenceAcks get seq nonEmpty =>
      log.debug("retry to persist operation {}", seq)
      persistence ! Persist(key, valueOption, seq)
      context.system.scheduler.scheduleOnce(100 milliseconds) {
        self ! PersistRetry(key, valueOption, seq)
      }

    case Persisted(key, seq) =>
      log.debug("operation {} has been persisted", seq)
      val requester = persistenceAcks(seq)
      persistenceAcks -= seq
      log.debug("send snapshot ack to {}", requester)
      requester ! SnapshotAck(key, seq)
      context.become(replica(expectedSeq + 1))
  }
}
