/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root: ActorRef = createRoot

  // optional
  var pendingQueue: Queue[Operation] = Queue.empty[Operation]

  // optional
  def receive: Receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case operation: Operation => root ! operation
//    case GC                   => context.become(garbageCollecting(???))
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = ???

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode], elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor with ActorLogging {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees: Map[Position, ActorRef] = Map[Position, ActorRef]()
  var removed: Boolean = initiallyRemoved

  // optional
  def receive: Receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(requester, id, newElem) =>
      newElem match {
        case _ if newElem == elem                                 => removed = false
                                                                     requester ! OperationFinished(id)
        case _ if newElem < elem && subtrees.get(Left).nonEmpty   => subtrees(Left) ! Insert(requester, id , newElem)
        case _ if newElem > elem && subtrees.get(Right).nonEmpty  => subtrees(Right) ! Insert(requester, id , newElem)
        case _ if newElem < elem && subtrees.get(Left).isEmpty    => subtrees += Left -> createNewNode(newElem)
                                                                     requester ! OperationFinished(id)
        case _ if newElem > elem && subtrees.get(Right).isEmpty   => subtrees += Right -> createNewNode(newElem)
                                                                     requester ! OperationFinished(id)
      }
    case Remove(requester, id, elemToDrop) =>
      elemToDrop match {
        case _ if elemToDrop == elem                                => removed = true
                                                                       requester ! OperationFinished(id)
        case _ if elemToDrop < elem && subtrees.get(Left).nonEmpty  => subtrees(Left) ! Remove(requester, id , elemToDrop)
        case _ if elemToDrop > elem && subtrees.get(Right).nonEmpty => subtrees(Right) ! Remove(requester, id , elemToDrop)
        case _                                                      => requester ! OperationFinished(id)
      }
    case Contains(requester, id, elemToFind) =>
      elemToFind match {
        case _ if elemToFind == elem && !removed                    =>
          log.debug("node {} - element {} found!", elem, elemToFind)
          requester ! ContainsResult(id, result = true)
        case _ if elemToFind < elem && subtrees.get(Left).nonEmpty  =>
          log.debug("node {} - search element {} in the left subtree", elem, elemToFind)
          subtrees(Left) ! Contains(requester, id , elemToFind)
        case _ if elemToFind > elem && subtrees.get(Right).nonEmpty =>
          log.debug("node {} - search element {} in the right subtree", elem, elemToFind)
          subtrees(Right) ! Contains(requester, id , elemToFind)
        case _                                                      =>
          log.debug("node {} - no subtree, stop the research of {}", elem, elemToFind)
          requester ! ContainsResult(id, result = false)
      }
  }

  def createNewNode(elem: Int): ActorRef =
    context.actorOf(BinaryTreeNode.props(elem, initiallyRemoved = false))

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = ???


}
