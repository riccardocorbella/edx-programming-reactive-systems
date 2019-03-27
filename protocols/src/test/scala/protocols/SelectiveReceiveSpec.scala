package protocols

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors._
import akka.actor.typed.scaladsl._
import akka.actor.testkit.typed.scaladsl.{BehaviorTestKit, TestInbox}
import org.scalacheck.Gen
import org.scalatest.{FunSuite, MustMatchers}
import org.scalatest.prop.PropertyChecks

trait SelectiveReceiveSpec extends FunSuite with PropertyChecks with MustMatchers {

    def behavior[T](inbox: TestInbox[T], size: Int, seq: List[T]) =
        SelectiveReceive(size, expectOne(inbox, seq))

    def expectOne[T](inbox: TestInbox[T], seq: List[T]): Behavior[T] =
        seq match {
            case x :: xs =>
                receiveMessagePartial {
                    case `x` =>
                        inbox.ref ! x
                        expectOne(inbox, xs)
                }
            case Nil => Behaviors.ignore
        }
    
    def expectStart[T](inbox: TestInbox[T], start: T, followUp: Behavior[T]): Behavior[T] =
        receiveMessagePartial {
            case x @ `start` =>
                inbox.ref ! x
                followUp
        }

  test("A SelectiveReceive Decorator must eventually execute the behavior") {
    val values = List("A", "B", "C")
    val abc = Gen.oneOf(values)
    val abcs = Gen.choose(0, 30).flatMap(Gen.listOfN(_, abc))

    forAll((abcs, "abcs")) { list =>
      val i = TestInbox[String]()
      val b = behavior(i, 30, values)
      val testkit = BehaviorTestKit(b, "eventually execute")
      list.foreach(value => {
        testkit.ref ! value
        testkit.runOne()
      })
      val delivered = i.receiveAll()
      delivered mustBe sorted
      values.foldLeft(true) { (prev, v) =>
        val contained = prev && list.contains(v)
        withClue(s"testing for $v when list=$list and delivered=$delivered: ") {
          delivered.count(_ == v) must be(if (contained) 1 else 0)
        }
        contained
      }
    }
  }

  test("A SelectiveReceive Decorator must tolerate worst-case sorting") {
    val values = (1 to 4).toList
    val i = TestInbox[Int]()
    val b = behavior(i, 3, values)
    val testkit = BehaviorTestKit(b, "worst-case sorting")
    values.reverse.foreach(value => {
      testkit.ref ! value
      testkit.runOne()
    })
    i.receiveAll() must be(values)
  }

  test("A SelectiveReceive Decorator must overflow (size 0)") {
    val values = List(1, 2)
    val i = TestInbox[Int]()
    val b = behavior(i, 0, values)
    val testkit = BehaviorTestKit(b, "overflow 0")
    a[StashOverflowException] mustBe thrownBy {
      testkit.ref ! 2
      testkit.runOne()
    }
  }

  test("A SelectiveReceive Decorator must overflow (size 1)") {
    val values = List(1, 2)
    val i = TestInbox[Int]()
    val b = behavior(i, 1, values)
    val testkit = BehaviorTestKit(b, "overflow 1")
    testkit.ref ! 2
    testkit.runOne()
    a[StashOverflowException] mustBe thrownBy {
      testkit.ref ! 2
      testkit.runOne()
    }
  }

  test("A SelectiveReceive Decorator must try in receive order") {
    val i = TestInbox[Int]()
    val b = SelectiveReceive(2, expectStart(i, 0,
      receiveMessage[Int] { t =>
        i.ref ! t
        same
      }
    ))
    val testkit = BehaviorTestKit(b, "receive order")
    testkit.ref ! 1
    testkit.runOne()
    testkit.ref ! 2
    testkit.runOne()
    i.receiveAll() mustBe Seq()
    testkit.ref ! 0
    testkit.runOne()
    i.receiveAll() mustBe Seq(0, 1, 2)
  }

  test("A SelectiveReceive Decorator must restart retrying at the head of the queue") {
    // hint: only the first parameter list participates in equality checking
    case class Msg(cls: Int)(val value: Int)

    val i = TestInbox[Msg]()
    val b = SelectiveReceive(3,
      expectStart(i, Msg(0)(0),
        expectStart(i, Msg(1)(0),
          receiveMessage[Msg] { t =>
            i.ref ! t
            same
          }
        )))
    val testkit = BehaviorTestKit(b, "receive order")
    testkit.ref ! Msg(2)(2)
    testkit.runOne()
    testkit.ref ! Msg(1)(1)
    testkit.runOne()
    testkit.ref ! Msg(2)(3)
    testkit.runOne()
    i.receiveAll() mustBe Seq()
    testkit.ref ! Msg(0)(0)
    testkit.runOne()
    i.receiveAll().map(_.value) mustBe Seq(0, 1, 2, 3)
  }

}