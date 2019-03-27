package protocols

import akka.Done
import org.scalacheck.Gen
import org.scalacheck.Arbitrary.arbitrary
import org.scalatest.{FunSuite, MustMatchers}
import org.scalatest.prop.PropertyChecks
import akka.actor.typed._
import akka.actor.typed.scaladsl._
import akka.actor.testkit.typed.Effect
import akka.actor.testkit.typed.scaladsl._

import scala.concurrent.duration._

trait TransactorSpec extends FunSuite with MustMatchers with PropertyChecks {

  import Transactor._

  test("A Transactor must compute") {
    val i = TestInbox[Int]()
    val done = TestInbox[String]()

    var lastSerial = 1L
    val serial = Gen.const(0) map { _ =>
      lastSerial += 1
      lastSerial
    }
    val extract = Gen.const(Extract[Int, Int](identity, i.ref))
    val map = Gen.oneOf(
      Gen.zip(arbitrary[Int], serial).map {
        case(x, id) => Modify[Int, String](x + _, id, s"add $x", done.ref)
      },
      Gen.zip(arbitrary[Int], serial).map {
        case(x, id) => Modify[Int, String](x * _, id, s"times $x", done.ref)
      }
    )
    val op = Gen.oneOf(extract, map)
    val ops = Gen.listOf(op)

    forAll((ops, "ops")) { list =>
      val start = 1
      val testkit = BehaviorTestKit(Transactor(start, 3.seconds))

      val sessionInbox = TestInbox[ActorRef[Session[Int]]]()
      testkit.ref ! Begin(sessionInbox.ref)
      testkit.runOne()
      val session = testkit.childTestKit(sessionInbox.receiveMessage())

      val end = list.foldLeft(start) { (current, op) =>
        session.ref ! op
        session.runOne()
        op match {
          case Extract(_, _) =>
            i.receiveAll() must be(Seq(current))
            current
          case Modify(f, _, reply, _) =>
            done.receiveAll() must be(Seq(reply))
            f(current)
          case _ => current
        }
      }
      session.ref ! Extract[Int, Int](identity, i.ref)
      session.runOne()
      i.receiveAll() must be(Seq(end))
    }
  }

  test("A Transactor must commit") {
    val done = TestInbox[String]()

    val start = 1
    val testkit = BehaviorTestKit(Transactor(start, 3.seconds))

    val sessionInbox = TestInbox[ActorRef[Session[Int]]]()
    testkit.ref ! Begin(sessionInbox.ref)
    testkit.runOne()
    val ref :: Nil = sessionInbox.receiveAll()
    val Effect.SpawnedAnonymous(_, _) :: Effect.Watched(`ref`) :: _ :: Nil = testkit.retrieveAllEffects()
    ref must be(testkit.childInbox(ref.path.name).ref)
    val session = testkit.childTestKit(ref)

    session.ref ! Modify((_: Int) + 1, 0, "done", done.ref)
    session.runOne()
    done.receiveAll() must be(Seq("done"))
    session.ref ! Commit("committed", done.ref)
    session.runOne()
    done.receiveAll() must be(Seq("committed"))
    session.ref ! Extract((x: Int) => x.toString, done.ref)
    session.runOne()
    done.receiveAll() mustBe empty

    testkit.runOne()
    testkit.selfInbox.hasMessages must be(false)

    testkit.ref ! Begin(sessionInbox.ref)
    testkit.runOne()
    val ref2 :: Nil = sessionInbox.receiveAll()
    val Effect.SpawnedAnonymous(_, _) :: Effect.Watched(`ref2`) :: _ :: Nil = testkit.retrieveAllEffects()
    ref2 must be(testkit.childInbox(ref2.path.name).ref)
    val session2 = testkit.childTestKit(ref2)

    val i = TestInbox[Int]()
    session2.ref ! Extract((x: Int) => x, i.ref)
    session2.runOne()
    i.receiveAll() must be(Seq(start + 1))
  }

  test("A Transactor must rollback") {
    val done = TestInbox[String]()

    val start = 1
    val testkit = BehaviorTestKit(Transactor(start, 3.seconds).asInstanceOf[Behavior[PrivateCommand[Int]]])

    val sessionInbox = TestInbox[ActorRef[Session[Int]]]()
    testkit.ref ! Begin(sessionInbox.ref)
    testkit.runOne()
    val ref :: Nil = sessionInbox.receiveAll()
    val Effect.SpawnedAnonymous(_, _) :: Effect.Watched(`ref`) :: _ :: Nil = testkit.retrieveAllEffects()
    ref must be(testkit.childInbox(ref.path.name).ref)
    val session = testkit.childTestKit(ref)

    session.ref ! Modify((_: Int) + 1, 0, "done", done.ref)
    session.runOne()
    done.receiveAll() must be(Seq("done"))
    session.ref ! Rollback()
    session.runOne()
    done.receiveAll() mustBe empty
    session.ref ! Extract((x: Int) => x.toString, done.ref)
    session.runOne()
    done.receiveAll() mustBe empty

    testkit.selfInbox.receiveAll() mustBe empty
    testkit.ref ! RolledBack(ref)
    testkit.runOne()
    testkit.retrieveAllEffects() must be(Seq(Effects.stopped(ref.path.name)))

    testkit.ref ! Begin(sessionInbox.ref)
    testkit.runOne()
    val ref2 :: Nil = sessionInbox.receiveAll()
    val Effect.SpawnedAnonymous(_, _) :: Effect.Watched(`ref2`) :: _ :: Nil = testkit.retrieveAllEffects()
    ref2 must be(testkit.childInbox(ref2.path.name).ref)
    val session2 = testkit.childTestKit(ref2)

    val i = TestInbox[Int]()
    session2.ref ! Extract((x: Int) => x, i.ref)
    session2.runOne()
    i.receiveAll() must be(Seq(start))
  }

  test("A Transactor must timeout") {
    val done = TestInbox[String]()

    val start = 1
    val testkit = BehaviorTestKit(Transactor(start, 3.seconds).asInstanceOf[Behavior[PrivateCommand[Int]]])

    val sessionInbox = TestInbox[ActorRef[Session[Int]]]()
    testkit.ref ! Begin(sessionInbox.ref)
    testkit.runOne()
    val ref :: Nil = sessionInbox.receiveAll()
    val Effect.SpawnedAnonymous(_, _) :: Effect.Watched(`ref`) :: _ :: Nil = testkit.retrieveAllEffects()
    ref must be(testkit.childInbox(ref.path.name).ref)
    val session = testkit.childTestKit(ref)

    session.ref ! Modify((_: Int) + 1, 0, "done", done.ref)
    session.runOne()
    done.receiveAll() must be(Seq("done"))

    testkit.ref ! RolledBack(ref)
    testkit.runOne()
    testkit.retrieveAllEffects() must be(Seq(Effects.stopped(ref.path.name)))

    // session child actor should be terminated now
    session.ref ! Extract((x: Int) => x.toString, done.ref)
    session.runOne()
    done.receiveAll() must be(Seq("2")) // BehaviorTestKit does not actually stop the child

    testkit.ref ! Begin(sessionInbox.ref)
    testkit.runOne()
    val ref2 :: Nil = sessionInbox.receiveAll()
    val Effect.SpawnedAnonymous(_, _) :: Effect.Watched(`ref2`) :: _ :: Nil = testkit.retrieveAllEffects()
    ref2 must be(testkit.childInbox(ref2.path.name).ref)
    val session2 = testkit.childTestKit(ref2)

    val i = TestInbox[Int]()
    session2.ref ! Extract((x: Int) => x, i.ref)
    session2.runOne()
    i.receiveAll() must be(Seq(start))
  }

  test("A Transactor must ignore new sessions while in a session") {
    val start = 1
    val testkit = BehaviorTestKit(Transactor(start, 3.seconds).asInstanceOf[Behavior[PrivateCommand[Int]]])

    val sessionInbox = TestInbox[ActorRef[Session[Int]]]()
    testkit.ref ! Begin(sessionInbox.ref)
    testkit.runOne()
    val ref :: Nil = sessionInbox.receiveAll()
    ref must be(testkit.childInbox(ref.path.name).ref)

    testkit.ref ! Begin(sessionInbox.ref)
    testkit.runOne()
    sessionInbox.receiveAll() mustBe empty

    testkit.ref ! RolledBack(ref)
    testkit.runOne()

    val ref2 :: Nil = sessionInbox.receiveAll()
    ref2 must be(testkit.childInbox(ref2.path.name).ref)
  }

  test("A Transactor must have idempotent modifications") {
    val extracted = TestInbox[Int]()
    val done = TestInbox[Done]()

    val start = 1
    val testkit = BehaviorTestKit(Transactor(start, 3.seconds))

    val sessionInbox = TestInbox[ActorRef[Session[Int]]]()
    testkit.ref ! Begin(sessionInbox.ref)
    testkit.runOne()
    val session = testkit.childTestKit(sessionInbox.receiveMessage())

    val modify = Modify[Int, Done](1 + _, 1L, Done, done.ref)
    session.ref ! modify
    session.runOne()
    done.receiveAll() must be(Seq(Done))
    session.ref ! modify
    session.runOne()
    done.receiveAll() must be(Seq(Done))
    session.ref ! Extract[Int, Int](identity, extracted.ref)
    session.runOne()
    extracted.receiveAll() must be(Seq(2))
  }

  test("A Transactor must properly ignore stale termination notices") {
    // the messages resulting from DeathWatch must not accumulate
    val sessionMock = TestInbox[Session[Int]]()
    val testkit = BehaviorTestKit(Transactor(0, 3.seconds))
    (1 to 31).foreach(_ => {
      testkit.ref.upcast[PrivateCommand[Int]] ! RolledBack(sessionMock.ref)
      testkit.runOne()
    })
  }

}
