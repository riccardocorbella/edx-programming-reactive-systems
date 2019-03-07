package kvstore

import akka.testkit.TestProbe
import org.scalatest.Matchers
import org.scalatest.FunSuiteLike
import scala.concurrent.duration._
import kvstore.Arbiter.{JoinedSecondary, Join}
import scala.util.Random
import scala.util.control.NonFatal

trait Step2_SecondarySpec extends FunSuiteLike with Matchers {
  this: KVStoreSuite =>

  test(
    "Step2-case1: Secondary (in isolation) should properly register itself to the provided Arbiter") {
    val arbiter = TestProbe()
    val secondary = system.actorOf(
      Replica.props(arbiter.ref, Persistence.props(flaky = false)),
      "step2-case1-secondary")

    arbiter.expectMsg(Join)
    ()
  }

  test("Step2-case2: Secondary (in isolation) must handle Snapshots") {
    import Replicator._

    val arbiter = TestProbe()
    val replicator = TestProbe()
    val secondary = system.actorOf(
      Replica.props(arbiter.ref, Persistence.props(flaky = false)),
      "step2-case2-secondary")
    val client = session(secondary)

    arbiter.expectMsg(Join)
    arbiter.send(secondary, JoinedSecondary)

    client.get("k1") should ===(None)

    replicator.send(secondary, Snapshot("k1", None, 0L))
    replicator.expectMsg(SnapshotAck("k1", 0L))
    client.get("k1") should ===(None)

    replicator.send(secondary, Snapshot("k1", Some("v1"), 1L))
    replicator.expectMsg(SnapshotAck("k1", 1L))
    client.get("k1") should ===(Some("v1"))

    replicator.send(secondary, Snapshot("k1", None, 2L))
    replicator.expectMsg(SnapshotAck("k1", 2L))
    client.get("k1") should ===(None)
  }

  test(
    "Step2-case3: Secondary should drop and immediately ack snapshots with older sequence numbers") {
    import Replicator._

    val arbiter = TestProbe()
    val replicator = TestProbe()
    val secondary = system.actorOf(
      Replica.props(arbiter.ref, Persistence.props(flaky = false)),
      "step2-case3-secondary")
    val client = session(secondary)

    arbiter.expectMsg(Join)
    arbiter.send(secondary, JoinedSecondary)

    client.get("k1") should ===(None)

    replicator.send(secondary, Snapshot("k1", Some("v1"), 0L))
    replicator.expectMsg(SnapshotAck("k1", 0L))
    client.get("k1") should ===(Some("v1"))

    replicator.send(secondary, Snapshot("k1", None, 0L))
    replicator.expectMsg(SnapshotAck("k1", 0L))
    client.get("k1") should ===(Some("v1"))

    replicator.send(secondary, Snapshot("k1", Some("v2"), 1L))
    replicator.expectMsg(SnapshotAck("k1", 1L))
    client.get("k1") should ===(Some("v2"))

    replicator.send(secondary, Snapshot("k1", None, 0L))
    replicator.expectMsg(SnapshotAck("k1", 0L))
    client.get("k1") should ===(Some("v2"))
  }

  test(
    "Step2-case4: Secondary should drop snapshots with future sequence numbers") {
    import Replicator._

    val arbiter = TestProbe()
    val replicator = TestProbe()
    val secondary = system.actorOf(
      Replica.props(arbiter.ref, Persistence.props(flaky = false)),
      "step2-case4-secondary")
    val client = session(secondary)

    arbiter.expectMsg(Join)
    arbiter.send(secondary, JoinedSecondary)

    client.get("k1") should ===(None)

    replicator.send(secondary, Snapshot("k1", Some("v1"), 1L))
    replicator.expectNoMessage(300.milliseconds)
    client.get("k1") should ===(None)

    replicator.send(secondary, Snapshot("k1", Some("v2"), 0L))
    replicator.expectMsg(SnapshotAck("k1", 0L))
    client.get("k1") should ===(Some("v2"))
  }

}
