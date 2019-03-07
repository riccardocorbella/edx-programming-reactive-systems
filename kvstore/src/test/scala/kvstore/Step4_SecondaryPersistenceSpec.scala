package kvstore

import org.scalatest.Matchers
import org.scalatest.FunSuiteLike
import akka.testkit.TestProbe
import scala.concurrent.duration._
import Arbiter._
import Persistence._

trait Step4_SecondaryPersistenceSpec
  extends FunSuiteLike
        with Matchers { this: KVStoreSuite =>

  test("Step4-case1: Secondary should not acknowledge snapshots until persisted") {
    import Replicator._

    val arbiter = TestProbe()
    val persistence = TestProbe()
    val replicator = TestProbe()
    val secondary = system.actorOf(Replica.props(arbiter.ref, probeProps(persistence)), "step4-case1-secondary")
    val client = session(secondary)

    arbiter.expectMsg(Join)
    arbiter.send(secondary, JoinedSecondary)

    client.get("k1") should ===(None)

    replicator.send(secondary, Snapshot("k1", Some("v1"), 0L))
    val persistId = persistence.expectMsgPF() {
      case Persist("k1", Some("v1"), id) => id
    }

    withClue("secondary replica should already serve the received update while waiting for persistence: ") {
      client.get("k1") should ===(Some("v1"))
    }

    replicator.expectNoMessage(500.milliseconds)

    persistence.reply(Persisted("k1", persistId))
    replicator.expectMsg(SnapshotAck("k1", 0L))
    client.get("k1") should ===(Some("v1"))
  }

  test("Step4-case2: Secondary should retry persistence in every 100 milliseconds") {
    import Replicator._

    val arbiter = TestProbe()
    val persistence = TestProbe()
    val replicator = TestProbe()
    val secondary = system.actorOf(Replica.props(arbiter.ref, probeProps(persistence)), "step4-case2-secondary")
    val client = session(secondary)

    arbiter.expectMsg(Join)
    arbiter.send(secondary, JoinedSecondary)

    client.get("k1") should ===(None)

    replicator.send(secondary, Snapshot("k1", Some("v1"), 0L))
    val persistId = persistence.expectMsgPF() {
      case Persist("k1", Some("v1"), id) => id
    }

    withClue("secondary replica should already serve the received update while waiting for persistence: ") {
      client.get("k1") should ===(Some("v1"))
    }

    // Persistence should be retried
    persistence.expectMsg(300.milliseconds, Persist("k1", Some("v1"), persistId))
    persistence.expectMsg(300.milliseconds, Persist("k1", Some("v1"), persistId))

    replicator.expectNoMessage(500.milliseconds)

    persistence.reply(Persisted("k1", persistId))
    replicator.expectMsg(SnapshotAck("k1", 0L))
    client.get("k1") should ===(Some("v1"))
  }

}
