package followers

import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.scaladsl.Framing.FramingException
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.{ActorAttributes, ActorMaterializer, Materializer}
import akka.testkit.TestKit
import akka.util.ByteString
import followers.model.{Event, Identity}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Span}
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.DurationInt
import scala.util.Random

class FollowersSuite extends TestKit(ActorSystem("FollowersSuite"))
  with FunSuiteLike
  with BeforeAndAfterAll
  with ScalaFutures {

  import system.dispatcher
  implicit val materializer: Materializer = ActorMaterializer()

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(3000, Millis)), interval = scaled(Span(1000, Millis)))

  override protected def afterAll(): Unit = {
    shutdown(system)
    super.afterAll()
  }

  test("reframedFlow: chunks containing exactly one message should pass through") {
    val incoming = List(ByteString("foo\n"), ByteString("bar\n"))
    val got = Source(incoming).via(Server.reframedFlow).runWith(Sink.seq).futureValue
    got === Seq("foo", "bar")
  }

  test("reframedFlow: chunks containing fragments of messages should be re-assembled") {
    val incoming = List(ByteString("f"), ByteString("oo\nb"), ByteString("ar\n"))
    val got = Source(incoming).via(Server.reframedFlow).runWith(Sink.seq).futureValue
    got === Seq("foo", "bar")
  }

  test("reframedFlow: reject an input stream that is completed in the middle of a frame") {
    val reframed = Source.single(ByteString("foo\nbar")).via(Server.reframedFlow).runWith(Sink.ignore)
    Await.ready(reframed, 1.second)
    assert(reframed.value.get.failed.get.isInstanceOf[FramingException])
  }

  test("eventParserFlow: successfully parse events") {
    val incoming = List(
      ByteString("666|F|6"), ByteString("0|50\n"),
      ByteString("1|U"), ByteString("|12|9\n"),
      ByteString("54232|B\n43|P|32|"), ByteString("56\n"),
      ByteString("634|S|32\n")
    )
    val got = Source(incoming).via(Server.eventParserFlow).runWith(Sink.seq).futureValue
    assert(got === Seq(
      Event.Follow(666, 60, 50),
      Event.Unfollow(1, 12, 9),
      Event.Broadcast(54232),
      Event.PrivateMsg(43, 32, 56),
      Event.StatusUpdate(634, 32)
    ))
  }

  test("reintroduceOrdering: pass through a sorted stream directly") {
    val incoming = List.tabulate(100)(n => Event.Follow(n + 1, 1, 1))

    val sorted = Source(incoming) via Server.reintroduceOrdering
    val got = sorted.runWith(Sink.seq).futureValue
    assert(got === incoming)
  }

  test("reintroduceOrdering: reintroduce ordering, 2 off") {
    val incoming = List(
      Event.Follow(2, 1, 2),
      Event.Follow(1, 1, 3),
      Event.Follow(3, 1, 4)
    )

    val sorted = Source(incoming) via Server.reintroduceOrdering
    val got = sorted.runWith(Sink.seq).futureValue
    assert(got === incoming.sortBy(_.sequenceNr))
  }


  test("followersFlow: add a follower") {
    val got =
      Source(List(Event.Follow(1, 1, 2)))
        .via(Server.followersFlow)
        .runWith(Sink.seq)
        .futureValue
    assert(got === Seq((Event.Follow(1, 1, 2), Map(1 -> Set(2)))))
  }

  test("followersFlow: remove a follower") {
    val got =
      Source(List(Event.Follow(1, 1, 2), Event.Unfollow(2, 1, 2)))
        .via(Server.followersFlow)
        .runWith(Sink.seq)
        .futureValue
    assert(got === Seq(
      (Event.Follow(1, 1, 2), Map(1 -> Set(2))),
      (Event.Unfollow(2, 1, 2), Map(1 -> Set.empty))
    ))
  }


  test("identityParserSink: extract identity") {
    assert(Source.single(ByteString("42\nignored"))
      .runWith(Server.identityParserSink).futureValue === Identity(42))
  }

  test("identityParserSink: re-frame incoming bytes") {
    val sink: Sink[ByteString, Future[Identity]] = Server.identityParserSink
    assert(Source(List(
      ByteString("1"), ByteString("2\n"),
      ByteString("ignored"), ByteString("\n")
    )).runWith(sink).futureValue === Identity(12))
  }

  test("isNotified: always notify users of broadcast messages") {
    for(userId <- 1 to 1000) {
      assert(Server.isNotified(userId)((Event.Broadcast(1), Map.empty)))
    }
  }

  test("isNotified: notify the followers of an user that updates his status") {
    assert(Server.isNotified(42)((Event.StatusUpdate(1, 12), Map(42 -> Set(12)))))
  }


  test("eventsFlow: downstream should receive completion when the event source is completed") {
    val server = new Server()
    // Feed the server with no events
    val eventsProbe = connectEvents(server)()
    // Check that no events are emitted by the hub
    val outProbe = server.broadcastOut.runWith(TestSink.probe)
    outProbe.ensureSubscription().request(Int.MaxValue)
    outProbe.expectNoMessage(10.millis)
    // The event source should complete without receiving any message
    eventsProbe.ensureSubscription().request(1).expectComplete()
  }

  test("incomingDataFlow: reframe, reorder and compute followers") {
    val server = new Server()
    val eventsProbe = connectEvents(server)(
      Event.StatusUpdate(2, 2),
      Event.Follow(1, 1, 2)
    )
    val outProbe = server.broadcastOut.runWith(TestSink.probe)
    outProbe.ensureSubscription().request(Int.MaxValue)
    outProbe.expectNext((Event.Follow(1, 1, 2), Map(1 -> Set(2))))
    outProbe.expectNext((Event.StatusUpdate(2, 2), Map(1 -> Set(2))))
    outProbe.expectNoMessage(50.millis)
    eventsProbe.ensureSubscription().request(1).expectComplete()
  }

  test("outgoingFlow: filter out events that should not be delivered to the given user") {
    val server = new Server()
    val client1 = connectClient(1, server)
    val client2 = connectClient(2, server)
    val client3 = connectClient(3, server)
    // Note that we have connected the clients before emitting
    // the events so that the clients won’t miss the events
    val eventsProbe = connectEvents(server)(
      Event.StatusUpdate(2, 2),
      Event.Follow(1, 1, 2)
    )

    client1.expectNext(Event.StatusUpdate(2, 2))
    client1.expectNoMessage(50.millis)
    client2.expectNext(Event.Follow(1, 1, 2))
    client2.expectNoMessage(50.millis)
    client3.expectNoMessage(50.millis)
    eventsProbe.ensureSubscription().request(1).expectComplete()
  }

  test("clientFlow: handle one client following another") {
    val server = new Server()
    val follower1 = connectClient(1, server)
    val follower2 = connectClient(2, server)
    val eventsProbe = connectEvents(server)(
      Event.Follow(1, 1, 2),
      Event.StatusUpdate(2, 2),
      Event.StatusUpdate(3, 1)
    )
    // ---- setup done ----

    follower2.expectNext(Event.Follow(1, 1, 2))
    follower2.expectNoMessage(50.millis)
    follower1.expectNext(Event.StatusUpdate(2, 2))
    follower1.expectNoMessage(50.millis)
    eventsProbe.ensureSubscription().request(1).expectComplete()
  }

  test("clientFlow: ensure that event before issuing a Follow is not sent to that follower") {

    val server = new Server()
    val follower1 = connectClient(1, server)
    val follower2 = connectClient(2, server)
    val eventsProbe = connectEvents(server)(
      Event.StatusUpdate(1, 1),
      Event.StatusUpdate(2, 2),
      Event.Follow(3, 1, 2)
    )
    // ---- setup done ----

    follower2.expectNext(Event.Follow(3, 1, 2))
    follower2.expectNoMessage(50.millis)
    eventsProbe.ensureSubscription().request(1).expectComplete()
  }

  test("clientFlow: ensure that users get notified from private messages") {
    val server = new Server()
    val follower1 = connectClient(1, server)
    val follower2 = connectClient(2, server)
    val follower3 = connectClient(3, server)
    val eventsProbe = connectEvents(server)(
      Event.Follow(1, 1, 3),
      Event.PrivateMsg(2, 1, 2)
    )
    // ---- setup done ----

    follower2.expectNext(Event.PrivateMsg(2, 1, 2))
    follower2.expectNoMessage(50.millis)

    follower1.expectNoMessage(50.millis)

    follower3.expectNext(Event.Follow(1, 1, 3))
    follower3.expectNoMessage(50.millis) // We don’t receive private messages targeted to user 2
    eventsProbe.ensureSubscription().request(1).expectComplete()
  }


  /**
    * Connects a user to the server and gets its event feed
    * @return A probe for the event feed
    */
  def connectClient(id: Int, server: Server): TestSubscriber.Probe[Event] = {
    Source.single(ByteString(s"$id\n"))
      .withAttributes(ActorAttributes.logLevels(Logging.InfoLevel, Logging.InfoLevel, Logging.InfoLevel))
      .async
      .via(server.clientFlow())
      .via(Server.eventParserFlow)
      .runWith(TestSink.probe)
      .ensureSubscription()
      .request(Int.MaxValue)
  }

  /**
    * Connects a feed of events to the server
    * @return A probe for the server response
    */
  def connectEvents(server: Server)(events: Event*): TestSubscriber.Probe[Nothing] = {
    Source(events.toList)
      .via(Flow[Event]/*.logAllEvents("write-events")*/)
      .map(_.render).async
      .via(server.eventsFlow)
      .runWith(TestSink.probe)
  }

}

