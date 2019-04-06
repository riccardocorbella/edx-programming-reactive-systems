package streaming

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.testkit.scaladsl.TestSource
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Span}
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

import scala.concurrent.duration._

class SimpleStreamingSuite extends TestKit(ActorSystem("SimpleStreaming"))
   with FunSuiteLike
      with BeforeAndAfterAll
   with Matchers
   with ScalaFutures {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(500, Millis)), interval = scaled(Span(1000, Millis)))

  implicit val mat: ActorMaterializer = ActorMaterializer.create(system)

  override def afterAll(): Unit = {
    concurrent.Await.ready(system.terminate(), 10.seconds)
  }

  val ints: Source[Int, NotUsed] = Source.fromIterator(() => Iterator.from(1)).take(100) // safety limit, to avoid infinite ones
  def numbers(to: Int): List[Int] = Iterator.from(1).take(to).toList

  test("map simple values to strings") {
    val n = 10

    val tenStrings = SimpleStreaming.mapToStrings(ints).take(n)
    val eventualStrings = tenStrings.runWith(Sink.seq)
    val s = eventualStrings.futureValue
    s shouldEqual numbers(n).map(_.toString)
  }

  test("filter even values") {
    val n = 10

    val tenStrings = ints.take(n).via(SimpleStreaming.filterEvenValues)
    val eventualStrings = tenStrings.runWith(Sink.seq)
    val s = eventualStrings.futureValue
    s shouldEqual numbers(n).filter(_ % 2 == 0)
  }

  test("provide a Flow that can filter out even numbers") {
    val n = 100

    val it = SimpleStreaming.filterUsingPreviousFilterFlowAndMapToStrings(ints).runWith(Sink.seq)
    val s = it.futureValue
    s shouldEqual numbers(n).filter(_ % 2 == 0).map(_.toString)
  }

  test("provide a Flow that can filter out even numbers by reusing previous flows") {
    val n = 100

    val toString = Flow[Int].map(_.toString)
    val it = SimpleStreaming.filterUsingPreviousFlowAndMapToStringsUsingTwoVias(ints, toString).runWith(Sink.seq)
    val s = it.futureValue
    s shouldEqual numbers(n).filter(_ % 2 == 0).map(_.toString)
  }

  test("firstElementSource should only emit one element") {
    val p = SimpleStreaming.firstElementSource(ints.drop(12)).runWith(TestSink.probe)
    p.ensureSubscription()
    p.request(100)
    p.expectNext(13)
    p.expectComplete()
    p.expectNoMessage(300.millis)
  }

  test("firstElementFuture should only emit one element") {
    val p = SimpleStreaming.firstElementFuture(ints.drop(12))(mat).futureValue
    p shouldEqual 13
  }

  // failure handling

  test("recover from a failure into a backup element") {
    val (p, source) = TestSource.probe[Int](system).preMaterialize()
    val r = SimpleStreaming.recoverSingleElement(source)
    val s = r.runWith(TestSink.probe)

    s.request(10)
    p.ensureSubscription()
    p.expectRequest()

    p.sendNext(1)
    p.sendNext(2)

    s.expectNext(1)
    s.expectNext(2)

    val ex = new IllegalStateException("Source failed for some reason, oops!")

    p.sendError(ex)
    s.expectNext(-1)
    s.expectComplete()
  }

  test("recover from a failure into a backup stream") {
    val (p, source) = TestSource.probe[Int](system).preMaterialize()
    val fallback = Source(List(10, 11))
    val r = SimpleStreaming
      .recoverToAlternateSource(source, fallback)
    val s = r.runWith(TestSink.probe)

    s.request(10)
    p.ensureSubscription()
    p.expectRequest()

    p.sendNext(1)
    p.sendNext(2)

    s.expectNext(1)
    s.expectNext(2)

    val ex = new IllegalStateException("Source failed for some reason, oops!")

    p.sendError(ex)
    s.expectNext(10)
     .expectNext(11)
  }


  // working with rate

  test("keep repeating last observed value") {
    val (sourceProbe, sinkProbe) =
      TestSource.probe[Int]
      .via(SimpleStreaming.keepRepeatingLastObservedValue)
      .toMat(TestSink.probe)(Keep.both)
      .run()

    sourceProbe.ensureSubscription()
    val r = sourceProbe.expectRequest()

    sourceProbe.sendNext(1)
    sinkProbe.request(1)
    sinkProbe.expectNext(1)
    sinkProbe.request(1)
    sinkProbe.expectNext(1)

    sourceProbe.sendNext(22)
    sinkProbe.request(3)
    sinkProbe.expectNext(22)
    sinkProbe.expectNext(22)
    sinkProbe.expectNext(22)

    sourceProbe.sendComplete()
  }

}

