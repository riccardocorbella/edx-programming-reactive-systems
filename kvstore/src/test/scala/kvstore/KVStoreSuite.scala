package kvstore

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll

class KVStoreSuite
  extends Step1_PrimarySpec
    with Step2_SecondarySpec
    with Step3_ReplicatorSpec
    with Step4_SecondaryPersistenceSpec
    with Step5_PrimaryPersistenceSpec
    with Step6_NewSecondarySpec
    with IntegrationSpec
    with Tools
    with BeforeAndAfterAll {

  implicit val system: ActorSystem = ActorSystem("KVStoreSuite")

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

}

