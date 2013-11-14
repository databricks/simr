package org.apache.spark.simr

import org.scalatest._
import org.scalatest.matchers._

import akka.actor._
import akka.testkit.TestActorRef
import akka.pattern.ask

class RelayServerSpec extends FlatSpec with ShouldMatchers {

    //TODO: Can't test bad param list since System.exit will be called
    "RelayServer" should "parseParams" in {
        RelayServer.parseParams(Array("hdfs", "host", "simr", "out", "main", "arg1", "arg2"))
        RelayServer.hdfsFile should be ("hdfs")
        RelayServer.hostname should be ("host")
        RelayServer.simrUrl should be ("simr")
        RelayServer.out_dir should be ("out")
        RelayServer.main_class should be ("main")
        RelayServer.program_args should be (Array("arg1", "arg2"))
    }

    // Todo: Kludge because calling RelayServer.setupActorSystem doesn't work as it tries to bind
    RelayClient.setupActorSystem
    implicit var actorSystem: ActorSystem = RelayClient.actorSystem

    val clientRef = TestActorRef[DummyClient]
    val client = clientRef.underlyingActor

    val serverRef = TestActorRef[TestRelayServer]
    val server = serverRef.underlyingActor

    server.client = clientRef
}

class TestRelayServer extends RelayServer(null, null, null, null)

class DummyClient extends RelayClient
