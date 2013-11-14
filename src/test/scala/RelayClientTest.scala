package org.apache.spark.simr

import org.scalatest._
import org.scalatest.matchers._

import akka.actor._
import akka.testkit.TestActorRef
import akka.pattern.ask

class RelayClientSpec extends FlatSpec with ShouldMatchers {

    //TODO: Can't test bad param list since System.exit will be called
    "RelayClient" should "parseParams" in {
        RelayClient.parseParams(Array("relay_file", "driver_file", "--readonly"))
        RelayClient.relayFile should be ("relay_file")
        RelayClient.driverFile should be ("driver_file")
        RelayClient.readOnly should be (true)

        RelayClient.parseParams(Array("--readonly", "relay_file", "driver_file"))
        RelayClient.relayFile should be ("relay_file")
        RelayClient.driverFile should be ("driver_file")
        RelayClient.readOnly should be (true)

        RelayClient.parseParams(Array("relay_file", "driver_file"))
        RelayClient.relayFile should be ("relay_file")
        RelayClient.driverFile should be ("driver_file")
    }

    RelayClient.setupActorSystem
    implicit var actorSystem: ActorSystem = RelayClient.actorSystem

    it should "setupActorSystem" in {
        actorSystem.getClass should be (classOf[akka.actor.ActorSystemImpl])
    }

    val clientRef = TestActorRef[RelayClient]
    val client = clientRef.underlyingActor

    val serverRef = TestActorRef[DummyServer]
    val server = serverRef.underlyingActor

    client.server = serverRef

    "RelayClient Actor" should "NewCommand" in {
        val msg = "New Command Test"
        clientRef ! NewCommand(msg)
        server.recievedNewCommand should be (msg)
    }

    it should "ReplInputLine" in {
        val msg = "Input Line"
        clientRef ! ReplInputLine(msg)
        server.recievedInputLine should be (msg)
    }

    it should "ShutdownSimr" in {
        clientRef ! ShutdownSimr()
        server.shutdown should be (true)
    }

    it should "ShutdownClient" in {
        clientRef ! ShutdownClient()
    }
}

class DummyServer extends RelayServer(null, null, null, null) {
    var recievedNewCommand = ""
    var recievedInputLine = ""
    var shutdown = false

    override def receive = {
        case NewCommand(str) =>
            recievedNewCommand = str
        case ReplInputLine(str) =>
            recievedInputLine = str
        case ShutdownSimr() =>
            shutdown = true
    }
}
