package org.apache.spark.simr

import java.io.{PipedWriter, PipedReader, BufferedReader}

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

    "A RelayServerActor" should "FlushMessages" in {
        val msg1 = "stdout"
        val msg2 = "stderr"
        val stdoutIn = new PipedReader();
        val stdoutOut = new PipedWriter(stdoutIn);
        stdoutOut.write(msg1)
        server.stdoutReader = new BufferedReader(stdoutIn)

        val stderrIn = new PipedReader();
        val stderrOut = new PipedWriter(stderrIn);
        stderrOut.write(msg2)
        server.stderrReader = new BufferedReader(stderrIn)

        serverRef ! FlushMessages()
        client.stdout should include (msg1)
        client.stderr should include (msg2)
    }
}

class TestRelayServer extends RelayServer(null, null, null, null)

class DummyClient extends RelayClient {
    var stdout: String = null
    var stderr: String = null

    override def receive = {
        case ReplOutput(buf: Array[Char], size: Int, outType: OutputType) =>
            outType match {
                case StdoutOutputType() =>
                    stdout = new String(buf)
                case StderrOutputType() =>
                    stderr = new String(buf)

            }
    }
}
