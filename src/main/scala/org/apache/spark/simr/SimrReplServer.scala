package org.apache.spark.simr

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.{PrintWriter, InputStreamReader, PipedReader, PipedWriter, BufferedReader, BufferedWriter,
       PipedInputStream, PipedOutputStream, Reader}
import scala.concurrent.ops.spawn
import akka.remote.RemoteActorRefProvider
import akka.util.Duration
import akka.actor.{ActorSystem, Props, ExtendedActorSystem, PoisonPill, ActorRef, Actor}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.repl.SparkILoop
import org.apache.spark.util.AkkaUtils
import org.apache.spark.Logging

case class InitClient(serverUrl: String)
case class NewClient()
case class NewCommand(str: String)
case class FlushMessages()

trait OutputType
case class BasicOutputType() extends OutputType
case class StdoutOutputType() extends OutputType
case class StderrOutputType() extends OutputType

case class ReplOutput(buf: Array[Char], size: Int, outType: OutputType)
case class ReplInputLine(line: String)
case class ShutdownSimr()

class SimrReplServer(simrUrl: String) extends Actor with Logging {

  var interp: SparkILoop = null

  val MAX_MSG: Int = 10*1024
  val buf: Array[Char] = new Array[Char](MAX_MSG)

  var client: ActorRef = null
  val replIn: PipedWriter = new PipedWriter()
  val replOut: PipedReader = new PipedReader()

  var replStdout: InputStreamReader = null
  var replStderr: InputStreamReader = null
  var prevReplStdout: PrintWriter = null
  var prevReplStderr: PrintWriter = null

  def start() {
    val BUFSIZE = 1024*100;
    logInfo("Starting SimrReplServer")

    val pr = new PipedReader(BUFSIZE)
    replIn.connect(pr)
    val bufIn = new BufferedReader(pr, BUFSIZE)

    val pw = new PipedWriter()
    replOut.connect(pw)
    val bufOut = new BufferedWriter(pw, BUFSIZE)

    val os = new PipedOutputStream()
    val is = new PipedInputStream(BUFSIZE)
    is.connect(os)
    replStdout = new InputStreamReader(is)
    prevReplStdout = new PrintWriter(scala.Console.out)
    scala.Console.setOut(os)

    val oserr = new PipedOutputStream()
    val iserr = new PipedInputStream(BUFSIZE)
    iserr.connect(oserr)
    replStderr = new InputStreamReader(iserr)
    prevReplStderr = new PrintWriter(scala.Console.err)
    scala.Console.setErr(oserr)

    lazy val urls = java.lang.Thread.currentThread.getContextClassLoader match {
      case cl: java.net.URLClassLoader => cl.getURLs.toList
      case _ => sys.error("classloader is not a URLClassLoader")
    }

    spawn { // in a separate thread, otherwise in/out/err piped streams might overflow due to no reader draining them
      logDebug("Launching Spark shell in separate thread")
      interp = new SparkILoop(bufIn, new PrintWriter(bufOut), simrUrl)

      org.apache.spark.repl.Main.interp = interp
      interp.setPrompt("\n" + SimrReplClient.SIMR_PROMPT)
//      interp.setPrompt("")

      interp.settings = new scala.tools.nsc.Settings
      val urlStrs = urls.map(_.toString.replaceAll("^file:/","/"))
      //    urlStrs.foreach(file => interp.addClasspath(file))

      interp.addAllClasspath(urlStrs)
      interp.process(Array[String]())
    }

  }

  override def preStart() {
  }

  def relayInput(input: Reader, outType: OutputType) {
    try {
      while (input.ready()) {
        val size = input.read(buf, 0, MAX_MSG)
        if (size > 0) {
          client ! ReplOutput(buf, size, outType)
          //          prevReplStdout.write(buf, 0, size)
        }
      }
    } catch {
      case ex: java.io.IOException =>
        val err = "IOException while reading during input relaying:\n" +
          ex.toString + "\n" + ex.getStackTraceString
        client ! ReplOutput(err.toCharArray, err.size, StderrOutputType())
    }
  }

  def receive = {
    case NewClient =>
      logInfo("Connected to client")
      start()
      client = sender
      val cancellable =
        context.system.scheduler.schedule(Duration("0 ms"), Duration("10 ms"), self, FlushMessages())

    case NewCommand(str: String) =>
      logDebug("Recieved input from client: " + str)
      replIn.write(str)

    case ReplInputLine(line: String) =>
      logDebug("Recieved input from client: " + line)
      replIn.write(line)

    case FlushMessages() if (client != null) =>
      logDebug("Flushing output to client")
      relayInput(replStdout, StdoutOutputType())
      relayInput(replStderr, StderrOutputType())
      relayInput(replOut, BasicOutputType())

    case ShutdownSimr() =>
      logInfo("Shutting down")
      interp.command("sc.stop()")
      self ! PoisonPill
      context.system.shutdown()
  }
}

object SimrReplServer extends Logging {
  val SIMR_SYSTEM_NAME = "SimrRepl"
  var hdfsFile: String = null
  var hostname: String = null
  var simrUrl: String = null
  var actorSystem: ActorSystem = null

  def parseParams(args: Array[String]) {
    if (args.length != 3) {
      println("Usage: SimrReplServer hdfs_file hostname simrUrl")
      System.exit(1)
    }
    hdfsFile = args(0)
    hostname = args(1)
    simrUrl = args(2)
  }

  def setupActorSystem(hostname: String) {
    System.setProperty("spark.akka.logLifecycleEvents", "true")
    val (as, port) = AkkaUtils.createActorSystem(SIMR_SYSTEM_NAME, hostname, 0)
    actorSystem = as
  }

  def writeReplUrl() {
    val provider = actorSystem.asInstanceOf[ExtendedActorSystem].provider
    val port: Int = provider.asInstanceOf[RemoteActorRefProvider].transport.address.port.get
    val SimrReplUrl = "akka://%s@%s:%d/user/SimrReplServer".format(SIMR_SYSTEM_NAME, hostname, port)

    logInfo("Simr REPL running here: " + SimrReplUrl)

    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val file = fs.create(new Path(hdfsFile), true)
    file.writeUTF(SimrReplUrl)
    file.close()
  }

  def main(args: Array[String]) {
    parseParams(args)
    setupActorSystem(hostname)
    val server = actorSystem.actorOf(Props(new SimrReplServer(simrUrl)), "SimrReplServer")
    writeReplUrl()

    actorSystem.awaitTermination()
  }
}

