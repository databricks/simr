package org.apache.spark.simr

import java.util.concurrent.TimeoutException
import java.net.{NetworkInterface, Inet4Address}

import scala.collection.JavaConversions

import akka.actor._
import akka.dispatch.Await
import akka.pattern.ask
import akka.util.Timeout
import akka.util.duration._
import jline_modified.console.ConsoleReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem, FileStatus, FSDataInputStream}

import org.apache.spark.util.AkkaUtils
import org.apache.spark.Logging

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


class RelayClient extends Actor with Logging {

  var server: ActorRef = null

  var frontCli: ActorRef = null

  var needToReply = false

  def receive = {
    case NewCommand(str) =>
      logDebug("client command: " + str)
      server ! NewCommand(str)

    case ReplInputLine(line) =>
      //      println("client command: " + str)
      logDebug("Sending to server: " + line)
      server ! ReplInputLine(line)
      frontCli = sender
      needToReply = true

    case InitClient(serverUrl: String) =>
      server = context.actorFor(serverUrl)
      logInfo("connecting to server")
      server ! NewClient

    case ReplOutput(buf: Array[Char], size: Int, outType: OutputType) =>
      logDebug("Received repl output")
      val out = outType match {
        case StdoutOutputType() => Console.out
        case StderrOutputType() => Console.err
      }
      out.print("\r")
      (0 to size-1).foreach(i => out.print(buf(i)))
      if (needToReply) {
        frontCli ! "continue"
        needToReply = false
      }

    case ShutdownSimr() =>
      logInfo("Sending shutdown to server")
      server ! ShutdownSimr()

    case ShutdownClient() =>
      self ! PoisonPill
      context.system.shutdown()
  }
}

object RelayClient extends Logging {
  val SIMR_PROMPT: String = "scala> "
  val SIMR_SYSTEM_NAME = "SimrRelay"

  var relayFile: String = null
  var driverFile: String = null
  var readOnly: Boolean = false
  var actorSystem: ActorSystem = null

  val conf = new Configuration()
  val fs = FileSystem.get(conf)

  def parseParams(raw_args: Array[String]) {
    val cmd = new CmdLine(raw_args)
    cmd.parse()
    val args = cmd.getArgs()

    if (args.length != 2) {
      println("Usage: RelayClient relay_hdfs_file driver_hdfs_file [--readonly]")
      System.exit(1)
    }
    relayFile = args(0)
    driverFile = args(1)
    readOnly = cmd.containsCommand("readonly")
  }

  def setupActorSystem() {
    logDebug("Setup actor system")
    val interfaces = JavaConversions.enumerationAsScalaIterator(NetworkInterface.getNetworkInterfaces)
    // Akka cannot use IPv6 addresses as identifiers, so we only consider IPv4 addresses
    var ip4Addr: Option[Inet4Address] = None
    for (i <- interfaces) {
      for (s <- JavaConversions.enumerationAsScalaIterator(i.getInetAddresses)) {
        if (s.isInstanceOf[Inet4Address]) ip4Addr = Some(s.asInstanceOf[Inet4Address])
      }
    }
    val akkaIpAddr =
      ip4Addr match {
        case Some(a) => a.getHostAddress
        case _ => "localhost"
      }
    System.setProperty("spark.akka.logLifecycleEvents", "true")
    val (as, port) = AkkaUtils.createActorSystem(SIMR_SYSTEM_NAME, akkaIpAddr, 0)
    actorSystem = as
  }

  def getRelayUrl() = {
    logDebug("Retrieving relay url from hdfs")
    val file = getFile(relayFile)
    val simrRelayUrl = file.readUTF()
    file.close()
    logDebug("RelayUrl: " + simrRelayUrl)
    simrRelayUrl
  }

  def getUiUrl() = {
    logDebug("Retrieving ui url from hdfs")
    val file = getFile(driverFile)
    val mUrl = file.readUTF()
    val nCores = file.readInt()
    val uiUrl = file.readUTF()
    file.close()
    logDebug("Spark UI Url: " + uiUrl)
    uiUrl
  }

  def getFile(hdfsFile: String): FSDataInputStream = {
    val MAXTRIES = 2*60*5;
    var tries = 0;
    val path = new Path(hdfsFile)
    var foundFile = false

    while (!foundFile && tries < MAXTRIES) {
      logDebug("Attempt: " + tries)

      var fstatArr: Array[FileStatus] = null
      try {
        fstatArr = fs.listStatus(path)
      } catch {
        case ioe: java.io.FileNotFoundException =>
          fstatArr = null
      }

      if (fstatArr != null && fstatArr.length > 0 && fstatArr(0).getLen > 0) {
        foundFile = true
      } else {
        try { Thread.sleep(500) } catch { case _ => Unit }
      }
      tries += 1
    }

    if (tries == MAXTRIES) {
      logDebug("Couldn't find HDFS file " + hdfsFile)
      System.exit(1)
    }

    fs.open(new Path(hdfsFile))
  }

  def readLoop(client: ActorRef) {
    logDebug("Starting client loop")
    val console = new ConsoleReader()
//    console.setPrompt(RelayClient.SIMR_PROMPT)
    console.setPrompt("")
    console.setPromptLen(RelayClient.SIMR_PROMPT.length)
    console.setSearchPrompt(RelayClient.SIMR_PROMPT)

    implicit val timeout = Timeout(2 seconds)

    var line: String = ""

    do {
      line = console.readLine()
      if (line != null) {
        val future = client ? ReplInputLine(line + "\n")
        try {
          val result = Await.result(future, timeout.duration).asInstanceOf[String]
        } catch { case ex: TimeoutException => Unit }
      }
    } while (line != null && line.stripLineEnd != "exit")
    client ! ShutdownSimr()
  }

  def main(args: Array[String]) {
    parseParams(args)
    val relayUrl = getRelayUrl()
    setupActorSystem()
    val client = actorSystem.actorOf(Props[RelayClient], "RelayClient")
    logInfo(relayUrl)
    client ! InitClient(relayUrl)
    val uiUrl = getUiUrl()
    logInfo("Spark UI: %s".format(uiUrl))

    if (readOnly) {
      actorSystem.awaitTermination()
    } else {
      readLoop(client)
      actorSystem.shutdown()
    }
  }
}

