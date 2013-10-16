package org.apache.spark.simr

import scala.collection.JavaConversions
import java.util.concurrent.TimeoutException
import java.net.{NetworkInterface, Inet4Address, InetAddress}
import akka.actor._
import akka.dispatch.Await
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout
import akka.util.duration._
import jline_modified.console.ConsoleReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.util.AkkaUtils

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


class SimrReplClient extends Actor {
  val log = Logging(context.system, this)

  var server: ActorRef = null

  var frontCli: ActorRef = null

  var needToReply = false

  def receive = {
    case NewCommand(str) =>
      println("client command: " + str)
      server ! NewCommand(str)

    case ReplInputLine(line) =>
      //      println("client command: " + str)
      server ! ReplInputLine(line)
      frontCli = sender
      needToReply = true

    case InitClient(serverUrl: String) =>
      server = context.actorFor(serverUrl)
      log.info("connecting to server")
      server ! NewClient

    case ReplOutput(buf: Array[Char], size: Int, outType: OutputType) =>
      val out = outType match {
        case StdoutOutputType() => Console.out
        case StderrOutputType() => Console.err
        case BasicOutputType() => Console.out
      }
      out.print("\r")
      (0 to size-1).foreach(i => out.print(buf(i)))
      if (needToReply) {
        frontCli ! "continue"
        needToReply = false
      }


    case ShutdownSimr() =>
      server ! ShutdownSimr()
  }
}

object SimrReplClient {
  val SIMR_PROMPT: String = "scala> "
  val SIMR_SYSTEM_NAME = "SimrRepl"

  var hdfsFile: String = null
  var actorSystem: ActorSystem = null

  def parseParams(args: Array[String]) {
    if (args.length != 1) {
      println("Usage: SimrReplClient hdfs_file")
      System.exit(1)
    }
    hdfsFile = args(0)
  }

  def setupActorSystem() {
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

  def getReplUrl() = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)

    val MAXTRIES = 2*60*5;
    var tries = 0;
    val path = new Path(hdfsFile)
    var foundFile = false

    while (!foundFile && tries < MAXTRIES) {
      val fstatArr = fs.listStatus(path)
      if (fstatArr != null && fstatArr.length > 0 && fstatArr(0).getLen > 0) {
        foundFile = true
      } else {
        try { Thread.sleep(500) } catch { case _ => Unit }
      }
      tries += 1
    }

    if (tries == MAXTRIES) {
      println("Couldn't find HDFS file " + hdfsFile)
      System.exit(1)
    }

    var file = fs.open(new Path(hdfsFile))
    val simrReplUrl = file.readUTF()
    file.close()
    simrReplUrl
  }

  def readLoop(client: ActorRef) {
    val console = new ConsoleReader()
//    console.setPrompt(SimrReplClient.SIMR_PROMPT)
    console.setPrompt("")
    console.setPromptLen(SimrReplClient.SIMR_PROMPT.length)
    console.setSearchPrompt(SimrReplClient.SIMR_PROMPT)

    implicit val timeout = Timeout(2 seconds)

    var line: String = ""

    do {
      line = console.readLine()
      if (line != null) {
        val future = client ? ReplInputLine(line + "\n")
        try {
          val result = Await.result(future, timeout.duration).asInstanceOf[String]
        } catch { case ex: TimeoutException => Unit }
      } else
        client ! ShutdownSimr()
    } while (line != null)
  }

  def main(args: Array[String]) {
    parseParams(args)
    val replUrl = getReplUrl()
    setupActorSystem()
    val client = actorSystem.actorOf(Props[SimrReplClient], "SimrReplClient")
    client ! InitClient(replUrl)

    readLoop(client)

    actorSystem.shutdown()
  }
}
