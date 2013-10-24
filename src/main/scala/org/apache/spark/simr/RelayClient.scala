package org.apache.spark.simr

import java.io.PrintWriter
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
import org.apache.hadoop.fs.{Path, FileSystem}

import org.apache.spark.util.AkkaUtils
import org.apache.spark.Logging
import org.apache.spark.repl.SparkILoop

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
        case BasicOutputType() => Console.out
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
  val SIMR_SYSTEM_NAME = "SimrRepl"

  var hdfsFile: String = null
  var readOnly: Boolean = false
  var actorSystem: ActorSystem = null

  def parseParams(raw_args: Array[String]) {
    val cmd = new CmdLine(raw_args)
    cmd.parse()
    val args = cmd.getArgs()

    if (args.length != 1) {
      println("Usage: RelayClient hdfs_file [--readonly]")
      System.exit(1)
    }
    hdfsFile = args(0)
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

  def getReplUrl() = {
    logDebug("Retrieving repl url from hdfs")
    val conf = new Configuration()
    val fs = FileSystem.get(conf)

    val MAXTRIES = 2*60*5;
    var tries = 0;
    val path = new Path(hdfsFile)
    var foundFile = false

    while (!foundFile && tries < MAXTRIES) {
      logDebug("Attempt: " + tries)
      val fstatArr = fs.listStatus(path)
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

    var file = fs.open(new Path(hdfsFile))
    val simrReplUrl = file.readUTF()
    file.close()
    logDebug("ReplUrl: " + simrReplUrl)
    simrReplUrl
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
    val replUrl = getReplUrl()
    setupActorSystem()
    val client = actorSystem.actorOf(Props[RelayClient], "RelayClient")
    logInfo(replUrl)
    client ! InitClient(replUrl)

    if (readOnly) {
      actorSystem.awaitTermination()
    } else {
      readLoop(client)
      actorSystem.shutdown()
    }
  }
}

object SimrRepl extends Logging {
  def main(args: Array[String]) {
    val simrUrl = args(0)

    lazy val urls = java.lang.Thread.currentThread.getContextClassLoader match {
      case cl: java.net.URLClassLoader => cl.getURLs.toList
      case _ => sys.error("classloader is not a URLClassLoader")
    }

    val interp = new SparkILoop(Console.in, new PrintWriter(Console.out, true), simrUrl)

    org.apache.spark.repl.Main.interp = interp
    interp.setPrompt("\n" + RelayClient.SIMR_PROMPT)

    interp.settings = new scala.tools.nsc.Settings
    val urlStrs = urls.map(_.toString.replaceAll("^file:/","/"))

    interp.addAllClasspath(urlStrs)
    interp.process(Array[String]())
    interp.command("sc.stop()")
  }
}

