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

import java.io.{PrintWriter, InputStreamReader, PipedReader, PipedWriter, BufferedReader,
BufferedWriter, PipedInputStream, PipedOutputStream, Reader, PrintStream, OutputStreamWriter}
import java.net.{URL, URLClassLoader}
import scala.concurrent.ops.spawn
import akka.remote.RemoteActorRefProvider
import akka.util.Duration
import akka.actor.{ActorSystem, Props, ExtendedActorSystem, PoisonPill, ActorRef, Actor}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem, FSDataOutputStream}
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
case class ShutdownClient()

class SimrReplServer(simrUrl: String, out_dir: String, main_class: String, program_args:
  Array[String]) extends Actor with Logging {

  var shellMode: Boolean = false;
  var interp: SparkILoop = null

  val MAX_MSG: Int = 10*1024
  val BUFSIZE = 1024*100;
  val buf: Array[Char] = new Array[Char](MAX_MSG)

  var client: ActorRef = null

  val conf = new Configuration()
  val fs = FileSystem.get(conf)

  val stdinWriter: PipedWriter = new PipedWriter();
  val stdinReader: BufferedReader = new BufferedReader(new PipedReader(stdinWriter))
  val stdinFile: BufferedWriter =
    new BufferedWriter(new OutputStreamWriter(fs.create(new Path(out_dir + "/driver.stdin"))))

  val stdoutIS: PipedInputStream = new PipedInputStream(BUFSIZE);
  val stdoutOS: PipedOutputStream = new PipedOutputStream(stdoutIS);
  val stdoutWriter: BufferedWriter = new BufferedWriter(new OutputStreamWriter(stdoutOS));
  val stdoutReader: BufferedReader = new BufferedReader(new InputStreamReader(stdoutIS))
  val stdoutFile: BufferedWriter =
    new BufferedWriter(new OutputStreamWriter(fs.create(new Path(out_dir + "/driver.stdout"))))

  val stderrIS: PipedInputStream = new PipedInputStream(BUFSIZE)
  val stderrOS: PipedOutputStream = new PipedOutputStream(stderrIS)
  val stderrWriter: BufferedWriter = new BufferedWriter(new OutputStreamWriter(stderrOS));
  val stderrReader: BufferedReader = new BufferedReader(new InputStreamReader(stderrIS))
  val stderrFile: BufferedWriter =
    new BufferedWriter(new OutputStreamWriter(fs.create(new Path(out_dir + "/driver.stderr"))))

  System.setOut(new PrintStream(stdoutOS))
  System.setErr(new PrintStream(stderrOS))
  scala.Console.setOut(stdoutOS)
  scala.Console.setErr(stderrOS)

  def this(simrUrl: String, out_dir: String) {
    this(simrUrl, out_dir, null, null)
    shellMode = true;
  }

  def start() {
    logInfo("Starting SimrReplServer")
    if (shellMode) {
      runShell()
    } else {
      runJar()
    }
  }

  def runShell() {
    lazy val urls = java.lang.Thread.currentThread.getContextClassLoader match {
      case cl: java.net.URLClassLoader => cl.getURLs.toList
      case _ => sys.error("classloader is not a URLClassLoader")
    }

    spawn { // in a separate thread, otherwise in/out/err piped streams might overflow due to no reader draining them
      logDebug("Launching Spark shell in separate thread")
      interp = new SparkILoop(stdinReader, new PrintWriter(stdoutOS), simrUrl)

      org.apache.spark.repl.Main.interp = interp
      interp.setPrompt("\n" + SimrReplClient.SIMR_PROMPT)

      interp.settings = new scala.tools.nsc.Settings
      val urlStrs = urls.map(_.toString.replaceAll("^file:/","/"))

      interp.addAllClasspath(urlStrs)
      interp.process(Array[String]())
    }
  }

  def runJar() {
    spawn {
      val mainCL: URLClassLoader = new URLClassLoader(Array[URL](), this.getClass().getClassLoader());
      val myClass = Class.forName(main_class, true, mainCL);
      val method = myClass.getDeclaredMethod("main", classOf[Array[String]])
      method.invoke(null, program_args.asInstanceOf[Array[Object]])

      self ! ShutdownSimr()
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
          storeInput(buf, size, outType)
        }
      }
    } catch {
      case ex: java.io.IOException =>
        val err = "IOException while reading during input relaying:\n" +
          ex.toString + "\n" + ex.getStackTraceString
        client ! ReplOutput(err.toCharArray, err.size, StderrOutputType())
    }
  }

  def storeInput(buf: Array[Char], size: Int, outType: OutputType) {
    val out = outType match {
      case StdoutOutputType() => stdoutFile
      case StderrOutputType() => stderrFile
      case BasicOutputType() => stdoutFile
    }
    out.write(buf, 0, size)
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
      stdinWriter.write(str)
      stdinFile.write(str + "\n")

    case ReplInputLine(line: String) =>
      logDebug("Recieved input from client: " + line)
      stdinWriter.write(line)
      stdinFile.write(line + "\n")

    case FlushMessages() if (client != null) =>
      logDebug("Flushing output to client")
      relayInput(stdoutReader, StdoutOutputType())
      relayInput(stderrReader, StderrOutputType())

    case ShutdownSimr() =>
      logInfo("Shutting down")
      stdoutFile.close()
      stderrFile.close()
      stdinFile.close()

      if (shellMode) {
        interp.command("sc.stop()")
      } else {
        client ! ShutdownClient()
      }
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
  var shellMode: Boolean = true
  var out_dir: String = null
  var main_class: String = null
  var program_args: Array[String] = null

  def parseParams(raw_args: Array[String]) {
    val cmd = new CmdLine(raw_args)
    cmd.parse()
    val args = cmd.getArgs()

    if (args.length == 4 && !cmd.containsCommand("jar")) {
      hdfsFile = args(0)
      hostname = args(1)
      simrUrl = args(2)
      out_dir = args(3)
      shellMode = true
    } else if (args.length >= 5 && cmd.containsCommand("jar")) {
      hdfsFile = args(0)
      hostname = args(1)
      simrUrl = args(2)
      out_dir = args(3)
      main_class = args(4)
      program_args = args.slice(5, args.length)
      shellMode = false
    } else {
      println("Usage: SimrReplServer hdfs_file hostname simrUrl")
      println("       SimrReplServer --jar hdfs_file hostname simrUrl hdfs_tmp_dir main_class [arg1] [arg2] ...")
      System.exit(1)
    }
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

    val tempPath = new Path(hdfsFile + "_tmp")
    val filePath = new Path(hdfsFile)

    val conf = new Configuration()
    val fs = FileSystem.get(conf)

    // Create temporary file to prevent race condition where ReplClient gets empty url file
    val temp = fs.create(tempPath, true)
    temp.writeUTF(SimrReplUrl)
    temp.close()

    // "Atomic" rename
    fs.rename(tempPath, filePath)
  }

  def main(args: Array[String]) {
    parseParams(args)
    setupActorSystem(hostname)

    if (shellMode) {
      val server = actorSystem.actorOf(Props(new SimrReplServer(simrUrl, out_dir)), "SimrReplServer")
    } else {
      val server = actorSystem.actorOf(Props(new SimrReplServer(simrUrl, out_dir, main_class, program_args)), "SimrReplServer")
    }

    writeReplUrl()

    actorSystem.awaitTermination()
  }
}

