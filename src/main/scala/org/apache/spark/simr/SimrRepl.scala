package org.apache.spark.simr

import java.io.PrintWriter

import org.apache.spark.Logging
import org.apache.spark.repl.SparkILoop

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
