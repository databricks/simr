package org.apache.spark.simr

import org.scalatest._
import org.scalatest.matchers._

class CmdLineSpec extends FlatSpec with ShouldMatchers {

    //TODO: Can't test bad param list since System.exit will be called
    "CmdLine" should "parse positional parameters" in {
        val cmdLine = new CmdLine(Array("arg1", "arg2", "arg3"))
        cmdLine.parse
        cmdLine.getArgs should be (Array("arg1", "arg2", "arg3"))
    }

    it should "parse optional parameters" in {
        var cmdLine = new CmdLine(Array("arg1", "arg2", "--opt1=opt1"))
        cmdLine.parse
        cmdLine.getArgs should be (Array("arg1", "arg2"))
        cmdLine.containsCommand("opt1") should be (true)
        cmdLine.getCmd("opt1").`val` should equal ("opt1")

        cmdLine = new CmdLine(Array("--opt1=opt1", "arg1", "arg2", "--opt2=opt2"))
        cmdLine.parse
        cmdLine.getArgs should be (Array("arg1", "arg2"))
        cmdLine.containsCommand("opt1") should be (true)
        cmdLine.getCmd("opt1").`val` should equal ("opt1")
        cmdLine.containsCommand("opt2") should be (true)
        cmdLine.getCmd("opt2").`val` should equal ("opt2")
    }

    it should "parse int parameters" in {
        var cmdLine = new CmdLine(Array("arg1", "arg2", "--opt1=1"))
        cmdLine.parse
        cmdLine.getArgs should be (Array("arg1", "arg2"))
        cmdLine.containsCommand("opt1") should be (true)
        cmdLine.getIntValue("opt1") should equal (1)

        cmdLine = new CmdLine(Array("arg1", "arg2", "--opt1=NAN"))
        cmdLine.parse
        cmdLine.getArgs should be (Array("arg1", "arg2"))
        cmdLine.containsCommand("opt1") should be (true)
        cmdLine.getIntValue("opt1") should equal (null)
    }
}

