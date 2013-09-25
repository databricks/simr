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

package org.apache.spark.simr;


import java.util.*;

import scala.actors.threadpool.Arrays;

class Cmd {
    public String key;
    public String val;

    public Cmd(String key, String val) {
        this.key = key;
        this.val = val;
    }

    public String toString() {
        String ret = "(key=" + key;
        if (val != null)
            ret += ", value=" + val;
        ret += ")";
        return ret;
    }
}

class CmdLine {

    List<String> args;
    Map<String,Cmd> cmds = new HashMap<String,Cmd>();

    public CmdLine(String[] args) {
        this.args = new LinkedList<String>(Arrays.asList(args));
    }

    public String[] getArgs() {
        return this.args.toArray(new String[]{});
    }

    public Map<String,Cmd> getCmds() {
        return cmds;
    }

    public Integer getIntValue(String key) {
        Cmd c = getCmd(key);
        if (c == null)
            return null;
        Integer ret;
        try {
            ret = new Integer(c.val);
        } catch (NumberFormatException e) {
            return null;
        }
        return ret;
    }

    public boolean containsCommand(String key) {
        return cmds.containsKey(key.toLowerCase());
    }

    public Cmd getCmd(String key) {
        if (cmds.containsKey(key))
            return cmds.get(key);
        return null;
    }

    public void parse() {
        Iterator<String> argIt = args.iterator();
        while (argIt.hasNext()) {
            String arg = argIt.next();
            if (arg.toLowerCase().startsWith("--")) {
                String key = null;
                String val = null;
                int ind = arg.indexOf("=");
                if (ind != -1) {
                    String[] keyval = arg.split("=", 2);
                    key = keyval[0].toLowerCase().substring(2);
                    val = keyval[1];
                } else key = arg.toLowerCase().substring(2);
                cmds.put(key, new Cmd(key, val));
                argIt.remove();
            }
        }
    }
}
