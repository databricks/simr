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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;

public class Simr {

    private Mapper.Context context;
    private Configuration conf;
    private FileSystem fs;

    private static final String ELECTIONDIR = "election"; // Directory used to do master election
    private static final String DRIVERURL = "driverurl";  // File used to store Spark driver URL
    private static final String OUTDIR = "output";  // File used to store Spark driver URL

    static class UrlCoresTuple {
        public String url;
        public int cores;

        public UrlCoresTuple(String _url, int _cores) {
            this.url = _url;
            this.cores = _cores;
        }
    }

    public Simr(Mapper.Context context) throws IOException {
        this.context = context;
        conf = context.getConfiguration();
        fs = FileSystem.get(conf);
    }

    /**
     * @return The IP of the first network interface on this machine as a string, null in the case
     * of an exception from the underlying network interface.
     */
    public static String getLocalIP() {
        String ip;
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface iface = interfaces.nextElement();

                if (iface.isLoopback() || !iface.isUp())
                    continue;

                Enumeration<InetAddress> addresses = iface.getInetAddresses();
                while(addresses.hasMoreElements()) {
                    InetAddress addr = addresses.nextElement();
                    ip = addr.getHostAddress();
                    return ip;
                }
            }
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public void redirectOutput(String filePrefix) throws IOException {
        Path outDir = new Path(conf.get("simr_out_dir") + "/" + OUTDIR);
        fs.mkdirs(outDir);
        FSDataOutputStream stdout = fs.create(
                new Path(conf.get("simr_out_dir") + "/" + OUTDIR + "/" + filePrefix + ".stdout"));
        FSDataOutputStream stderr = fs.create(
                new Path(conf.get("simr_out_dir") + "/" + OUTDIR + "/" + filePrefix + ".stderr"));
        System.setOut(new PrintStream(stdout));
        System.setErr(new PrintStream(stdout));
    }

    public void startMaster() {
        String master_url = "simr://" + conf.get("simr_tmp_dir") + "/" + DRIVERURL;
        String main_class = conf.get("simr_main_class");
        String rest_args = conf.get("simr_rest_args");

        String[] program_args = rest_args.replaceAll("\\%master\\%", master_url).split(" ");

        try {
            redirectOutput("driver");
            URLClassLoader mainCL = new URLClassLoader(new URL[]{}, this.getClass().getClassLoader());
            Class myClass = Class.forName(main_class, true, mainCL);
            Method method = myClass.getDeclaredMethod("main", new Class[]{String[].class});
            Object result = method.invoke(null, new Object[]{program_args});
        } catch (Exception ex) { System.out.println(ex); }

    }

    public void startWorker() throws IOException {
        UrlCoresTuple uc = getMasterURL();
        if (uc == null)
            return;
        int uniqueId = context.getTaskAttemptID().getTaskID().getId();
        int maxCores = uc.cores;
        String masterUrl = uc.url;

        String[] exList = new String[]{
                masterUrl,
                Integer.toString(uniqueId),
                getLocalIP(),
                Integer.toString(maxCores)};

        redirectOutput("worker" + uniqueId);

        org.apache.spark.executor.StandaloneExecutorBackend.main(exList);
    }

    public UrlCoresTuple getMasterURL() throws IOException {
        String simrDirName = conf.get("simr_tmp_dir");
        boolean gotDriverUrl = false;
        int MAXTRIES = 10;
        int tries = 0;
        String mUrl = "";
        int maxCores = 1;
        Path driverFile = new Path(simrDirName + "/" + DRIVERURL);

        while (!gotDriverUrl && tries++ < MAXTRIES) {
            FileStatus[] lsArr = fs.listStatus(driverFile);
            if (lsArr.length != 0 && lsArr[0].getLen() > 0) {
                gotDriverUrl = true;
                FSDataInputStream inPortFile =  fs.open(driverFile);
                mUrl = inPortFile.readUTF();
                maxCores = inPortFile.readInt();
                inPortFile.close();
            } else  {
                try {
                    Thread.sleep(4000);
                } catch(Exception ex) {}
            }
        }
        if (gotDriverUrl)
            return new UrlCoresTuple(mUrl, maxCores);
        else
            return null;
    }

    public boolean isMaster() throws IOException {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);

        String electionDirName = conf.get("simr_tmp_dir") + "/" + ELECTIONDIR;

        try {
            fs.mkdirs(new Path(electionDirName));  // create election directory
        } catch (Exception ex) {}

        String myTaskId = context.getTaskAttemptID().getTaskID().toString();

        Path myIpFile = new Path(electionDirName + "/" + myTaskId);
        FSDataOutputStream outf = fs.create(myIpFile, true);
        outf.close();

        // look for file with smallest timestamp
        long firstMapperTime = Long.MAX_VALUE;
        String firstMapperId = "";
        for (FileStatus fstat : fs.listStatus(new Path(electionDirName + "/"))) {
            long modTime = fstat.getModificationTime();
            if (modTime < firstMapperTime) {
                firstMapperTime = modTime;
                firstMapperId = fstat.getPath().getName();
            }
        }
        return myTaskId.equals(firstMapperId);
    }

    public void run() throws IOException {
        if (isMaster()) {
            startMaster();
        } else {
            startWorker();
        }

        fs.delete(new Path(conf.get("simr_tmp_dir")), true); // delete tmp dir

        for (FileStatus fstat : fs.listStatus(conf.get("simr_tmp_dir"))) {  // delete output files
            if (fstat.getPath().getName().startsWith("part-m-")) {
                fs.delete(fstat.getPath(), false);
            }
        }
    }

}