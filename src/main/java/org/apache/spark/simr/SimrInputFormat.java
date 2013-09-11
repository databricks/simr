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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

class SimrInputFormat extends InputFormat<Text, Text> {
    /**
     * Generate the specified number of dummy splits as given by simr
     */
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        Configuration conf = context.getConfiguration();
        int clusterSize = Integer.parseInt(conf.get("simr_cluster_size"));
        InputSplit[] result = new InputSplit[clusterSize];

        for(int i=0; i < result.length; ++i) {
            result[i] = new FileSplit(new Path("dummy-split-" + i), 0, 1,
                    (String[])null);
        }
        return Arrays.asList(result);
    }

    /**
     * Return a single record (filename, "") where the filename is taken from
     * the file split.
     */
    static class SimrRecordReader extends RecordReader<Text, Text> {
        Path name;
        boolean first = true;

        public void initialize(InputSplit split, TaskAttemptContext context)  {
            name = ((FileSplit) split).getPath();
        }

        public boolean nextKeyValue() {
            if (first) {
                first = false;
                return true;
            }
            return false;
        }

        public Text getCurrentKey() {
            return new Text(name.getName());
        }

        public Text getCurrentValue() {
            return new Text("");
        }

        public void close() { }

        public float getProgress() { return 0.0f; }
    }

    public RecordReader<Text, Text> createRecordReader(InputSplit split,
                                                       TaskAttemptContext context) {
        return new SimrRecordReader();
    }
}
