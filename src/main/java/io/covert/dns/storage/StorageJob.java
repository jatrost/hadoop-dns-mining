/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.covert.dns.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class StorageJob extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new StorageJob(), args);
	}
	
	@Override
	public int run(String[] args) throws Exception {
				
		String inDir = args[0];			
		Configuration conf = getConf();
		Job job = new Job(conf);
		job.setJarByClass(getClass());
		job.setJobName(StorageJob.class.getSimpleName()+": inDir="+inDir);
				
		job.setMapperClass(StorageMapper.class);
		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(inDir));
		
		// This job doesn't write output via Hadoop, it uses the configured storage modules
		job.setOutputFormatClass(NullOutputFormat.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.submit();
		
		return job.waitForCompletion(true)?0:1;
	}
}
