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
package io.covert.dns.geo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GeoJob extends Configured implements Tool  {

	private static void usage(String msg)
	{
		System.err.println("Usage: hadoop jar JARFILE.jar "+GeoJob.class.getName()+" <maxmindDB> <masxmindASNDB> <inDir> <outDir>");
		System.err.println("    maxmindDB - maxmind geo db");
		System.err.println("    maxmindASNDB - maxmind ASN db");
		System.err.println("    inDir  - HDFS input dir");
		System.err.println("    outDir - HDFS output dir");
		System.exit(-1);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		if(args.length != 4)
		{
			usage("");
		}
		
		String dbfile = args[0];
		String asnDbfile = args[1];
		String inDir = args[2];
		String outDir = args[3];
		
		Configuration conf = getConf();
		conf.set("maxmind.geo.database.file", dbfile);
		conf.set("maxmind.asn.database.file", asnDbfile);
		
		Job job = new Job(conf);
		job.setJobName(GeoJob.class.getSimpleName()+": dbfile="+dbfile+", asnDB="+asnDbfile+" inDir="+inDir+", outDir="+outDir);
		job.setJarByClass(getClass());
		
		job.setMapperClass(GeoMapper.class);
		job.setNumReduceTasks(0);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(inDir));
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, new Path(outDir));
		SequenceFileOutputFormat.setCompressOutput(job, true);
		job.submit();
		
		int retVal = job.waitForCompletion(true)?0:1;
		return retVal;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new GeoJob(), args);
	}
}
