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
package io.covert.dns.collection;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mortbay.log.Log;

public class CollectionJob extends Configured implements Tool  {

	private static void usage(String msg)
	{
		System.err.println("Usage: hadoop jar JARFILE.jar "+CollectionJob.class.getName()+" <requestClass> <requestTypes> <inDir> <outDir>");
		System.err.println("    requestClass - request class, e.g. IN, CH, etc");
		System.err.println("    requestTypes - resource record types (comma delim), e.g. A,MX,NS etc");
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
		
		String dclass = args[0];
		String types = args[1];
		String inDir = args[2];
		String outDir = args[3];
		
		Configuration conf = getConf();
		
		if(conf.get("dns.collection.num.resolvers") == null)
			conf.setInt("dns.collection.num.resolvers", 50);
		if(conf.get("dns.collection.nameservers") == null)
			conf.set("dns.collection.nameservers", "127.0.0.1");
		
		Job job = new Job(conf);
		job.setJobName(CollectionJob.class.getSimpleName()+": types="+types+", dclass="+dclass+
					   " inDir="+inDir+", outDir="+outDir+", resolvers="+conf.get("dns.collection.nameservers"));
		job.setJarByClass(getClass());
		
		job.setMapperClass(CollectionMapper.class);
		job.setNumReduceTasks(0);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		
		job.setInputFormatClass(DnsRequestInputFormat.class);
		DnsRequestInputFormat.setInputPaths(job, new Path(inDir));
		DnsRequestInputFormat.configure(job, dclass.toUpperCase(), Arrays.asList(types.split(",")), Arrays.asList(""));
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, new Path(outDir));
		SequenceFileOutputFormat.setCompressOutput(job, true);
		job.submit();
		
		int retVal = job.waitForCompletion(true)?0:1;
		
		CounterGroup counters          = job.getCounters().getGroup(CollectionMapper.RESOLVER_GROUP);		
		Counter constructMessageMS     = counters.findCounter(CollectionMapper.CONSTRUCT_MESSAGE_MS);
		Counter parseResponseMS        = counters.findCounter(CollectionMapper.PARSE_RESPONSE_MS);
		Counter performRequestMS       = counters.findCounter(CollectionMapper.PERFORM_REQUEST_MS);
		Counter totalRequestHandlingMS = counters.findCounter(CollectionMapper.TOTAL_REQUEST_HANDLING_MS);
		
		Log.info("Total ConstructMessage percent: "+ (double)(constructMessageMS.getValue()*100L)/((double)totalRequestHandlingMS.getValue()));
		Log.info("Total ParseResponse percent:    "+ (double)(parseResponseMS.getValue()*100L)/((double)totalRequestHandlingMS.getValue()));
		Log.info("Total PerformRequest percent:   "+ (double)(performRequestMS.getValue()*100L)/((double)totalRequestHandlingMS.getValue()));
		
		return retVal;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new CollectionJob(), args);
	}
}
