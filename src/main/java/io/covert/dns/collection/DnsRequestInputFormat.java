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

import io.covert.util.Utils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.xbill.DNS.DClass;
import org.xbill.DNS.Type;

public class DnsRequestInputFormat extends FileInputFormat<Text, DnsRequest> {
	
	@Override
	public RecordReader<Text, DnsRequest> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		
		int dclass = DClass.value(conf.get("dns.request.dclass", "IN"));
		List<String> subdomains = Arrays.asList(conf.get("dns.requests.subdomains", "").split(","));
		
		List<Integer> types = new LinkedList<Integer>();
		for(String type : conf.get("dns.request.types", "A").split(","))
			types.add(Type.value(type));
		
		return new DnsRequestRecordReader(subdomains, types, dclass);
	}
	
	public static void configure(Job job, String dclass, Collection<String> recordTypes, Collection<String> subDomains)
	{
		Configuration conf = job.getConfiguration();
		conf.set("dns.request.dclass", dclass);
		conf.set("dns.request.types", Utils.join(",", recordTypes));
		conf.set("dns.requests.subdomains", Utils.join(",", subDomains));
	}
}
