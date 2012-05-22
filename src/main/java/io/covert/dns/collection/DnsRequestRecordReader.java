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

import io.covert.util.Pair;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class DnsRequestRecordReader extends RecordReader<Text, DnsRequest> {

	Pair<Text, DnsRequest> pair = null;
	LineRecordReader lineReader = new LineRecordReader();
	
	Iterable<String> subdomains;
	Iterable<Integer> types;
	int dclass;
	
	List<Pair<Text, DnsRequest>> outstandingRequests = new LinkedList<Pair<Text,DnsRequest>>();
	
	public DnsRequestRecordReader(Iterable<String> subdomains, Iterable<Integer> types, int dclass)
	{
		this.subdomains = subdomains;
		this.types = types;
		this.dclass = dclass;
	}
	
	@Override
	public void close() throws IOException {
		lineReader.close();
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		
		if(pair == null)
			return null;
		
		return pair.getKey();
	}

	@Override
	public DnsRequest getCurrentValue() throws IOException, InterruptedException {
		if(pair == null)
			return null;
		
		return pair.getValue();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return lineReader.getProgress();
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		lineReader.initialize(split, context);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		
		if(outstandingRequests.size() > 0)
		{
			pair = outstandingRequests.remove(0);
			return true;
		}
		
		if(lineReader.nextKeyValue())
		{
			Text line = lineReader.getCurrentValue();
			for(Integer type : types)
			{
				for(String subDomain: subdomains)
				{
					String name = subDomain;
					if(name.equals(""))
					{
						// allow for lookups on just the domain without any subdomains
						name = line.toString().trim();
					}
					else if(name.endsWith("."))
					{
						name += line.toString().trim();
					}
					else
					{
						name += "."+line.toString().trim();
					}
					
					if(!name.endsWith("."))
					{
						name += ".";
					}
					
					outstandingRequests.add(new Pair<Text, DnsRequest>(line, new DnsRequest(name, type, dclass)));
				}
			}
			
			if(outstandingRequests.size() > 0)
			{
				pair = outstandingRequests.remove(0);
				return true;
			}
		}
		
		return false;
	}
}
