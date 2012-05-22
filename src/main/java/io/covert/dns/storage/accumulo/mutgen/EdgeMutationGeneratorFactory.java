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
package io.covert.dns.storage.accumulo.mutgen;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class EdgeMutationGeneratorFactory implements MutationGeneratorFactory {

	@Override
	public MutationGenerator create(Configuration conf) throws Exception {
		
		Multimap<String, String> edges = HashMultimap.create();
		for(String edge : conf.get("edge.mutation.generator.edges").split(","))
		{
			String names[] = edge.split(":", 2);
			edges.put(names[0], names[1]);
		}
		
		System.out.println(edges);
		
		return new EdgeMutationGenerator(conf.get("edge.mutation.generator.table"), 
				conf.get("edge.mutation.generator.data.type"), edges, 
				conf.getBoolean("edge.mutation.generator.bidirection", true), 
				conf.getBoolean("edge.mutation.generator.univar.stats", true));
	}
	
	public static void configure(Job job, String table, String dataType, boolean bidirectional, boolean univariateStats, Multimap<String, String> edges)
	{
		Configuration conf = job.getConfiguration();
		
		conf.set("edge.mutation.generator.table", table);
		conf.set("edge.mutation.generator.data.type", dataType);
		
		conf.setBoolean("edge.mutation.generator.bidirection", bidirectional);
		conf.setBoolean("edge.mutation.generator.univar.stats", univariateStats);
		
		StringBuilder s = new StringBuilder();
		boolean first = true;
		for(String name1 : edges.keySet())
		{
			for(String name2 : edges.get(name1))
			{
				if(first)
				{
					first = false;
					s.append(name1).append(":").append(name2);
				}
				else
				{
					s.append(",").append(name1).append(":").append(name2);
				}
			}
		}
		conf.set("edge.mutation.generator.edges", s.toString());
	}
}
