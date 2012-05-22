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
package io.covert.dns.storage.accumulo;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import io.covert.dns.storage.StorageModule;
import io.covert.dns.storage.StorageModuleFactory;
import io.covert.dns.storage.accumulo.mutgen.MutationGenerator;
import io.covert.dns.storage.accumulo.mutgen.MutationGeneratorFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

public class AccumuloStorageModuleFactory implements StorageModuleFactory{

	@Override
	public StorageModule create(Configuration conf) throws Exception {
		
		List<MutationGenerator> generators = new LinkedList<MutationGenerator>();
		for(String factoryClass : conf.get("accumulo.storage.module.mutation.generator.factories").split(","))
		{
			MutationGeneratorFactory mutGenFact = ((Class<MutationGeneratorFactory>)Class.forName(factoryClass)).newInstance();
			generators.add(mutGenFact.create(conf));
		}
		
		String inst = conf.get("accumulo.storage.module.instance.name");
		String zooKeepers = conf.get("accumulo.storage.module.zookeepers");
		String user = conf.get("accumulo.storage.module.user");
		String password = conf.get("accumulo.storage.module.password");
		
		long maxMemory = conf.getLong("accumulo.storage.module.max.memory", 10*1024*1024);
		long maxLatency = conf.getLong("accumulo.storage.module.max.latency", 30*1000);
		int maxWriteThreads = conf.getInt("accumulo.storage.module.max.write.threads", 5);
		
		return new AccumuloStorageModule(inst, zooKeepers, user, password, maxMemory, maxLatency, maxWriteThreads, generators);
	}
	
	public static void configure(Job job, String inst, String zooKeepers, String user, String password, 
			long maxMemory, long maxLatency, int maxWriteThreads, Collection<Class<? extends MutationGeneratorFactory>> generatorFactoryClasses)
	{
		Configuration conf = job.getConfiguration();
		StringBuilder factories = new StringBuilder();
		boolean first = true;
		
		for(Class<? extends MutationGeneratorFactory> clz : generatorFactoryClasses)
		{
			if(first)
			{
				first = false;
				factories.append(clz.getName());
			}
			else
			{
				factories.append(",").append(clz.getName());
			}
		}
		
		conf.set("storage.module.factory", AccumuloStorageModuleFactory.class.getName());
		conf.set("accumulo.storage.module.mutation.generator.factories", factories.toString());
		conf.set("accumulo.storage.module.instance.name", inst);
		conf.set("accumulo.storage.module.zookeepers", zooKeepers);
		conf.set("accumulo.storage.module.user", user);
		conf.set("accumulo.storage.module.password", password);
		conf.setLong("accumulo.storage.module.max.memory", maxMemory);
		conf.setLong("accumulo.storage.module.max.latency", maxLatency);
		conf.setInt("accumulo.storage.module.max.write.threads", maxWriteThreads);
	}
}
