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

public class EventMutuationGeneratorFactory implements MutationGeneratorFactory {

	@Override
	public MutationGenerator create(Configuration conf) throws Exception {
		
		return new EventMutuationGenerator(
			conf.get("event.mutation.generator.table"), 
			conf.get("event.mutation.generator.data.type")
		); 
	}
	
	public static void configure(Job job, String table, String dataType)
	{
		Configuration conf = job.getConfiguration();
		conf.set("event.mutation.generator.table", table);
		conf.set("event.mutation.generator.data.type", dataType);	
	}
}
