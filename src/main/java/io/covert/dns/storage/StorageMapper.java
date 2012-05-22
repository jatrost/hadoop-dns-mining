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

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class StorageMapper extends Mapper<Text, Text, NullWritable, NullWritable> {

	StorageModule storageModule = null;
	ObjectMapper objectMapper = new ObjectMapper();
	
	protected void setup(Context context) throws java.io.IOException, InterruptedException 
	{
		Configuration conf = context.getConfiguration();
		try {
			Class<StorageModuleFactory> clz = 
				(Class<StorageModuleFactory>)Class.forName(conf.get("storage.module.factory"));
			StorageModuleFactory factory = clz.newInstance();
			storageModule = factory.create(conf);
			
		} catch (Exception e) 
		{
			throw new RuntimeException(e);
		}
	}
	
	protected void map(Text jsonRecord, Text empty, Context context) throws java.io.IOException, InterruptedException 
	{
		Map<String, Object> record;
		try {
			record = objectMapper.readValue(jsonRecord.getBytes(), new TypeReference<Map<String, Object>>(){});
		} catch (Exception e) {
			context.getCounter("ERROR:"+e.getClass().getSimpleName(), "BAD_RECORDS").increment(1);
			return;
		}
		
		try {
			storageModule.store(record);
			context.getCounter(storageModule.getClass().getSimpleName(), "RECORDS_STORED").increment(1);
		} catch (Exception e) {
			context.getCounter("ERROR:"+e.getClass().getSimpleName(), 
							   "FAILED_STORE:"+storageModule.getClass().getSimpleName()).increment(1);
		}
		context.getCounter("TOTAL", "RECORDS").increment(1);
		context.progress();
	}
	
	protected void cleanup(Context context) throws java.io.IOException, InterruptedException 
	{
		storageModule.flush();
		storageModule.close();
	}
}
