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
package io.covert.dns.filtering;

import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class FilterMapper extends Mapper<Text, Text, Text, Text> {

	public static final String FILTER_JEXL_EXPRESSION = "filter.jexl.expression";
	private static final String MATCHED = "MATCHED";
	private static final String NON_MATCHED = "NON-MATCHED";
	Filter filter;
	ObjectMapper objectMapper = new ObjectMapper();
	
	protected void setup(Context context) throws java.io.IOException ,InterruptedException 
	{
		filter = new Filter(context.getConfiguration().get(FILTER_JEXL_EXPRESSION));
	}
	
	protected void map(Text jsonRecord, Text empty, Context context) throws java.io.IOException ,InterruptedException 
	{
		Map<String, Object> record = objectMapper.readValue(jsonRecord.getBytes(), new TypeReference<Map<String, Object>>(){});
		if(filter.filter(record))
		{
			context.getCounter(FilterMapper.class.getSimpleName(), MATCHED).increment(1);
			context.write(jsonRecord, empty);
		}
		else
		{
			context.getCounter(FilterMapper.class.getSimpleName(), NON_MATCHED).increment(1);
		}
		context.progress();
	}
	
}
