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
package io.covert.dns.extract;

import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class ExtractorMapper extends Mapper<Text, Text, Text, Text> {

	public static final String EXTRACTOR_JEXL_EXPRESSION = "extractor.jexl.expression";
	public static final String STRIP_OUTER_QUOTES = "extractor.strip.outer.quotes";
	
	Extractor extractor;
	boolean stripOuterQuotes;
	
	ObjectMapper objectMapper = new ObjectMapper();
	Text outKey = new Text();
	
	protected void setup(Context context) throws java.io.IOException ,InterruptedException 
	{
		extractor = new Extractor(context.getConfiguration().get(EXTRACTOR_JEXL_EXPRESSION));
		stripOuterQuotes = context.getConfiguration().getBoolean(STRIP_OUTER_QUOTES, true);
	}
	
	protected void map(Text jsonRecord, Text empty, Context context) throws java.io.IOException ,InterruptedException 
	{
		Map<String, Object> record = objectMapper.readValue(jsonRecord.getBytes(), new TypeReference<Map<String, Object>>(){});
		Object result = extractor.extract(record);
		if(result == null)
		{
			context.getCounter(ExtractorMapper.class.getSimpleName(), "NO RESULT").increment(1);
		}
		else
		{
			context.getCounter(ExtractorMapper.class.getSimpleName(), "RESULT").increment(1);
			String val = objectMapper.writeValueAsString(result);
			if(stripOuterQuotes)
				val = val.replaceAll("^\"+", "").replaceAll("\"+$", "");
			outKey.set(val);
			context.write(outKey, empty);
		}
		context.progress();
	}	
}
