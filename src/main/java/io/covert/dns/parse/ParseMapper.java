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
package io.covert.dns.parse;

import io.covert.dns.util.JsonUtils;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.xbill.DNS.Message;
import org.xbill.DNS.Record;
import org.xbill.DNS.Section;

public class ParseMapper extends Mapper<Text, BytesWritable, Text, Text> {
	
	Text outKey = new Text("");
	Text outVal = new Text("");
	
	boolean ignoreTTL = true;
	
	protected void setup(Context context) throws java.io.IOException, InterruptedException 
	{
		ignoreTTL = context.getConfiguration().getBoolean("parse.ignore.ttl", true);
	}
	
	protected void map(Text key, BytesWritable value, Context context) throws java.io.IOException ,InterruptedException 
	{
		try {
			Message msg = new Message(value.getBytes());
			
			int[] sections = {Section.ANSWER, Section.ADDITIONAL, Section.AUTHORITY};
			for(int section : sections)
			{
				for(Record record : msg.getSectionArray(section))
				{
					String json = JsonUtils.toJson(record, ignoreTTL);
					outKey.set(json);
					context.write(outKey, outVal);
				}
			}
		} catch (Exception e) {
			context.getCounter(getClass().getSimpleName(), "PARSE_FAIL").increment(1);
			e.printStackTrace();
		}
	}
}
