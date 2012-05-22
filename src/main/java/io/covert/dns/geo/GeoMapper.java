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
package io.covert.dns.geo;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.maxmind.geoip.LookupService;

public class GeoMapper extends Mapper<Text, Text, Text, Text> {

	IpGeo geo;
	ObjectMapper objectMapper = new ObjectMapper();
	Text outKey = new Text();
	
	protected void setup(Context context) throws java.io.IOException ,InterruptedException 
	{
		Configuration conf = context.getConfiguration();
		geo = new IpGeo(conf.get("maxmind.geo.database.file"), conf.get("maxmind.asn.database.file"), LookupService.GEOIP_MEMORY_CACHE);
	}
	
	protected void map(Text json, Text empty, Context context) throws java.io.IOException ,InterruptedException 
	{
		Map<String, Object> rec = objectMapper.readValue(json.getBytes(), new TypeReference<Map<String, Object>>(){});
		String ip = (String)rec.get("addr");
		if(ip != null)
		{
			// annotate with IP Geo info...
			Map<String, Object> loc = geo.getLocation(ip);
			rec.putAll(loc);
			outKey.set(objectMapper.writeValueAsBytes(rec));
			context.write(outKey, empty);
		}
		else
		{
			// No Geo, but pass through anyway
			context.write(json, empty);
		}		
	}
	
	protected void cleanup(Context context) throws java.io.IOException ,InterruptedException 
	{
		if(geo != null)
			geo.close();
	}
}
