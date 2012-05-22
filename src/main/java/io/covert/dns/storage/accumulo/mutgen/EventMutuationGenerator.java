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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.accumulo.core.data.Mutation;

public class EventMutuationGenerator implements MutationGenerator {

	String table;
	String dataType;
	
	public EventMutuationGenerator(String table, String dataType)
	{
		this.table = table;
		this.dataType = dataType;
	}
	
	@Override
	public Map<String, Collection<Mutation>> generate(Map<String, Object> record) {
		
		// normalize the record a bit and create a UUID out of it
		String data = new TreeMap<String, Object>(record).toString();
		String uuid = UUID.nameUUIDFromBytes(data.getBytes()).toString().replaceAll("-", "");
		
		Mutation m = new Mutation(uuid);
		for(String name : record.keySet())
		{
			m.put(dataType, name.toUpperCase(), record.get(name).toString());
		}
		
		return (Map<String, Collection<Mutation>>)
				Collections.singletonMap(
					table, 
					(Collection<Mutation>)Collections.singletonList(m)
				);
	}
}
