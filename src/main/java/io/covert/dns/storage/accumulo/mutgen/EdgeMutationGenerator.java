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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class EdgeMutationGenerator implements MutationGenerator {

	String edgeTable;
	String dataType;
	boolean biDir;
	boolean univarStats;
	Multimap<String, String> edges;
	
	public EdgeMutationGenerator(String edgeTable, String dataType, Multimap<String, String> edges, boolean biDir, boolean univarStats)
	{
		this.edgeTable = edgeTable;
		this.dataType = dataType;
		this.biDir = biDir;
		this.edges = edges;
		this.univarStats = univarStats;
	}
	
	@Override
	public Map<String, Collection<Mutation>> generate(Map<String, Object> record) {
		
		String type = (String)record.get("type");
		if(type == null){
			type = "UNKNOWN";
		}
		
		Map<String, Collection<Mutation>> muts = new HashMap<String, Collection<Mutation>>();
		List<Mutation> mutations = new LinkedList<Mutation>();
		muts.put(edgeTable, mutations);
		
		for(String entity1Name : edges.keySet())
		{
			Object entity1Value = record.get(entity1Name);
			if(entity1Value == null)
				continue;
			
			for(String entity2Name : edges.get(entity1Name))
			{
				Object entity2Value = record.get(entity2Name);
				if(entity2Value == null)
					continue;
				
				Mutation m = new Mutation(val(entity1Value)+"\0"+name(entity1Name)+"\0"+name(entity2Name)+"\0"+val(entity2Value));
				m.put("e\0"+type, dataType, "1");
				mutations.add(m);
				
				if(biDir)
				{
					if(!edges.containsEntry(entity2Name, entity1Name))
					{ // if the Map of edges already contains this bir edge, we don't want to add another...
						
						m = new Mutation(val(entity2Value)+"\0"+name(entity2Name)+"\0"+name(entity1Name)+"\0"+val(entity1Value));
						m.put("e\0"+type, dataType, "1");
						mutations.add(m);
					}
				}
			}
		}
		
		if(univarStats)
		{
			for(String key : record.keySet())
			{
				Mutation m = new Mutation(val(record.get(key)));
				m.put("s\0"+type, dataType+"\0"+name(key), "1");
				mutations.add(m);
			}
		}
		
		return muts;
	}
	
	private static String val(Object o)
	{
		return o.toString().toLowerCase();
	}
	
	private static String name(Object o)
	{
		return o.toString().toUpperCase();
	}
}
