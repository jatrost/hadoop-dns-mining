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

import io.covert.dns.storage.StorageModule;
import io.covert.dns.storage.accumulo.mutgen.MutationGenerator;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Mutation;

public class AccumuloStorageModule implements StorageModule {

	Collection<MutationGenerator> generators = new LinkedList<MutationGenerator>();
	MultiTableBatchWriter bw;
	Connector conn;
	
	public AccumuloStorageModule(String inst, String zooKeepers, String user, String password, long maxMemory, long maxLatency, int maxWriteThreads, Collection<MutationGenerator> generators) 
		throws AccumuloException, AccumuloSecurityException
	{
		this.generators.addAll(generators);
		this.conn = new ZooKeeperInstance(inst, zooKeepers).getConnector(user, password);
		this.bw = conn.createMultiTableBatchWriter(maxMemory, maxLatency, maxWriteThreads);
	}
	
	@Override
	public void store(Map<String, Object> record) throws IOException {
		
		for(MutationGenerator generator : generators)
		{
			Map<String, Collection<Mutation>> muts = generator.generate(record);
			for(Entry<String, Collection<Mutation>> e : muts.entrySet())
			{
				try {
					BatchWriter writer = bw.getBatchWriter(e.getKey());
					for(Mutation mut : e.getValue())
					{
						writer.addMutation(mut);
					}
				} catch (AccumuloException e1) {
					throw new IOException(e1);
				} catch (AccumuloSecurityException e1) {
					throw new IOException(e1);
				} catch (TableNotFoundException e1) {
					throw new IOException(e1);
				}
			}
		}
	}

	@Override
	public void close() throws IOException {
		try {
			bw.close();
		} catch (MutationsRejectedException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void flush() throws IOException {
		try {
			bw.flush();
		} catch (MutationsRejectedException e) {
			throw new IOException(e);
		}
	}

}
