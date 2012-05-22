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
package io.covert.dns.collection;

import io.covert.util.Pair;
import io.covert.util.Utils;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.collections.Bag;
import org.apache.commons.collections.bag.HashBag;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.xbill.DNS.DClass;
import org.xbill.DNS.Message;
import org.xbill.DNS.Name;
import org.xbill.DNS.Rcode;
import org.xbill.DNS.Record;
import org.xbill.DNS.Resolver;
import org.xbill.DNS.SimpleResolver;
import org.xbill.DNS.TextParseException;
import org.xbill.DNS.Type;

public class ResolverThread extends Thread {

	private static final Logger LOG = Logger.getLogger(ResolverThread.class);
	volatile boolean keepRunning = true;

	Random random = new Random();
	ConcurrentLinkedQueue<DnsRequest> inQueue;
	AtomicLong inQueueSize;
	ConcurrentLinkedQueue<Pair<Record, Message>> outQueue;
	Resolver[] resolvers;
	String[] nameservers;
	Bag rcodes = new HashBag();
	
	long constructMessageMS = 0;
	long performRequestMS = 0;
	long parseResponseMS = 0;
	long totalRequestHandlingMS = 0;
	
	long numRequests = 0;
	long lookupsFailures = 0;
	long requestTimeouts = 0;
	long requestParseFailures = 0;
	
	public ResolverThread(
			ConcurrentLinkedQueue<DnsRequest> inQueue,
			AtomicLong inQueueSize,
			ConcurrentLinkedQueue<Pair<Record, Message>> outQueue, 
			String[] nameservers,
			int timeoutSecs) {
		super();
		this.inQueue = inQueue;
		this.inQueueSize = inQueueSize;
		this.outQueue = outQueue;
		this.nameservers = nameservers;
		
		resolvers = new Resolver[nameservers.length];
		for(int i = 0 ; i < resolvers.length; ++i)
		{
			try {
				resolvers[i] = new SimpleResolver(nameservers[i]);
				resolvers[i].setTimeout(timeoutSecs);
			} catch (UnknownHostException e) {
				throw new RuntimeException("Could not initial resolver for host="+nameservers[i]);
			}
		}
	}

	@Override
	public void run() {
	
		while(keepRunning || !inQueue.isEmpty())
		{
			try {
				DnsRequest req = inQueue.remove();
				inQueueSize.decrementAndGet();
				long elapsed = System.currentTimeMillis();
				Pair<Record, Message> resp = process(req);
				elapsed = System.currentTimeMillis() - elapsed;
				totalRequestHandlingMS += elapsed;
				numRequests++;
				
				if(resp != null)
					outQueue.add(resp);
				
			} catch (NoSuchElementException e) {
				Utils.sleep(100);
			}
		}
	}
	
	private Pair<Record, Message> process(DnsRequest req)
	{
		Record requestRecord = null;
		Pair<Record, Message> result = null;
		
		// pick a random nameserver
		int index = random.nextInt(resolvers.length);
		String nameserver = nameservers[index];
		
		long elapsed = System.currentTimeMillis();
		Message request;
		
		try {
			requestRecord = Record.newRecord(Name.fromString(req.getName()), req.getRequestType(), req.getDclass());
			request = Message.newQuery(requestRecord);
			
		} catch (TextParseException e) {
			LOG.error("Failed to parse name: "+req);
			++requestParseFailures;
			return null;
		}
		elapsed = System.currentTimeMillis() - elapsed;
		constructMessageMS += elapsed;
		
		elapsed = System.currentTimeMillis();
		Message response = null;
		try {
			response = resolvers[index].send(request);
			rcodes.add(Rcode.string(response.getRcode()));
			result = new Pair<Record, Message>(requestRecord, response);
		}
		catch(SocketTimeoutException e)
		{
			LOG.error("Timed out when resolving name: "+req+" at nameserver: "+nameserver+", reason: "+e.getMessage());
			++requestTimeouts;
		}
		catch (IOException e) {
			LOG.error("Failed resolving name: "+req+" at nameserver: "+nameserver+", reason: "+e.getMessage());
			++lookupsFailures;
		}
		elapsed = System.currentTimeMillis() - elapsed;
		performRequestMS += elapsed;
		
		return result;
	}
	
	public void stopRunning()
	{
		this.keepRunning = false;
	}

	public long getConstructMessageMS() {
		return constructMessageMS;
	}

	public long getPerformRequestMS() {
		return performRequestMS;
	}

	public long getParseResponseMS() {
		return parseResponseMS;
	}

	public long getTotalRequestHandlingMS() {
		return totalRequestHandlingMS;
	}
	
	public long getNumRequests() {
		return numRequests;
	}
	
	public long getLookupsFailures() {
		return lookupsFailures;
	}
	
	public long getRequestParseFailures() {
		return requestParseFailures;
	}
	
	public long getRequestTimeouts() {
		return requestTimeouts;
	}
	
	public Bag getRcodes() {
		return rcodes;
	}
	
	public static void main(String[] args) throws Exception {
		
		ConcurrentLinkedQueue<DnsRequest> inQueue = new ConcurrentLinkedQueue<DnsRequest>();
		AtomicLong inQueueSize = new AtomicLong(0);
		ConcurrentLinkedQueue<Pair<Record, Message>> outQueue = new ConcurrentLinkedQueue<Pair<Record, Message>>();
		String[] nameservers = new String[]{"8.8.8.8"};
		
		inQueue.add(new DnsRequest("www6.google.com.", Type.AAAA, DClass.IN));
		inQueue.add(new DnsRequest("ipv6.google.com.", Type.AAAA, DClass.IN));
		inQueue.add(new DnsRequest("gmail.com.", Type.AAAA, DClass.IN));
		inQueueSize.incrementAndGet();
		inQueueSize.incrementAndGet();
		inQueueSize.incrementAndGet();
		
		ResolverThread res = new ResolverThread(inQueue, inQueueSize, outQueue, nameservers, 5);
		res.start();
		res.stopRunning();
		res.join();
		
		Pair<Record, Message> result = outQueue.remove();
		System.out.println(result);
		
		result = outQueue.remove();
		System.out.println(result);
		
		result = outQueue.remove();
		System.out.println(result);
	}
}
