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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.mortbay.log.Log;
import org.xbill.DNS.DClass;
import org.xbill.DNS.Message;
import org.xbill.DNS.Record;
import org.xbill.DNS.Type;

public class CollectionMapper extends Mapper<Text, DnsRequest, Text, BytesWritable>{

	private static final String REQUEST_TIMEOUTS = "REQUEST_TIMEOUTS";
	private static final String RCODES_GROUP = "RCODES";
	public static final String COUNTER_GROUP = CollectionMapper.class.getSimpleName();
	public static final String RESOLVER_GROUP = ResolverThread.class.getSimpleName();
	
	public static final String QUEUE_FULL = "QUEUE_FULL";
	public static final String GOOD_NAME = "GOOD_NAME";
	public static final String BAD_NAME = "BAD_NAME";
	public static final String PERFORM_REQUEST_MS = "PerformRequestMS";
	public static final String TOTAL_REQUEST_HANDLING_MS = "TotalRequestHandlingMS";
	public static final String PARSE_RESPONSE_MS = "ParseResponseMS";
	public static final String CONSTRUCT_MESSAGE_MS = "ConstructMessageMS";
	public static final String NUM_REQUESTS = "NUM_REQUESTS";
	public static final String LOOKUP_FAILURES = "LOOKUP_FAILURES";
	public static final String REQUEST_PARSE_FAILURES = "REQUEST_PARSE_FAILURES";
	
	final ConcurrentLinkedQueue<DnsRequest> inQueue = new ConcurrentLinkedQueue<DnsRequest>();
	final ConcurrentLinkedQueue<Pair<Record, Message>> outQueue = new ConcurrentLinkedQueue<Pair<Record, Message>>();
	final AtomicLong inQueueSize = new AtomicLong(0);
	List<ResolverThread> threads = new LinkedList<ResolverThread>();
	WriterThread writer;
	long maxOutstandingRequests;
	
	protected void setup(Context context) throws java.io.IOException ,InterruptedException 
	{
		Configuration conf = context.getConfiguration();
		
		writer = new WriterThread(outQueue, context);
		writer.start();
		
		int numThreads = conf.getInt("dns.collection.num.resolvers", 50);
		String[] nameservers = conf.get("dns.collection.nameservers").split(",");
		maxOutstandingRequests = conf.getLong("dns.collection.max.outstanding.requests", 5000);
		int timeoutSecs = conf.getInt("dns.collection.timeout.secs", 5);
		
		if(nameservers.length == 0)
		{
			throw new IOException("dns.collection.num.resolvers was not defined correctly");
		}
		
		for(int i = 0; i < numThreads; ++i)
		{
			ResolverThread res = new ResolverThread(inQueue, inQueueSize, outQueue, nameservers, timeoutSecs);
			res.start();
			threads.add(res);
		}
	}
	
	// parse input query request
	// put request on queue if queue isn't too big
	// 
	
	@Override
	protected void map(Text domain, DnsRequest request, org.apache.hadoop.mapreduce.Mapper<Text,DnsRequest,Text, BytesWritable>.Context context) 
		throws java.io.IOException ,InterruptedException 
	{
		inQueue.add(request);
		long queueSize = inQueueSize.incrementAndGet();
		context.setStatus("Queue size: "+queueSize);
		while(queueSize >= maxOutstandingRequests)
		{
			context.getCounter(COUNTER_GROUP, QUEUE_FULL).increment(1);
			context.setStatus("Queue size: "+queueSize);
			Utils.sleep(100);
			queueSize = inQueueSize.get();
		}
	}
	
	// wait until in queue is empty
	// notify all threads to finish
	// wait for outQueue to go to zero, sleep a few seconds, stop writer thread
	
	protected void cleanup(Context context) 
		throws java.io.IOException ,InterruptedException 
	{
		context.setStatus("Cleanup: Queue size: "+ inQueueSize.get());
		Log.info("Stopping Resolver Threads ...");
		for(ResolverThread res : threads)
		{
			res.stopRunning();
		}
		
		while(inQueueSize.get() > 0)
		{
			context.setStatus("Cleanup: Queue size: "+ inQueueSize.get());
			Log.info("Cleanup: Queue size: "+ inQueueSize.get());
			Utils.sleep(1000);
		}
		
		Log.info("Joining Resolver Threads ...");
		for(ResolverThread res : threads)
		{
			res.join();
			context.getCounter(RESOLVER_GROUP, CONSTRUCT_MESSAGE_MS).increment(res.getConstructMessageMS());
			context.getCounter(RESOLVER_GROUP, PARSE_RESPONSE_MS).increment(res.getParseResponseMS());
			context.getCounter(RESOLVER_GROUP, TOTAL_REQUEST_HANDLING_MS).increment(res.getTotalRequestHandlingMS());
			context.getCounter(RESOLVER_GROUP, PERFORM_REQUEST_MS).increment(res.getPerformRequestMS());
			
			context.getCounter(RESOLVER_GROUP, NUM_REQUESTS).increment(res.getNumRequests());
			context.getCounter(RESOLVER_GROUP, LOOKUP_FAILURES).increment(res.getLookupsFailures());
			context.getCounter(RESOLVER_GROUP, REQUEST_TIMEOUTS).increment(res.getRequestTimeouts());
			context.getCounter(RESOLVER_GROUP, REQUEST_PARSE_FAILURES).increment(res.getRequestParseFailures());
			
			for(Object rcode : res.getRcodes().uniqueSet())
			{
				int count = res.getRcodes().getCount(rcode);
				context.getCounter(RCODES_GROUP, rcode.toString()).increment(count);
			}
			
			Log.info("This thread perfomed: "+res.getNumRequests()+" DNS requests");
			Log.info("ConstructMessage percent: "+ (double)(res.getConstructMessageMS()*100L)/((double)res.getTotalRequestHandlingMS()));
			Log.info("ParseResponse percent: "+  (double)(res.getParseResponseMS()*100L)/((double)res.getTotalRequestHandlingMS()));
			Log.info("PerformRequest percent: "+ (double)(res.getPerformRequestMS()*100L)/((double)res.getTotalRequestHandlingMS()));
			Log.info("---");
		}
		
		Log.info("Stopping Writer ...");
		writer.stopRunning();
		writer.join();
		Log.info("Writer Joined");
	}
	
	
	
	private static class WriterThread extends Thread
	{
		Context context;
		ConcurrentLinkedQueue<Pair<Record, Message>> outQueue;
		volatile boolean keepRunning = true;
		
		StringBuilder buffer = new StringBuilder();
		Text outKey = new Text();
		BytesWritable outVal = new BytesWritable();
		
		public WriterThread(ConcurrentLinkedQueue<Pair<Record, Message>> outQueue, Context context)
		{
			this.context = context;
			this.outQueue = outQueue;
		}
		
		private void stopRunning() {
			keepRunning = false;
		}
		
		@Override
		public void run() {
			while(keepRunning || !outQueue.isEmpty())
			{
				try {
					Pair<Record, Message> value = outQueue.remove();
					
					buffer.setLength(0);
					buffer.append(value.getKey().getName().toString()).append("\t");
					buffer.append(DClass.string(value.getKey().getDClass())).append("\t");
					buffer.append(Type.string(value.getKey().getType()));					
					outKey.set(buffer.toString());
					
					byte[] result = value.getValue().toWire();
					outVal.set(result, 0, result.length);
					
					context.write(outKey, outVal);
				} catch (Exception e) {
					Utils.sleep(100);
				}
			}
		}
	}
}
