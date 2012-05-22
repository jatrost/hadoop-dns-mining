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
package io.covert.dns.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.xbill.DNS.AAAARecord;
import org.xbill.DNS.ARecord;
import org.xbill.DNS.CNAMERecord;
import org.xbill.DNS.DClass;
import org.xbill.DNS.Header;
import org.xbill.DNS.MXRecord;
import org.xbill.DNS.Message;
import org.xbill.DNS.NSRecord;
import org.xbill.DNS.Opcode;
import org.xbill.DNS.PTRRecord;
import org.xbill.DNS.Rcode;
import org.xbill.DNS.Record;
import org.xbill.DNS.SOARecord;
import org.xbill.DNS.SRVRecord;
import org.xbill.DNS.Section;
import org.xbill.DNS.TXTRecord;
import org.xbill.DNS.Type;

public class JsonUtils {
	
	 private static final ObjectWriter json = new ObjectMapper().writer();
	
	private static Map<String, Object> convertToMap(Record rec, boolean ignoreTTL)
	{
		Map<String, Object> rmap = new HashMap<String, Object>();
		rmap.put("dclass", DClass.string(rec.getDClass()));
		rmap.put("name", rec.getName().toString());
		rmap.put("type", Type.string(rec.getType()));
		
		if(!ignoreTTL)
			rmap.put("ttl", rec.getTTL());
		
		if (rec instanceof ARecord) {
			ARecord arec = (ARecord) rec;
			rmap.put("addr", arec.getAddress().getHostAddress());
		}
		else if (rec instanceof AAAARecord) {
			AAAARecord arec = (AAAARecord) rec;
			rmap.put("addrv6", arec.getAddress().getHostAddress());
		}
		else if (rec instanceof PTRRecord) {
			PTRRecord ptr = (PTRRecord) rec;
			rmap.put("target", ptr.getTarget().toString());
		}
		else if (rec instanceof TXTRecord) {
			TXTRecord textRec = (TXTRecord) rec;
			rmap.put("text", textRec.toString());
		}
		else if (rec instanceof CNAMERecord) {
			CNAMERecord cname = (CNAMERecord) rec;
			rmap.put("alias", cname.getAlias().toString());
		}
		else if (rec instanceof MXRecord) {
			MXRecord mxrec = (MXRecord) rec;
			rmap.put("priority", mxrec.getPriority());
			rmap.put("target", mxrec.getTarget().toString());
		}
		else if (rec instanceof NSRecord) {
			NSRecord nsrec = (NSRecord) rec;
			rmap.put("target", nsrec.getTarget().toString());
		}
		else if (rec instanceof SRVRecord) {
			SRVRecord srv = (SRVRecord) rec;
			rmap.put("target", srv.getTarget().toString());
			rmap.put("port", srv.getPort());
			rmap.put("priority", srv.getPriority());
			rmap.put("weight", srv.getWeight());
		}
		else if (rec instanceof SOARecord) {
			SOARecord soa = (SOARecord) rec;
			rmap.put("admin", soa.getAdmin().toString());
			rmap.put("expire", soa.getExpire());
			rmap.put("host", soa.getHost().toString());
			rmap.put("min", soa.getMinimum());
			rmap.put("refresh", soa.getRefresh());
			rmap.put("retry", soa.getRetry());
			rmap.put("serial", soa.getSerial());
		}
		else
		{
			rmap.put("rdata", rec.rdataToString());
		}
		return rmap;
	}
	
	private static Map<String, Object> convertToMap(Record rec)
	{
		return convertToMap(rec, false);
	}
	
	private static List<Map<String, Object>> convertToList(Record[] records)
	{
		List<Map<String, Object>> recs = new ArrayList<Map<String,Object>>();
		for(Record rec : records)
		{
			recs.add(convertToMap(rec));
		}
		return recs;
	}
	
	private static void addRecords(Message resp, int section, Map<String, Object> map, String name)
	{
		Record[] records = resp.getSectionArray(section);
		if(records != null && records.length > 0)
		{
			map.put(name, convertToList(records));
		}
	}
	
	public static String toJson(Record rec)
	{
		return toJson(rec, false);
	}
	
	public static String toJson(Record rec, boolean ignoreTTL)
	{
		try {
			return json.writeValueAsString(convertToMap(rec, ignoreTTL));
		} catch (Exception e) {
			return null;
		}
	}
	
	public static String toJson(Message resp)
	{
		Map<String, Object> map = new HashMap<String, Object>();
		Header header = resp.getHeader();
		map.put("opcode", Opcode.string(header.getOpcode()));
		map.put("rcode", Rcode.string(header.getRcode()));
		map.put("flags", header.printFlags().trim().split("\\s+"));
		map.put("id", header.getID());
		addRecords(resp, Section.QUESTION, map, "question");
		addRecords(resp, Section.ANSWER, map, "answers");
		addRecords(resp, Section.ADDITIONAL, map, "additional");
		addRecords(resp, Section.AUTHORITY, map, "authority");
		
		try {

			return json.writeValueAsString(map);
			
		} catch (IOException e) {
			return null;
		}
	}
}
