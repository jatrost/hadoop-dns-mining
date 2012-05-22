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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections.map.LRUMap;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;

import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;

public class IpGeo {

	private static final String UNKNOWN = "UNKNOWN";
	private static Map<String, Object> UNKNOWN_GEO = new HashMap<String, Object>();
	
	private static final String RFC1918 = "RFC1918";
	private static Map<String, Object> RFC1918_GEO = new HashMap<String, Object>();
	
	private static final Pattern asnumPattern = Pattern.compile("^AS(\\d+)\\s+(.+)$");
	
	private static final String[] rfc1918Slash8Prefixes = {
		"10.", "192.", "172."
	};
	
	private static final String[] rfc1918AddressPrefixes = {
		"10.", "192.168.",
		"172.16.", "172.17.", "172.18.", "172.19.",
		"172.20.", "172.21.", "172.22.", "172.23.",
		"172.24.", "172.25.", "172.26.", "172.27.",
		"172.28.", "172.29.", "172.30.", "172.31.",
	};
	
	static
	{
		put(UNKNOWN_GEO, "city", UNKNOWN);
		put(UNKNOWN_GEO, "cc", UNKNOWN);
		put(UNKNOWN_GEO, "country", UNKNOWN);
		put(UNKNOWN_GEO, "lat", 0.0f);
		put(UNKNOWN_GEO, "long", 0.0f);	
		put(UNKNOWN_GEO, "asn", UNKNOWN);
		put(UNKNOWN_GEO, "org", UNKNOWN);
		
		put(RFC1918_GEO, "city", RFC1918);
		put(RFC1918_GEO, "cc", RFC1918);
		put(RFC1918_GEO, "country", RFC1918);
		put(RFC1918_GEO, "lat", 0.0f);
		put(RFC1918_GEO, "long", 0.0f);	
		put(RFC1918_GEO, "asn", RFC1918);
		put(RFC1918_GEO, "org", RFC1918);
	}
	
	LookupService maxmind;
	LookupService asnLookup;
	LRUMap lru = new LRUMap(10000);
	
	public IpGeo(String dbFile, int options) throws IOException
	{
		maxmind = new LookupService(dbFile, LookupService.GEOIP_MEMORY_CACHE);
	}
	
	public IpGeo(String locationDbFile, String asnDbFile, int options) throws IOException
	{
		maxmind = new LookupService(locationDbFile, options);
		asnLookup = new LookupService(asnDbFile, options);
	}
	
	public Map<String, Object> getLocation(String ip) throws JsonGenerationException, JsonMappingException, IOException
	{
		for(String prefix : rfc1918Slash8Prefixes)
		{
			if(ip.startsWith(prefix))
			{
				for(String addrPrefix : rfc1918AddressPrefixes)
				{
					if(ip.startsWith(addrPrefix))
						return RFC1918_GEO;
				}
				break;
			}
		}
		
		Object val = lru.get(ip);
		if(val != null)
			return (Map<String, Object>)val;
		
		Location loc = maxmind.getLocation(ip);
		if(loc == null)
		{
			lru.put(ip, UNKNOWN_GEO);
			return UNKNOWN_GEO;
		}
		
		Map<String, Object> rec = new HashMap<String, Object>();
		put(rec, "city", loc.city);
		put(rec, "cc", loc.countryCode);
		put(rec, "country", loc.countryName);
		put(rec, "lat", loc.latitude);
		put(rec, "long", loc.longitude);
		
		if(asnLookup != null)
		{
			String org = asnLookup.getOrg(ip);
			if(org != null)
			{
				Matcher mat = asnumPattern.matcher(org);
				if(mat.matches())
				{
					put(rec, "asn", mat.group(1));
					put(rec, "org", mat.group(2));
				}
				else
				{
					put(rec, "org", org);
				}
			}
				
		}
		
		lru.put(ip, rec);
		return rec;
	}
	
	private static void put(Map<String, Object> rec , String name, Object val)
	{
		if(val != null)
			rec.put(name, val.toString().toUpperCase());
	}
	
	public void close() {
		lru.clear();
		maxmind.close();
		
		if(asnLookup != null)
		{
			asnLookup.close();
		}
	}
}
