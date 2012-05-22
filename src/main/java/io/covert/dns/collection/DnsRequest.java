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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.xbill.DNS.DClass;
import org.xbill.DNS.Type;

public class DnsRequest implements WritableComparable<DnsRequest> {

	String name = "";
	int requestType = 0;
	int dclass = 0;
	
	public DnsRequest(){}
	
	public DnsRequest(String name, int requestType, int dclass) {
		super();
		this.name = name;
		this.requestType = requestType;
		this.dclass = dclass;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		name = in.readUTF();
		requestType = in.readInt();
		dclass = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeInt(requestType);
		out.writeInt(dclass);
	}

	protected String getName() {
		return name;
	}

	protected void setName(String name) {
		this.name = name;
	}

	protected int getRequestType() {
		return requestType;
	}

	protected void setRequestType(int requestType) {
		this.requestType = requestType;
	}

	protected int getDclass() {
		return dclass;
	}

	protected void setDclass(int dclass) {
		this.dclass = dclass;
	}

	@Override
	public int compareTo(DnsRequest o) {
		
		int ret = name.compareTo(o.name);
		if(ret == 0)
		{
			if(requestType > o.requestType)
				return 1;
			else if(requestType < o.requestType)
				return -1;
			else
			{
				if(dclass > o.dclass)
					return 1;
				else if(dclass < o.dclass)
					return -1;
				else
					return 0;
			}
		}	

		return ret;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + dclass;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + requestType;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DnsRequest other = (DnsRequest) obj;
		if (dclass != other.dclass)
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (requestType != other.requestType)
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		
		return "[name="+name+",dclass="+DClass.string(dclass)+",type="+Type.string(requestType)+"]";
	}
}
