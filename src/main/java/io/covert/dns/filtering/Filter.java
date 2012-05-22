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
package io.covert.dns.filtering;

import java.util.Map;

import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class Filter {
	
	static
	{
		// the WARN messages occur if the expression contains a var that is not in the record.
		// not good for many expressions, esp ones filtering on IP GEO info
		Logger.getLogger("org.apache.commons.jexl2").setLevel(Level.ERROR);
	}
	
	JexlEngine engine = new JexlEngine();;
	Expression expr;
	
	public Filter(String jexlExpression)
	{
		expr = engine.createExpression(jexlExpression);
	}
	
	public boolean filter(Map<String, Object> record)
	{
		JexlContext context = new MapContext(record);
		Object val = expr.evaluate(context);
		if (val instanceof Boolean) {
			Boolean bool = (Boolean) val;
			return bool;
		}
		return false;
	}
}
