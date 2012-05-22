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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.xbill.DNS.Message;

public class DumpResponses extends Configured implements Tool {

	public static void main(String[] args) throws Exception{
		ToolRunner.run(new DumpResponses(), args);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		
		Text key = new Text();
		BytesWritable val = new BytesWritable();
		
		FileStatus[] listing;
		Path inpath = new Path(args[0]);
		if( fs.getFileStatus(inpath) != null && fs.getFileStatus(inpath).isDir() )
			listing = fs.listStatus(inpath);
		else
			listing = fs.globStatus(inpath);
		
		for(FileStatus f : listing)
		{
			if(f.isDir() || f.getPath().getName().startsWith("_"))
				continue;
			
			System.out.println("Opennning "+f.getPath()+" ...");
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, f.getPath(), conf);
			
			while(reader.next(key, val))
			{
				Message msg = new Message(val.getBytes());
				System.out.println(key+": "+msg);
				System.out.println("---");
			}
			reader.close();
		}		
		return 0;
	}
}
