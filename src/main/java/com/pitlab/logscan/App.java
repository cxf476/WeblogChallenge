package com.pitlab.logscan;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception
    {
    	ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		List<String> list= new ArrayList<>();
		for(int i =0; i<700000;++i) {
			list.add("A");
		}
		DataSet<String> ds = env.fromCollection(list);
		//List<DataSet<String>> dds = Lists.newArrayList();
		for(int i=0;i<1000;++i) {
			//dds.add(ds);
			ds = ds.map(t->t);
			ds.collect();
			Thread.sleep(1000);
		}
    }
}
