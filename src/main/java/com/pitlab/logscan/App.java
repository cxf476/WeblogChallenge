package com.pitlab.logscan;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pitlab.logscan.data.LogEntry;
import com.pitlab.logscan.flink.mapreduce.SessionTimeGroupReducer;
import com.pitlab.logscan.flink.mapreduce.UniqueUrlSessionMapper;
import com.pitlab.logscan.flink.mapreduce.AverageGroupReducer;
import com.pitlab.logscan.flink.mapreduce.EntryLoadMapper;
import com.pitlab.logscan.flink.mapreduce.SessionGroupReducer;


/**
 * Hello world!
 *
 */
public class App {
    private final static Logger logger = LoggerFactory.getLogger(App.class);
    public static void main( String[] args ) throws Exception {
    	ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//    	List<Tuple3<String, String, Integer>> lists = new ArrayList<>();
//    	lists.add(new Tuple3<>("cxf476_https://www.google.com", "cxf476", 1));
//    	lists.add(new Tuple3<>("cxf476_https://www.google.com", "cxf476", 1));
//    	lists.add(new Tuple3<>("cxf476_https://www.baidu.com", "cxf476", 1));
//    	lists.add(new Tuple3<>("cxf476_https://www.sogou.com", "cxf476", 1));
//    	lists.add(new Tuple3<>("cxf477_https://www.google.com", "cxf477", 1));
//    	lists.add(new Tuple3<>("cxf477_https://www.sogou.com", "cxf477", 1));
//    	DataSet<Tuple3<String, String, Integer>> sets = env.fromCollection(lists);
//    	sets.distinct(0).groupBy(1).aggregate(Aggregations.SUM, 2).print();
//    	System.exit(0);
    	
    	Configuration parameters = new Configuration();
    	// set the recursive enumeration parameter
    	parameters.setBoolean("recursive.file.enumeration", true);
    	// load log data from directory
    	DataSet<String> logs = env.readTextFile("data").withParameters(parameters);
    	// transform string to LogEntry object
    	DataSet<Tuple3<String, LocalDateTime, LogEntry>> entries = logs.map(new EntryLoadMapper());
    	//step1: Sessionize the web log by client IP
    	DataSet<Tuple2<String, LogEntry>> sessions = entries.groupBy(0).sortGroup(1, Order.ASCENDING).reduceGroup(new SessionGroupReducer());

    	//step2:calculate the average session times
    	DataSet<Tuple1<Long>> sessionTimes = sessions.groupBy(0).reduceGroup(new SessionTimeGroupReducer());
    	DataSet<Long> avgTime = sessionTimes.reduceGroup(new AverageGroupReducer());
    	
    	//step3:calculate unique URL visits per session, be aware the first parameter in uvSet is a random value
    	DataSet<Tuple3<String, String, Integer>> uvSet = sessions.map(new UniqueUrlSessionMapper()).distinct(0).groupBy(1).aggregate(Aggregations.SUM, 2);

    	
    	logger.info("loaded {} records from {} lines, avg session Time is {} seconds", sessions.count(), logs.count(), avgTime.collect().get(0));
    }
}
