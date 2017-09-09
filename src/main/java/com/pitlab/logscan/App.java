package com.pitlab.logscan;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public class App {
    private final static Logger logger = LoggerFactory.getLogger(App.class);
    public static void main( String[] args ) throws Exception
    {
    	ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    	Configuration parameters = new Configuration();
    	// set the recursive enumeration parameter
    	parameters.setBoolean("recursive.file.enumeration", true);
    	// pass the configuration to the data source
    	DataSet<String> logs = env.readTextFile("data").withParameters(parameters);
    	logger.info("loaded {} records", logs.count());
    }
}
