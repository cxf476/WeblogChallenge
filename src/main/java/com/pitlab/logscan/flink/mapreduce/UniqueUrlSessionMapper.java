package com.pitlab.logscan.flink.mapreduce;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import com.pitlab.logscan.data.LogEntry;

/**
 * Input: clientIP_sessionID, LogEntry
 * output: clientIP_sessionID_Url, sessionID, 1
 */
public class UniqueUrlSessionMapper implements MapFunction<Tuple2<String, LogEntry>, Tuple3<String, String, Integer>> {

  private static final long serialVersionUID = -7823593034712970052L;

  @Override
  public Tuple3<String, String, Integer> map(Tuple2<String, LogEntry> value) throws Exception {
    String url = value.f1.getUrl();
    String sessionUrl = value.f0 + "_" +  url;// sessionUrl format: ip_sessionId_url
    return new Tuple3<>(sessionUrl, value.f0, 1);
  }

}
