package com.pitlab.logscan.flink.mapreduce;

import java.time.Duration;
import java.time.LocalDateTime;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pitlab.logscan.data.LogEntry;
import com.pitlab.logscan.util.Consts;

public class SessionGroupReducer implements GroupReduceFunction<Tuple3<String, LocalDateTime, LogEntry>, Tuple2<String, LogEntry>> {
  private static final long serialVersionUID = 8581285416865820205L;
  private static final Logger logger = LoggerFactory.getLogger(SessionGroupReducer.class);
  
  protected String buildSessionIp(String ip, int sessionID) {
    return String.format("%s_%d", ip, sessionID);
  }

  @Override
  public void reduce(Iterable<Tuple3<String, LocalDateTime, LogEntry>> inputs,
      Collector<Tuple2<String, LogEntry>> outputs) throws Exception {
    LocalDateTime prevTime = null;
    int sessionID = 0;
    for(Tuple3<String, LocalDateTime, LogEntry> input : inputs) {
      if(prevTime == null) {
        prevTime = input.f1;
      }
      Duration dur = Duration.between(prevTime, input.f1);
      long diff = dur.toMinutes();
      if(diff>Consts.SESSION_ACTIVE_THRESHOLD) {
        ++sessionID;//this is a new session
      }
      //logger.info("for ip {}, prev={}, cur={}, diff= {}", input.f0, prevTime.toString(), input.f1.toString(), diff);
      prevTime = input.f1;
      outputs.collect(new Tuple2<>(buildSessionIp(input.f0, sessionID), input.f2));
    }
  }
  

}
