package com.pitlab.logscan.flink.mapreduce;

import java.time.Duration;
import java.time.LocalDateTime;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import com.pitlab.logscan.data.LogEntry;

public class SessionTimeGroupReducer implements GroupReduceFunction<Tuple2<String, LogEntry>, Tuple1<Long>> {
  private static final long serialVersionUID = 435227991849446620L;

  @Override
  public void reduce(Iterable<Tuple2<String, LogEntry>> values, Collector<Tuple1<Long>> out)
      throws Exception {
    LocalDateTime startTime = null;
    LocalDateTime endTime = null;
    
    for(Tuple2<String, LogEntry> value : values) {
      LocalDateTime curTime = value.f1.getTimeStamp();
      if(startTime == null) {
        endTime = startTime = curTime;
      } else {
        if(startTime.isAfter(curTime)) {
          startTime = curTime;
        }
        if(endTime.isBefore(curTime)) {
          endTime = curTime;
        }
      }
    }
    if(startTime != null && endTime!= null) {
      out.collect(new Tuple1<>(Duration.between(startTime, endTime).getSeconds()));
    }
  }

}
