package com.pitlab.logscan.flink.mapreduce;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class AverageGroupReducer implements GroupReduceFunction<Tuple2<Long, String>, Long> {
  private static final long serialVersionUID = -4245776888044342936L;

  @Override
  public void reduce(Iterable<Tuple2<Long, String>> values, Collector<Long> out) throws Exception {
    Long count = 0L;
    Long sum = 0L;
    for(Tuple2<Long, String> value : values) {
      ++count;
      sum+=value.f0;
    }
    if(count>0) {
      out.collect(sum/count);
    }
  }

}
