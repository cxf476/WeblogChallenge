package com.pitlab.logscan.flink.mapreduce;

import org.apache.flink.api.common.functions.MapFunction;

import com.pitlab.logscan.data.LogEntry;

public class EntryLoadMapper implements MapFunction<String, LogEntry> {

  private static final long serialVersionUID = 5449479641077669644L;

  @Override
  public LogEntry map(String line) throws Exception {
    return new LogEntry(line);
  }

}
