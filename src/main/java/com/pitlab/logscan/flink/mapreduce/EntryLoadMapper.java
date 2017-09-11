package com.pitlab.logscan.flink.mapreduce;

import java.time.LocalDateTime;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import com.pitlab.logscan.data.LogEntry;

public class EntryLoadMapper implements MapFunction<String, Tuple3<String, LocalDateTime, LogEntry>> {

  private static final long serialVersionUID = 5449479641077669644L;

  @Override
  public Tuple3<String, LocalDateTime, LogEntry> map(String line) throws Exception {
    LogEntry entry = new LogEntry(line);
    return new Tuple3<>(entry.getClientIP(), entry.getTimeStamp(), entry);
  }

}
