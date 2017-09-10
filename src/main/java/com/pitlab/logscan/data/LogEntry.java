package com.pitlab.logscan.data;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class LogEntry implements Serializable {
  private static final long serialVersionUID = -8224886009741842927L;
  
  private final LocalDateTime timeStamp;
  private final String clientIP;
  private final String[] attributes;

  public LogEntry(String line) {
    attributes = line.split(" ");
    timeStamp = LocalDateTime.parse(attributes[0], DateTimeFormatter.ISO_DATE_TIME);
    String client = attributes[2];
    clientIP = client.substring(0, client.indexOf(':'));
    
    System.out.println(timeStamp + "," + clientIP);
  }
  
  public LocalDateTime getTimeStamp() {
    return timeStamp;
  }
  
  public String getClientIP() {
    return clientIP;
  }
}
