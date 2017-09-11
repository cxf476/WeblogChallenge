package com.pitlab.logscan.data;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.flink.hadoop.shaded.com.google.common.base.Preconditions;

public class LogEntry implements Serializable {
  private static final long serialVersionUID = -8224886009741842927L;
  
  private final LocalDateTime timeStamp;
  private final String clientIP;
  private final String url;
  private final String[] attributes;
  
  private final static int INDEX_TIMESTAMP = 0;
  private final static int INDEX_CLIENT_IP = 2;
  private final static int INDEX_URL = 12;

  public LogEntry(String line) {
    attributes = line.split(" ");
    timeStamp = LocalDateTime.parse(attributes[INDEX_TIMESTAMP], DateTimeFormatter.ISO_DATE_TIME);
    String client = attributes[INDEX_CLIENT_IP];
    clientIP = client.substring(0, client.indexOf(':'));
    
    String urlParam = attributes[INDEX_URL];
    Preconditions.checkArgument(urlParam.toLowerCase().startsWith("http"), "the request protocol is not http/https: " + urlParam);
    if(urlParam.contains("?")) {
      url =urlParam.substring(0, urlParam.indexOf('?'));
    } else {
      url = urlParam;
    }
  }
  
  public LocalDateTime getTimeStamp() {
    return timeStamp;
  }
  
  public String getClientIP() {
    return clientIP;
  }
  
  public String getUrl() {
    return url;
  }
}
