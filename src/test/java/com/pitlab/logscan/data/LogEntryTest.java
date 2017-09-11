package com.pitlab.logscan.data;

import java.text.ParseException;
import org.junit.Test;

import junit.framework.Assert;

public class LogEntryTest {
  
  @Test
  public void testInitEntry_givenCorrectLogLine_shouldGenerateExpectedAttributes() throws ParseException {
    String line = "2015-07-22T12:00:27.539654Z marketpalce-shop 1.39.97.17:42956 10.0.6.99:80 0.000047 0.009735 0.000029 200 200 0 211 "
        + "\"GET https://paytm.com:443/shop/cart HTTP/1.1\" \"Mozilla/5.0 (Linux; U; Android 4.2.1; en-US; Q700 Build/XOLOQ700) "
        + "AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 UCBrowser/10.5.2.582 U3/0.8.0 Mobile Safari/534.30\" ECDHE-RSA-AES128-SHA TLSv1";
    LogEntry entry = new LogEntry(line);
    Assert.assertEquals(entry.getTimeStamp().toString(), "2015-07-22T12:00:27.539654");
    Assert.assertEquals(entry.getClientIP(), "1.39.97.17");
    Assert.assertEquals(entry.getUrl(), "https://paytm.com:443/shop/cart");
  }
  
  @Test
  public void testInitEntry_givenUrlWithParameters_shouldGenerateCorrectUrl() throws ParseException {
    String line = "2015-07-22T12:00:27.539654Z marketpalce-shop 1.39.97.17:42956 10.0.6.99:80 0.000047 0.009735 0.000029 200 200 0 211 "
        + "\"GET https://paytm.com:443/shop/cart?user=ua HTTP/1.1\" \"Mozilla/5.0 (Linux; U; Android 4.2.1; en-US; Q700 Build/XOLOQ700) "
        + "AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 UCBrowser/10.5.2.582 U3/0.8.0 Mobile Safari/534.30\" ECDHE-RSA-AES128-SHA TLSv1";
    LogEntry entry = new LogEntry(line);
    Assert.assertEquals(entry.getTimeStamp().toString(), "2015-07-22T12:00:27.539654");
    Assert.assertEquals(entry.getClientIP(), "1.39.97.17");
    Assert.assertEquals(entry.getUrl(), "https://paytm.com:443/shop/cart");
  }
  
  @Test(expected = RuntimeException.class)
  public void testInitEntry_givenInCorrectLogLine_shouldThrowException() {
    String line = "2015-07-22T12:00:27.539654Z marketpalce-shop";
    LogEntry entry = new LogEntry(line);
  }
  
  @Test(expected = IllegalArgumentException.class) 
  public void testInitEntry_givenFtpProtol_shouldThrowException() {
    String line = "2015-07-22T12:00:27.539654Z marketpalce-shop 1.39.97.17:42956 10.0.6.99:80 0.000047 0.009735 0.000029 200 200 0 211 "
        + "\"GET ftp://paytm.com:443/shop/cart ftp/1.1\" \"Mozilla/5.0 (Linux; U; Android 4.2.1; en-US; Q700 Build/XOLOQ700) "
        + "AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 UCBrowser/10.5.2.582 U3/0.8.0 Mobile Safari/534.30\" ECDHE-RSA-AES128-SHA TLSv1";
    LogEntry entry = new LogEntry(line);
  }
}
