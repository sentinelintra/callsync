package ru.sentinelcredit.callsync.service;

import ru.sentinelcredit.callsync.model.TimezoneType;

import java.util.HashMap;
import java.sql.*;

public class CampaignTimezone {

  private HashMap<String, TimezoneType> timeZone = new HashMap<String, TimezoneType>();

  public CampaignTimezone(Connection con) {

    try {
      Statement st = con.createStatement();
      ResultSet rs = st.executeQuery("select z.ROW_ID, g.TZ_DBID, g.NAME from S_TIMEZONE z, CX_GEN_TIME_ZONE_IDS g where z.NAME = g.NAME");

      while (rs.next()) {
        TimezoneType tz = new TimezoneType();
        tz.setTzDbId(rs.getInt(2));
        tz.setTzDbId2(Integer.valueOf(rs.getString(3).replaceAll("\\D+","").replace("00", "")));

        timeZone.put(rs.getString(1), tz);
      }

      try { rs.close(); st.close(); } catch (Exception ignore) { }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public Integer getTzDbId (String timezoneId) {
    if (timezoneId == null)
      return 0;

    Integer n = timeZone.get(timezoneId).getTzDbId();

    // http://jira.sentinelintra.net:8080/browse/STS-102
    if (n == -1)
      n = 186;

    return n;
  }

  public Integer getTzDbId2 (String timezoneId) {
    Integer n = timeZone.get(timezoneId).getTzDbId2();

    // http://jira.sentinelintra.net:8080/browse/STS-102
    if (n == -1)
      n = 3;

    return n;
  }
}