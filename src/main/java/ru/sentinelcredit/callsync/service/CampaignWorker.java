package ru.sentinelcredit.callsync.service;

import org.slf4j.MDC;
import lombok.extern.slf4j.Slf4j;

import ru.sentinelcredit.callsync.model.CampConType;
import ru.sentinelcredit.callsync.model.CampaignType;
import ru.sentinelcredit.callsync.model.ConfigType;
import ru.sentinelcredit.callsync.model.PhoneType;

import java.sql.*;
import java.util.*;
import java.util.Date;

@Slf4j
public class CampaignWorker implements Runnable {
  
  private String doStop = "true";
  private CampaignType campaignType = null;
  private CampaignTimezone campaignTimezone;
  private Set<String> genTableName;
  private ConfigType configType;
  private Connection conS, conG;
  private Set<String> genCampCon = new HashSet();
  private Set<String> crmCampCon = new HashSet();
  //private Set<String> crmRetainCon = new HashSet();
  private HashMap<String, CampConType> campconMap = new HashMap<String, CampConType>();

  public synchronized void doStart() { this.doStop = "false"; }
  public synchronized void doFinish() { this.doStop = "finish"; }
  public synchronized boolean keepFinishing() { return this.doStop.equals("finish"); }
  public synchronized boolean keepRuning() { return this.doStop.equals("false"); }

  public CampaignType getCampaignType () { return this.campaignType; }
  public void setCampaignType (CampaignType campaignType) {
      this.campaignType = campaignType;
  }
  public void setCampaignTimezone (CampaignTimezone campaignTimezone) {
      this.campaignTimezone = campaignTimezone;
  }
  public void setConfigType (ConfigType configType) {
        this.configType = configType;
  }
  public void setGenTableName (Set<String> genTableName) {
      this.genTableName = genTableName;
  }

  public synchronized void doStop() {
      campaignType.deactivateCampaign();
      this.doStop = "true";
  }

  private Integer insertPhones(String conId) {
      PreparedStatement pst = null;
      ResultSet rs = null;
      Set<PhoneType> phoneTypes = new HashSet();
      Integer cntPhones = 0;

      //log.trace("insertPhones start conId => {}", conId);

      String timezoneId = campconMap.get(conId).getTimezoneId();
      String campConId = campconMap.get(conId).getCampConId();
      String campaignId = campaignType.getCampaignId();
      String genTableName = campaignType.getGenTableName();
      Integer chainId = campconMap.get(conId).getChainId();
      Integer tzDbId = campaignTimezone.getTzDbId(timezoneId);
      Integer tzDbId2 = campaignTimezone.getTzDbId2(timezoneId);
      Integer dailyFrom = campaignType.getDtFromX(tzDbId);
      Integer dailyTill = campaignType.getDtToX(tzDbId);
      Date ccUpdateId = campaignType.getCcUpdateId();

      // http://jira.sentinelintra.net:8080/browse/SBLSC-52
      if (campconMap.get(conId).getDmStrat().contains("MNA") || campconMap.get(conId).getDmStrat().contains("MNP"))
          dailyTill -= 7 * 3600;

      try {
          pst = conS.prepareStatement(
              "select addr, contact_info_type, x_chain_sq, m_attempts from siebel.cx_soft_phones_for_cl where per_id = ?");
          pst.setFetchSize(configType.getIFetchSize());
          pst.setString(1, conId);
          rs = pst.executeQuery();

          int uniqueId = 0;
          while (rs.next()) {
              String addr = rs.getString(1);
              Integer infoType = rs.getInt(2);
              Integer chainSq = rs.getInt(3);
              Integer attempt = rs.getInt(4);
              cntPhones++;

              // Для OVV_CALL передаем 0, по письму Бычкова от Пн 27.01.2020 16:02
              if (campaignType.getCampaignId().equals("1-Z97PC4M"))
                  attempt = 0;

              // http://jira.sentinelintra.net:8080/browse/SBLSC-26
              if (attempt != 15) {
                  PhoneType pt = new PhoneType();
                  pt.setCrmContactId(conId);
//                  pt.setRecordId(uniqueId);
                  pt.setContactInfo(addr);
                  pt.setContactInfoType(infoType);
                  pt.setAttempt(attempt);
                  //pt.setDailyFrom(dailyFrom);
                  //pt.setDailyTill(dailyTill);
                  //pt.setTzDbId(tzDbId);
                  //pt.setChainId(chainId);
                  pt.setChainN(uniqueId++);
                  //pt.setDailyFrom2(dailyFrom);
                  //pt.setDailyTill2(dailyTill);
                  pt.setCrmCampConId(campConId);
                  pt.setChainSequence(chainSq);
                  pt.setContactInfoType2(infoType);
                  //pt.setTzDbId2(tzDbId2);
                  pt.setColLeadId(conId);
                  pt.setCrmCampaignId(campaignId);
                  pt.setBatchId(1);
                  //pt.setCcUpdateId(ccUpdateId);
                  phoneTypes.add(pt);
              }
          }
      } catch (Exception e) {
          log.error("{}", e);
      } finally {
          try { rs.close(); } catch (Exception ignore) { }
          try { pst.close(); } catch (Exception ignore) { }
      }

      try {
          if (phoneTypes.size() > 0) {
              pst = conG.prepareStatement("insert into GENESYSSQL." + genTableName +
                      "( crm_contact_id, record_id, contact_info, contact_info_type, record_type, record_status, attempt," +
                      " daily_from, daily_till, tz_dbid, chain_id, chain_n, daily_from2, daily_till2, crm_camp_con_id," +
                      " msk_music_file, chain_sequence, contact_info_type2, tz_dbid2, call_result, col_lead_id," +
                      " crm_campaign_id, x_batch_id, x_cc_upd_id )" +
                      " values ( ?, my_sync_service_seq.nextval, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

              Integer n = 0;
              Iterator<PhoneType> i = phoneTypes.iterator();
              while (i.hasNext()) {
                  PhoneType pt = i.next();

                  pst.setString(1, pt.getCrmContactId());   // crm_contact_id
//                  pst.setInt(2, pt.getRecordId());        // record_id
                  pst.setString(2, pt.getContactInfo());    // contact_info
                  pst.setInt(3, pt.getContactInfoType());   // contact_info_type
                  pst.setInt(4, pt.recordType);             // record_type
                  pst.setInt(5, pt.recordStatus);           // record_status
                  pst.setInt(6, pt.getAttempt());           // attempt
                  pst.setInt(7, dailyFrom);                 // daily_from
                  pst.setInt(8, dailyTill);                 // daily_till
                  pst.setInt(9, tzDbId);                    // tz_dbid
                  pst.setInt(10, chainId);                  // chain_id
                  pst.setInt(11, pt.getChainN());           // chain_n
                  pst.setInt(12, dailyFrom);                // daily_from2
                  pst.setInt(13, dailyTill);                // daily_till2
                  pst.setString(14, pt.getCrmCampConId());  // crm_camp_con_id
                  pst.setInt(15, pt.mskMusicFile);          // msk_music_file
                  pst.setInt(16, pt.getChainSequence());    // chain_sequence
                  pst.setInt(17, pt.getContactInfoType2()); // contact_info_type2
                  pst.setInt(18, tzDbId2);                  // tz_dbid2
                  pst.setInt(19, pt.callResult);            // call_result
                  pst.setString(20, pt.getCrmContactId());  // col_lead_id
                  pst.setString(21, pt.getCrmCampaignId()); // crm_campaign_id
                  pst.setInt(22, pt.getBatchId());          // x_batch_id
                  pst.setDate(23, new java.sql.Date(ccUpdateId.getTime()));              // x_cc_upd_id
                  pst.addBatch();
                  n++;

                  if (n % configType.getIBatchSize() == 0) {
                      pst.executeBatch();
                      conG.commit();
                      n = 0;
                  }
                  
                  log.trace("insertPhones conId => {} phone => {}", conId, pt.getContactInfo());
              }

              if (n > 0) {
                  pst.executeBatch();
                  conG.commit();
              }
          }
      } catch (Exception e) {
          log.error("{}", e);
      } finally {
          try { pst.close(); } catch (Exception ignore) { }
      }

      //log.trace("insertPhones stop conId => {}", conId);

      return cntPhones;
  }

  private void startCampCon(String campConId) {
    PreparedStatement pst = null;

    log.trace("startCampCon start campConId => {}", campConId);

    try {
          pst = conG.prepareStatement(
                  "update genesyssql." + campaignType.getGenTableName() + " set record_status = 1 where crm_contact_id = ? ");

          pst.setString(1, campConId);
          pst.execute();
          conG.commit();
      } catch (Exception e) {
          log.error("{}", e);
      } finally {
          try { pst.close(); } catch (Exception ignore) { }
      }

      log.trace("startCampCon finished");
  }

  private String checkCampCon(String campConId) {
      String stopFlg = "N";
      CallableStatement stmt = null;

      log.trace("checkCampCon start campConId => {}", campConId);

      try {
          stmt = conS.prepareCall(" { call siebel.cx_soft.con_check ( ?, ? ) }");
          stmt.setString(1, campConId);
          stmt.registerOutParameter(2, Types.VARCHAR);
          stmt.executeUpdate();
          stopFlg = stmt.getString(2);
      } catch (Exception e) {
          log.error("{}", e);
      } finally {
          try { stmt.close(); } catch (Exception ignore) { }
      }

      log.trace("checkCampCon stop campConId => {}", campConId);

      return stopFlg;
  }

  private String checkGenRetrived (String conId) {
      PreparedStatement pst = null;
      ResultSet rs = null;
      String stopFlg = "N";

      log.trace("checkGenRetrived start conId => {}", conId);

      try {
          pst = conG.prepareStatement("select 1 from genesyssql." + campaignType.getGenTableName() + " where crm_contact_id = ?");
          pst.setString(1, conId);
          rs = pst.executeQuery();

          if (rs.next())
              stopFlg = "Y";
      } catch (Exception e) {
          log.error("{}", e);
      } finally {
          try { pst.close(); } catch (Exception ignore) { }
          try { rs.close(); } catch (Exception ignore) { }
      }

      log.trace("checkGenRetrived stop conId => {} => {}", conId, stopFlg);

      return stopFlg;
  }

  private String checkGenCon(String conId) {
    PreparedStatement pst = null;
    ResultSet rs = null;
    String stopFlg = "N";

    log.trace("checkGenCon start conId => {}", conId);

    Iterator<String> i = genTableName.iterator();
    while (i.hasNext()) {
        String tableName = i.next();
        if (tableName.equals(campaignType.getGenTableName()))
            continue;

        try {
            pst = conG.prepareStatement("select 1 from genesyssql." + tableName + " where crm_contact_id = ?");
            pst.setString(1, conId);
            rs = pst.executeQuery();

            if (rs.next()) {
                stopFlg = tableName;
            }
        } catch (Exception e) {
            log.error("{}", e);
        } finally {
            try { pst.close(); } catch (Exception ignore) { }
            try { rs.close(); } catch (Exception ignore) { }
        }

        if (stopFlg != "N")
            break;
    }

    log.trace("checkGenCon stop conId => {} => {}", conId, stopFlg);

    return stopFlg;
  }

  private void prepareSetData() {
      Statement st = null;
      ResultSet rs = null;
      PreparedStatement pst = null;
      Set<String> genBaseCampCon = new HashSet();
      Set<String> crmBaseCampCon = new HashSet();
      long startMillis = System.currentTimeMillis();

      log.trace("PrepareSetData start");

      genCampCon.clear();
      crmCampCon.clear();
      //crmRetainCon.clear();

      try {
          log.trace("PrepareSetData genesyssql start");

          st = conG.createStatement();
          st.setFetchSize(configType.getIFetchSize());
          rs = st.executeQuery("select distinct crm_contact_id from genesyssql." + campaignType.getGenTableName());

          genBaseCampCon.clear();
          while (rs.next())
              genBaseCampCon.add(rs.getString(1));
      } catch (Exception e) {
          log.error("{}", e);
      } finally {
          try { rs.close(); } catch (Exception ignore) { }
          try { st.close(); } catch (Exception ignore) { }
      }

      log.trace("PrepareSetData genesyssql finished, {} rows fetched", genBaseCampCon.size());
      log.trace("PrepareSetData siebel start");

      try {
          long startExecMillis = System.currentTimeMillis();

          pst = conS.prepareStatement(
                  "select /*+ PARALLEL(2) */ cc.row_id, cc.con_per_id, c.timezone_id, c.x_chain_id, " +
                          "listagg ( a.x_cur_dm_strat, ',' ) within group ( order by cc.con_per_id ), cc.x_cc_upd_id " +
                          "from siebel.s_camp_con cc, siebel.s_contact c, siebel.s_asset a " +
                          "where cc.con_per_id = c.row_id " +
                          "and cc.con_per_id = a.pr_con_id " +
                          "and ( a.x_date_reviews is null or a.x_date_reviews > sysdate ) " +
                          "and cc.src_id = ? /*and ( cc.x_cc_upd_id <> ? or cc.x_cc_upd_id is null ) and c.row_id = '1-10RIOS'*/" +
                          "group by cc.row_id, cc.con_per_id, c.timezone_id, c.x_chain_id, cc.x_cc_upd_id");
          pst.setFetchSize(configType.getIFetchSize());
          pst.setString(1, campaignType.getCampaignId());
          //pst.setDate(2, new java.sql.Date(campaignType.getCcUpdateId().getTime()));
          rs = pst.executeQuery();

          long endExecMillis = System.currentTimeMillis();
          log.trace("Siebel execute time {} sec", (endExecMillis-startExecMillis)/1000);

          crmCampCon.clear();
          campconMap.clear();
          crmBaseCampCon.clear();
          Integer n = 0;
          while (rs.next()) {
              /*PreparedStatement psa = conS.prepareStatement("select distinct x_cur_dm_strat from siebel.s_asset where ( x_date_reviews is null or x_date_reviews > sysdate ) and pr_con_id = ?");
              psa.setFetchSize(configType.getIFetchSize());
              psa.setString(1, rs.getString(2));
              ResultSet rs1 = psa.executeQuery();

              String dmStrat = "";
              while (rs1.next()) {
                  dmStrat += ((dmStrat.equals("")) ? "": ",") + rs1.getString(1);
              }

              try { rs1.close(); } catch (Exception ignore) { }
              try { psa.close(); } catch (Exception ignore) { }*/

              crmBaseCampCon.add(rs.getString(2));

              if (rs.getDate(6) == null || campaignType.getCcUpdateId().getTime() != rs.getDate(6).getTime()) {
                  CampConType cc = new CampConType();
                  cc.setCampConId(rs.getString(1));
                  cc.setTimezoneId(rs.getString(3));
                  cc.setChainId(rs.getInt(4));
                  cc.setDmStrat(rs.getString(5));
                  campconMap.put(rs.getString(2), cc);
                  crmCampCon.add(rs.getString(2));
              }

              n++;
              if ( n % configType.getIBatchSize() == 0) {
                  log.trace("PrepareSetData siebel fetched {}", n/configType.getIBatchSize());
              }
          }
      } catch (Exception e) {
              log.error("{}", e);
      } finally {
          try { rs.close(); } catch (Exception ignore) { }
          try { pst.close(); } catch (Exception ignore) { }
      }

      log.trace("PrepareSetData siebel finished, {} rows fetched", crmBaseCampCon.size());

      genCampCon.clear();
      genCampCon.addAll(genBaseCampCon);
      genCampCon.removeAll(crmBaseCampCon);

      //crmCampCon.clear();
      //crmCampCon.addAll(crmBaseCampCon);
      //crmCampCon.removeAll(genBaseCampCon);

      //crmRetainCon.clear();
      //crmRetainCon.addAll(crmBaseCampCon);
      //crmRetainCon.retainAll(genBaseCampCon);

      long endMillis = System.currentTimeMillis();

      log.trace("PrepareSetData finished ({}, {}) in {} sec", genCampCon.size(), crmCampCon.size()/*, crmRetainCon.size()*/, (endMillis-startMillis)/1000);
  }

  private void deleteGenTableData(String conId, String updateType) {
      PreparedStatement pst = null;

      log.trace("deleteGenTableData start conId => {} updateType => {}", conId, updateType);

      try {
          pst = conG.prepareStatement("update genesyssql." + campaignType.getGenTableName() + " set portfolio_name = to_char(sysdate)||'" + updateType + "' " +
                  "where crm_contact_id = ? and portfolio_name is null");
          pst.setString(1, conId);
          pst.execute();
      } catch (Exception e) {
          log.error("{}", e);
      } finally {
          try { pst.close(); } catch (Exception ignore) { }
      }

      try {
          pst = conG.prepareStatement("delete from genesyssql." + campaignType.getGenTableName() + " where " +
                  "crm_contact_id = ? and record_status <> case when sysdate - ? >= ? / 60 / 24 then 99 else 2 end");

          pst.setString(1, conId);
          pst.setDate(2, new java.sql.Date(campaignType.getCcUpdateId().getTime()));
          pst.setInt(3, configType.getICountOfMinWait());
          pst.execute();
          conG.commit();
      } catch (Exception e) {
          log.error("{}", e);
      } finally {
          try { pst.close(); } catch (Exception ignore) { }
      }

      log.trace("deleteGenTableData stop conId => {}", conId);
  }

  private void deleteGenTableData() {
      PreparedStatement pst = null;
      long startMillis = System.currentTimeMillis();

      log.trace("deleteGenTableData start");

      try {
          if (genCampCon.size() > 0) {
              pst = conG.prepareStatement("update genesyssql." + campaignType.getGenTableName() + " set portfolio_name = to_char(sysdate)||'x' where crm_contact_id = ? and portfolio_name is null");

              Integer n = 0;
              Iterator<String> i = genCampCon.iterator();
              while (i.hasNext()) {
                  pst.setString(1, i.next());
                  pst.addBatch();
                  n++;

                  if (n % configType.getIBatchSize() == 0) {
                      pst.executeBatch();
                      conG.commit();
                      n = 0;
                  }
              }

              if (n > 0) {
                  pst.executeBatch();
                  conG.commit();
              }
          }
      } catch (Exception e) {
          log.error("{}", e);
      } finally {
          try { pst.close(); } catch (Exception ignore) { }
      }

      try {
          if (genCampCon.size() > 0) {
              pst = conG.prepareStatement("delete from genesyssql." + campaignType.getGenTableName() + " where " +
                      "crm_contact_id = ? and record_status <> case when sysdate - ? >= ? / 24 / 60 then 99 else 2 end");

              Integer n = 0;
              Iterator<String> i = genCampCon.iterator();
              while (i.hasNext()) {
                  pst.setString(1, i.next());
                  pst.setDate(2, new java.sql.Date(campaignType.getCcUpdateId().getTime()));
                  pst.setInt(3, configType.getICountOfMinWait());
                  pst.addBatch();
                  n++;

                  if (n % configType.getIBatchSize() == 0) {
                      pst.executeBatch();
                      conG.commit();
                      n = 0;
                  }
              }

              if (n > 0) {
                  pst.executeBatch();
                  conG.commit();
              }
          }
      } catch (Exception e) {
          log.error("{}", e);
      } finally {
          try { pst.close(); } catch (Exception ignore) { }
      }

      try {
          pst = conG.prepareStatement("delete from genesyssql." + campaignType.getGenTableName() + " where " +
                  "record_status = 2 and portfolio_name is not null and x_cc_upd_id < trunc ( sysdate )");

          pst.execute();
          conG.commit();
      } catch (Exception e) {
          log.error("{}", e);
      } finally {
          try { pst.close(); } catch (Exception ignore) { }
      }

      long endMillis = System.currentTimeMillis();

      log.trace("deleteGenTableData finished in {} sec", (endMillis-startMillis)/1000);
  }

  private void updateCampCon (String campConId, String comment) {
      PreparedStatement pst = null;

      log.trace("updateCampCon start campConId => {}", campConId);

      try {
          pst = conS.prepareStatement("update siebel.s_camp_con set X_CC_UPD_ID = ?, X_UPD_COMMENTS = ? where row_id = ?");
          pst.setDate(1,  new java.sql.Date((comment.equals("Ожидание выхода старых строк из КЛ")) ? 0L: campaignType.getCcUpdateId().getTime()));
          pst.setString(2, comment);
          pst.setString(3, campConId);
          pst.execute();
          conS.commit();

      } catch (Exception e) {
          log.error("{}", e);
      } finally {
          try { pst.close(); } catch (Exception ignore) { }
      }

      log.trace("updateCampCon stop campConId => {}", campConId);
  }

  /*private void skipCampCon() {
      PreparedStatement pst = null;
      long startMillis = System.currentTimeMillis();

      log.trace("skipCampCon start");

      try {
          if (crmRetainCon.size() > 0) {
              pst = conS.prepareStatement("update siebel.s_camp_con set X_CC_UPD_ID = ?, X_UPD_COMMENTS = ? where src_id = ? and con_per_id = ?");

              Integer n = 0;
              Iterator<String> i = crmRetainCon.iterator();
              while (i.hasNext()) {
                  pst.setDate(1,  new java.sql.Date(campaignType.getCcUpdateId().getTime()));
                  pst.setString(2, "Ожидание выхода старых строк из КЛ");
                  pst.setString(3, campaignType.getCampaignId());
                  pst.setString(4, i.next());
                  pst.addBatch();
                  n++;

                  if (n % configType.getIBatchSize() == 0) {
                      pst.executeBatch();
                      conS.commit();
                      n = 0;
                  }
              }

              if (n > 0) {
                  pst.executeBatch();
                  conS.commit();
              }
          }
      } catch (Exception e) {
          log.error("{}", e);
      } finally {
          try { pst.close(); } catch (Exception ignore) { }
      }

      long endMillis = System.currentTimeMillis();

      log.trace("skipCampCon finished in {} sec", (endMillis-startMillis)/1000);
  }*/

  private void syncClient() {

      long startMillis = System.currentTimeMillis();

      log.trace("syncClient {} start", campaignType.getCcUpdateIdFull());

      if (crmCampCon.size() > 0) {
          Iterator<String> i = crmCampCon.iterator();
          while (i.hasNext() && keepRuning()) {
              String conId = i.next();
              CampConType cc = campconMap.get(conId);
              if (checkCampCon(cc.getCampConId()).equals("N")) {
                  deleteGenTableData(conId, "R");
                  if (checkGenRetrived(conId).equals("N")) {
                      if (insertPhones(conId) == 0) {
                          updateCampCon(cc.getCampConId(), "Нет телефонов");
                      } else {
                          String anotherCamp = checkGenCon(conId);
                          if (anotherCamp.equals("N")) {
                              //startCampCon(conId);
                              updateCampCon(cc.getCampConId(), "");
                          } else {
                              updateCampCon(cc.getCampConId(), "Присутствует в другом КЛ (" + anotherCamp + ")");
                          }
                      }
                  } else {
                      updateCampCon(cc.getCampConId(), "Ожидание выхода старых строк из КЛ");
                  }
              } else {
                  deleteGenTableData(conId, "L");
                  if (checkGenRetrived(conId).equals("N"))
                    updateCampCon(cc.getCampConId(), "Нет договоров в работе");
              }
          }
      }

      long endMillis = System.currentTimeMillis();

      log.trace("syncClient {} finished in {} sec", campaignType.getCcUpdateIdFull(), (endMillis-startMillis)/1000);
  }
  
  @Override
  public void run() {

      MDC.put("logThreadName", Thread.currentThread().getName());

      try {
          Class.forName("oracle.jdbc.driver.OracleDriver");
          conS = DriverManager.getConnection(configType.getUrlS(), configType.getUsernameS(), configType.getPasswordS());
          conG = DriverManager.getConnection(configType.getUrlG(), configType.getUsernameG(), configType.getPasswordG());
          conS.setAutoCommit(false);
          conG.setAutoCommit(false);

          while (true) {
              if (keepRuning())
                  prepareSetData();

              if (keepRuning())
                  deleteGenTableData();

            //if (keepRuning())
            //    skipCampCon();

              if (keepRuning()) {
                  syncClient();
                  doFinish();
              }

            // log.trace("{}", Thread.currentThread().getName(), keepRuning());

              Thread.sleep(10L * 1000L);
          }
      } catch (InterruptedException e) {
          log.error("{}", e);
      } catch (Exception e) {
          log.error("{}", e);
      }

      //remember remove this
      MDC.remove("logThreadName");
  }
}
