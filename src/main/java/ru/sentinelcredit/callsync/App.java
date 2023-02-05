package ru.sentinelcredit.callsync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.sentinelcredit.callsync.model.CampaignType;
import ru.sentinelcredit.callsync.model.ConfigType;
//import lombok.extern.slf4j.Slf4j;
import ru.sentinelcredit.callsync.service.CampaignTimezone;
import ru.sentinelcredit.callsync.service.CampaignWorker;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.charset.Charset;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.HashSet;
import java.util.HashMap;
import java.sql.*;
import java.util.Set;

//@Slf4j
public class App 
{
    final static Logger log = LoggerFactory.getLogger(App.class);

    public static void main( String[] args )
    {
        ConfigType configType = new ConfigType(args);
        Set<String> genTableName = new HashSet();

        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            Connection con = DriverManager.getConnection(configType.getUrlS(), configType.getUsernameS(), configType.getPasswordS());
            if (con != null)
                log.trace("Connected to the database {}", configType.getUrlS());
            else
                log.trace("Failed to make connection {}", configType.getUrlS());

            con.setAutoCommit(false);

            Statement st0 = null;
            ResultSet rs0 = null;
            try {
                st0 = con.createStatement();
                st0.setFetchSize(configType.getIFetchSize());
                rs0 = st0.executeQuery(
                        "select x_gen_table_name from siebel.s_src where x_gen_table_name is not null and tmpl_id = '1'");

                while (rs0.next()) {
                    genTableName.add(rs0.getString(1));
                }
            } finally {
                try { rs0.close(); } catch (Exception ignore) { }
                try { st0.close(); } catch (Exception ignore) { }
            }

            CampaignTimezone campaignTimezone = new CampaignTimezone(con);
            HashMap<String, CampaignWorker> campaignMap = new HashMap<String, CampaignWorker>();

            while (true) {
                Statement st = con.createStatement();
                st.setFetchSize(configType.getIFetchSize());
                ResultSet rs = st.executeQuery(
                        "select s.row_id, s.name, s.x_in_work, s.x_time_from_weekend, s.x_time_to_weekend, s.x_time_from_weekday, s.x_time_to_weekday, s.x_gen_table_name, s.x_cc_upd_id, x.attrib_08, s.last_upd " +
                                "from siebel.s_src s, siebel.s_src_x x "+
                                "where s.row_id = x.par_row_id and s.x_gen_table_name is not null and s.tmpl_id = '1' /*and s.row_id = '1-VUX2IYN'*/");

                while (rs.next()) {
                    String campaignId = rs.getString(1);
                    Boolean workStatus = rs.getBoolean(3);
                    String inStat = rs.getString(10);

                    CampaignWorker campaignWorker = null;
                    if (campaignMap.containsKey(campaignId)) {
                        campaignWorker = campaignMap.get(campaignId);
                    } else {
                        campaignWorker = new CampaignWorker();
                        campaignWorker.setCampaignTimezone(campaignTimezone);
                        campaignWorker.setConfigType(configType);
                        campaignWorker.setGenTableName(genTableName);
                        //campaignWorker.setAppender();
                        campaignMap.put(campaignId, campaignWorker);
                    }

                    Boolean needStartThread = false;
                    CampaignType campaignType = campaignWorker.getCampaignType();
                    if (campaignType == null) {
                        campaignType = new CampaignType(con);
                        needStartThread = true;
                    }

                    campaignType.setCampaignId(campaignId);
                    campaignType.setInStat(inStat);
                    campaignType.setInWork(workStatus);
                    campaignType.setCampaignName(rs.getString(2));
                    campaignType.setTimeFromWeekend(rs.getDate(4));
                    campaignType.setTimeToWeekend(rs.getDate(5));
                    campaignType.setTimeFromWeekday(rs.getDate(6));
                    campaignType.setTimeToWeekday(rs.getDate(7));
                    campaignType.setGenTableName(rs.getString(8));
                    campaignType.setCcUpdateId(rs.getDate(9));
                    campaignType.setLastUpdate(rs.getDate(11));

                    campaignWorker.setCampaignType(campaignType);

                    if (needStartThread) {
                        Thread thread = new Thread(campaignWorker, campaignId);
                        thread.start();
                    }

                    if (campaignWorker.keepFinishing()) {
                        campaignWorker.doStop();
                    } else if (workStatus && !campaignWorker.keepRuning()) {
                        campaignWorker.doStart();
                    } else if (!workStatus && campaignWorker.keepRuning()) {
                        campaignWorker.doStop();
                    }
                }

                try { rs.close(); } catch (Exception ignore) { }
                try { st.close(); } catch (Exception ignore) { }

                Thread.sleep(10L * 1000L);
            }
        } catch (SQLException e) {
            log.error(e.getMessage());
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }
}
