package ru.sentinelcredit.callsync.model;

import  lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

@Data
@Slf4j
public class CampaignType {
    private String campaignId;
    private String campaignName;
    private Boolean inWork;
    private Date timeFromWeekend;
    private Date timeToWeekend;
    private Date timeFromWeekday;
    private Date timeToWeekday;
    private String genTableName;
    private Date ccUpdateId;
    private String inStat;
    private Date lastUpdate;
    private Connection conS;

    public CampaignType (Connection conS) { this.conS = conS; }

    private long getTimeFromWeekend() {
        return timeFromWeekend.getTime() / 1000 % 86400 + ( 3 * 60 * 60 );
    }

    private long getTimeToWeekend() {
        return timeToWeekend.getTime() / 1000 % 86400 + ( 3 * 60 * 60 );
    }

    private long getTimeFromWeekday() {
        return timeFromWeekday.getTime() / 1000 % 86400 + ( 3 * 60 * 60 );
    }

    private long getTimeToWeekday() {
        return timeToWeekday.getTime() / 1000 % 86400 + ( 3 * 60 * 60 );
    }

    public Integer getDtFromX(Integer tzDbId) {
        long n = 0;
        Calendar c = Calendar.getInstance();
        int dayOfWeek = c.get(Calendar.DAY_OF_WEEK);

        if (dayOfWeek == 6 || dayOfWeek == 7) n = getTimeFromWeekend(); else n = getTimeFromWeekday();

        // http://jira.sentinelintra.net:8080/browse/STS-102
        if (tzDbId == -1)
            n += 3600; // +1ч

        return (int) n;
    }

    public Integer getDtToX(Integer tzDbId) {
        long n = 0;
        Calendar c = Calendar.getInstance();
        int dayOfWeek = c.get(Calendar.DAY_OF_WEEK);

        if (dayOfWeek == 6 || dayOfWeek == 7) n = getTimeToWeekend(); else n = getTimeToWeekday();

        // http://jira.sentinelintra.net:8080/browse/STS-102
        if (tzDbId == -1)
            n -= 32400; // -9ч

        return (int) n;
    }

    public String getCcUpdateIdFull() {
        SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String formatted = format1.format(ccUpdateId);
        return formatted;
    }

    public Date getCcUpdateIdOnlyDate() {
        Calendar cal = Calendar.getInstance(); // locale-specific
        cal.setTime(this.ccUpdateId);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return new Date(cal.getTimeInMillis());
    }

    public Boolean canDeactivate() {
        PreparedStatement pst = null;
        ResultSet rs = null;

        // Кампейн не трогали более часа
        if ((new Date().getTime() - lastUpdate.getTime()) / 1000 - ( 3 * 60 * 60 ) > 1 * 60 * 60)
            return true;

        try {
            pst = conS.prepareStatement("select 1 from siebel.s_camp_con where src_id = ? and x_cc_upd_id <> ?");
            pst.setFetchSize(1);
            pst.setString(1, campaignId);
            pst.setDate(2, new java.sql.Date(ccUpdateId.getTime()));
            rs = pst.executeQuery();

            // кампейн не трогали более N- минут и нет строк с неактуальным ИД обновления
            if ((new Date().getTime() - lastUpdate.getTime()) / 1000 - ( 3 * 60 * 60 ) > 15 * 60 && !rs.next())
                return true;
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            try { pst.close(); } catch (Exception ignore) { }
            try { rs.close(); } catch (Exception ignore) { }
        }

        return false;
    }

    public void deactivateCampaign() {
        PreparedStatement pst = null;

        if (!canDeactivate())
            return;

        try {
            pst = conS.prepareStatement("update siebel.s_src set x_in_work = ? where row_id = ?");
            pst.setString(1, "N");
            pst.setString(2, campaignId);
            pst.execute();
            conS.commit();
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            try { pst.close(); } catch (Exception ignore) { }
        }
    }
}
