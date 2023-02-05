package ru.sentinelcredit.callsync.model;

import lombok.Data;
import java.sql.Date;

@Data
public class PhoneType {
    private String crmContactId;
//    private Integer recordId;
    private String contactInfo;
    private Integer contactInfoType;
    public final static Integer recordType = 2;
    public final static Integer recordStatus = 1;
    private Integer attempt;
//    private Integer dailyFrom;
//    private Integer dailyTill;
//    private Integer tzDbId;
//    private Integer chainId;
    private Integer chainN;
//    private Integer dailyFrom2;
//    private Integer dailyTill2;
    private String crmCampConId;
    public final static Integer mskMusicFile = 9;
    private Integer chainSequence;
    private Integer contactInfoType2;
//    private Integer tzDbId2;
    public final static Integer callResult = 28;
    private String colLeadId;
    private String crmCampaignId;
    private Integer batchId;
//    private Date ccUpdateId;
}
