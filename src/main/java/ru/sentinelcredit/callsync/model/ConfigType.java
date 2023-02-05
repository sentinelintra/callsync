package ru.sentinelcredit.callsync.model;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Properties;

@Slf4j
@Data
public final class ConfigType {

    private static final String appPropFile = "application.properties";
    private static final String appTestFile = "application-test.properties";
    private String urlS;
    private String urlG;
    private String usernameS;
    private String usernameG;
    private String passwordS;
    private String passwordG;
    private Integer iCountOfMinWait;
    private Integer iBatchSize = 2000;
    private Integer iFetchSize = 2000;
    private Properties prop = null;

    private void setConfig (Properties prop) {
        if (prop.getProperty("siebelSrcUrl") != null) this.urlS = prop.getProperty("siebelSrcUrl");
        if (prop.getProperty("genesysSrcUrl") != null) this.urlG = prop.getProperty("genesysSrcUrl");
        if (prop.getProperty("siebelUsername") != null) this.usernameS =  prop.getProperty("siebelUsername");
        if (prop.getProperty("genesysUsername") != null) this.usernameG =  prop.getProperty("genesysUsername");
        if (prop.getProperty("siebelPassword") != null) this.passwordS =  prop.getProperty("siebelPassword");
        if (prop.getProperty("genesysPassword") != null) this.passwordG =  prop.getProperty("genesysPassword");
        if (prop.getProperty("iCountOfMinWait") != null) this.iCountOfMinWait = Integer.valueOf(prop.getProperty("iCountOfMinWait"));
    }

    private void loadConfigData(Boolean testMode) {
        try {
            //FileInputStream propsInput = new FileInputStream(appPropFile);
            ClassLoader classLoader = getClass().getClassLoader();
            //InputStream propsInput = classLoader.getResourceAsStream(appPropFile, "UTF-8");
            Reader propsInput = new InputStreamReader(classLoader.getResourceAsStream(appPropFile), "UTF-8");
            prop = new Properties();
            prop.load(propsInput);
            setConfig(prop);
        } catch (IOException e) {
            log.error(e.getMessage());
        }

        try {
            if (testMode) {
                //FileInputStream propsInput = new FileInputStream(appTestFile);
                ClassLoader classLoader = getClass().getClassLoader();
                //InputStream propsInput = classLoader.getResourceAsStream(appTestFile, "UTF-8");
                Reader propsInput = new InputStreamReader(classLoader.getResourceAsStream(appTestFile), "UTF-8");
                Properties prop = new Properties();
                prop.load(propsInput);
                setConfig(prop);
            }
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    public ConfigType(Boolean testMode) {
        loadConfigData(testMode);
    }

    public ConfigType(String[] args) {
        Boolean testMode = false;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("TestMode")) {
                testMode = true;
                break;
            }
        }

        loadConfigData(testMode);
    }

    public String getStringProperty (String name) {
        return prop.getProperty(name);
    }

    public Integer getIntegerProperty (String name) {
        return Integer.valueOf(prop.getProperty(name));
    }
}
