package com.shc.rtp.common;

import java.io.InputStream;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.SystemConfiguration;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.jasypt.properties.EncryptableProperties;

/**
 * Created by uchaudh on 10/15/2015.
 */
public class NPOSConfiguration {
    private final String encryptionKey = "rtpEncKey_1_2_3";
    private final String encryptionAlgorithm = "PBEWITHSHA1ANDRC2_40";

    private Configuration configuration;

    public NPOSConfiguration() {
        this.initializeConfigurations();
    }

    public int getInt(String key) {
        return this.configuration.getInt(key);
    }

    public String getString(String key) {
        return this.configuration.getString(key);
    }

    private void initializeConfigurations() {
        try {
            synchronized (this) {
                StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
                encryptor.setPassword(encryptionKey);
                encryptor.setAlgorithm(encryptionAlgorithm);
                Properties properties = new EncryptableProperties(encryptor);
                CompositeConfiguration compositeConfiguration = new CompositeConfiguration();
                compositeConfiguration.addConfiguration(new SystemConfiguration());

                InputStream propertyFileStream = Thread.currentThread().getContextClassLoader()
                        .getResourceAsStream("application_integration.properties");
                properties.load(propertyFileStream);
                Set<Object> mapKeys = properties.keySet();
                for (Object currentkey : mapKeys) {
                    compositeConfiguration.addProperty(currentkey.toString(), properties.get(currentkey));
                }
                this.configuration = compositeConfiguration;
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }

    }

}