package com.banary.mapred;

import org.apache.hadoop.conf.Configuration;

public class ConfigurationMain {

    public static void main(String[] args) {
        String xmlUrl = "~/workspace/canary/scalaDemo/src/main/resources/configuration.xml";
        Configuration config = new Configuration();
        config.addResource(xmlUrl);
        System.out.println(config.get("color"));
        System.out.println(config.get("Color", "White"));
    }

    public static Configuration loadConfigurationByXML(String xmlUrl){
        Configuration config = new Configuration();
        config.addDefaultResource(xmlUrl);
        System.setProperty("color", "1");
        return config;
    }
}
