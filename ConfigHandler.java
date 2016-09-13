package com.apple.siri.audioapi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by blindblom on 4/10/16.
 */
public class ConfigHandler {
  private static final Logger logger = LoggerFactory.getLogger(ConfigHandler.class);
  private static ConfigHandler instance = null;
  private static Configuration config = null;
  private static Properties clientMetaConfig = new Properties();
  private static Properties wosMetaConfig = new Properties();

  private ConfigHandler() { }

  public static Configuration getHBaseConfig() throws IOException {
    initialize();
    return config;
  }

  public static Properties getClientMetaConfig() throws IOException {
    initialize();
    return clientMetaConfig;
  }

  public static Properties getWosMetaConfig() throws IOException {
    initialize();
    return wosMetaConfig;
  }

  private static void initialize() throws IOException {
    if (instance == null) {
      instance = new ConfigHandler();
      try {
        instance.getConfig();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  private Configuration getConfig() throws Exception {
    if (config == null){
      Properties properties = new Properties();
      InputStream inputStream;
      config = HBaseConfiguration.create();
      inputStream = this.getClass().getResourceAsStream("/config.properties");
      try {
        properties.load(inputStream);
      } catch (Exception e) {
        throw new FileNotFoundException("Unable to find config.properties file in resources directory");
      }

      for(String property : properties.stringPropertyNames()){
        logger.info("Reading property " + property + " = " + properties.getProperty(property));
        if (property.startsWith("audioapi.")) {
          clientMetaConfig.setProperty(property, properties.getProperty(property));
        } else if (property.startsWith("wos.")) {
          logger.info("Reading WOS property " + property + " = " + properties.getProperty(property));
          wosMetaConfig.setProperty(property, properties.getProperty(property));
          logger.info("Set WOS property " + property + " = " + wosMetaConfig.getProperty(property));
        } else {
          config.set(property, properties.getProperty(property));
        }
      }
    }
    return config;
  }
}
