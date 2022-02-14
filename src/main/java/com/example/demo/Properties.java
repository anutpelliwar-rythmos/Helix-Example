package com.example.demo;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.annotation.Configuration;
import lombok.Data;

@Configuration
@ConfigurationProperties(prefix = "demo")
@Data
@RefreshScope
public class Properties {
  private String zookeeperAddress;
  private String helixClusterName;
  private Integer partitionCount;
  private String stateModel;
  private String resourceName;
}
