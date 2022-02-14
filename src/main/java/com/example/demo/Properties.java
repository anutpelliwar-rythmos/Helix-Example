package com.example.demo;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.annotation.Configuration;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Configuration
@ConfigurationProperties(prefix = "demo")
@Data
@RefreshScope
@AllArgsConstructor
@NoArgsConstructor
public class Properties {
  private String zookeeperAddress;
  private String helixClusterName;
  private Integer partitionCount;
  private String stateModel;
  private String resourceName;
}
