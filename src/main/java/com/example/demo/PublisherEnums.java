package com.example.demo;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum PublisherEnums {

  OPLOG_PARTION("oplog-partition"), CONSUMER_ID("consumer-id");

  @Getter
  private final String value;

}