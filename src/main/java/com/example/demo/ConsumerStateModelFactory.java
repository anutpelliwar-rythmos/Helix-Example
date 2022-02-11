package com.example.demo;

import org.apache.helix.participant.statemachine.StateModelFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;

public class ConsumerStateModelFactory extends StateModelFactory<ConsumerStateModel> {
	private final String consumerId;
	  private final String mqServer;

	  @Autowired
	  private AutowireCapableBeanFactory beanFactory;   
	  
	  public ConsumerStateModelFactory(String consumerId, String mqServer) {
	    this.consumerId = consumerId;
	    this.mqServer= mqServer;
	  }

	  @Override
	  public ConsumerStateModel createNewStateModel(String resource, String partition) {
	    ConsumerStateModel model = new ConsumerStateModel(consumerId, partition, mqServer);
	    beanFactory.autowireBean(model);
	    return model;
	  }
	}