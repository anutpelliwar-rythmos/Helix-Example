package com.example.demo;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.log4j.MDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;

@StateModelInfo(initialState = "OFFLINE", states = {
	    "ONLINE", "ERROR"
	})
	public class ConsumerStateModel extends StateModel {
	
	  private static Logger LOG = LoggerFactory.getLogger(ConsumerStateModel.class);

	  private final String consumerId;
	  private final String partition;
	  private final String  mqServer;
	  private ConsumerThread _thread = null;
	  
	  @Autowired
	  private AutowireCapableBeanFactory beanFactory;


	  public ConsumerStateModel(String consumerId, String partition,  String mqServer) {
	   this. partition = partition;
	   this. consumerId = consumerId;
	   this. mqServer = mqServer;
	  }

	  @Transition(to = "ONLINE", from = "OFFLINE")
	  public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
	    LOG.debug(consumerId + " becomes ONLINE from OFFLINE for " + partition);

	    if (_thread == null) {
	    	 MDC.put(PublisherEnums.OPLOG_PARTION.getValue(), partition);
	            MDC.put(PublisherEnums.CONSUMER_ID.getValue(), consumerId);
	      LOG.debug("Starting ConsumerThread for " + partition + "...");
	      _thread = new ConsumerThread(partition, mqServer, consumerId);
	      beanFactory.autowireBean(_thread);
	      _thread.start();
	      LOG.debug("Starting ConsumerThread for " + partition + " done");

	    }
	  }

	  @Transition(to = "OFFLINE", from = "ONLINE")
	  public void onBecomeOfflineFromOnline(Message message, NotificationContext context)
	      throws InterruptedException {
	    LOG.debug(consumerId + " becomes OFFLINE from ONLINE for " + partition);

	    if (_thread != null) {
	      LOG.debug("Stopping " + consumerId + " for " + partition + "...");

	      _thread.interrupt();
	      _thread.join(2000);
	      _thread = null;
	      LOG.debug("Stopping " + consumerId + " for " + partition + " done");

	    }
	  }

	  @Transition(to = "DROPPED", from = "OFFLINE")
	  public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
	    LOG.debug(consumerId + " becomes DROPPED from OFFLINE for " + partition);
	  }

	  @Transition(to = "OFFLINE", from = "ERROR")
	  public void onBecomeOfflineFromError(Message message, NotificationContext context) {
	    LOG.debug(consumerId + " becomes OFFLINE from ERROR for " + partition);
	  }

	  @Override
	  public void reset() {
	    LOG.warn("Default reset() invoked");

	    if (_thread != null) {
	      LOG.debug("Stopping " + consumerId + " for " + partition + "...");

	      _thread.interrupt();
	      try {
	        _thread.join(2000);
	      } catch (InterruptedException e) {
	        // TODO Auto-generated catch block
	        e.printStackTrace();
	      }
	      _thread = null;
	      LOG.debug("Stopping " + consumerId + " for " + partition + " done");

	    }
	  }
	}
