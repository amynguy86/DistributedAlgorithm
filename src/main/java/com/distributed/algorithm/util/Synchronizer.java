package com.distributed.algorithm.util;
import java.util.Collection;
import java.util.Collections;
/*
 * This could be improved my having a general Synchronizer that sends null messages to indicate to other nodes end of round rather than using one synchronizer
 * per algorithm which although saves some messages but it becomes hard to manage and extend.
 */
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.MessageEndpoint;
import org.springframework.integration.annotation.Router;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.context.IntegrationFlowContext;
import org.springframework.integration.dsl.context.IntegrationFlowRegistration;
import org.springframework.integration.ip.tcp.TcpSendingMessageHandler;
import org.springframework.integration.ip.tcp.connection.TcpNetClientConnectionFactory;
import org.springframework.integration.router.AbstractMessageRouter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.Assert;

import com.distributed.algorithm.Project1Application;
import com.distributed.algorithm.Project1Application.TcpRouter;

@MessageEndpoint
public class Synchronizer {

	static Logger logger = LoggerFactory.getLogger(Synchronizer.class);
	@Autowired
	Config config;
		
	private static class ConsumerQueue<T>{
		public Consumer<T> consumer;
		private QueueChannel[] channel;
		
		public ConsumerQueue() {
			channel = new QueueChannel[2];
		}
		
		public void addChannel(int index,QueueChannel msgChannel) {
			Assert.state(index<2,"Cannot add more than 2 channels");
			channel[index]=msgChannel;
		}
		
		public QueueChannel getOddChannel() {
			return channel[1];
		}
		
		public QueueChannel getEvenChannel() {
			return channel[0];
		}
	}
	
	Map<AlgorithmType, ConsumerQueue<MessageFormat>> consumerMap;

	public Synchronizer() {
		consumerMap = new HashMap<>();
	}

	public void registerConsumer(AlgorithmType algoType, Consumer<MessageFormat> consumer) {
		ConsumerQueue consumerQueue = new ConsumerQueue();
		consumerQueue.consumer=consumer;
		consumerQueue.addChannel(0, new QueueChannel());
		consumerQueue.addChannel(1, new QueueChannel());
		
		
		this.consumerMap.put(algoType,consumerQueue);
		
		
		QueueChannel evenQueue = consumerQueue.getEvenChannel();
		QueueChannel oddQueue = consumerQueue.getOddChannel();
		
	
		new Thread(() -> {
			int msgCount = 0;
			QueueChannel queue;
			queue = evenQueue;
			while (consumerMap.containsKey(algoType)) {
				Message<MessageFormat> msg = (Message<MessageFormat>) queue.receive(1000);
				if (msg != null) {
					logger.debug("Getting Message for Consumer for AlgoType: " + algoType.toString());
					logger.debug("Msg Recieved: "+ msg.getPayload().toString());
					msgCount++;
					if(msg.getPayload().getAlgoData()!=null) //no Algo Data implies a NULL Message!cd ..
						consumer.accept(msg.getPayload());

					if (msgCount == config.getNeibours().size()) {
						queue = queue == oddQueue ? evenQueue : oddQueue;
						msgCount = 0;
						if (consumer != null)
							consumer.accept(null); // a null message indicates end of round
						// send Round update Event
					}
				}
			}
			evenQueue.clear();
			oddQueue.clear();
		}).start();
		
	}

	public void UnRegisterConsumer(AlgorithmType algoType) {
		if (this.consumerMap.containsKey(algoType)) {
			consumerMap.remove(algoType);
		}
	}
	
	@Router(inputChannel = "fromTcp", defaultOutputChannel = "errorChannel")
	public void route(Message<byte[]> message) {

		
		MessageFormat msgFormat = MessageFormat.buildMessage(message.getPayload());
		Message<MessageFormat> msg = new Message<MessageFormat>() {

			@Override
			public MessageFormat getPayload() {
				// TODO Auto-generated method stub
				return msgFormat;
			}

			@Override
			public MessageHeaders getHeaders() {
				// TODO Auto-generated method stub
				return message.getHeaders();
			}

		};
				
		if (msgFormat.getRoundNo() % 2 != 0) {
			consumerMap.get(msgFormat.getAlgorithmType()).getOddChannel().send(msg);
			
		} else {
			consumerMap.get(msgFormat.getAlgorithmType()).getEvenChannel().send(msg);
		}
	}
}