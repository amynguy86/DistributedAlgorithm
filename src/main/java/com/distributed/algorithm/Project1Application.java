package com.distributed.algorithm;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.context.IntegrationFlowContext;
import org.springframework.integration.dsl.context.IntegrationFlowRegistration;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.util.Assert;

import com.distributed.algorithm.bfs.BFS;
import com.distributed.algorithm.mst.MST;
import com.distributed.algorithm.palev.Algorithm;

import org.springframework.integration.ip.tcp.TcpReceivingChannelAdapter;
import org.springframework.integration.ip.tcp.TcpSendingMessageHandler;
import org.springframework.integration.ip.tcp.connection.AbstractServerConnectionFactory;
import org.springframework.integration.ip.tcp.connection.TcpNetClientConnectionFactory;
import org.springframework.integration.ip.tcp.connection.TcpNetServerConnectionFactory;
import org.springframework.integration.router.AbstractMessageRouter;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
   

@SpringBootApplication
@EnableIntegration
@IntegrationComponentScan
public class Project1Application {

	static Logger logger = LoggerFactory.getLogger(Project1Application.class);

	@Value("${server.port:2281}")
	private int port;

	public static void main(String[] args) {
		org.springframework.context.ConfigurableApplicationContext context = SpringApplication
				.run(Project1Application.class, args);
		try {
			 logger.debug("Debug is On");
			 //context.getBean(Algorithm.class).init();
			 //context.getBean(BFS.class).init();
			  context.getBean(MST.class).init();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
	}

	@MessagingGateway(defaultRequestChannel = "toTcp.input")
	public interface ToTCP {
		public void send(byte[] data, @Header("host") String host) throws MessagingException;

	}

	@Bean
	public IntegrationFlow toTcp() {
		return f -> f.route(new TcpRouter());
	}

	@Bean
	public TcpNetServerConnectionFactory serverCF() {
		TcpNetServerConnectionFactory tcpServerFactory = new TcpNetServerConnectionFactory(port);
		return tcpServerFactory;
	}

	@Bean
	public TcpReceivingChannelAdapter tcpInAdapter(AbstractServerConnectionFactory connectionFactory) {
		TcpReceivingChannelAdapter tcpIn = new TcpReceivingChannelAdapter();
		tcpIn.setConnectionFactory(connectionFactory);
		tcpIn.setOutputChannelName("fromTcp");
		return tcpIn;

	}

	public static class TcpRouter extends AbstractMessageRouter {
		@Value("${max.cached:10}")
		private int maxCached;
		
		@SuppressWarnings("serial")
		private final LinkedHashMap<String, MessageChannel> subFlows = new LinkedHashMap<String, MessageChannel>(
				this.maxCached, .75f, true) {

			@Override
			protected boolean removeEldestEntry(Entry<String, MessageChannel> eldest) {
				if (size() > maxCached) {
					removeSubFlow(eldest);
					return true;
				} else {
					return false;
				}
			}

		};

		@Autowired
		private IntegrationFlowContext flowContext;

		//This gets called by the Integration flow
		@Override
		protected synchronized Collection<MessageChannel> determineTargetChannels(Message<?> message) {
			MessageChannel channel = this.subFlows
					.get(message.getHeaders().get("host", String.class));
			if (channel == null) {
				channel = createNewSubflow(message);
			}
			return Collections.singletonList(channel);
		}

		private MessageChannel createNewSubflow(Message<?> message) {
			String hostPort = (String) message.getHeaders().get("host");
			Assert.state(hostPort != null, "host and/or port header missing");
			String[] hostPortArray = hostPort.split(":");
			String host = hostPortArray[0];
			int port= Integer.parseInt(hostPortArray[1]);
	

			TcpNetClientConnectionFactory cf = new TcpNetClientConnectionFactory(host, port);
			TcpSendingMessageHandler handler = new TcpSendingMessageHandler();
			handler.setConnectionFactory(cf);
			IntegrationFlow flow = f -> f.handle(handler);
			IntegrationFlowRegistration flowRegistration = this.flowContext.registration(flow).addBean(cf)
					.id(hostPort + ".flow").register();

			MessageChannel inputChannel = flowRegistration.getInputChannel();
			this.subFlows.put(hostPort, inputChannel);
			return inputChannel;
		}

		private void removeSubFlow(Entry<String, MessageChannel> eldest) {
			String hostPort = eldest.getKey();
			this.flowContext.remove(hostPort + ".flow");
		}
	}

}
