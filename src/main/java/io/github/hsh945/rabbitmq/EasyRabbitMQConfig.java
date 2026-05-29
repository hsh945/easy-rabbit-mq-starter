package io.github.hsh945.rabbitmq;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Easy RabbitMQ 自动装配中心
 * 根据 spring.rabbitmq.easy-mq 相关配置动态注入 Spring AMQP 基础设施 Bean
 *
 * @author eric
 */
@Configuration
@EnableConfigurationProperties(EasyMqProperties.class)
@ConditionalOnProperty(prefix = "spring.rabbitmq.easy-mq", name = "enabled", havingValue = "true")
public class EasyRabbitMQConfig {

	@Value("${spring.rabbitmq.host:localhost}")
	private String host;

	@Value("${spring.rabbitmq.port:5672}")
	private int port;

	@Value("${spring.rabbitmq.username:guest}")
	private String username;

	@Value("${spring.rabbitmq.password:guest}")
	private String password;

	@Value("${spring.rabbitmq.virtual-host:/}")
	private String virtualHost;

	@Value("${spring.rabbitmq.connection-timeout:60000}")
	private int connectionTimeout;

	/**
	 * 动态注册交换机、队列和绑定关系的核心 Bean
	 */
	@Bean
	public Declarables dynamicDeclarables(EasyMqProperties properties, @Autowired(required = false) List<MessageHandler<?>> handlers) {
		List<Declarable> declarables = new ArrayList<>();

		// 1. 声明主业务交换机 (TopicExchange)
		String exchangeName = properties.getExchange();
		TopicExchange exchange = new TopicExchange(exchangeName, true, false);
		declarables.add(exchange);

		// 2. 如果配置了消费队列名称，则声明队列、延时交换机、延时队列及相应的绑定关系
		String queueName = properties.getQueue();
		if (StringUtils.hasText(queueName)) {
			// 声明死信交换机 (TopicExchange)
			TopicExchange dlxExchange = new TopicExchange(properties.getDlxExchange(), true, false);
			declarables.add(dlxExchange);

			// 声明当前系统的专属死信队列
			String dlxQueueName = queueName + ".dlx";
			Queue dlxQueue = new Queue(dlxQueueName, true);
			declarables.add(dlxQueue);

			// 绑定专属死信队列到死信交换机 (使用 queueName + "-dlx-routing" 做精确路由)
			Binding dlxBinding = BindingBuilder.bind(dlxQueue)
					.to(dlxExchange)
					.with(queueName + "-dlx-routing");
			declarables.add(dlxBinding);

			// 声明业务主队列 (绑定死信交换机和死信路由键)
			Map<String, Object> queueArgs = new HashMap<>();
			queueArgs.put("x-dead-letter-exchange", properties.getDlxExchange());
			queueArgs.put("x-dead-letter-routing-key", queueName + "-dlx-routing");
			Queue queue = new Queue(queueName, true, false, false, queueArgs);
			declarables.add(queue);

			// 声明延时交换机 (TopicExchange)
			String delayExchangeName = properties.getDelayExchange();
			TopicExchange delayExchange = new TopicExchange(delayExchangeName, true, false);
			declarables.add(delayExchange);

			// 声明延时队列 (设置死信到主业务交换机)
			Map<String, Object> delayQueueArgs = new HashMap<>();
			delayQueueArgs.put("x-dead-letter-exchange", exchangeName);
			// 未指定死信路由键，则延时到期后会使用原消息发送时的原始 routingKey (即 messageType)
			Queue delayQueue = new Queue(queueName + ".delay", true, false, false, delayQueueArgs);
			declarables.add(delayQueue);

			// 根据系统中所有被 IOC 托管的 MessageHandler 动态绑定路由键
			if (handlers != null) {
				for (MessageHandler<?> handler : handlers) {
					String messageType = handler.getMessageType();
					if (StringUtils.hasText(messageType)) {
						// 绑定业务主队列到主业务交换机
						Binding mainBinding = BindingBuilder.bind(queue)
								.to(exchange)
								.with(messageType);
						declarables.add(mainBinding);

						// 绑定延时队列到延时交换机
						Binding delayBinding = BindingBuilder.bind(delayQueue)
								.to(delayExchange)
								.with(messageType);
						declarables.add(delayBinding);
					}
				}
			}
		}

		return new Declarables(declarables);
	}

	// ==================== 统一基础设施与序列化配置 ====================

	@Bean
	@ConditionalOnMissingBean
	public Jackson2JsonMessageConverter jsonMessageConverter() {
		return new Jackson2JsonMessageConverter();
	}

	@Bean
	@ConditionalOnMissingBean
	public ConnectionFactory connectionFactory() {
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		connectionFactory.setHost(host);
		connectionFactory.setPort(port);
		connectionFactory.setUsername(username);
		connectionFactory.setPassword(password);
		connectionFactory.setVirtualHost(virtualHost);
		connectionFactory.setConnectionTimeout(connectionTimeout);
		return connectionFactory;
	}

	@Bean
	@ConditionalOnMissingBean
	public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
		RabbitTemplate template = new RabbitTemplate(connectionFactory);
		template.setMessageConverter(jsonMessageConverter());
		return template;
	}

	@Bean
	public MessageSender messageSender(RabbitTemplate rabbitTemplate, ObjectMapper objectMapper, EasyMqProperties properties) {
		return new MessageSender(rabbitTemplate, objectMapper, properties);
	}

	@Bean
	public MessageHandlerRegistry messageHandlerRegistry(@Autowired(required = false) List<MessageHandler<?>> handlers) {
		return new MessageHandlerRegistry(handlers);
	}

	@Bean
	@ConditionalOnProperty(prefix = "spring.rabbitmq.easy-mq", name = "queue")
	public MessageListener messageListener(MessageHandlerRegistry handlerRegistry, ObjectMapper objectMapper) {
		return new MessageListener(handlerRegistry, objectMapper);
	}

}
