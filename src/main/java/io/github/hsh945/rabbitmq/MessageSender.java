package io.github.hsh945.rabbitmq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import java.util.UUID;

/**
 * 消息发送服务客户端门面，提供统一的普通/延迟消息发布功能
 *
 * @author eric
 */
@Slf4j
public class MessageSender {

	private final RabbitTemplate rabbitTemplate;
	private final ObjectMapper objectMapper;
	private final EasyMqProperties easyMqProperties;

	public MessageSender(RabbitTemplate rabbitTemplate, ObjectMapper objectMapper, EasyMqProperties easyMqProperties) {
		this.rabbitTemplate = rabbitTemplate;
		this.objectMapper = objectMapper;
		this.easyMqProperties = easyMqProperties;
	}

	/**
	 * 发送普通消息 (使用 Starter 默认业务交换机，以 messageType 作为路由键)
	 *
	 * @param messageType 消息类型，对应消费端的 RoutingKey
	 * @param payload     业务数据实体对象 (框架将自动采用 Jackson 序列化为 JSON 传输)
	 * @param <T>         业务数据泛型
	 */
	public <T> void sendDirectMessage(String messageType, T payload) {
		sendDirectMessage(easyMqProperties.getExchange(), messageType, messageType, payload);
	}

	/**
	 * 发送普通消息 (支持自定义交换机和路由键，用于与其他非 Starter 系统对接的特殊场景)
	 *
	 * @param exchange    自定义交换机名称
	 * @param routingKey  自定义路由键
	 * @param messageType 消息类型标识
	 * @param payload     业务数据实体对象
	 * @param <T>         业务数据泛型
	 */
	public <T> void sendDirectMessage(String exchange, String routingKey, String messageType, T payload) {
		try {
			String payloadJson = objectMapper.writeValueAsString(payload);
			log.info("Sending direct message. Exchange: {}, RoutingKey: {}, MessageType: {}, Payload: {}", 
					exchange, routingKey, messageType, payloadJson);
			MessageWrapper wrapper = buildWrapper(messageType, payloadJson);
			rabbitTemplate.convertAndSend(exchange, routingKey, wrapper);
		}
		catch (JsonProcessingException e) {
			throw new IllegalArgumentException("Failed to serialize message payload to JSON", e);
		}
	}

	/**
	 * 发送延时消息 (使用 Starter 默认延迟交换机，以 messageType 作为路由键)
	 *
	 * @param messageType  消息类型，对应消费端的 RoutingKey
	 * @param payload      业务数据实体对象
	 * @param delaySeconds 延时秒数
	 * @param <T>          业务数据泛型
	 */
	public <T> void sendDelayMessage(String messageType, T payload, long delaySeconds) {
		sendDelayMessage(easyMqProperties.getDelayExchange(), messageType, messageType, payload, delaySeconds);
	}

	/**
	 * 发送延时消息 (支持自定义交换机和路由键，用于高定制化延时路由场景)
	 *
	 * @param exchange     自定义延时交换机名称
	 * @param routingKey   自定义路由键
	 * @param messageType  消息类型标识
	 * @param payload      业务数据实体对象
	 * @param delaySeconds 延时秒数 (单位：秒)
	 * @param <T>          业务数据泛型
	 */
	public <T> void sendDelayMessage(String exchange, String routingKey, String messageType, T payload, long delaySeconds) {
		try {
			String payloadJson = objectMapper.writeValueAsString(payload);
			log.info("Sending delay message. Exchange: {}, RoutingKey: {}, MessageType: {}, Delay: {}s, Payload: {}", 
					exchange, routingKey, messageType, delaySeconds, payloadJson);
			MessageWrapper wrapper = buildWrapper(messageType, payloadJson);
			rabbitTemplate.convertAndSend(
					exchange,
					routingKey,
					wrapper,
					message -> {
						// 通过 RabbitMQ TTL (Time-To-Live) 设置过期时间
						message.getMessageProperties().setExpiration(String.valueOf(delaySeconds * 1000));
						return message;
					}
			);
		}
		catch (JsonProcessingException e) {
			throw new IllegalArgumentException("Failed to serialize message payload to JSON", e);
		}
	}

	private MessageWrapper buildWrapper(String messageType, String payloadJson) {
		return new MessageWrapper(
				messageType,
				payloadJson,
				UUID.randomUUID().toString(),
				System.currentTimeMillis()
		);
	}

}
