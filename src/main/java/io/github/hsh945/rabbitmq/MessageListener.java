package io.github.hsh945.rabbitmq;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

/**
 * 统一通用业务消息监听器
 *
 * @author eric
 */
@ConditionalOnProperty(prefix = "spring.rabbitmq.easy-mq", name = "queue")
@Slf4j
public class MessageListener extends AbstractMessageListener {

	public MessageListener(MessageHandlerRegistry handlerRegistry, ObjectMapper objectMapper) {
		super(handlerRegistry, objectMapper);
	}

	@RabbitListener(queues = "${spring.rabbitmq.easy-mq.queue}")
	public void onMessage(MessageWrapper wrapper) throws Exception {
		try {
			log.info("Received RabbitMQ message. MessageId: {}, Type: {}", wrapper.getMessageId(), wrapper.getMessageType());
			dispatch(wrapper);
		}
		catch (Exception e) {
			log.error("Failed to process message in handler. Initiating Spring local retry & DLQ mechanism. MessageId: {}, Type: {}, Error: {}", 
					wrapper.getMessageId(), wrapper.getMessageType(), e.getMessage(), e);
			// 抛出异常以触发 Spring AMQP 声明的自动重试及死信兜底流转
			throw e;
		}
	}

}
