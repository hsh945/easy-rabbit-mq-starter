package io.github.hsh945.rabbitmq;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 抽象消息分发监听基类
 *
 * @author eric
 */
public abstract class AbstractMessageListener {

	protected final MessageHandlerRegistry handlerRegistry;
	protected final ObjectMapper objectMapper;

	protected AbstractMessageListener(MessageHandlerRegistry handlerRegistry, ObjectMapper objectMapper) {
		this.handlerRegistry = handlerRegistry;
		this.objectMapper = objectMapper;
	}

	/**
	 * 解析接收到的 MessageWrapper 载荷，并安全分发给具体的 Handler 进行消费
	 *
	 * @param wrapper 统一消息传输包装
	 * @throws Exception 业务处理异常、解析异常或未匹配到处理器的异常
	 */
	protected void dispatch(MessageWrapper wrapper) throws Exception {
		MessageHandler<?> handler = handlerRegistry.getHandler(wrapper.getMessageType());
		if (handler == null) {
			throw new IllegalArgumentException("No message handler registered for type: [" + wrapper.getMessageType() + "]");
		}

		Class<?> messageClass = handler.getMessageClass();
		Object message = objectMapper.readValue(wrapper.getPayload(), messageClass);
		if (!messageClass.isInstance(message)) {
			throw new ClassCastException("Parsed message type does not match. Expected: " 
					+ messageClass + ", but got: " + message.getClass());
		}

		@SuppressWarnings("unchecked")
		MessageHandler<Object> typedHandler = (MessageHandler<Object>) handler;
		typedHandler.handle(message);
	}

}
