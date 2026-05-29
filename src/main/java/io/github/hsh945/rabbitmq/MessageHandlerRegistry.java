package io.github.hsh945.rabbitmq;

import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 消息处理器注册中心，负责托管并索引系统中所有的 MessageHandler
 *
 * @author eric
 */
public class MessageHandlerRegistry {

	private final Map<String, MessageHandler<?>> handlerMap = new HashMap<>();

	public MessageHandlerRegistry(List<MessageHandler<?>> handlers) {
		if (!CollectionUtils.isEmpty(handlers)) {
			for (MessageHandler<?> handler : handlers) {
				String messageType = handler.getMessageType();
				if (handlerMap.containsKey(messageType)) {
					throw new IllegalStateException("Duplicate message handler detected for type: [" + messageType + "]");
				}
				handlerMap.put(messageType, handler);
			}
		}
	}

	/**
	 * 根据消息类型获取对应的处理器实例
	 *
	 * @param messageType 消息类型
	 * @return 对应的 MessageHandler，如果不存在则返回 null
	 */
	public MessageHandler<?> getHandler(String messageType) {
		return handlerMap.get(messageType);
	}

}
