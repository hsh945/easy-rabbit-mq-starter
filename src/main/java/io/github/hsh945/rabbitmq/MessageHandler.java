package io.github.hsh945.rabbitmq;

/**
 * 业务消息处理器接口，由各子系统具体的消费业务逻辑去实现
 *
 * @param <T> 泛型，声明当前处理器支持消费的真实 Java 对象类型
 * @author eric
 */
public interface MessageHandler<T> {

	/**
	 * 获取当前处理器支持消费的特定消息类型 (即 RoutingKey)
	 *
	 * @return 消息路由键/类型名称
	 */
	String getMessageType();

	/**
	 * 具体的业务消费逻辑
	 *
	 * @param message 已经过反序列化后的真实业务 Java 对象
	 */
	void handle(T message);

	/**
	 * 获取该消息反序列化时对应的 Class 结构，以便框架层进行 Jackson 自动解析
	 *
	 * @return 业务对象的 Class 类型
	 */
	Class<T> getMessageClass();

}
