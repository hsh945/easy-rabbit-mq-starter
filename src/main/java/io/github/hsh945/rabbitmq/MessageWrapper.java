package io.github.hsh945.rabbitmq;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serial;
import java.io.Serializable;

/**
 * 统一消息传输包装载荷
 *
 * @author eric
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MessageWrapper implements Serializable {

	@Serial
	private static final long serialVersionUID = 1L;

	/**
	 * 消息路由类型，用于在消费端标识具体业务类型并精准分发到对应的 MessageHandler
	 */
	private String messageType;

	/**
	 * 消息载荷 JSON 字符串
	 */
	private String payload;

	/**
	 * 消息全局唯一ID (用于防重、幂等或日志链路追踪)
	 */
	private String messageId;

	/**
	 * 消息创建时间戳
	 */
	private long createTime;

}
