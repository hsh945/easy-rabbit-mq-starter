package io.github.hsh945.rabbitmq;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Easy RabbitMQ 自动配置属性类
 *
 * @author eric
 */
@Data
@ConfigurationProperties(prefix = "spring.rabbitmq.easy-mq")
public class EasyMqProperties {

	/**
	 * 是否启用通用 RabbitMQ Starter 模块
	 */
	private boolean enabled = false;

	/**
	 * 消费方队列名称 (每个微服务子系统独有，纯生产端若不消费可不配置)
	 */
	private String queue;

	/**
	 * 默认业务交换机名称
	 */
	private String exchange = "easy-exchange";

	/**
	 * 默认延时交换机名称
	 */
	private String delayExchange = "easy-exchange-delay";

	/**
	 * 默认死信交换机名称
	 */
	private String dlxExchange = "easy-exchange-dlx";

}
