package com.shopmart.order.event;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventPublisher {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public void publish(OrderEvent event) {
        String key = String.valueOf(event.getOrderId());
        kafkaTemplate.send(KafkaTopics.ORDER, key, event)
                .whenComplete((result, error) -> {
                    if (error == null) {
                        log.info("Published Saga event type={}, orderId={}", event.getType(), event.getOrderId());
                    } else {
                        log.error("Cannot publish Saga event type={}, orderId={}",
                                event.getType(), event.getOrderId(), error);
                    }
                });
    }
}
