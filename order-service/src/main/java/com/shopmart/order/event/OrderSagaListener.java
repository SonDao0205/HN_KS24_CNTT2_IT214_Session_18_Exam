package com.shopmart.order.event;

import com.shopmart.order.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderSagaListener {

    private final OrderService orderService;

    @KafkaListener(topics = KafkaTopics.ORDER)
    public void handle(OrderEvent event) {
        if (event.getType() == null) {
            log.warn("Ignoring Saga event without type for orderId={}", event.getOrderId());
            return;
        }

        switch (event.getType()) {
            case INVENTORY_FAILED -> orderService.cancelOrder(
                    event.getOrderId(), messageOrDefault(event, "Không thể giữ tồn kho"), event.getAmount());
            case PAYMENT_COMPLETED -> orderService.completeOrder(event.getOrderId(), event.getAmount());
            case INVENTORY_RELEASED -> orderService.cancelOrder(
                    event.getOrderId(), messageOrDefault(event, "Thanh toán thất bại, tồn kho đã được hoàn"),
                    event.getAmount());
            default -> log.debug("Order service ignores Saga event type={} for orderId={}",
                    event.getType(), event.getOrderId());
        }
    }

    private String messageOrDefault(OrderEvent event, String defaultMessage) {
        return event.getMessage() == null || event.getMessage().isBlank()
                ? defaultMessage
                : event.getMessage();
    }
}
