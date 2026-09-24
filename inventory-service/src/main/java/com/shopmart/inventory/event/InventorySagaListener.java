package com.shopmart.inventory.event;

import com.shopmart.inventory.dto.ProductResponse;
import com.shopmart.inventory.service.ProductService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Slf4j
@Component
@RequiredArgsConstructor
public class InventorySagaListener {

    private final ProductService productService;
    private final OrderEventPublisher eventPublisher;

    @KafkaListener(topics = KafkaTopics.ORDER)
    public void handle(OrderEvent event) {
        if (event.getType() == null) {
            log.warn("Ignoring Saga event without type for orderId={}", event.getOrderId());
            return;
        }

        switch (event.getType()) {
            case ORDER_CREATED -> reserveInventory(event);
            case PAYMENT_FAILED -> releaseInventory(event);
            default -> log.debug("Inventory service ignores Saga event type={} for orderId={}",
                    event.getType(), event.getOrderId());
        }
    }

    private void reserveInventory(OrderEvent event) {
        try {
            ProductResponse product = productService.decreaseStock(event.getProductId(), event.getQuantity());
            BigDecimal totalAmount = product.getPrice().multiply(BigDecimal.valueOf(event.getQuantity()));
            log.info("Inventory RESERVED for orderId={}, productId={}, quantity={}",
                    event.getOrderId(), event.getProductId(), event.getQuantity());
            eventPublisher.publish(copy(event, SagaEventType.INVENTORY_RESERVED, totalAmount,
                    "Đã giữ tồn kho"));
        } catch (RuntimeException exception) {
            log.error("Inventory FAILED for orderId={}: {}", event.getOrderId(), exception.getMessage());
            eventPublisher.publish(copy(event, SagaEventType.INVENTORY_FAILED, event.getAmount(),
                    exception.getMessage()));
        }
    }

    private void releaseInventory(OrderEvent event) {
        try {
            productService.increaseStock(event.getProductId(), event.getQuantity());
            log.info("Inventory RELEASED for orderId={}, productId={}, quantity={}",
                    event.getOrderId(), event.getProductId(), event.getQuantity());
            eventPublisher.publish(copy(event, SagaEventType.INVENTORY_RELEASED, event.getAmount(),
                    "Thanh toán thất bại; tồn kho đã được hoàn. " + messageOrEmpty(event)));
        } catch (RuntimeException exception) {
            log.error("Cannot release inventory for orderId={}: {}",
                    event.getOrderId(), exception.getMessage(), exception);
        }
    }

    private OrderEvent copy(OrderEvent source, SagaEventType type, BigDecimal amount, String message) {
        return OrderEvent.builder()
                .orderId(source.getOrderId())
                .productId(source.getProductId())
                .quantity(source.getQuantity())
                .amount(amount)
                .type(type)
                .message(message)
                .build();
    }

    private String messageOrEmpty(OrderEvent event) {
        return event.getMessage() == null ? "" : event.getMessage();
    }
}
