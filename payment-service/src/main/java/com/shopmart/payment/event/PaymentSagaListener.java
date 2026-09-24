package com.shopmart.payment.event;

import com.shopmart.payment.dto.PaymentRequest;
import com.shopmart.payment.dto.PaymentResponse;
import com.shopmart.payment.entity.PaymentStatus;
import com.shopmart.payment.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentSagaListener {

    private final PaymentService paymentService;
    private final OrderEventPublisher eventPublisher;

    @KafkaListener(topics = KafkaTopics.ORDER)
    public void handle(OrderEvent event) {
        if (event.getType() != SagaEventType.INVENTORY_RESERVED) {
            return;
        }

        try {
            PaymentResponse payment = paymentService.processPayment(
                    new PaymentRequest(event.getOrderId(), event.getAmount()));
            publishResult(event, payment);
        } catch (IllegalStateException duplicatePayment) {
            log.warn("Payment was already handled for orderId={}; reusing the stored result",
                    event.getOrderId());
            try {
                publishResult(event, paymentService.getByOrderId(event.getOrderId()));
            } catch (RuntimeException lookupError) {
                publishFailure(event, lookupError);
            }
        } catch (RuntimeException exception) {
            publishFailure(event, exception);
        }
    }

    private void publishResult(OrderEvent event, PaymentResponse payment) {
        if (payment.getStatus() == PaymentStatus.SUCCESS) {
            eventPublisher.publish(copy(event, SagaEventType.PAYMENT_COMPLETED, payment.getMessage()));
        } else {
            eventPublisher.publish(copy(event, SagaEventType.PAYMENT_FAILED, payment.getMessage()));
        }
    }

    private void publishFailure(OrderEvent event, RuntimeException exception) {
        log.error("Payment processing error for orderId={}: {}",
                event.getOrderId(), exception.getMessage(), exception);
        eventPublisher.publish(copy(event, SagaEventType.PAYMENT_FAILED, exception.getMessage()));
    }

    private OrderEvent copy(OrderEvent source, SagaEventType type, String message) {
        return OrderEvent.builder()
                .orderId(source.getOrderId())
                .productId(source.getProductId())
                .quantity(source.getQuantity())
                .amount(source.getAmount())
                .type(type)
                .message(message)
                .build();
    }
}
