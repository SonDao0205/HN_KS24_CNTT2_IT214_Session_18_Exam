package com.shopmart.order.service.impl;

import com.shopmart.order.dto.OrderRequest;
import com.shopmart.order.dto.OrderResponse;
import com.shopmart.order.entity.Order;
import com.shopmart.order.entity.OrderStatus;
import com.shopmart.order.event.OrderEvent;
import com.shopmart.order.event.OrderEventPublisher;
import com.shopmart.order.event.SagaEventType;
import com.shopmart.order.exception.ResourceNotFoundException;
import com.shopmart.order.repository.OrderRepository;
import com.shopmart.order.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderServiceImpl implements OrderService {

    private final OrderRepository orderRepository;
    private final OrderEventPublisher eventPublisher;

    @Override
    @Transactional
    public OrderResponse createOrder(OrderRequest request) {
        Order order = Order.builder()
                .customerId(request.getCustomerId())
                .productId(request.getProductId())
                .quantity(request.getQuantity())
                .status(OrderStatus.PENDING)
                .build();
        Order saved = orderRepository.save(order);
        log.info("Created order id={} with status PENDING", saved.getId());

        eventPublisher.publish(OrderEvent.builder()
                .orderId(saved.getId())
                .productId(saved.getProductId())
                .quantity(saved.getQuantity())
                .type(SagaEventType.ORDER_CREATED)
                .message("Đơn hàng đã được tạo, bắt đầu Saga")
                .build());

        return OrderResponse.from(saved);
    }

    @Override
    public OrderResponse getOrderById(Long id) {
        return OrderResponse.from(findOrder(id));
    }

    @Override
    public List<OrderResponse> getAllOrders() {
        return orderRepository.findAll().stream()
                .map(OrderResponse::from)
                .toList();
    }

    @Override
    @Transactional
    public OrderResponse completeOrder(Long orderId) {
        return completeOrder(orderId, null);
    }

    @Override
    @Transactional
    public OrderResponse completeOrder(Long orderId, BigDecimal totalAmount) {
        Order order = findOrder(orderId);
        if (order.getStatus() == OrderStatus.CANCELLED) {
            log.warn("Ignoring PAYMENT_COMPLETED because order id={} is already CANCELLED", orderId);
            return OrderResponse.from(order);
        }
        if (totalAmount != null) {
            order.setTotalAmount(totalAmount);
        }
        order.setStatus(OrderStatus.COMPLETED);
        order.setFailureReason(null);
        log.info("Order id={} COMPLETED", orderId);
        return OrderResponse.from(orderRepository.save(order));
    }

    @Override
    @Transactional
    public OrderResponse cancelOrder(Long orderId, String reason) {
        return cancelOrder(orderId, reason, null);
    }

    @Override
    @Transactional
    public OrderResponse cancelOrder(Long orderId, String reason, BigDecimal totalAmount) {
        Order order = findOrder(orderId);
        if (order.getStatus() == OrderStatus.COMPLETED) {
            log.warn("Ignoring cancellation because order id={} is already COMPLETED", orderId);
            return OrderResponse.from(order);
        }
        if (totalAmount != null) {
            order.setTotalAmount(totalAmount);
        }
        order.setStatus(OrderStatus.CANCELLED);
        order.setFailureReason(reason);
        log.error("Order id={} CANCELLED: {}", orderId, reason);
        return OrderResponse.from(orderRepository.save(order));
    }

    private Order findOrder(Long id) {
        return orderRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Không tìm thấy đơn hàng id=" + id));
    }
}
