package com.shopmart.order.service;

import com.shopmart.order.dto.OrderRequest;
import com.shopmart.order.dto.OrderResponse;

import java.math.BigDecimal;
import java.util.List;

public interface OrderService {

    /** Tạo đơn hàng ở trạng thái PENDING và khởi động Saga. */
    OrderResponse createOrder(OrderRequest request);

    OrderResponse getOrderById(Long id);

    List<OrderResponse> getAllOrders();

    /** Saga kết thúc thành công -> COMPLETED. */
    OrderResponse completeOrder(Long orderId);

    /** Saga kết thúc thành công và cập nhật tổng tiền nhận từ inventory-service. */
    OrderResponse completeOrder(Long orderId, BigDecimal totalAmount);

    /** Saga thất bại (đã compensate) -> CANCELLED kèm lý do. */
    OrderResponse cancelOrder(Long orderId, String reason);

    /** Saga thất bại, cập nhật tổng tiền nếu bước giữ kho đã tính được. */
    OrderResponse cancelOrder(Long orderId, String reason, BigDecimal totalAmount);
}
