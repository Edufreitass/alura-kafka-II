package br.com.alura.ecommerce.service.fraud.detector;

import java.math.BigDecimal;

public class Order {

	private final String userId, orderId;
	// Esse tipo BigDecimal representa um ponto flutuante,
	// que permite ter melhor precisão nas casas decimais
	private final BigDecimal amount;

	public Order(String userId, String orderId, BigDecimal amount) {
		this.userId = userId;
		this.orderId = orderId;
		this.amount = amount;
	}

	public String getUserId() {
		return userId;
	}

	public BigDecimal getAmount() {
		return amount;
	}

	@Override
	public String toString() {
		return "Order [userId=" + userId + ", orderId=" + orderId + ", amount=" + String.format("%.2f", amount) + "]";
	}

}
