package br.com.alura.ecommerce.service.neworder;

import java.math.BigDecimal;

public class Order {

	private final String userId, orderId;
	// Esse tipo BigDecimal representa um ponto flutuante,
	// que permite ter melhor precisão nas casas decimais
	private final BigDecimal amount;
	private final String email;

	public Order(String userId, String orderId, BigDecimal amount, String email) {
		this.userId = userId;
		this.orderId = orderId;
		this.amount = amount;
		this.email = email;
	}

	@Override
	public String toString() {
		return "Order [userId=" + userId + ", orderId=" + orderId + ", amount=" + String.format("%.2f", amount)
				+ ", email=" + email + "]";
	}

}
