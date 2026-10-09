package org.bp.payment.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PaymentRequest {
	private PaymentCard paymentCard;
	private Amount amount;
	public PaymentCard getPaymentCard() {
		return paymentCard;
	}
	public void setPaymentCard(PaymentCard paymentCard) {
		this.paymentCard = paymentCard;
	}
	public Amount getAmount() {
		return amount;
	}
	public void setAmount(Amount amount) {
		this.amount = amount;
	}

}
