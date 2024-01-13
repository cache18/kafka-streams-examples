package com.course.kafka.broker.message;

public class SubscriptionOfferMessage {

	private String username;
	private String duration;
	private String subscriptionNumber;

	public String getDuration() {
		return duration;
	}

	public String getSubscriptionNumber() {
		return subscriptionNumber;
	}

	public String getUsername() {
		return username;
	}

	public void setDuration(String duration) {
		this.duration = duration;
	}

	public void setSubscriptionNumber(String subscriptionNumber) {
		this.subscriptionNumber = subscriptionNumber;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	@Override
	public String toString() {
		return "SubscriptionOfferMessage [username=" + username + ", duration=" + duration + ", subscriptionNumber="
				+ subscriptionNumber + "]";
	}

}
