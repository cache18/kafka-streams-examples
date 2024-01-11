package com.course.kafka.broker.message;

public class OrderReplyMessage {

    private String message;

    public OrderReplyMessage() {}

    public OrderReplyMessage(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return "OrderReplyMessage{" +
                "message='" + message + '\'' +
                '}';
    }
}
