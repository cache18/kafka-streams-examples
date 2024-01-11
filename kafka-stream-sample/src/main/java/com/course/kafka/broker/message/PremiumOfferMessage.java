package com.course.kafka.broker.message;

public class PremiumOfferMessage {

    private String username;
    private String level;
    private String purchaseNumber;

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public String getPurchaseNumber() {
        return purchaseNumber;
    }

    public void setPurchaseNumber(String purchaseNumber) {
        this.purchaseNumber = purchaseNumber;
    }

    @Override
    public String toString() {
        return "PremiumOfferMessage{" +
                "username='" + username + '\'' +
                ", level='" + level + '\'' +
                ", purchaseNumber='" + purchaseNumber + '\'' +
                '}';
    }
}
