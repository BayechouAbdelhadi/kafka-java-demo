package com.example.kafka.notification.exception;

public class TransientNotificationException extends RuntimeException {
    public TransientNotificationException(String message) {
        super(message);
    }
}
