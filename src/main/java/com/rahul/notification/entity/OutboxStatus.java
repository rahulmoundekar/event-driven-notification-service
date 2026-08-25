package com.rahul.notification.entity;

public enum OutboxStatus {

    NEW,
    PROCESSING,
    PUBLISHED,
    FAILED
}