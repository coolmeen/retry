package com.kafka.retry.retry.consumer;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class RetryState {

    Integer retryCount;
    Integer retryInterval;
}
