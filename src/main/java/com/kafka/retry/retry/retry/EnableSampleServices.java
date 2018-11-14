package com.kafka.retry.retry.retry;

import org.springframework.context.annotation.Import;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(value = RetentionPolicy.RUNTIME)
@Import(SampleServiceConfiguration.class)
public @interface EnableSampleServices {}