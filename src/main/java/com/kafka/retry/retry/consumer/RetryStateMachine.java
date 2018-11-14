package com.kafka.retry.retry.consumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.List;

@Component
public class RetryStateMachine {


    private List<Integer> minIntervalList = Arrays.asList(1,5,30,60,144);




    public RetryState getNextState(RetryState retryState) {
        if(retryState.getRetryCount() >  3 )
            return new RetryState(0,getNextInterval(retryState.getRetryInterval()));

        return new RetryState(retryState.getRetryCount() + 1,retryState.getRetryInterval());
    }

    private int getNextInterval(int retryInterval) {
        int indexOfCurrent = minIntervalList.indexOf(retryInterval);

        if(indexOfCurrent == minIntervalList.size())
            throw new RetryExhaustedException();

        return minIntervalList.get(indexOfCurrent + 1);

    }

    public List<Integer> getMinIntervalList() {
        return minIntervalList;
    }

    public void setMinIntervalList(List<Integer> minIntervalList) {
        this.minIntervalList = minIntervalList;
    }




}
