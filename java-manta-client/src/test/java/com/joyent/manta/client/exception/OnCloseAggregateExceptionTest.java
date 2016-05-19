package com.joyent.manta.client.exception;

import com.joyent.manta.exception.OnCloseAggregateException;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class OnCloseAggregateExceptionTest {
    public void canAggregateExceptions() {
        String msg = "Exception message";
        OnCloseAggregateException exception = new OnCloseAggregateException(msg);

        for (int i = 1; i < 11; i++) {
            Exception inner = new RuntimeException("Exception " + i);
            exception.aggregateException(inner);
        }

        int entries = exception.getContextEntries().size();
        Assert.assertEquals(entries, 10);
    }
}
