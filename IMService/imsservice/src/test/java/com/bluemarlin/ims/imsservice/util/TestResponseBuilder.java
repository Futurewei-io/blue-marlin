package com.bluemarlin.ims.imsservice.util;

import com.bluemarlin.ims.imsservice.exceptions.IMSException;
import org.junit.Test;
import org.springframework.http.ResponseEntity;

import static org.junit.Assert.assertNotNull;

public class TestResponseBuilder {

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void build() {
        ResponseEntity res = ResponseBuilder.build(new Object(), 100);
        assertNotNull(res);
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void buildError() {
        Exception exc = new Exception();
        ResponseEntity res = ResponseBuilder.buildError(exc);
        assertNotNull(res);
        exc = new IMSException("imserr");
        res = ResponseBuilder.buildError(exc);
        assertNotNull(res);
    }
}