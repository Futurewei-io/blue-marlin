package com.bluemarlin.ims.imsservice.util;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestIMSLogger {

    @Test
    public void instance() {
        assertNotNull(IMSLogger.instance());
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void info() {
        IMSLogger log = new IMSLogger();
        log.info("info");
        assertNotNull(log);
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void testInfo() {
        IMSLogger log = new IMSLogger();
        log.info("String", 123);
        assertNotNull(log);
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void error() {
        IMSLogger log = new IMSLogger();
        log.error("error");
        assertNotNull(log);
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void testError() {
        IMSLogger log = new IMSLogger();
        log.error("String", 123);
        assertNotNull(log);
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void debug() {
        IMSLogger log = new IMSLogger();
        log.debug("debug");
        assertNotNull(log);
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void testDebug() {
        IMSLogger log = new IMSLogger();
        log.debug("String", 123);
        assertNotNull(log);
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void warn() {
        IMSLogger log = new IMSLogger();
        log.warn("warn");
        assertNotNull(log);
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void testWarn() {
        IMSLogger log = new IMSLogger();
        log.warn("String", 123);
        assertNotNull(log);
    }
}