package org.apache.bluemarlin.ims.imsservice.model;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class TestIMSRequestQuery {

    @Test
    public void getTargetingChannel() {
        IMSRequestQuery reqQry = new IMSRequestQuery();

        TargetingChannel tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_f"));
        double price = 0;
        List<Range> ranges = new ArrayList<>();
        Range r1 = new Range();
        r1.setSt("2018-01-05");
        r1.setEd("2018-01-05");
        r1.setSh("0");
        r1.setEh("23");
        ranges.add(r1);

        reqQry.setTargetingChannel(tc);

        assertEquals(tc, reqQry.getTargetingChannel());
    }

    @Test
    public void setTargetingChannel() {
        IMSRequestQuery reqQry = new IMSRequestQuery();

        TargetingChannel tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_f"));
        double price = 0;
        List<Range> ranges = new ArrayList<>();
        Range r1 = new Range();
        r1.setSt("2018-01-05");
        r1.setEd("2018-01-05");
        r1.setSh("0");
        r1.setEh("23");
        ranges.add(r1);

        reqQry.setTargetingChannel(tc);
        assertEquals(tc, reqQry.getTargetingChannel());

        tc = new TargetingChannel();
        tc.setR(Arrays.asList("city1"));
        reqQry.setTargetingChannel(tc);
        assertEquals(tc, reqQry.getTargetingChannel());

        tc = new TargetingChannel();
        tc.setIpl(Arrays.asList("1156510600"));
        reqQry.setTargetingChannel(tc);
        assertEquals(tc, reqQry.getTargetingChannel());
    }

    @Test
    public void setToday() {
        IMSRequestQuery reqQry = new IMSRequestQuery();
        assertNull(reqQry.getToday());
        reqQry.setToday("2018-01-05");
        assertEquals("2018-01-05", reqQry.getToday());
    }

    @Test
    public void getToday() {
        IMSRequestQuery reqQry = new IMSRequestQuery();
        assertNull(reqQry.getToday());
        reqQry.setToday("2018-01-06");
        assertEquals("2018-01-06", reqQry.getToday());
    }

    @Test
    public void setPrice() {
        IMSRequestQuery reqQry = new IMSRequestQuery();
        reqQry.setPrice(54321);
        assertEquals(54321, reqQry.getPrice(), 0);
    }

    @Test
    public void getPrice() {
        IMSRequestQuery reqQry = new IMSRequestQuery();
        reqQry.setPrice(12345);
        assertEquals(12345, reqQry.getPrice(), 0);
    }

    @Test
    public void getDays() {
        IMSRequestQuery reqQry = new IMSRequestQuery();
        assertNull(reqQry.getDays());
        List<Range> ranges = new ArrayList<>();
        Range r1 = new Range();
        r1.setSt("2018-01-05");
        r1.setEd("2018-01-05");
        r1.setSh("0");
        r1.setEh("23");
        Range r2 = new Range();
        r2.setSt("2018-01-06");
        r2.setEd("2018-01-06");
        r2.setSh("0");
        r2.setEh("23");

        ranges.add(r1);
        ranges.add(r2);
        reqQry.setDays(ranges);
        assertEquals(ranges, reqQry.getDays());
    }

    @Test
    public void setDays() {
        IMSRequestQuery reqQry = new IMSRequestQuery();
        assertNull(reqQry.getDays());
        List<Range> ranges = new ArrayList<>();
        Range r1 = new Range();
        r1.setSt("2018-01-05");
        r1.setEd("2018-01-05");
        r1.setSh("0");
        r1.setEh("23");
        Range r2 = new Range();
        r2.setSt("2018-01-06");
        r2.setEd("2018-01-06");
        r2.setSh("0");
        r2.setEh("23");
        Range r3 = new Range();
        r3.setSt("2018-01-07");
        r3.setEd("2018-01-07");
        r3.setSh("0");
        r3.setEh("23");
        ranges.add(r1);
        ranges.add(r2);
        ranges.add(r3);
        reqQry.setDays(ranges);
        assertEquals(ranges, reqQry.getDays());
    }


    @Test
    public void testToString() {
        IMSRequestQuery reqQry = new IMSRequestQuery();
        TargetingChannel tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_f"));
        double price = 20;
        List<Range> ranges = new ArrayList<>();
        Range r1 = new Range();
        r1.setSt("2018-01-05");
        r1.setEd("2018-01-05");
        r1.setSh("0");
        r1.setEh("23");
        ranges.add(r1);
        reqQry.setTargetingChannel(tc);

        List<Range> ranges2 = new ArrayList<>();
        Range r2 = new Range();
        r2.setSt("2018-01-06");
        r2.setEd("2018-01-06");
        r2.setSh("0");
        r2.setEh("23");
        Range r3 = new Range();
        r3.setSt("2018-01-07");
        r3.setEd("2018-01-07");
        r3.setSh("0");
        r3.setEh("23");
        ranges2.add(r2);
        ranges2.add(r3);
        reqQry.setDays(ranges2);

        String exp = "{targetingChannel={gender:[g_f],price_type:NONE,},price=9.223372036854776E18,ranges=Range{st='2018-01-06,ed='2018-01-06,sh=0,eh=23}Range{st='2018-01-07,ed='2018-01-07,sh=0,eh=23}}";
        assertEquals(exp, reqQry.toString());
    }
}