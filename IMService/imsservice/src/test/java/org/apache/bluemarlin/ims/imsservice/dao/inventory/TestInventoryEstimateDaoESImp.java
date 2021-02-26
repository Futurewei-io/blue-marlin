package org.apache.bluemarlin.ims.imsservice.dao.inventory;

import org.apache.bluemarlin.ims.imsservice.dao.booking.TestBookingDaoESImp;
import org.apache.bluemarlin.ims.imsservice.esclient.ESClient;
import org.apache.bluemarlin.ims.imsservice.esclient.ESRichClientImp;
import org.apache.bluemarlin.ims.imsservice.model.*;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static org.junit.Assert.*;

public class TestInventoryEstimateDaoESImp {

    static InputStream DEF_INPUT = TestBookingDaoESImp.class.getClassLoader().getResourceAsStream("db-test.properties");
    static Properties DEF_PROP;
    static RestHighLevelClient DEF_RHLCLI;
    static ESClient DEF_ESCLI;
    static TargetingChannel DEF_TC;
    static List<Range> DEF_RANGES;
    static Day DEF_DAY;

    static {
        DEF_PROP = new Properties();
        try {
            DEF_PROP.load(DEF_INPUT);
        } catch (IOException e) {
            e.printStackTrace();
        }

        DEF_RHLCLI = new RestHighLevelClient(RestClient.builder(
                new HttpHost(DEF_PROP.getProperty("db.host.urls"),
                        Integer.parseInt(DEF_PROP.getProperty("db.host.ports")), "http")));

        DEF_ESCLI = new ESRichClientImp(DEF_RHLCLI);

        DEF_TC = new TargetingChannel();
        DEF_TC.setG(Arrays.asList("g_m"));
        DEF_TC.setA(Arrays.asList("2"));

        DEF_RANGES = new ArrayList<>();
        Range r1 = new Range();
        r1.setSt("2018-11-07");
        r1.setEd("2018-11-07");
        r1.setSh("0");
        r1.setEh("23");
        DEF_RANGES.add(r1);

        String dayStr = "20180105";
        DEF_DAY = new Day(dayStr);
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void getHourlyPredictions() throws IOException {
        InventoryEstimateDaoESImp invEstImp = new InventoryEstimateDaoESImp(DEF_PROP);
        invEstImp.setESClient(DEF_ESCLI);
        DayImpression res = invEstImp.getHourlyPredictions(DEF_TC, 1.0);
        assertNotNull(res);
    }

    @Test
    public void getPredictions_SIGMA_TCs_PI_BNs() throws IOException {
        InventoryEstimateDaoESImp invEstImp = new InventoryEstimateDaoESImp(DEF_PROP);
        invEstImp.setESClient(DEF_ESCLI);
        List<TargetingChannel> tcs = new ArrayList(), bns = new ArrayList();
        TargetingChannel tc1 = new TargetingChannel(), tc2 = new TargetingChannel(), tc3 = new TargetingChannel();
        tc1.setA(Arrays.asList("2"));
        tc1.setG(Arrays.asList("g_m"));
        tc2.setA(Arrays.asList("3"));
        tc3.setR(Arrays.asList("456"));
        tcs.add(tc1);
        tcs.add(tc2);
        tcs.add(tc3);
        TargetingChannel bn1 = new TargetingChannel(), bn2 = new TargetingChannel(), bn3 = new TargetingChannel();
        bn1.setA(Arrays.asList("3"));
        bn2.setG(Arrays.asList("g_f"));
        bn3.setR(Arrays.asList("456"));
        bns.add(bn1);
        bns.add(bn2);
        bns.add(bn3);

        Impression res = invEstImp.getPredictions_SIGMA_TCs_PI_BNs(DEF_DAY, tcs, bns);
        assertNotNull(res);
        String exp = "{{h:0,t:h0}{h:0,t:h1}{h:0,t:h2}{h:0,t:h3}}";
        assertEquals(exp, res.toString());
    }

    @Test
    public void getPredictions_PI_TCs_DOT_qBAR() throws IOException {
        InventoryEstimateDaoESImp invEstImp = new InventoryEstimateDaoESImp(DEF_PROP);
        invEstImp.setESClient(DEF_ESCLI);
        List<TargetingChannel> tcs = new ArrayList();
        TargetingChannel tc1 = new TargetingChannel(), tc2 = new TargetingChannel(), tc3 = new TargetingChannel();
        tc1.setA(Arrays.asList("2"));
        tc1.setG(Arrays.asList("g_m"));
        tc2.setA(Arrays.asList("3"));
        tc3.setR(Arrays.asList("456"));
        tcs.add(tc1);
        tcs.add(tc2);
        tcs.add(tc3);
        Impression res = invEstImp.getPredictions_PI_TCs_DOT_qBAR(DEF_DAY, tcs, DEF_TC);
        assertNotNull(res);
        String exp = "{{h:0,t:h0}{h:0,t:h1}{h:0,t:h2}{h:0,t:h3}}";
        assertEquals(exp, res.toString());
    }

    @Test
    public void getPredictions_PI_BNs_MINUS_SIGMA_BMs_DOT_q() throws IOException {
        InventoryEstimateDaoESImp invEstImp = new InventoryEstimateDaoESImp(DEF_PROP);
        invEstImp.setESClient(DEF_ESCLI);
        List<TargetingChannel> bms = new ArrayList(), bns = new ArrayList();

        TargetingChannel bn1 = new TargetingChannel(), bn2 = new TargetingChannel(), bn3 = new TargetingChannel();
        bn1.setA(Arrays.asList("3"));
        bn2.setG(Arrays.asList("g_f"));
        bn3.setR(Arrays.asList("456"));
        bns.add(bn1);
        bns.add(bn2);
        bns.add(bn3);

        TargetingChannel bm1 = new TargetingChannel(), bm2 = new TargetingChannel(), bm3 = new TargetingChannel();
        bm1.setA(Arrays.asList("2"));
        bm1.setG(Arrays.asList("g_m"));
        bm2.setA(Arrays.asList("3"));
        bm3.setR(Arrays.asList("456"));
        bms.add(bm1);
        bms.add(bm2);
        bms.add(bm3);

        Impression res = invEstImp.getPredictions_PI_BNs_MINUS_SIGMA_BMs_DOT_q(DEF_DAY, new ArrayList(), bms, DEF_TC);
        assertNotNull(res);
        String exp = "{{h:0,t:h0}{h:0,t:h1}{h:0,t:h2}{h:0,t:h3}}";
        assertEquals(exp, res.toString());
        res = invEstImp.getPredictions_PI_BNs_MINUS_SIGMA_BMs_DOT_q(DEF_DAY, bns, bms, DEF_TC);
        assertNotNull(res);
        assertEquals(exp, res.toString());
    }

    @Test
    public void getPredictions_SIGMA_BMs_PLUS_qBAR_DOT_BNs() throws IOException {
        InventoryEstimateDaoESImp invEstImp = new InventoryEstimateDaoESImp(DEF_PROP);
        invEstImp.setESClient(DEF_ESCLI);
        List<TargetingChannel> bms = new ArrayList(), bns = new ArrayList();

        TargetingChannel bn1 = new TargetingChannel(), bn2 = new TargetingChannel(), bn3 = new TargetingChannel();
        bn1.setA(Arrays.asList("3"));
        bn2.setG(Arrays.asList("g_f"));
        bn3.setR(Arrays.asList("456"));
        bns.add(bn1);
        bns.add(bn2);
        bns.add(bn3);

        TargetingChannel bm1 = new TargetingChannel(), bm2 = new TargetingChannel(), bm3 = new TargetingChannel();
        bm1.setA(Arrays.asList("2"));
        bm1.setG(Arrays.asList("g_m"));
        bm2.setA(Arrays.asList("3"));
        bm3.setR(Arrays.asList("456"));
        bms.add(bm1);
        bms.add(bm2);
        bms.add(bm3);

        Impression res = invEstImp.getPredictions_SIGMA_BMs_PLUS_qBAR_DOT_BNs(DEF_DAY, bms, DEF_TC, new ArrayList());
        assertNotNull(res);
        String exp = "{{h:0,t:h0}{h:0,t:h1}{h:0,t:h2}{h:0,t:h3}}";
        assertEquals(exp, res.toString());
        res = invEstImp.getPredictions_SIGMA_BMs_PLUS_qBAR_DOT_BNs(DEF_DAY, bms, DEF_TC, bns);
        assertNotNull(res);
        assertEquals(exp, res.toString());
    }

    @Test
    public void getPredictions_PI_TCs() throws IOException {
        InventoryEstimateDaoESImp invEstImp = new InventoryEstimateDaoESImp(DEF_PROP);
        invEstImp.setESClient(DEF_ESCLI);
        List<TargetingChannel> tcs = new ArrayList(), bns = new ArrayList();
        TargetingChannel tc1 = new TargetingChannel(), tc2 = new TargetingChannel(), tc3 = new TargetingChannel();
        tc1.setA(Arrays.asList("2"));
        tc1.setG(Arrays.asList("g_m"));
        tc2.setA(Arrays.asList("3"));
        tc3.setR(Arrays.asList("456"));
        tcs.add(tc1);
        tcs.add(tc2);
        tcs.add(tc3);
        Impression res = invEstImp.getPredictions_PI_TCs(DEF_DAY, new ArrayList());
        String exp = "{{h:0,t:h0}{h:0,t:h1}{h:0,t:h2}{h:0,t:h3}}";
        assertEquals(exp, res.toString());
        res = invEstImp.getPredictions_PI_TCs(DEF_DAY, tcs);
        assertNotNull(res);
        assertEquals(exp, res.toString());
    }

    @Test
    public void aggregatePredictionsFullDaysWithRegionRatio() throws IOException {
        InventoryEstimateDaoESImp invEstImp = new InventoryEstimateDaoESImp(DEF_PROP);
        invEstImp.setESClient(DEF_ESCLI);
        List<Range> ranges = new ArrayList<>();
        Range r1 = new Range();
        r1.setSt("2018-01-07");
        r1.setEd("2018-01-07");
        r1.setSh("0");
        r1.setEh("23");
        Range r2 = new Range();
        r2.setSt("2018-01-09");
        r2.setEd("2018-01-09");
        r2.setSh("5");
        r2.setEh("22");
        Range r3 = new Range();
        r3.setSt("2018-11-05");
        r3.setEd("2018-11-05");
        r3.setSh("7");
        r3.setEh("11");
        Range r4 = new Range();
        r4.setSt("2018-11-07");
        r4.setEd("2018-11-07");
        r4.setSh("0");
        r4.setEh("23");
        ranges.add(r1);
        ranges.add(r2);
        ranges.add(r3);
        ranges.add(r4);
        List<Day> sortedDays = Day.buildSortedDays(ranges);
        Set<Day> daySet = new HashSet<>(sortedDays);
        Map<Day, Impression> res = invEstImp.aggregatePredictionsFullDaysWithRegionRatio(DEF_TC, daySet);
        assertNotNull(res);
    }

    @Test
    public void aggregatePredictionsFullDays() throws IOException {
        InventoryEstimateDaoESImp invEstImp = new InventoryEstimateDaoESImp(DEF_PROP);
        invEstImp.setESClient(DEF_ESCLI);
        List<Range> ranges = new ArrayList<>();
        Range r1 = new Range();
        r1.setSt("2018-01-07");
        r1.setEd("2018-01-07");
        r1.setSh("0");
        r1.setEh("23");
        Range r2 = new Range();
        r2.setSt("2018-01-09");
        r2.setEd("2018-01-09");
        r2.setSh("5");
        r2.setEh("22");
        Range r3 = new Range();
        r3.setSt("2018-11-05");
        r3.setEd("2018-11-05");
        r3.setSh("7");
        r3.setEh("11");
        Range r4 = new Range();
        r4.setSt("2018-11-07");
        r4.setEd("2018-11-07");
        r4.setSh("0");
        r4.setEh("23");
        ranges.add(r1);
        ranges.add(r2);
        ranges.add(r3);
        ranges.add(r4);
        List<Day> sortedDays = Day.buildSortedDays(ranges);
        Set<Day> daySet = new HashSet<>(sortedDays);
        Map<Day, Impression> res = invEstImp.aggregatePredictionsFullDays(DEF_TC, daySet);
        assertNotNull(res);

        Day key = new Day("2018-01-07");
        Impression val = res.get(key);
        String exp = "{{h:0,t:h0}{h:10280000,t:h1}{h:10212400,t:h2}{h:0,t:h3}}";
        assertEquals(exp, val.toString());
        key = new Day("2018-01-09");
        val = res.get(key);
        exp = "{{h:0,t:h0}{h:0,t:h1}{h:0,t:h2}{h:0,t:h3}}";
        assertEquals(exp, val.toString());
        key = new Day("2018-01-09");
        val = res.get(key);
        exp = "{{h:0,t:h0}{h:0,t:h1}{h:0,t:h2}{h:0,t:h3}}";
        assertEquals(exp, val.toString());
        key = new Day("2018-11-07");
        val = res.get(key);
        exp = "{{h:0,t:h0}{h:0,t:h1}{h:0,t:h2}{h:0,t:h3}}";
        assertEquals(exp, val.toString());
    }
}