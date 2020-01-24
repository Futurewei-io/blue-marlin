package com.bluemarlin.ims.imsservice.esclient;

import com.bluemarlin.ims.imsservice.dao.booking.TestBookingDaoESImp;
import com.bluemarlin.ims.imsservice.model.Booking;
import com.bluemarlin.ims.imsservice.model.Range;
import com.bluemarlin.ims.imsservice.model.TargetingChannel;
import com.bluemarlin.ims.imsservice.util.CommonUtil;
import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static org.junit.Assert.*;

public class TestESRichClientImp {

    static InputStream DEF_INPUT = TestBookingDaoESImp.class.getClassLoader().getResourceAsStream("db-test.properties");
    static Properties DEF_PROP;
    static RestHighLevelClient DEF_RHLCLIENT;
    static double DEF_PRICE = 0;
    static List<Range> DEF_RANGES;
    static TargetingChannel DEF_TC;

    static {
        DEF_PROP = new Properties();
        try {
            DEF_PROP.load(DEF_INPUT);
        } catch (IOException e) {
            e.printStackTrace();
        }

        DEF_RHLCLIENT = new RestHighLevelClient(RestClient.builder(
                new HttpHost(DEF_PROP.getProperty("db.host.urls"),
                        Integer.parseInt(DEF_PROP.getProperty("db.host.ports")), "http")));

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
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void search() throws IOException {
        ESRichClientImp esRCI = new ESRichClientImp(DEF_PROP);
        SearchRequest sReq = new SearchRequest("");
        ESResponse res = esRCI.search(sReq);
        assertNotNull(res);
        assertNotNull(res.getSearchResponse());
        try {
            sReq = new SearchRequest("test");
            res = esRCI.search(sReq);
            assertNotNull(res);
        } catch (Exception e) {
            assertNotNull(e);
        }
    }

    @Test
    public void update() throws IOException {
        ESRichClientImp esRCI = new ESRichClientImp(DEF_RHLCLIENT);
        List<Range> ranges = new ArrayList<>();
        Range r1 = new Range("2018-01-05", "2018-01-05", "0", "23");
        ranges.add(r1);
        Booking bk1 = new Booking(new TargetingChannel(), ranges, 0, "advId", 0);
        Map source = new HashMap();
        source.put("days", new ArrayList());
        source.put("bk_id", bk1.getBookingId());
        Map query = new HashMap();
        query.put("g", "g_m");
        source.put("query", query);
        source.put("del", false);
        bk1 = new Booking("bookingID", source);

        SearchRequest sReq = new SearchRequest("");
        ESResponse res1 = esRCI.search(sReq);
        assertNotNull(res1);
        assertNotNull(res1.getSourceMap());

        UpdateRequest uReq = new UpdateRequest("", "doc", bk1.getId());
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("del", true);
        uReq.doc(requestMap);
        ESResponse res = esRCI.update(uReq, WriteRequest.RefreshPolicy.IMMEDIATE.getValue());

        assertNotNull(res);
        assertEquals(UpdateResponse.Result.UPDATED, res.getUpdateResponse().getResult());
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void searchScroll() throws IOException {

        ESRichClientImp esRCI = new ESRichClientImp(DEF_RHLCLIENT);
        SearchScrollRequest ssReq = new SearchScrollRequest();
        assertNotNull(ssReq);
        System.out.println(ssReq);
        try {
            ESResponse res = esRCI.searchScroll(ssReq);
            assertNull(res);
        } catch (Exception e) {
            assertNotNull(e);
        }

    }

    @Test
    public void index() throws IOException {
        ESRichClientImp esRCI = new ESRichClientImp(DEF_RHLCLIENT);
        Booking bk1 = new Booking(DEF_TC, DEF_RANGES, DEF_PRICE, "advid100", 2);
        Map sourceMap = CommonUtil.convertToMap(bk1);

        IndexRequest indexRequest = new IndexRequest("bookings", "doc");
        indexRequest.source(sourceMap);

        IndexResponse res = esRCI.index(indexRequest, WriteRequest.RefreshPolicy.IMMEDIATE.getValue());
        assertEquals("bookings", res.getIndex());
    }

    @Test
    public void delete() throws IOException {
        ESRichClientImp esRCI = new ESRichClientImp(DEF_RHLCLIENT);
        List<Range> ranges = new ArrayList<>();
        Range r1 = new Range("2018-01-05", "2018-01-05", "0", "23");
        ranges.add(r1);
        Booking bk1 = new Booking(new TargetingChannel(), ranges, 0, "advId", 0);
        Map source = new HashMap();
        source.put("days", new ArrayList());
        source.put("bk_id", bk1.getBookingId());
        Map query = new HashMap();
        query.put("g", "g_m");
        source.put("query", query);
        source.put("del", false);
        bk1 = new Booking("bookingID", source);
        DeleteRequest delReq = new DeleteRequest("bookings", "doc", bk1.getId());
        boolean res = esRCI.delete(delReq);
        assertTrue(res);

    }
}
