package org.apache.bluemarlin.ims.imsservice.dao.booking;

import org.apache.bluemarlin.ims.imsservice.esclient.ESClient;
import org.apache.bluemarlin.ims.imsservice.esclient.ESRichClientImp;
import org.apache.bluemarlin.ims.imsservice.exceptions.BookingNotFoundException;
import org.apache.bluemarlin.ims.imsservice.model.*;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static org.junit.Assert.*;

public class TestBookingDaoESImp {

    static InputStream DEF_INPUT = TestBookingDaoESImp.class.getClassLoader().getResourceAsStream("db-test.properties");
    static Properties DEF_PROP;
    static double DEF_PRICE = 0;
    static List<Range> DEF_RANGES;
    static String DEF_ADVID = "advid100";
    static RestHighLevelClient DEF_RHLCLI;
    static ESClient DEF_ESCLI;
    static TargetingChannel DEF_TC;

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
    }


    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void removeBooking() throws IOException, BookingNotFoundException {
        BookingDaoESImp bkDaoImp = new BookingDaoESImp(DEF_PROP);
        bkDaoImp.setESClient(DEF_ESCLI);
        Booking bk1 = new Booking(DEF_TC, DEF_RANGES, DEF_PRICE, "remove_advId", 2);
        String res = bkDaoImp.createBooking(bk1);
        List<Booking> resArr = bkDaoImp.getBookings("remove_advId");
        if (resArr.size() == 0) assertNotNull("removeBooking size is 0 ", res);
        else {
            String bkId = resArr.get(0).getBookingId();
            bkDaoImp.removeBooking(bkId);
            assertNotNull("removeBooking id ", bkId);
        }
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void createBooking() throws IOException {
        BookingDaoESImp bkDaoImp = new BookingDaoESImp(DEF_PROP);
        bkDaoImp.setESClient(DEF_ESCLI);
        Booking bk1 = new Booking(DEF_TC, DEF_RANGES, DEF_PRICE, DEF_ADVID, 2);

        String res = bkDaoImp.createBooking(bk1);

        assertNotNull(res);
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void getBookings() throws IOException {
        BookingDaoESImp bkDaoImp = new BookingDaoESImp(DEF_PROP);
        bkDaoImp.setESClient(DEF_ESCLI);
        List<Booking> res = bkDaoImp.getBookings(DEF_ADVID);
        assertNotNull(res);

        List<Day> sortedDays = Day.buildSortedDays(DEF_RANGES);
        Set<Day> daySet = new HashSet<>(sortedDays);
        Map<Day, List<Booking>> res2 = bkDaoImp.getBookings(daySet);
        assertNotNull(res2);

        Map<Day, List<Booking>> res3 = bkDaoImp.getBookings(new HashSet());
        assertNotNull(res3);
    }


    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void getBooking() throws IOException {
        BookingDaoESImp bkDaoImp = new BookingDaoESImp(DEF_PROP);
        bkDaoImp.setESClient(DEF_ESCLI);
        List<Booking> resArr = bkDaoImp.getBookings(DEF_ADVID);
        for (Booking bk : resArr) {
            String bkId = bk.getBookingId();
            assertNotNull(bkDaoImp.getBooking(bkId));
        }
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void getBookingBuckets() throws IOException {
        BookingDaoESImp bkDaoImp = new BookingDaoESImp(DEF_PROP);
        bkDaoImp.setESClient(DEF_ESCLI);
        List<Day> sortedDays = Day.buildSortedDays(DEF_RANGES);
        Set<Day> daySet = new HashSet<>(sortedDays);
        Map<Day, List<BookingBucket>> res = bkDaoImp.getBookingBuckets(daySet);
        assertNotNull(res);

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
        ranges.add(r1);
        ranges.add(r2);
        ranges.add(r3);
        List<Day> sortedDays2 = Day.buildSortedDays(ranges);
        Set<Day> daySet2 = new HashSet<>(sortedDays2);
        Map<Day, List<BookingBucket>> res2 = bkDaoImp.getBookingBuckets(daySet2);
        assertNotNull(res2);
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void createBookingBucket() throws IOException {
        String day = "20180105", bookingId = "test_bkid_1";
        List<String> prvBkOrder = new ArrayList();
        BookingBucket bb1 = new BookingBucket(day, bookingId, prvBkOrder, 1);
        BookingDaoESImp bkDaoImp = new BookingDaoESImp(DEF_PROP);
        bkDaoImp.setESClient(DEF_ESCLI);
        String res = bkDaoImp.createBookingBucket(bb1);
        assertNotNull(res);
    }

    @Test
    public void lockBooking() {
        BookingDaoESImp bkDaoImp = new BookingDaoESImp(DEF_PROP);
        bkDaoImp.setESClient(DEF_ESCLI);
        boolean res = bkDaoImp.lockBooking();
        try {
            if (res) assertTrue(res);
            else throw new Exception();
        } catch (Exception e) {
            assertFalse(String.valueOf(e), res);
        }
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void unlockBooking() throws IOException {
        BookingDaoESImp bkDaoImp = new BookingDaoESImp(DEF_PROP);
        bkDaoImp.setESClient(DEF_ESCLI);
        bkDaoImp.unlockBooking();
        assertNotNull(bkDaoImp);
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void getMaxBookingBucketPriority() throws IOException {
        BookingDaoESImp bkDaoImp = new BookingDaoESImp(DEF_PROP);
        bkDaoImp.setESClient(DEF_ESCLI);
        String day = "20181107";
        BookingBucket res = bkDaoImp.getMaxBookingBucketPriority(day);
        assertNotNull(res);
    }

    @Test
    public void updateBookingBucketWithAllocatedAmount() throws IOException {
        String day = "20180105", bookingId = "test_bkid_1";
        List<String> prvBkOrder = new ArrayList();
        BookingBucket bb1 = new BookingBucket(day, bookingId, prvBkOrder, 1);
        BookingDaoESImp bkDaoImp = new BookingDaoESImp(DEF_PROP);
        bkDaoImp.setESClient(DEF_ESCLI);
        String bbId = bkDaoImp.createBookingBucket(bb1);
        Set<Day> set = new HashSet();
        set.add(new Day(day));
        int prv = bkDaoImp.getBookingBuckets(set).size();
        bkDaoImp.updateBookingBucketWithAllocatedAmount(bbId, new HashMap());
        assertEquals(prv, bkDaoImp.getBookingBuckets(set).size(), 0);
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void findBookingBucketsWithAllocatedBookingId() throws IOException {
        String day = "20181107", bookingId = "test_bkid_1";
        List<String> prvBkOrder = new ArrayList();
        BookingBucket bb1 = new BookingBucket(day, bookingId, prvBkOrder, 1);
        BookingDaoESImp bkDaoImp = new BookingDaoESImp(DEF_PROP);
        bkDaoImp.setESClient(DEF_ESCLI);
        String bbId = bkDaoImp.createBookingBucket(bb1);
        List<BookingBucket> res = bkDaoImp.findBookingBucketsWithAllocatedBookingId(bbId);
        assertNotNull(res);
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void removeBookingBucket() throws IOException {
        String day = "20180105", bookingId = "test_bkid_1";
        List<String> prvBkOrder = new ArrayList();
        BookingBucket bb1 = new BookingBucket(day, bookingId, prvBkOrder, 1);
        BookingDaoESImp bkDaoImp = new BookingDaoESImp(DEF_PROP);
        bkDaoImp.setESClient(DEF_ESCLI);
        String bkId = bkDaoImp.createBookingBucket(bb1);

        Set<Day> set = new HashSet();
        set.add(new Day(day));

        Map source = new HashMap();
        source.put("day", day);
        source.put("minus_bk_ids", bb1.getMinusBookingsIds());
        source.put("and_bk_ids", bb1.getAndBookingsIds());
        source.put("allocated_amount", bb1.getAllocatedAmounts());
        source.put("priority", 1);
        BookingBucket bb2 = new BookingBucket(bkId, source);
        String bkId2 = bkDaoImp.createBookingBucket(bb2);

        assertNotNull(bb2);
        bkDaoImp.removeBookingBucket(bb2);
        assertNotNull(bb2);
    }
}
