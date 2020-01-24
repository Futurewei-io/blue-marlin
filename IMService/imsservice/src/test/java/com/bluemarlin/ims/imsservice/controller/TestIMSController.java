package com.bluemarlin.ims.imsservice.controller;


import com.bluemarlin.ims.imsservice.model.*;
import com.bluemarlin.ims.imsservice.service.BookingService;
import com.bluemarlin.ims.imsservice.service.InventoryEstimateService;
import com.bluemarlin.ims.imsservice.service.UserEstimateService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpSession;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;


@RunWith(MockitoJUnitRunner.Silent.class)
@ContextConfiguration(classes = {TestContext.class, IMSController.class})

@WebAppConfiguration
public class TestIMSController {

    private MockMvc mockMvc;

    @Autowired
    WebApplicationContext wac;

    @Autowired
    MockHttpSession session;

    @Autowired
    MockHttpServletRequest request;

    @Mock
    private InventoryEstimateService invEstSrv;

    @Mock
    private BookingService bkSrv;

    @Mock
    private UserEstimateService usrEstSrv;

    @InjectMocks
    private IMSController imsCtrl;

    @Before
    public void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(imsCtrl).build();
    }

    @Test
    public void ping() throws Exception {
        MvcResult res = mockMvc.perform(get("/ping").accept(
                MediaType.APPLICATION_JSON)).andExpect(status().isOk()).andReturn();

        String content = res.getResponse().getContentAsString();
        System.out.println("ping " + content);
    }

    @Test
    public void getInventory() throws Exception {
        IMSRequestQuery payload = new IMSRequestQuery();
        TargetingChannel tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_f"));
        tc.setA(Arrays.asList("3"));
        payload.setTargetingChannel(tc);
        List<Range> ranges = new ArrayList<>();
        Range r1 = new Range();
        r1.setSt("2018-01-05");
        r1.setEd("2018-01-05");
        r1.setSh("0");
        r1.setEh("23");
        ranges.add(r1);
        payload.setDays(ranges);
        payload.setPrice(100D);

        when(invEstSrv.aggregateInventory(payload.getTargetingChannel(), payload.getDays(), payload.getPrice()))
                .thenReturn(new InventoryResult());

        String reqJson = "{\"targetingChannel\": {\"g\":[\"g_f\"],\"a\":[\"3\"]},\"price\":100,\"days\": [{\"st\": \"2018-01-05\",\"ed\": \"2018-01-05\",\"sh\": 0,\"eh\": 23}]}";

        MvcResult res = (MvcResult) mockMvc.perform(post("/api/inventory/count").contentType(
                MediaType.APPLICATION_JSON).content(reqJson))
                .andReturn();
        System.out.println("getInventory " + res.toString());
    }

    @Test
    public void getUserEstimate() throws Exception {
        IMSRequestQuery payload = new IMSRequestQuery();
        TargetingChannel tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_f"));
        tc.setA(Arrays.asList("3"));
        payload.setTargetingChannel(tc);
        List<Range> ranges = new ArrayList<>();
        Range r1 = new Range();
        r1.setSt("2018-01-05");
        r1.setEd("2018-01-05");
        r1.setSh("0");
        r1.setEh("23");
        ranges.add(r1);
        payload.setDays(ranges);
        payload.setPrice(100D);
        Date today = new Date();
        when(usrEstSrv.getUserEstimate(payload.getTargetingChannel(), payload.getDays(), payload.getPrice(), today))
                .thenReturn(new InventoryResult());

        String reqJson = "{\"targetingChannel\": {\"g\":[\"g_f\"],\"a\":[\"3\"]},\"price\":100,\"days\": [{\"st\": \"2018-01-05\",\"ed\": \"2018-01-05\",\"sh\": 0,\"eh\": 23}], \"today\": \"2018-01-06\"}";

        MvcResult res = (MvcResult) mockMvc.perform(post("/api/users/count").contentType(
                MediaType.APPLICATION_JSON).content(reqJson))
                .andReturn();
        System.out.println("getUserEstimate " + res.toString());
    }

    @Test
    public void book() throws Exception {
        IMSBookingRequest payload = new IMSBookingRequest();
        TargetingChannel tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_f"));
        tc.setA(Arrays.asList("3"));
        payload.setTargetingChannel(tc);
        List<Range> ranges = new ArrayList<>();
        Range r1 = new Range();
        r1.setSt("2018-01-05");
        r1.setEd("2018-01-05");
        r1.setSh("0");
        r1.setEh("23");
        ranges.add(r1);
        payload.setDays(ranges);
        payload.setPrice(10D);
        payload.setAdvID("advid100");
        payload.setRequestCount(10);
        when(bkSrv.book(payload.getTargetingChannel(), payload.getDays(), payload.getPrice(), payload.getAdvID(), payload.getRequestCount()))
                .thenReturn(new BookResult("bookId", 10L));

        String reqJson = "{\"targetingChannel\": {\"g\": [\"g_f\"],\"a\": [\"3\"]},\"advId\": \"advid100\",\"price\": 10,\"days\": [{\"st\": \"2018-01-05\",\"ed\": \"2018-01-05\",\"sh\": 0,\"eh\": 23}],\"requestCount\": 10}";

        MvcResult res = (MvcResult) mockMvc.perform(post("/api/bookings").contentType(
                MediaType.APPLICATION_JSON).content(reqJson))
                .andReturn();
        System.out.println("book " + res.toString());
    }


    @Test
    public void removeBooking() {
    }

    @Test
    public void getAdvID() throws Exception {
        String dummyId = "advid100";

        MvcResult res = mockMvc.perform(get("/api/{advId}/bookings", dummyId).accept(
                MediaType.APPLICATION_JSON)).andExpect(status().isOk()).andReturn();
        System.out.println("getAdvID " + res.toString());
    }

    @Test
    public void getBookingById() throws Exception {
        TargetingChannel tc1 = new TargetingChannel();
        List<Range> ranges = new ArrayList<>();
        Range r1 = new Range("2018-01-05", "2018-01-05", "0", "23");
        ranges.add(r1);
        Booking bk1 = new Booking(new TargetingChannel(), ranges, 0, "", 0);

        String dummyId = "cb2z3W4BBdaKrM1DQxpx";

        MvcResult res = mockMvc.perform(get("/api/bookings/{bookingId}", dummyId).accept(
                MediaType.APPLICATION_JSON)).andExpect(status().isOk()).andReturn();

        System.out.println("getBookingById " + res.toString());
    }

    @Test
    public void chart() throws Exception {
        IMSRequestQuery payload = new IMSRequestQuery();
        TargetingChannel tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_f"));
        tc.setA(Arrays.asList("3"));
        payload.setTargetingChannel(tc);

        when(invEstSrv.getInventoryDateEstimate(payload.getTargetingChannel()))
                .thenReturn(new DayImpression());

        String reqJson = "{\"targetingChannel\": {\"g\":[\"g_f\"],\"a\":[\"3\"]},\"price\":100}";

        MvcResult res = (MvcResult) mockMvc.perform(post("/api/chart").contentType(
                MediaType.APPLICATION_JSON).content(reqJson))
                .andReturn();
        System.out.println("chart " + res.toString());
    }


}