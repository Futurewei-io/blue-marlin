/**
 * Copyright 2019, Futurewei Technologies
 * <p>
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bluemarlin.ims.imsservice.controller;

import org.apache.bluemarlin.ims.imsservice.model.*;
import org.apache.bluemarlin.ims.imsservice.service.BookingService;
import org.apache.bluemarlin.ims.imsservice.service.InventoryEstimateService;
import org.apache.bluemarlin.ims.imsservice.service.UserEstimateService;
import org.apache.bluemarlin.ims.imsservice.util.CommonUtil;
import org.apache.bluemarlin.ims.imsservice.util.IMSLogger;
import org.apache.bluemarlin.ims.imsservice.util.ResponseBuilder;
import org.json.JSONException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping(value = "/", produces = {"application/json; charset=utf-8"})
public class IMSController
{

    private static final IMSLogger LOGGER = IMSLogger.instance();

    private static final int HTTP_OK_CODE = 200;

    @Autowired
    private InventoryEstimateService inventoryEstimateService;

    @Autowired
    private BookingService bookingService;

    @Autowired
    private UserEstimateService userEstimateService;

    /**
     * This end-point is to test if the web-framework and servlet dispatcher work.
     *
     * @return
     */
    @RequestMapping(value = "/ping", method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity ping()
    {
        Map response = new HashMap();
        response.put("message", "pong");
        return ResponseBuilder.build(response, HTTP_OK_CODE);
    }

    /**
     * This end-point returns inventory for a targeting channel considering time period and price.
     * The inventory calculation process considers all the bookings.
     * Targeting Channel is a set of attributes that specify a group of users (audience) plus the media.
     *
     * @param payload
     * @return
     */
    @RequestMapping(value = "/api/inventory/count", method = RequestMethod.POST)
    @ResponseBody
    public ResponseEntity getInventory(@RequestBody IMSRequestQuery payload)
    {
        InventoryResult result;
        try
        {
            result = inventoryEstimateService.aggregateInventory(payload.getTargetingChannel(), payload.getDays(),
                    payload.getPrice());

            String logMessage = String.format("Targeting channel: %s estimate: %d",
                    payload.getTargetingChannel().getQueryKey(), result.getAvailCount());
            LOGGER.info(logMessage);
        }
        catch (Exception e)
        {
            LOGGER.error(e.getMessage());
            return ResponseBuilder.buildError(e);
        }

        return ResponseBuilder.build(result, HTTP_OK_CODE);
    }

    /**
     * This end-point returns daily inventories for a targeting channel considering day period (no price).
     * The inventory calculation process considers all the bookings.
     * Targeting Channel is a set of attributes that specify a group of users (audience) plus the media.
     *
     * @param payload
     * @return
     */
    @RequestMapping(value = "/api/inventory/daily/count", method = RequestMethod.POST)
    @ResponseBody
    public ResponseEntity getDailyInventory(@RequestBody IMSRequestQuery payload)
    {
        Map<Day, Impression.ImpressionShort> result;
        try
        {
            result = inventoryEstimateService.aggregateDailyInventory(payload.getTargetingChannel(), payload.getDays(),
                    payload.getPrice());

            String logMessage = String.format("Targeting channel: %s - daily count",
                    payload.getTargetingChannel().getQueryKey());
            LOGGER.info(logMessage);
        }
        catch (Exception e)
        {
            LOGGER.error(e.getMessage());
            return ResponseBuilder.buildError(e);
        }

        return ResponseBuilder.build(result, HTTP_OK_CODE);
    }

    /**
     * This end-point returns number of unique users for a targeting channel in a time period.
     *
     * @param payload
     * @return
     */
    @RequestMapping(value = "/api/users/count", method = RequestMethod.POST)
    @ResponseBody
    public ResponseEntity getUserEstimate(@RequestBody IMSRequestQuery payload)
    {
        InventoryResult result;
        try
        {
            Date today = new Date();
            if (!CommonUtil.isBlank(payload.getToday()))
            {
                today = CommonUtil.dayToDate(payload.getToday());
            }

            result = userEstimateService.getUserEstimate(payload.getTargetingChannel(), payload.getDays(),
                    payload.getPrice(), today);
            String logMessage = String.format("Targeting channel: %s estimate: %d",
                    payload.getTargetingChannel().getQueryKey(), result.getAvailCount());

            LOGGER.info(logMessage);
        }
        catch (Exception e)
        {
            LOGGER.error(e.getMessage());
            return ResponseBuilder.buildError(e);
        }

        return ResponseBuilder.build(result, HTTP_OK_CODE);
    }

    /**
     * This end-point books a number of inventory for a time period.
     *
     * @param payload
     * @return
     */
    @RequestMapping(value = "/api/bookings", method = RequestMethod.POST)
    @ResponseBody
    public ResponseEntity book(@RequestBody IMSBookingRequest payload)
    {
        BookResult bookResult;
        try
        {
            bookResult = bookingService.book(payload.getTargetingChannel(), payload.getDays(), payload.getPrice(), payload.getAdvID(), payload.getRequestCount());
        }
        catch (Exception e)
        {
            LOGGER.error(e.getMessage());
            return ResponseBuilder.buildError(e);
        }
        return ResponseBuilder.build(bookResult, HTTP_OK_CODE);
    }

    /**
     * This end-point removes a booking.
     *
     * @param bookingId
     * @return
     */
    @RequestMapping(value = "/api/bookings/{bookingId}", method = RequestMethod.DELETE)
    @ResponseBody
    public ResponseEntity removeBooking(@PathVariable("bookingId") String bookingId)
    {
        try
        {
            bookingService.removeBooking(bookingId);
        }
        catch (Exception e)
        {
            LOGGER.error(e.getMessage());
            return ResponseBuilder.buildError(e);
        }
        return ResponseBuilder.build(null, HTTP_OK_CODE);
    }

    /**
     * This end-point returns all the bookings that belong to an advertiser.
     *
     * @param advId
     * @return
     */
    @RequestMapping(value = "/api/{advId}/bookings", method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity getAdvID(@PathVariable("advId") String advId)
    {
        List<Booking> result;

        try
        {
            result = bookingService.getBookingsByAdvId(advId);
        }
        catch (Exception e)
        {
            LOGGER.error(e.getMessage());
            return ResponseBuilder.buildError(e);
        }

        return ResponseBuilder.build(result, HTTP_OK_CODE);
    }

    /**
     * This end-point returns one specific booking.
     *
     * @param bookingId
     * @return
     */
    @RequestMapping(value = "/api/bookings/{bookingId}", method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity getBookingById(@PathVariable("bookingId") String bookingId)
    {
        Booking result;
        try
        {
            result = bookingService.getBookingById(bookingId);
        }
        catch (Exception e)
        {
            LOGGER.error(e.getMessage());
            return ResponseBuilder.buildError(e);
        }
        return ResponseBuilder.build(result, HTTP_OK_CODE);
    }

    /**
     * This end-point returns hourly traffic (number of impressions) for a targeting channel.
     *
     * @param payload
     * @return
     * @throws JSONException
     */
    @RequestMapping(value = "/api/chart", method = RequestMethod.POST)
    @ResponseBody
    public ResponseEntity chart(@RequestBody IMSRequestQuery payload) throws JSONException
    {
        DayImpression result;
        try
        {
            result = inventoryEstimateService.getInventoryDateEstimate(payload.getTargetingChannel());
        }
        catch (Exception e)
        {
            LOGGER.error(e.getMessage());
            return ResponseBuilder.buildError(e);
        }
        return ResponseBuilder.build(result, HTTP_OK_CODE);
    }
}
