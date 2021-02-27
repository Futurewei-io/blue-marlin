/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0.html
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.bluemarlin.ims.imsservice.service;

import org.apache.bluemarlin.ims.imsservice.dao.booking.BookingDao;
import org.apache.bluemarlin.ims.imsservice.dao.inventory.InventoryEstimateDao;
import org.apache.bluemarlin.ims.imsservice.dao.tbr.TBRDao;
import org.apache.bluemarlin.ims.imsservice.dao.users.UserEstimateDao;
import org.apache.bluemarlin.ims.imsservice.util.IMSLogger;
import org.springframework.beans.factory.annotation.Autowired;

public class BaseService
{

    protected static final IMSLogger LOGGER = IMSLogger.instance();

    @Autowired
    protected BookingDao bookingDao;

    @Autowired
    protected InventoryEstimateDao inventoryEstimateDao;

    @Autowired
    protected TBRDao tbrDao;

    @Autowired
    protected UserEstimateDao userEstimateDao;

}
