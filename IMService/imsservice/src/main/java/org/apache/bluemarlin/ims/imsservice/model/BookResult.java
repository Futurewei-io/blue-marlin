/**
 * Copyright 2019, Futurewei Technologies
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bluemarlin.ims.imsservice.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BookResult
{
    @JsonProperty("bk_id")
    private String bookId;

    @JsonProperty("total_booked")
    private Long totalBooked;

    public BookResult(String bookId, Long totalBooked)
    {
        this.bookId = bookId;
        this.totalBooked = totalBooked;
    }

    public void setTotalBooked(Long totalBooked)
    {
        this.totalBooked = totalBooked;
    }

    public String getBookId()
    {
        return bookId;
    }

    public Long getTotalBooked()
    {
        return totalBooked;
    }
}
