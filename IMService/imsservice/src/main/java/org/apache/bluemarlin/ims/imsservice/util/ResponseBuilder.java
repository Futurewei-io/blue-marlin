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

package org.apache.bluemarlin.ims.imsservice.util;

import org.apache.bluemarlin.ims.imsservice.exceptions.IMSException;
import org.apache.bluemarlin.ims.imsservice.exceptions.NoInventoryExceptions;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.ResponseEntity;

import java.util.HashMap;
import java.util.Map;

/**
 * This class includes set of static methods to build REST end-point responses.
 */
public class ResponseBuilder
{

    private static final String MESSAGE_KEY = "message";
    private static final String CODE_KEY = "code";
    private static final String EXCEPTION_KEY = "exception";

    private ResponseBuilder()
    {
        throw new IllegalStateException("This is utility class.");
    }

    /**
     * This method returns REST end-point response with response value of object.
     *
     * @param object
     * @param httpCode
     * @return
     */
    public static ResponseEntity build(Object object, int httpCode)
    {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> map = new HashMap<>();
        if (object != null)
        {
            if (!(object instanceof Map))
            {
                Map<String, Object> wrapper = new HashMap<>();
                wrapper.put("result", object);
                object = wrapper;
            }
            map = mapper.convertValue(object, Map.class);
        }
        return ResponseEntity.status(httpCode).body(map);
    }

    /**
     * This methods returns an error message for REST end-point.
     *
     * @param exception : exception object that is indicates the error.
     * @return
     */
    public static ResponseEntity buildError(Exception exception)
    {
        int httpCode = 500;
        Map<String, Object> map = new HashMap<>();
        if (StringUtils.isBlank(exception.getMessage()))
        {
            map.put(MESSAGE_KEY, exception.getMessage());
        }
        else
        {
            map.put(MESSAGE_KEY, "Internal Error");
        }
        map.put(CODE_KEY, httpCode);
        map.put(EXCEPTION_KEY, exception.toString());

        /**
         * In case to customize the message for a specific exception
         */
        if (exception instanceof IMSException)
        {
            httpCode = NoInventoryExceptions.HTTP_CODE;
            map.put(CODE_KEY, httpCode);
            map.put(MESSAGE_KEY, exception.getMessage());
            return ResponseEntity.status(httpCode).body(map);
        }

        return ResponseEntity.status(httpCode).body(map);
    }
}
