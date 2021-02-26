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

package org.apache.bluemarlin.ims.imsservice.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IMSLogger
{
    protected static final Logger logger = LoggerFactory.getLogger(IMSLogger.class);

    public static IMSLogger instance()
    {
        return new IMSLogger();
    }

    public void info(String message)
    {
        logger.info(message);
    }

    public void info(String format, Object arg)
    {
        logger.info(format, arg);
    }

    public void error(String message)
    {
        logger.error(message);
    }

    public void error(String format, Object arg)
    {
        logger.error(format, arg);
    }

    public void debug(String message)
    {
        logger.debug(message);
    }

    public void debug(String format, Object arg)
    {
        logger.debug(format, arg);
    }

    public void warn(String message)
    {
        logger.warn(message);
    }

    public void warn(String format, Object arg)
    {
        logger.warn(format, arg);
    }
}
