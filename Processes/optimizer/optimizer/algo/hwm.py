#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
 
#  http://www.apache.org/licenses/LICENSE-2.0.html

#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from pyspark.sql.types import BooleanType, MapType, StringType, IntegerType
from pyspark.sql.functions import udf
from pyspark.sql import functions as sql_functions

# user defined agg function, params are lists, return is a number

# allocations: [{'b1':100,'b2':200},{...}]
# amounts : [1000,2000,...]


def total_inventory_agg(allocations, amounts):
    result = 0
    for value in amounts:
        result += value
    for allocated in allocations:
        for _, value in allocated.items():
            result -= value
    return result


def hwm_allocation(df, bookings, days):

    # sort the bookings
    sorted_bookings = __sort_bookings(bookings)

    sorted_days = __sort_days(days)

    _audf = udf(total_inventory_agg, IntegerType())

    # for each booking use hwm for the allocation
    for day in sorted_days:
        for booking in sorted_bookings:
            # calculate the total amount of inventory for connected bookings
            df_connected_source = df.filter(udf(lambda day, ands: day in days and booking['bk_id'] in ands, BooleanType())
                                            (df.day, df.ands))

            # total_inventory is sigma of (amount - booked in allocation)
            total_inventory = df_connected_source.agg(_audf(sql_functions.collect_list(
                'allocated'), sql_functions.collect_list('amount'))).collect()[0][0]

            # if total inventory is zero means there is no allocation on bb
            if total_inventory and total_inventory > 0:
                # we assume that amount of booking is evenly distributed between days
                bi = booking['amount'] * 1.0 / len(booking['days'])
                ai = min(bi / total_inventory, 1.0)
                _, df = update_allocation_for_booking(
                    df, day, booking['bk_id'], ai)
    return df


# total_inventory = total inventory of the connected nodes
def update_allocation_for_booking(df, day, booking_bk_id, ai):
    def helper(df_day, df_ands, df_allocated, df_amount):
        result = df_allocated
        if day == df_day and booking_bk_id in df_ands:
            total_booked_from_bb = 0
            for _, value in df_allocated.items():
                total_booked_from_bb += value

            available_inventory_from_bb = df_amount - total_booked_from_bb
            amount_allocated_from_resource = available_inventory_from_bb * ai
            result[booking_bk_id] = round(amount_allocated_from_resource)
        return result
    _udf = udf(helper, MapType(StringType(), IntegerType()))
    if df:
        df = df.withColumn('new_allocated', _udf(
            df.day, df.ands, df.allocated, df.amount))
        df = df.drop('allocated').withColumnRenamed(
            'new_allocated', 'allocated')
    return (helper, df)


def __sort_bookings(bookings):
    #bookings_sorted = sorted(bookings, key = lambda x: x['bk_id'], reverse=True)
    #bookings_sorted = sorted(bookings_sorted, key = lambda x: -x['amount'])
    #return bookings_sorted
    return bookings


def __sort_days(days):
    return sorted(days)

# input resources={'res_id_1':100,...}
# input demands={'b1':100,'b2':200,...}
# response allocation_map={'res_id_1':{'b1':10,'b2':10},...}
# note: all the resources are connected to all demands


def hwm_generic_allocation(resources, resources_order_list, demands, demands_order_list):
    allocation_map = {}
    total_resources = sum(resources.values())
    allocated_amount = 0
    for demand_id in demands_order_list:
        bi = demands[demand_id]
        ti = total_resources - allocated_amount
        if ti <= 0:
            break
        ai = min(bi * 1.0 / ti, 1)
        bi_left = bi
        for res_id in resources_order_list:
            if res_id not in allocation_map:
                allocation_map[res_id] = {}
                
            available_amount_on_resource = resources[res_id] - sum(allocation_map[res_id].values())

            resource_allocated_amount = ai * available_amount_on_resource
            real_allocated_amount = max(round(resource_allocated_amount), 1)
            real_allocated_amount = min(real_allocated_amount, bi_left)

            allocation_map[res_id][demand_id] = real_allocated_amount
            allocated_amount += real_allocated_amount
            bi_left -= real_allocated_amount
            if bi_left <= 0:
                break
    return allocation_map
