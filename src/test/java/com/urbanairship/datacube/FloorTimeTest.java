/**
 * Copyright (C) 2012 Neofonie GmbH
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.urbanairship.datacube;

import com.urbanairship.datacube.bucketers.HourDayMonthBucketer;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

public class FloorTimeTest {

    @Test
    public void testFullHour() {
        DateTime input = new DateTime(2012, 7, 19, 14, 40, 12, 34);
        DateTime expected = new DateTime(2012, 7, 19, 14, 0, 0, 0);

        DateTime actual = HourDayMonthBucketer.hourFloor(input);

        Assert.assertEquals("flooring hour failed", expected, actual);
    }

    @Test
    public void testFullDay() {
        DateTime input = new DateTime(2012, 7, 19, 14, 40, 12, 34);
        DateTime expected = new DateTime(2012, 7, 19, 0, 0, 0, 0);

        DateTime actual = HourDayMonthBucketer.dayFloor(input);

        Assert.assertEquals("flooring day failed", expected, actual);
    }

    @Test
    public void testFullWeek() {
        DateTime input = new DateTime(2012, 7, 19, 14, 40, 12, 34);
        DateTime expected = new DateTime(2012, 7, 16, 0, 0, 0, 0);

        DateTime actual = HourDayMonthBucketer.weekFloor(input);

        Assert.assertEquals("flooring week failed", expected, actual);
    }

    @Test
    public void testFullWeekAtMonthEdge() {
        DateTime input = new DateTime(2012, 8, 3, 14, 40, 12, 34);
        DateTime expected = new DateTime(2012, 7, 30, 0, 0, 0, 0);

        DateTime actual = HourDayMonthBucketer.weekFloor(input);

        Assert.assertEquals("flooring week at month edge failed", expected, actual);
    }

    @Test
    public void testFullWeekAtYearEdge() {
        DateTime input = new DateTime(2014, 1, 3, 14, 40, 12, 34);
        DateTime expected = new DateTime(2013, 12, 30, 0, 0, 0, 0);

        DateTime actual = HourDayMonthBucketer.weekFloor(input);

        Assert.assertEquals("flooring week at year edge failed", expected, actual);
    }

    @Test
    public void testFullMonth() {
        DateTime input = new DateTime(2012, 7, 19, 14, 40, 12, 34);
        DateTime expected = new DateTime(2012, 7, 1, 0, 0, 0, 0);

        DateTime actual = HourDayMonthBucketer.monthFloor(input);

        Assert.assertEquals("flooring month failed", expected, actual);
    }

    @Test
    public void testFullYear() {
        DateTime input = new DateTime(2012, 7, 19, 14, 40, 12, 34);
        DateTime expected = new DateTime(2012, 1, 1, 0, 0, 0, 0);

        DateTime actual = HourDayMonthBucketer.yearFloor(input);

        Assert.assertEquals("flooring year failed", expected, actual);
    }

}
