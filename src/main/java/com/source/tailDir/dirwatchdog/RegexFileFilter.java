/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.source.tailDir.dirwatchdog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.regex.Pattern;

/**
 * This returns true if the filename (not the directory part) matches the
 * specified regular expression.
 * <p/>
 * This class is not thread safe because pattern is not thread safe.
 */
public class RegexFileFilter implements FileFilter {
    // public static final Logger logger = LoggerFactory.getLogger(RegexFileFilter.class);
    private Pattern pattern; // not thread safe

    public RegexFileFilter(String regex) {
        this.pattern = Pattern.compile(regex);
    }

    @Override
    public boolean accept(File candidate) {
        String fileName = candidate.getName();
        if ((candidate.isDirectory()) || (fileName.startsWith("."))) {
            return false;
        }
        Calendar calendar = Calendar.getInstance();
        Date date = calendar.getTime();

        String formatStr = new SimpleDateFormat("yyyyMMdd").format(date);
        boolean isPattern = pattern.matcher(candidate.getName()).matches();
        boolean isContains = fileName.contains(formatStr);
        // logger.info("file: {}, & isPattern: {}, & isContains: {}", fileName, isPattern, isContains);
        return isPattern && isContains;
    }
}
