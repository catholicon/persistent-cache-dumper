/*
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

package com.chetanmeh.cache;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Map;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.type.StringDataType;

public class DumpCache {

    public static void main(String... args) throws Exception {
        PrintWriter write = new PrintWriter(System.out);
        if (args.length == 0) {
            write.println("Options: <cacheFileName> ...");
            write.println("Files are accessed in read-only mode; " +
                    "to analyze a running system you need to copy the cache file first.");
            write.println("Output format is CSV (',' replaced with '#')");
            write.println("To import in H2, use: " +
                    "create table cache as select * from csvread('cache.csv', null, 'fieldDelimiter=')");
        }
        for (String a : args) {
            if (a.startsWith("out=")) {
                String out = a.substring("out=".length());
                write = new PrintWriter(new BufferedWriter(new FileWriter(out)));
            } else {
                String fileName = a;
                dump(write, fileName);
            }
        }
    }

    static void dump(PrintWriter write, String fileName) {
        MVStore s = new MVStore.Builder().readOnly().
                fileName(fileName).open();
        Map<String, String> meta = s.getMetaMap();
        write.println("map,length,key");

        for (String n : meta.keySet()) {
            if (n.startsWith("name.")) {
                String mapName = n.substring(5, n.length());
                MVMap.Builder<String, String> b =
                        new MVMap.Builder<String, String>().
                                keyType(StringDataType.INSTANCE).valueType(
                                StringDataType.INSTANCE);
                MVMap<String, String> m = s.openMap(mapName, b);
                dump(write, m);
            }
        }
        s.close();
    }

    static void dump(PrintWriter write, MVMap<String, String> m) {
        String mapName = m.getName().toLowerCase();
        for (Map.Entry<String, String> e : m.entrySet()) {
            String k = e.getKey();
            k = k.replace(',', '#');
            int length = e.getValue().length();
            write.printf("%s,%d,%s%n", mapName, length, k);
        }
    }
}
