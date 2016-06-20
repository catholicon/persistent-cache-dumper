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
            write.println("Options: [options] <cacheFileName> ...");
            write.println("  [path=/prefix] (only list entries starting with this path)");
            write.println("  [revision=prefix] (only list revisions that start with this prefix)");
            write.println("  [+values] (also print values, not just keys and value length)");
            write.println("  [+dumpMap] (dump map without trying to parse its key/values)");
            write.println("  [map=mapName] (only print contents of this map)");
            write.println("  [out=outputFile] (print to this file instead of stdout)");
            write.println("Map names and statistic are listed if just the file name is specified.");
            write.println("To list all keys, just specify '/' and the file name.");
            write.println("To dump multiples files in one go, add multiple file names.");
            write.println("Files are accessed in read-only mode; " +
                    "to analyze a running system you need to copy the cache file first.");
            write.println("Output format is CSV (',' replaced with '#')");
            write.println("To import in H2, use: " +
                    "create table cache as select * from csvread('cache.csv', null, 'fieldDelimiter=')");
        }
        String path = "";
        String revision = "";
        String map = "";
        boolean dumpMap = false;
        boolean values = false;
        for (String a : args) {
            if (a.startsWith("path=")) {
                path = a.substring("path=".length());
            } else if (a.startsWith("revision=")) {
                revision = a.substring("revision=".length());
            } else if (a.startsWith("map=")) {
                map = a.substring("map=".length());
            } else if (a.startsWith("+values")) {
                values = true;
            } else if (a.startsWith("-values")) {
                values = false;
            } else if (a.startsWith("+dumpMap")) {
                dumpMap = true;
            } else if (a.startsWith("-dumpMap")) {
                dumpMap = false;
            } else if (a.startsWith("out=")) {
                String out = a.substring("out=".length());
                write = new PrintWriter(new BufferedWriter(new FileWriter(out)));
            } else {
                String fileName = a;
                dump(write, path, revision, map, fileName, dumpMap, values);
            }
        }

        write.close();
    }

    static void dump(PrintWriter write, String path, String revision,
                     String map, String fileName, boolean dumpMap, boolean values) {
        MVStore s = new MVStore.Builder().readOnly().
                fileName(fileName).open();
        Map<String, String> meta = s.getMetaMap();
        boolean statsOnly = "".equalsIgnoreCase(map) &&
                "".equals(revision) &&
                "".equals(path) &&
                !dumpMap;
        if (!statsOnly && !dumpMap) {
            if (values) {
                write.println("map,path,revision,p2,value");
            } else {
                write.println("map,path,revision,p2,length");
            }
        }
        for (String n : meta.keySet()) {
            if (n.startsWith("name.")) {
                String mapName = n.substring(5, n.length());
                if (map.length() > 0 && !map.equalsIgnoreCase(mapName)) {
                    continue;
                }
                MVMap.Builder<String, String> b =
                        new MVMap.Builder<String, String>().
                                keyType(StringDataType.INSTANCE).valueType(
                                StringDataType.INSTANCE);
                MVMap<String, String> m = s.openMap(mapName, b);
                if (statsOnly) {
                    statistics(write, m);
                } else if (dumpMap) {
                    dump(write, m);
                } else {
                    dump(write, m, path, revision, values);
                }
            }
        }
        s.close();
    }

    static void statistics(PrintWriter write, MVMap<String, String> m) {
        write.println("Map: " + m.getName().toLowerCase());
        write.println("entryCount: " + m.sizeAsLong());
        long keyLen = 0, valueLen = 0;
        for (Map.Entry<String, String> e : m.entrySet()) {
            String k = e.getKey();
            String v = e.getValue();
            keyLen += k.length();
            valueLen += v.length();
        }
        write.println("keyLen: " + keyLen);
        write.println("valueLen: " + valueLen);
        write.println();
    }

    static void dump(PrintWriter write, MVMap<String, String> m) {
        String mapName = m.getName().toLowerCase();
        for (Map.Entry<String, String> e : m.entrySet()) {
            String k = e.getKey();
            k = k.replace('|', '#');

            String v = e.getValue();
            v = v.replace('|', '#');

            write.println(mapName + "|" + k + "|" + v);
        }
    }

    static void dump(PrintWriter write, MVMap<String, String> m, String path,
                     String revision, boolean values) {
        String mapName = m.getName().toLowerCase();
        for (Map.Entry<String, String> e : m.entrySet()) {
            String k = e.getKey();
            int slash = k.indexOf('/');
            String r2 = "";
            if (!k.startsWith("/") && slash > 0) {
                r2 = k.substring(0, slash).replace(',', '#');
                k = k.substring(slash);
            }
            String v = e.getValue();
            if (!"/".equals(path) && !k.startsWith(path)) {
                continue;
            }
            int lastAt = k.lastIndexOf('@');
            String r = "";
            if (lastAt > 0) {
                r = k.substring(lastAt + 1).replace(',', '#');
                k = k.substring(0, lastAt);
            }
            if (!"".equals(revision) && !r.startsWith(revision)) {
                continue;
            }
            if (!values) {
                v = "" + v.length();
            } else {
                v = v.replace(',', '#');
            }
            k = k.replace(',', '#');
            write.println(mapName + "," + k + "," + r + "," + r2 + "," + v);
        }
    }
}
