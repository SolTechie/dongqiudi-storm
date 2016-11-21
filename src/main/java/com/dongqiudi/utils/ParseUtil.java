package com.dongqiudi.utils;

import com.alibaba.fastjson.JSON;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Joshua on 16/11/15.
 */
public class ParseUtil {

    public static final String COMMENT = "comment";
    public static final String ARTICLE = "article";

    private Map<String, Pattern> patterns;

    private ParseUtil() {
        patterns = new HashMap<String, Pattern>();
        patterns.put(COMMENT, Pattern.compile("/articles/create_comment/([0-9]+)"));
        patterns.put(ARTICLE, Pattern.compile("/article/([0-9]+)\\.html"));
    }

    static private ParseUtil instance = new ParseUtil();

    static public ParseUtil getInstance() {
        return instance;
    }

    public Map<String, String> parseNgxAccLog(String rawLine) {
        try {
            return (Map<String, String>) JSON.parse(rawLine);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public String parseUri(String uri, String type) {
        Pattern pattern = patterns.get(type);
        try {
            Matcher match = pattern.matcher(uri);
            if (match.find()) {
                return type + ":" + match.group(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
