package com.source.tailDir.interceptor;

import com.google.common.collect.Lists;
import com.source.tailDir.Common.Constants;
import com.source.tailDir.db.JdbcUtils;
import com.source.tailDir.json.JSONArray;
import com.source.tailDir.json.JSONException;
import com.source.tailDir.json.JSONObject;
import com.source.tailDir.util.DESUtil;
import com.source.tailDir.util.IPLocation;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by ibm on 2015/10/23.
 */
public class FormatInterceptor implements Interceptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(FormatInterceptor.class);

    private Map<String, String> appDataMap = new HashMap<String, String>();

    private IPLocation ipLocation;

    @Override
    public void initialize() {
        initAppData();
        initLocation();
    }

    /**
     * 初始化appkey
     */
    private void initAppData() {
        Connection connection = JdbcUtils.getConnection();
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            final String dbPrefix = JdbcUtils.getDbPrefix().trim();
            final String sql = "select productkey, product_id, channel_id from " + dbPrefix + "channel_product a left join " + dbPrefix + "product b on a.product_id = b.id where b.active = 1";
            preparedStatement = connection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String productKey = resultSet.getString("productkey");
                String productId = resultSet.getString("product_id");
                String channelId = resultSet.getString("channel_id");
                appDataMap.put(productKey, productId + "@" + channelId);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            JdbcUtils.closeConnection();
        }
    }

    /**
     * 初始化IP LOCATION
     */
    private void initLocation() {
        ipLocation = new IPLocation("17monipdb.dat");
    }

    @Override
    public Event intercept(Event event) {
        String body = new String(event.getBody(), Charset.forName("UTF-8"));
        JSONObject jsonObject;
        try {
            jsonObject = new JSONObject(body);
        } catch (JSONException e) {
            return null;
        }

        String key = jsonObject.getString("appkey");
        String deviceid = jsonObject.getString("deviceid");

        if (null == deviceid) {
            return null;
        }
        // productId and chanelId
        String pcId = appDataMap.get(key);
        if (null == pcId) {
            return null;
        }
        String[] pcIds = pcId.split("@");

        if (pcIds.length != 2) {
            return null;
        }
        jsonObject.put("productid", pcIds[0]);
        jsonObject.put("channelid", pcIds[1]);

        // ip location
        final String clientip = jsonObject.optString("clientip").trim();
        final String[] locs;
        final String[] location = {Constants.UNKNOWN, Constants.UNKNOWN, Constants.UNKNOWN};
        if (clientip.matches(Constants.IPADDRESS_PATTERN)) {
            locs = ipLocation.find(clientip);
            if (locs.length > 0) {
                System.arraycopy(locs, 0, location, 0, Math.min(locs.length, location.length));
            }
        }
        jsonObject.put("country", location[0]);
        jsonObject.put("region", location[1]);
        jsonObject.put("city", location[2]);

        // SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        // jsonObject.put("localtime", sdf.format(Calendar.getInstance().getTime()));
        LOGGER.info("public Event intercept(Event event) jsonObject: {}", jsonObject);
        return EventBuilder.withBody(jsonObject.toString(), Charset.forName("UTF-8"));
    }

    public static void main(String[] args) {
        String body = "2015-11-13 12:24:26||192.168.1.107||KFiO2S+ERu5tMO3oayRDFq77Bj3eqC66Mspk5tDt3mhc6r9Rrdie/mMAqQ7g4AE1D8Hs3LIjIWWKppR7JDzlT+q5faByk9lOfakCGxKrdhXoUtzh9PtSVLBQQDVEK64UTjzdcVJ+UmROKtR9EL50jKW7gU/2skVjurrlSdEMx1bqhw2cLe/vesvRXuaqnasbo5R9DBe+r5jqhw2cLe/vesYjZSZqtj856SmIyltA7mCxrZNUiP3VH9gTsJLKuupsNoBzfrcdChYa8WavQeuax984MFfcHlW63PT5gls1P2fV62XLqcioKdnJRLP/atyKR8LQ+6ABGyViwx7S//XzGHOwzbDntP1j9YERlS1L4QS5VTAOownqNmLDHtL/9fMYsUCYhS2b/9SjHP2cr7yFLSEfT8Zy+BtZyLx66zN0Vs9XUmekg+nZoPgTNC8JCiECiWqGR7NktEkTzVM/nbT8GWnF8Hizle/290CDgRs9zbtOPN1xUn5SZLAtEl1KCLPBjyWtSPQtzUbqTXubRuTVVYqhDjR0gomMCPGg+2Y1+AJklS719RyWWl+JltDgYAW0rklLcATsKQIjuVorIPBFrzmr820RbvlDiReCJpYZ4pUwIXdYfeZ4L9ACe+Z5LbL7g6Qm9FwIPsFiwx7S//XzGHRAAUh06Vjd2rZqzeLsJWgBInX9eRVwZtzpwaZuZnSU/H4gknTbWr7RhuRiJKkpplN6W42d8+DgVTOwlo25LVOQeOGApf/f2qZ/umYZe7wuz5sZRUqG4evvRBZMD00KP036b2S5sk+fGHXbEvMtFKD9bvCsKGALm7UjDDNk8Z2C";

        List<Event> events = new ArrayList<Event>();
        try {
            events.add(EventBuilder.withBody(body.getBytes("UTF-8")));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        FormatInterceptor formatInterceptor = new FormatInterceptor();
        formatInterceptor.initLocation();
        formatInterceptor.intercept(events);
        try {
            System.out.println(DESUtil.decryptDES("Izcmdv7QPcQ=", null));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> intercepted = Lists.newArrayListWithCapacity(events.size());
        for (Event event : events) {
            String body = new String(event.getBody(), Charset.forName("UTF-8"));
            LOGGER.info("body: {}", body);

            String[] bodys = body.split("\\|\\|");
            LOGGER.info("bodys 0: {},  1: {},  2: {}", bodys[0], bodys[1], bodys[2]);
            if (bodys.length < 3) {
                continue;
            }

            // event message
            String content = bodys[2];

            try {
                String bodyStr;
                if (isEncrypt(content)) {
                    bodyStr = DESUtil.decryptDES(content, null);
                } else {
                    bodyStr = DESUtil.decode(content.replace("content=", ""), null);
                }

                JSONObject jsonObject = new JSONObject(bodyStr);
                JSONArray jsonArray = jsonObject.getJSONArray("data");
                if (null != jsonArray) {
                    for (int i = 0; i < jsonArray.length(); i++) {
                        JSONObject newJsonObject = jsonArray.getJSONObject(i);
                        newJsonObject.put("localtime", bodys[0]);
                        newJsonObject.put("clientip", bodys[1]);
                        String tempBody = newJsonObject.toString();
                        Event buildEvent = EventBuilder.withBody(tempBody.getBytes("UTF-8"));
                        Event interceptedEvent = intercept(buildEvent);
                        if (interceptedEvent != null) {
                            intercepted.add(interceptedEvent);
                        }
                    }
                }
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
        return intercepted;
    }

    private static boolean isEncrypt(String body) {
        if (body.startsWith("content=")) {
            return false;
        }
        return true;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            LOGGER.info("Creating FormatInterceptor...");
            return new FormatInterceptor();
        }

        @Override
        public void configure(Context context) {
        }
    }
}
