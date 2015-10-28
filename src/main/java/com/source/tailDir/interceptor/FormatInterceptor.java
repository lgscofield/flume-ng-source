package com.source.tailDir.interceptor;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.source.tailDir.db.JdbcUtils;
import com.source.tailDir.json.JSONException;
import com.source.tailDir.json.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ibm on 2015/10/23.
 */
public class FormatInterceptor implements Interceptor {
    private static final Logger LOGGER = LoggerFactory.getLogger(FormatInterceptor.class);

    private Map<String, String> appDataMap = new HashMap<String, String>();

    @Override
    public void initialize() {
        initAppData();
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

    @Override
    public Event intercept(Event event) {
        LOGGER.debug("FormatInterceptor.intercept().event: {}", event);
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
        String[] pcIds = pcId.split("@");

        if (pcIds.length != 2) {
            return null;
        }
        jsonObject.put("productid", pcIds[0]);
        jsonObject.put("channelid", pcIds[1]);

        LOGGER.error("jsonObject.toString(): {}", jsonObject.toString());
        return EventBuilder.withBody(jsonObject.toString(), Charset.forName("UTF-8"));
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> intercepted = Lists.newArrayListWithCapacity(events.size());
        for (Event event : events) {
            Event interceptedEvent = intercept(event);
            if (interceptedEvent != null) {
                intercepted.add(interceptedEvent);
            }
        }
        LOGGER.error("FormatInterceptor.intercept().intercepted: {}, intercepted.size(): {}", intercepted, intercepted.size());
        return intercepted;
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
