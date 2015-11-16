package com.source.tailDir.interceptor;

import com.google.common.collect.Lists;
import com.source.tailDir.Common.Constants;
import com.source.tailDir.db.JdbcUtils;
import com.source.tailDir.json.JSONArray;
import com.source.tailDir.json.JSONException;
import com.source.tailDir.json.JSONObject;
import com.source.tailDir.util.DESUtil;
import com.source.tailDir.util.IPLocation;
import org.apache.commons.lang.StringUtils;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Interceptor for event rebuild.
 * <p/>
 * Created by ibm on 2015/10/23.
 */
public class FormatInterceptor implements Interceptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(FormatInterceptor.class);

    private String dbPrefix;

    private Map<String, String> appDataMap = new HashMap<String, String>();

    private List<String> blacklist = new ArrayList<String>();

    private IPLocation ipLocation;

    @Override
    public void initialize() {
        this.dbPrefix = JdbcUtils.getDbPrefix().trim();
        this.initAppData();
        this.initLocation();
    }

    /**
     * 初始化appkey
     */
    private void initAppData() {
        Connection connection = JdbcUtils.getConnection();
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
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

    private String queryProductInfo(String appKey) {
        if (StringUtils.isEmpty(appKey) || blacklist.contains(appKey)) {
            return null;
        }
        String tempAppKey = appKey.trim();
        final String sql = "select productkey, product_id, channel_id from " + dbPrefix + "channel_product a left join " + dbPrefix + "product b on a.product_id = b.id where b.active = 1 And a.productkey = " + tempAppKey;
        Connection connection = JdbcUtils.getConnection();
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = connection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();

            int count = 0;
            while (resultSet.next()) {
                count++;
                String productKey = resultSet.getString("productkey");
                String productId = resultSet.getString("product_id");
                String channelId = resultSet.getString("channel_id");
                String info = productId + "@" + channelId;
                appDataMap.put(productKey, info);
            }
            if (count > 1) {
                return null;
            }
            return appDataMap.get(tempAppKey);
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
            JdbcUtils.closeConnection();
        }
        if (appDataMap.isEmpty()) {
            blacklist.add(tempAppKey);
        }
        return null;
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
            LOGGER.error("The deviceId is invalid!");
            return null;
        }
        // productId and chanelId
        String pcId = StringUtils.isEmpty(appDataMap.get(key)) ? queryProductInfo(key) : appDataMap.get(key);
        if (null == pcId) {
            LOGGER.error("The appKey: {} has not invalid key!", key);
            return null;
        }
        String[] pcIds = pcId.split("@");

        if (pcIds.length != 2) {
            LOGGER.error("The productId or channelId is invalid!");
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
        String body = "2015-11-16 12:03:17||112.17.239.174||content=%7B%22data%22%3A%5B%7B%22platform%22%3A%22android%22%2C%22session_id%22%3A%229b0eb6f3b329534f7c8ec177e50ddebf%22%2C%22cellid%22%3A%2282763588%22%2C%22ismobiledevice%22%3Atrue%2C%22havewifi%22%3Afalse%2C%22appkey%22%3A%2272745e80fd1f11e493f800163e0240bd%22%2C%22resolution%22%3A%22480x854%22%2C%22network%22%3A%22cmnet%22%2C%22lac%22%3A%2255381%22%2C%22version%22%3A%223.1.0%22%2C%22os_version%22%3A%224.3%22%2C%22deviceid%22%3A%22865019027480965%22%2C%22havebt%22%3Atrue%2C%22phonetype%22%3A1%2C%22havegps%22%3A%22false%22%2C%22modulename%22%3A%22A788t%22%2C%22time%22%3A%222015-11-16+12%3A03%3A14%22%2C%22useridentifier%22%3A%22d41d8cd98f00b204e9800998ecf8427e%22%2C%22wifimac%22%3A%2270%3A72%3A0d%3A2a%3A6a%3A40%22%2C%22devicename%22%3A%22LENOVO+Lenovo+A788t%22%2C%22mccmnc%22%3A%2246000%22%2C%22imsi%22%3A%22460026679432736%22%2C%22language%22%3A%22zh%22%2C%22havegravity%22%3Atrue%7D%5D%7D";

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
        return !body.startsWith("content=");
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
