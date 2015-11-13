package com.source.tailDir.util;

import com.source.tailDir.json.JSONArray;
import com.source.tailDir.json.JSONException;
import com.source.tailDir.json.JSONObject;
import org.apache.commons.lang.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.security.Key;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;


/***
 * DES ECB对称加密 解密
 *
 * @author ibm
 */
public class DESUtil {

    private static final String ENCRYPT_KEY = "M!@L&#$)";

    /**
     * 自定义一个key
     *
     * @param keyRule
     */
    public static byte[] getKey(String keyRule) {
        Key key = null;
        byte[] keyByte = keyRule.getBytes();
        // 创建一个空的八位数组,默认情况下为0
        byte[] byteTemp = new byte[8];
        // 将用户指定的规则转换成八位数组
        for (int i = 0; i < byteTemp.length && i < keyByte.length; i++) {
            byteTemp[i] = keyByte[i];
        }
        key = new SecretKeySpec(byteTemp, "DES");
        return key.getEncoded();
    }

    /**
     * 加密数据
     *
     * @param encryptString 注意：这里的数据长度只能为8的倍数
     * @param encryptKey
     * @return
     * @throws Exception
     */
    public static String encryptDES(String encryptString, String encryptKey) throws Exception {
        String encryptKey1 = StringUtils.isEmpty(encryptKey) ? ENCRYPT_KEY : encryptKey;
        SecretKeySpec key = new SecretKeySpec(getKey(encryptKey1), "DES");
        Cipher cipher = Cipher.getInstance("DES/ECB/PKCS5Padding");
        cipher.init(Cipher.ENCRYPT_MODE, key);
        byte[] encryptedData = cipher.doFinal(encryptString.getBytes());
        return Base64.encode(encryptedData);
    }

    /***
     * 解密数据
     *
     * @param decryptString
     * @param decryptKey
     * @return
     * @throws Exception
     */
    public static String decryptDES(String decryptString, String decryptKey) throws Exception {
        String decryptKey1 = StringUtils.isEmpty(decryptKey) ? ENCRYPT_KEY : decryptKey;
        SecretKeySpec key = new SecretKeySpec(getKey(decryptKey1), "DES");
        Cipher cipher = Cipher.getInstance("DES/ECB/PKCS5Padding");
        cipher.init(Cipher.DECRYPT_MODE, key);
        byte decryptedData[] = cipher.doFinal(Base64.decode(decryptString));
        return new String(decryptedData);
    }

    /**
     * Decode
     *
     * @param content
     * @param enc
     * @return
     * @throws UnsupportedEncodingException
     */
    public static String decode(String content, String enc) throws UnsupportedEncodingException {
        return URLDecoder.decode(content, null == enc ? "UTF-8" : enc);
    }

    /**
     * Encode
     *
     * @param content
     * @param enc
     * @return
     * @throws UnsupportedEncodingException
     */
    public static String encode(String content, String enc) throws UnsupportedEncodingException {
        return URLEncoder.encode(content, null == enc ? "UTF-8" : enc);
    }

    public static void main(String[] args) throws Exception {
        String post = "6nf6UW9j3/jKcCssUFR1Sb6mMp4l2XIWStsyqHzKuDLxU+glOgMl+GtthBze7qfqTybK5ng5ULF3PiUjyK/+6mtthBze7qfq7bk0cAM4wDIcoGvRoS10ftDycbJnjgPVXYR/ycUEUY9SxyHWwu3WhGtthBze7qfqpq1TDZDwWE+MhpUw/vbhOLDNmgEwp4Pzw2cOmz8MTk7pg+Gs65VFdHgiNN5X+Qt3pq1TDZDwWE+4milLM65fT1TPbA1/AN7t0PJxsmeOA9VSMRkSHVoyxXc+JSPIr/7qa22EHN7up+r01PS+FPFiN6ji0/TSPmJ60Mt33yrtW09GK/UlQ8wOUnTkA6dEYQ9weCI03lf5C3emrVMNkPBYTy1uez2QZkltPFMTtKg1I7zQy3ffKu1bTx8W9DMlSAPpRmBUOPtw+v+ciPH6IuqYEXc+JSPIr/7qa22EHN7up+pq0eXRfKIYqN77kAN2J2U8OFq+BMB4fR46dYbixV7ERc8haOiWugYC8Uc+v00xFEOmrVMNkPBYT3c+JSPIr/7q8Wavzcw8hcYmk7QvUMxEhtDycbJnjgPVPEBxqhpx669/TdcR8vxUeHgiNN5X+Qt3pq1TDZDwWE+JUa8DchrvMtnJRLP/atyKven3OV+zbXQh6PyBD6At7vFHPr9NMRRDpq1TDZDwWE93PiUjyK/+6gccheRoy24ZtC/SW+ajjULQ8nGyZ44D1V2Ef8nFBFGP3R8BXe7wgHrxU+glOgMl+GtthBze7qfqul/Kdp3t2/IsXSAfOOC2/vv3Es2TA1bkRyy8cTracTC13+wjwcMmJnd77qzCyHjYizF5NLw6ytWhQg16IifNza2op4+NkRxXdz4lI8iv/uprbYQc3u6n6uWAPMU7GGAPaWJqLIeWfbSchRNDt8JCmqpYHuvnhUAIUsch1sLt1oRrbYQc3u6n6qatUw2Q8FhP+TTQ2ypUD5PRgnOUuyMuKNGVnKmpigZTh+ciSlEertn+uwTFVhrryaHZNdD3qsr+4CMFamCbAZSciPH6IuqYEXc+JSPIr/7qa22EHN7up+qNK37koG+iTmkksKTUUJhR+/cSzZMDVuTyFQjC8baBRcrd3i3ZrOERpq1TDZDwWE93PiUjyK/+6vaxDaQ9NV3dEhGNPcJEB2bgITdAB33wqECjL9fCoz8nD/lG7ruoVZ93PiUjyK/+6gNruDfYWqDA0bZGXii/HEqcw9a/FyFOMlPyqSrVHTvxggW4nHGA7ZGiYwn+O3yDRcTWWOIBJV9cJrFIubl2nhiciPH6IuqYEXc+JSPIr/7qa22EHN7up+p48131BkQPWt3Q4ZFN+dQT0PJxsmeOA9VlGEPLuteZbPFHPr9NMRRDpq1TDZDwWE93PiUjyK/+6mxMkXaKHwZht4ntKsfuuuvgITdAB33wqNcKNnaWEIM5D/lG7ruoVZ9HDPFp/Bbq0w1mjlqoYNBmvoxXntJU2DJPxMogPiuc4w==";
        System.out.println(URLDecoder.decode(decryptDES(post, null), "UTF-8"));
        String code = "content=%7B%0A%20%20%22data%22%20:%20%5B%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22modulename%22%20:%20%22iPhone%22,%0A%20%20%20%20%20%20%22version%22%20:%20%221.0%22,%0A%20%20%20%20%20%20%22mccmnc%22%20:%20%22%22,%0A%20%20%20%20%20%20%22devicename%22%20:%20%22x86_64%22,%0A%20%20%20%20%20%20%22longitude%22%20:%20%22118.390900%22,%0A%20%20%20%20%20%20%22time%22%20:%20%222015-11-12%2019:23:08%22,%0A%20%20%20%20%20%20%22latitude%22%20:%20%2232.098889%22,%0A%20%20%20%20%20%20%22isjailbroken%22%20:%20%220%22,%0A%20%20%20%20%20%20%22platform%22%20:%20%22iPhone%20OS%22,%0A%20%20%20%20%20%20%22deviceid%22%20:%20%225EAA2D8A-5B6C-487F-B34D-709A33F7C590%22,%0A%20%20%20%20%20%20%22language%22%20:%20%22en-US%22,%0A%20%20%20%20%20%20%22appkey%22%20:%20%22e8d8f8f0844d11e5977bbc305bdddde2%22,%0A%20%20%20%20%20%20%22useridentifier%22%20:%20%22%22,%0A%20%20%20%20%20%20%22os_version%22%20:%20%229.1%22,%0A%20%20%20%20%20%20%22session_id%22%20:%20%22519D95211DDDA20B611CA2BC1F9E1AA4%22,%0A%20%20%20%20%20%20%22resolution%22%20:%20%22640x960%22,%0A%20%20%20%20%20%20%22network%22%20:%20%22WIFI%22%0A%20%20%20%20%7D%0A%20%20%5D%0A%7D";
        String body = decode(code, null);
        JSONObject jsonObject = null;
        JSONObject jsonObject2 = null;
        try {
            jsonObject = new JSONObject(body);
            jsonObject2 = new JSONObject(post);
        } catch (JSONException e) {
            System.out.println(",.,,,.,.,.." + jsonObject2);
        }
        if (null != jsonObject) {
            System.out.println(jsonObject.get("content"));
            ;
            System.out.println(jsonObject2.get("content"));
        }

        String test = "{\"data\":[{\"session_id\":\"863d92fa262ac9a01fc33aec20c78810\",\"time\":\"2015-11-12 19:07:24\",\"useridentifier\":\"d41d8cd98f00b204e9800998ecf8427e\",\"appkey\":\"ff8bad604c6511e5b62d00163e0240bd\",\"label\":\"\",\"attachment\":\"\",\"acc\":\"1\",\"activity\":\"CobubSampleActivity\",\"event_identifier\":\"quit\",\"deviceid\":\"860275021656704\",\"version\":\"1.0\"}]}";
        System.out.println(URLEncoder.encode(test, "UTF-8"));
    }
}

