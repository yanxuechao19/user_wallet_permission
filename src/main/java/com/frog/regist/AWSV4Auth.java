package com.frog.regist;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
/**
 * packageName com.commercial
 *
 * @author yanxuechao
 * @version JDK 8
 * @className AWSV4Auth
 * @date 2024/7/8
 * @description TODO
 */
public class AWSV4Auth {

    private AWSV4Auth() {
    }

    public static class Builder {

        private String accessKeyID;
        private String secretAccessKey;
        private String regionName;
        private String serviceName;
        private String httpMethodName;
        private String canonicalURI;
        private TreeMap<String, String> queryParametes;
        private TreeMap<String, String> awsHeaders;
        private String payload;
        private boolean debug = false;

        public Builder(String accessKeyID, String secretAccessKey) {
            this.accessKeyID = accessKeyID;
            this.secretAccessKey = secretAccessKey;
        }

        public Builder regionName(String regionName) {
            this.regionName = regionName;
            return this;
        }

        public Builder serviceName(String serviceName) {
            this.serviceName = serviceName;
            return this;
        }

        public Builder httpMethodName(String httpMethodName) {
            this.httpMethodName = httpMethodName;
            return this;
        }

        public Builder canonicalURI(String canonicalURI) {
            this.canonicalURI = canonicalURI;
            return this;
        }

        public Builder queryParametes(TreeMap<String, String> queryParametes) {
            this.queryParametes = queryParametes;
            return this;
        }

        public Builder awsHeaders(TreeMap<String, String> awsHeaders) {
            this.awsHeaders = awsHeaders;
            return this;
        }

        public Builder payload(String payload) {
            this.payload = payload;
            return this;
        }

        public Builder debug() {
            this.debug = true;
            return this;
        }

        public AWSV4Auth build() {
            return new AWSV4Auth(this);
        }
    }

    private String accessKeyID;
    private String secretAccessKey;
    private String regionName;
    private String serviceName;
    private String httpMethodName;
    private String canonicalURI;
    private TreeMap<String, String> queryParametes;
    private TreeMap<String, String> awsHeaders;
    private String payload;
    private boolean debug = false;

    /* Other variables */
    private final String HMACAlgorithm = "AWS4-HMAC-SHA256";
    private final String aws4Request = "aws4_request";
    private String strSignedHeader;
    private String xAmzDate;
    private String currentDate;

    private AWSV4Auth(Builder builder) {
        accessKeyID = builder.accessKeyID;
        secretAccessKey = builder.secretAccessKey;
        regionName = builder.regionName;
        serviceName = builder.serviceName;
        httpMethodName = builder.httpMethodName;
        canonicalURI = builder.canonicalURI;
        queryParametes = builder.queryParametes;
        awsHeaders = builder.awsHeaders;
        payload = builder.payload;
        debug = builder.debug;

        /* Get current timestamp value.(UTC) */
        xAmzDate = getTimeStamp();
        currentDate = getDate();
    }

    /**
     * 任务 1：针对签名版本 4 创建规范请求
     *
     * @return
     */
    private String prepareCanonicalRequest() {
        StringBuilder canonicalURL = new StringBuilder("");

        /* Step 1.1 以HTTP方法(GET, PUT, POST, etc.)开头, 然后换行. */
        canonicalURL.append(httpMethodName).append("\n");

        /* Step 1.2 添加URI参数，换行. */
        canonicalURI = canonicalURI == null || canonicalURI.trim().isEmpty() ? "/" : canonicalURI;
        canonicalURL.append(canonicalURI).append("\n");

        /* Step 1.3 添加查询参数，换行. */
        StringBuilder queryString = new StringBuilder("");
        if (queryParametes != null && !queryParametes.isEmpty()) {
            for (Map.Entry<String, String> entrySet : queryParametes.entrySet()) {
                String key = entrySet.getKey();
                String value = entrySet.getValue();
                queryString.append(key).append("=").append(encodeParameter(value)).append("&");
            }

            queryString.deleteCharAt(queryString.lastIndexOf("&"));

            queryString.append("\n");
        } else {
            queryString.append("\n");
        }
        canonicalURL.append(queryString);

        /* Step 1.4 添加headers, 每个header都需要换行. */
        StringBuilder signedHeaders = new StringBuilder("");
        if (awsHeaders != null && !awsHeaders.isEmpty()) {
            for (Map.Entry<String, String> entrySet : awsHeaders.entrySet()) {
                String key = entrySet.getKey();
                String value = entrySet.getValue();
                signedHeaders.append(key).append(";");
                canonicalURL.append(key).append(":").append(value).append("\n");
            }
            canonicalURL.append("\n");
        } else {
            canonicalURL.append("\n");
        }

        /* Step 1.5 添加签名的headers并换行. */
        strSignedHeader = signedHeaders.substring(0, signedHeaders.length() - 1); // 删掉最后的 ";"
        canonicalURL.append(strSignedHeader).append("\n");

        /* Step 1.6 对HTTP或HTTPS的body进行SHA256处理. */
        payload = payload == null ? "" : payload;
        canonicalURL.append(generateHex(payload));

        if (debug) {
            System.out.println("##Canonical Request:\n" + canonicalURL.toString());
        }

        return canonicalURL.toString();
    }

    /**
     * 任务 2：创建签名版本 4 的待签字符串
     *
     * @param canonicalURL
     * @return
     */
    private String prepareStringToSign(String canonicalURL) {
        String stringToSign = "";

        /* Step 2.1 以算法名称开头，并换行. */
        stringToSign = HMACAlgorithm + "\n";

        /* Step 2.2 添加日期，并换行. */
        stringToSign += xAmzDate + "\n";

        /* Step 2.3 添加认证范围，并换行. */
        stringToSign += currentDate + "/" + regionName + "/" + serviceName + "/" + aws4Request + "\n";

        /* Step 2.4 添加任务1返回的规范URL哈希处理结果，然后换行. */
        stringToSign += generateHex(canonicalURL);

        if (debug) {
            System.out.println("##String to sign:\n" + stringToSign);
        }

        return stringToSign;
    }

    /**
     * 任务 3：为 AWS Signature 版本 4 计算签名
     *
     * @param stringToSign
     * @return
     */
    private String calculateSignature(String stringToSign) {
        try {
            /* Step 3.1 生成签名的key */
            byte[] signatureKey = getSignatureKey(secretAccessKey, currentDate, regionName, serviceName);

            /* Step 3.2 计算签名. */
            byte[] signature = HmacSHA256(signatureKey, stringToSign);

            /* Step 3.2.1 对签名编码处理 */
            String strHexSignature = bytesToHex(signature);
            return strHexSignature;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    /**
     *任务 4：将签名信息添加到请求并返回headers
     *
     * @return
     */
    public Map<String, String> getHeaders() {
        awsHeaders.put("x-amz-date", xAmzDate);

        /* 执行任务 1: 创建aws v4签名的规范请求字符串. */
        String canonicalURL = prepareCanonicalRequest();

        /* 执行任务 2: 创建用来认证的字符串 4. */
        String stringToSign = prepareStringToSign(canonicalURL);

        /* 执行任务 3: 计算签名. */
        String signature = calculateSignature(stringToSign);

        if (signature != null) {
            Map<String, String> header = new HashMap<String, String>(0);
            header.put("x-amz-date", xAmzDate);
            header.put("Authorization", buildAuthorizationString(signature));

            if (debug) {
                System.out.println("##Signature:\n" + signature);
                System.out.println("##Header:");
                for (Map.Entry<String, String> entrySet : header.entrySet()) {
                    System.out.println(entrySet.getKey() + " = " + entrySet.getValue());
                }
                System.out.println("================================");
            }
            return header;
        } else {
            if (debug) {
                System.out.println("##Signature:\n" + signature);
            }
            return null;
        }
    }

    /**
     * 连接前几步处理的字符串生成Authorization header值.
     *
     * @param strSignature
     * @return
     */
    private String buildAuthorizationString(String strSignature) {
        return HMACAlgorithm + " "
                + "Credential=" + accessKeyID + "/" + getDate() + "/" + regionName + "/" + serviceName + "/" + aws4Request + ", "
                + "SignedHeaders=" + strSignedHeader + ", "
                + "Signature=" + strSignature;
    }

    /**
     * 将字符串16进制化.
     *
     * @param data
     * @return
     */
    private String generateHex(String data) {
        MessageDigest messageDigest;
        try {
            messageDigest = MessageDigest.getInstance("SHA-256");
            messageDigest.update(data.getBytes("UTF-8"));
            byte[] digest = messageDigest.digest();
            return String.format("%064x", new java.math.BigInteger(1, digest));
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 以给定的key应用HmacSHA256算法处理数据.
     *
     * @param data
     * @param key
     * @return
     * @throws Exception
     * @reference:
     * http://docs.aws.amazon.com/general/latest/gr/signature-v4-examples.html#signature-v4-examples-java
     */
    private byte[] HmacSHA256(byte[] key, String data) throws Exception {
        String algorithm = "HmacSHA256";
        Mac mac = Mac.getInstance(algorithm);
        mac.init(new SecretKeySpec(key, algorithm));
        return mac.doFinal(data.getBytes("UTF8"));
    }

    /**
     * 生成AWS 签名
     *
     * @param key
     * @param date
     * @param regionName
     * @param serviceName
     * @return
     * @throws Exception
     * @reference
     * http://docs.aws.amazon.com/general/latest/gr/signature-v4-examples.html#signature-v4-examples-java
     */
    private byte[] getSignatureKey(String key, String date, String regionName, String serviceName) throws Exception {
        byte[] kSecret = ("AWS4" + key).getBytes("UTF8");
        byte[] kDate = HmacSHA256(kSecret, date);
        byte[] kRegion = HmacSHA256(kDate, regionName);
        byte[] kService = HmacSHA256(kRegion, serviceName);
        byte[] kSigning = HmacSHA256(kService, aws4Request);
        return kSigning;
    }

    final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();

    /**
     * 将字节数组转换为16进制字符串
     *
     * @param bytes
     * @return
     */
    private String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars).toLowerCase();
    }

    /**
     * 获取yyyyMMdd'T'HHmmss'Z'格式的当前时间
     *
     * @return
     */
    private String getTimeStamp() {
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));//server timezone
        return dateFormat.format(new Date());
    }

    /**
     * 获取yyyyMMdd格式的当前日期
     *
     * @return
     */
    private String getDate() {
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));//server timezone
        return dateFormat.format(new Date());
    }

    /**
     * UTF-8编码
     * @param param
     * @return
     */
    private String encodeParameter(String param){
        try {
            return URLEncoder.encode(param, "UTF-8");
        } catch (Exception e) {
            return URLEncoder.encode(param);
        }
    }
}
