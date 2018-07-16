package org.mengyun.compensable.transaction.utils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Properties;

public class RedisUtils {

    private static final Logger logger = LoggerFactory.getLogger(RedisUtils.class);

    private static final String MIN_VERSION_FOR_SCAN = "2.8";

    static public boolean isSupportScanCommand(Jedis jedis) {

        if (jedis == null) {
            logger.info("jedis is null,return false");
            return false;
        }
        String serverInfo = jedis.info("Server");
//    serverInfo =  "# Server\r\nredis_version:2.6.17\r\nredis_git_sha1:00000000\r\nredis_git_dirty:0\r\nredis_mode:standalone\r\nos:Linux 2.6.18-274.17.1.el5PAE i686\r\narch_bits:32\r\nmultiplexing_api:epoll\r\ngcc_version:4.1.2\r\nprocess_id:2794\r\nrun_id:1be6a14c2cc8fd480f3499b5b8c943878d61fe3a\r\ntcp_port:6379\r\nuptime_in_seconds:4653334\r\nuptime_in_days:53\r\nhz:10\r\nlru_clock:55788"

        Properties properties = new Properties();
        try {
            properties.load(new StringReader(serverInfo));
        } catch (IOException e) {
            logger.info("parse redis version failed, return false");
            return false;
        }

        String redisVersion = properties.getProperty("redis_version");

        String[] redisVersions = redisVersion.split("\\.");
        String[] minVersions = MIN_VERSION_FOR_SCAN.split("\\.");

        for (int i = 0; i < minVersions.length; i++) {
            if (i < redisVersions.length) {
                if (Integer.valueOf(redisVersions[i]) > Integer.valueOf(minVersions[i])) {
                    return true;
                } else if (!redisVersions[i].equals(minVersions[i])) {
                    return false;
                }
            } else {
                break;
            }
        }
        return true;
    }

}

