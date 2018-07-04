package org.mengyun.compensable.transaction.utils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

public class RedisUtils {

    static final Logger logger = LoggerFactory.getLogger(RedisUtils.class);

    static public boolean isSupportScanCommand(Jedis jedis) {

        if (jedis == null) {
            logger.info("jedis is null,return false");
            return false;
        }

        String serverInfo = jedis.info("Server");

        int versionIndex = serverInfo.indexOf("redis_version");

        String infoWithVersionAhead = serverInfo.substring(versionIndex);

        int versionOverIndex = infoWithVersionAhead.indexOf("\r");

        String serverVersion = infoWithVersionAhead.substring(0, versionOverIndex);

        String leastVersionForScan = "redis_version:2.8";

        if (StringUtils.isNotEmpty(serverVersion)) {

            logger.info("redis server:{}", serverVersion);

            return serverVersion.compareTo(leastVersionForScan) >= 0;
        } else {
            return false;
        }


    }
}
