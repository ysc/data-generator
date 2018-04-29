package org.apdplat.data.generator;

import org.apdplat.data.generator.generator.Generator;
import org.apdplat.data.generator.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ysc on 19/04/2018.
 */
public class Start {
    private static final Logger LOGGER = LoggerFactory.getLogger(Index.class);

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        new Generator().run();
        new Index().run();
        LOGGER.info("生成数据及索引总耗时: {}", TimeUtils.getTimeDes(System.currentTimeMillis()-start));
    }
}
