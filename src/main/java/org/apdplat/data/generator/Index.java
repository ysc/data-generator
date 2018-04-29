package org.apdplat.data.generator;

import org.apdplat.data.generator.mysql2es.*;
import org.apdplat.data.generator.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Index {
    private static final Logger LOGGER = LoggerFactory.getLogger(Index.class);

    public void run(){
        long start = System.currentTimeMillis();
        new Brand().run();
        new Category().run();
        new Area().run();
        new Contract().run();
        new ContractDetail().run();
        new Customer().run();
        new Item().run();
        new SalesStaff().run();
        LOGGER.info("数据索引耗时: {}", TimeUtils.getTimeDes(System.currentTimeMillis()-start));
    }

    public static void main(String[] args) {
        new Index().run();
    }
}
