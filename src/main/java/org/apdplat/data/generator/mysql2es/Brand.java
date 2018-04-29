package org.apdplat.data.generator.mysql2es;

import org.apdplat.data.generator.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ysc on 29/04/2018.
 */
public class Brand extends Output{
    private static final Logger LOGGER = LoggerFactory.getLogger(Brand.class);

    private static final String INDEX = "brand";
    private static final String TYPE = "brand";

    private void generateCommand() {
        String sql = "select id, name from brand";
        generateCommand("brand", sql, INDEX, TYPE, INDEX+".sh");
    }

    @Override
    protected Map<String, Object> getRow(ResultSet rs){
        Map<String, Object> data = new HashMap<>();
        try {
            int id = rs.getInt("id");
            String name = rs.getString("name");

            data.put("id", id);
            data.put("name", name);
        }catch (Exception e){
            LOGGER.error("获取数据异常", e);
        }
        return data;
    }

    @Override
    public void run(){
        long start = System.currentTimeMillis();
        generateCommand();
        LOGGER.info("生成品牌JSON文档耗时: {}", TimeUtils.getTimeDes(System.currentTimeMillis()-start));
    }

    public static void main(String[] args) throws Exception{
        new Brand().run();
    }
}
