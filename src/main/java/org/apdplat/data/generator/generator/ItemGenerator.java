package org.apdplat.data.generator.generator;

import org.apdplat.data.generator.mysql.MySQLUtils;
import org.apdplat.data.generator.utils.MultiResourcesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by ysc on 18/04/2018.
 */
public class ItemGenerator {
    private static final Logger LOGGER = LoggerFactory.getLogger(ItemGenerator.class);

    private static final List<String> COLORS = MultiResourcesUtils.load("color.txt");

    public static void clear(){
        MySQLUtils.clean("item");
    }

    public static Map<Integer, Float> generate(int itemCount, int batchSize, int priceLimit, int categoryCount, int brandCount){
        Map<Integer, Float> map = new HashMap<>();
        Connection con = MySQLUtils.getConnection();
        if(con == null){
            return map;
        }
        PreparedStatement pst = null;
        ResultSet rs = null;
        String sql = "insert into item (id, name, price, category_id, brand_id, discount, color) values(?, ?, ?, ?, ?, ?, ?);";
        try {
            con.setAutoCommit(false);
            pst = con.prepareStatement(sql);
            for(int i=0; i<itemCount; i++){
                int category_id = new Random(System.nanoTime()).nextInt(categoryCount)+1;
                int brand_id = new Random(System.nanoTime()).nextInt(brandCount)+1;
                double discount = (new Random(System.nanoTime()).nextInt(5)+5)/10.0;
                int color_index = new Random(System.nanoTime()).nextInt(COLORS.size());
                String color = COLORS.get(color_index);
                float price = Float.parseFloat(new Random(System.nanoTime()).nextInt(priceLimit)+1+"."+new Random(System.nanoTime()).nextInt(10)+new Random(System.nanoTime()).nextInt(10));
                pst.setInt(1, i+1);
                pst.setString(2, "商品"+(i+1));
                pst.setFloat(3, price);
                pst.setInt(4, category_id);
                pst.setInt(5, brand_id);
                pst.setFloat(6, (float)discount);
                pst.setString(7, color);
                pst.addBatch();
                map.put(i+1, price);
                if((i+1) % batchSize == 0) {
                    pst.executeBatch();
                }
            }
            pst.executeBatch();
            con.commit();
            LOGGER.info("保存到数据库成功");
        } catch (Exception e) {
            LOGGER.error("保存到数据库失败", e);
        } finally {
            MySQLUtils.close(con, pst, rs);
        }
        return map;
    }

    public static void main(String[] args) {
        clear();
        generate(10, 1000, 10000, 1000, 500);
    }
}
