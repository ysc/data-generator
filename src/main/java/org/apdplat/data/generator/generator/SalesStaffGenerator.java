package org.apdplat.data.generator.generator;

import org.apdplat.data.generator.mysql.MySQLUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.List;
import java.util.Random;

/**
 * Created by ysc on 18/04/2018.
 */
public class SalesStaffGenerator {
    private static final Logger LOGGER = LoggerFactory.getLogger(SalesStaffGenerator.class);

    public static void clear(){
        MySQLUtils.clean("sales_staff");
    }

    public static List<String> generate(int areaCount, int customerCount, int batchSize){
        return generate(areaCount, customerCount, batchSize, null);
    }
    public static List<String> generate(int areaCount, int salesStaffCount, int batchSize, Collection<String> exclude){
        Connection con = MySQLUtils.getConnection();
        if(con == null){
            return null;
        }
        List<String> names = PeopleNames.getNames(salesStaffCount);
        PreparedStatement pst = null;
        ResultSet rs = null;
        String sql = "insert into sales_staff (id, name, gender, area_id) values(?, ?, ?, ?);";
        try {
            con.setAutoCommit(false);
            pst = con.prepareStatement(sql);
            for(int i=0; i<names.size(); i++){
                int r = new Random(System.nanoTime()).nextInt(names.size());
                int area_id = new Random(System.nanoTime()).nextInt(areaCount)+1;
                String gender = r > names.size()/2 ? "男" : "女";
                pst.setInt(1, i+1);
                pst.setString(2, names.get(i));
                pst.setString(3, gender);
                pst.setInt(4, area_id);
                pst.addBatch();

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
        return names;
    }

    public static void main(String[] args) {
        clear();
        generate(30, 3, 1000);
    }
}
