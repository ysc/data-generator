package org.apdplat.data.generator.generator;

import org.apdplat.data.generator.mysql.MySQLUtils;
import org.apdplat.data.generator.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ysc on 18/04/2018.
 */
public class DayDimensionGenerator {
    private static final Logger LOGGER = LoggerFactory.getLogger(DayDimensionGenerator.class);

    public static List<String> generate(int startYear, int startMonth, int startDay, int batchSize){
        List<String> dayStrs = new ArrayList<>();
        Connection con = MySQLUtils.getConnection();
        if(con == null){
            return dayStrs;
        }
        PreparedStatement pst = null;
        ResultSet rs = null;
        try {
            con.setAutoCommit(false);
            String sql = "insert into day_dimension (day_str, dayofyear, dayofweek, weekofyear, month, dayofmonth, quarter, year) values(?, ?, ?, ?, ?, ?, ?, ?);";
            pst = con.prepareStatement(sql);
            List<Map<String, Object>> dayData = getDayData(startYear, startMonth, startDay);
            LOGGER.info("天维度数据条数: {}", dayData.size());
            for(int i=0; i<dayData.size(); i++){
                Map<String, Object> map = dayData.get(i);
                dayStrs.add(map.get("day_str").toString());
                pst.setString(1, map.get("day_str").toString());
                pst.setInt(2, Integer.parseInt(map.get("dayofyear").toString()));
                pst.setInt(3, Integer.parseInt(map.get("dayofweek").toString()));
                pst.setInt(4, Integer.parseInt(map.get("weekofyear").toString()));
                pst.setInt(5, Integer.parseInt(map.get("month").toString()));
                pst.setInt(6, Integer.parseInt(map.get("dayofmonth").toString()));
                pst.setInt(7, Integer.parseInt(map.get("quarter").toString()));
                pst.setInt(8, Integer.parseInt(map.get("year").toString()));
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
        return dayStrs;
    }

    private static List<Map<String, Object>> getDayData(int startYear, int startMonth, int startDay){
        List<Map<String, Object>> data = new ArrayList<>();
        LocalDateTime start = LocalDateTime.of(startYear, startMonth, startDay, 0, 0, 0, 0);
        LocalDateTime end = LocalDateTime.now();
        while(start.isBefore(end)) {
            String date = TimeUtils.toString(start, "yyyy-MM-dd");
            int dayofweek = start.getDayOfWeek().getValue();
            int dayofyear = start.getDayOfYear();
            int weekofyear = ((dayofyear-1) / 7)+1;
            int month = start.getMonth().getValue();
            int dayofmonth = start.getDayOfMonth();
            int quarter = ((month-1) / 3) + 1;
            int year = start.getYear();
            Map<String, Object> map = new HashMap<>();
            map.put("day_str", date+" 00:00:00");
            map.put("dayofweek", dayofweek);
            map.put("dayofyear", dayofyear);
            map.put("weekofyear", weekofyear);
            map.put("month", month);
            map.put("dayofmonth", dayofmonth);
            map.put("quarter", quarter);
            map.put("year", year);
            data.add(map);
            start = start.plusDays(1);
        }
        return data;
    }

    public static void clear(){
        MySQLUtils.clean("day_dimension");
    }

    public static void main(String[] args) {
        MySQLUtils.clean("day_dimension");
        generate(2000, 1, 1, 1000);
    }
}
