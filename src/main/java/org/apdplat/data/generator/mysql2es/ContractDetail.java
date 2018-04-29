package org.apdplat.data.generator.mysql2es;

import org.apdplat.data.generator.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ysc on 18/04/2018.
 */
public class ContractDetail extends Output{
    private static final Logger LOGGER = LoggerFactory.getLogger(ContractDetail.class);

    private static final String INDEX = "detail";
    private static final String TYPE = "detail";

    private void generateCommand() {
        String sql = "select contract_detail.id, contract.contract_price, contract.state, contract.sign_day, sales_staff.`name` as sales_staff_name, sales_staff.gender as sales_staff_gender, customer.`name` as customer_name, customer.gender as customer_gender, area.city as customer_city, area.city_full as customer_city_full, area.province as customer_province, area.province_full as customer_province_full, day_dimension.dayofweek, day_dimension.weekofyear, day_dimension.`month`, day_dimension.dayofmonth, day_dimension.`quarter`, day_dimension.`year`, day_dimension.dayofyear, contract_detail.detail_price as contract_detail_price, contract_detail.item_quantity, item.`name` as item_name, item.price as item_price, area.longitude, area.latitude, item.discount, item.color, brand.name as brand_name, category.name as category_name from contract_detail, item, contract, customer, sales_staff, area, day_dimension, brand, category where contract_detail.contract_id=contract.id and contract_detail.item_id=item.id and contract.sign_day=day_dimension.day_str and contract.customer_id=customer.id and contract.sales_staff_id=sales_staff.id and customer.area_id=area.id and item.category_id=category.id and item.brand_id=brand.id";
        generateCommand("contract_detail", sql, INDEX, TYPE, INDEX+".sh");
    }

    @Override
    protected Map<String, Object> getRow(ResultSet rs){
        Map<String, Object> data = new HashMap<>();
        try {
            int id = rs.getInt("id");
            float contract_price = rs.getFloat("contract_price");
            String state = rs.getString("state");
            String sign_day = rs.getString("sign_day").replace(" ", "T");
            String sales_staff_name = rs.getString("sales_staff_name");
            String sales_staff_gender = rs.getString("sales_staff_gender");
            String customer_name = rs.getString("customer_name");
            String customer_gender = rs.getString("customer_gender");
            String customer_city = rs.getString("customer_city");
            String customer_city_full = rs.getString("customer_city_full");
            String customer_province = rs.getString("customer_province");
            String customer_province_full = rs.getString("customer_province_full");
            int dayofweek = rs.getInt("dayofweek");
            int weekofyear = rs.getInt("weekofyear");
            int month = rs.getInt("month");
            int dayofmonth = rs.getInt("dayofmonth");
            int quarter = rs.getInt("quarter");
            int year = rs.getInt("year");
            int dayofyear = rs.getInt("dayofyear");
            float contract_detail_price = rs.getFloat("contract_detail_price");
            float item_quantity = rs.getFloat("item_quantity");
            String item_name = rs.getString("item_name");
            float item_price = rs.getFloat("item_price");
            float longitude = rs.getFloat("longitude");
            float latitude = rs.getFloat("latitude");
            float discount = rs.getFloat("discount");
            String color = rs.getString("color");
            String brand_name = rs.getString("brand_name");
            String category_name = rs.getString("category_name");

            data.put("id", id);
            data.put("contract_price", contract_price);
            data.put("state", state);
            data.put("sign_day", sign_day);
            data.put("sales_staff_name", sales_staff_name);
            data.put("sales_staff_gender", sales_staff_gender);
            data.put("customer_name", customer_name);
            data.put("customer_gender", customer_gender);
            data.put("customer_city", customer_city);
            data.put("customer_city_full", customer_city_full);
            data.put("customer_province", customer_province);
            data.put("customer_province_full", customer_province_full);
            data.put("dayofweek", dayofweek);
            data.put("weekofyear", weekofyear);
            data.put("month", month);
            data.put("dayofmonth", dayofmonth);
            data.put("quarter", quarter);
            data.put("year", year);
            data.put("dayofyear", dayofyear);
            data.put("contract_detail_price", contract_detail_price);
            data.put("item_quantity", item_quantity);
            data.put("item_name", item_name);
            data.put("item_price", item_price);
            data.put("discount", discount);
            data.put("color", color);
            data.put("brand_name", brand_name);
            data.put("category_name", category_name);
            Map<String, Float> location = new HashMap<>();
            location.put("lon", longitude);
            location.put("lat", latitude);
            data.put("geo_location", location);
        }catch (Exception e){
            LOGGER.error("获取数据异常", e);
        }
        return data;
    }

    @Override
    public void run(){
        long start = System.currentTimeMillis();
        generateCommand();
        LOGGER.info("生成合同详情JSON文档耗时: {}", TimeUtils.getTimeDes(System.currentTimeMillis()-start));
    }

    public static void main(String[] args) throws Exception{
        new ContractDetail().run();
    }
}
