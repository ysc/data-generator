package org.apdplat.data.generator.generator;

import org.apdplat.data.generator.utils.Config;
import org.apdplat.data.generator.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by ysc on 18/04/2018.
 */
public class Generator {

    private static final Logger LOGGER = LoggerFactory.getLogger(Generator.class);

    public static void run() {
        long start = System.currentTimeMillis();
        ////清除数据
        ContractDetailGenerator.clear();
        ContractGenerator.clear();
        CustomerGenerator.clear();
        SalesStaffGenerator.clear();
        AreaGenerator.clear();
        DayDimensionGenerator.clear();
        ItemGenerator.clear();
        CategoryGenerator.clear();
        BrandGenerator.clear();

        int batchSize = Config.getIntValue("batchSize") == -1 ? 1000 : Config.getIntValue("batchSize");
        ////起始年月日
        int startYear = Config.getIntValue("startYear") == -1 ? 2000 : Config.getIntValue("startYear");
        int startMonth = Config.getIntValue("startMonth") == -1 ? 1 : Config.getIntValue("startMonth");
        int startDay = Config.getIntValue("startDay") == -1 ? 1 : Config.getIntValue("startDay");
        int endYear = Config.getIntValue("endYear") == -1 ? 2018 : Config.getIntValue("endYear");
        int endMonth = Config.getIntValue("endMonth") == -1 ? 4 : Config.getIntValue("endMonth");
        int endDay = Config.getIntValue("endDay") == -1 ? 18 : Config.getIntValue("endDay");

        //区域数
        int areaCount = AreaGenerator.generate();
        List<String> dayStrs = DayDimensionGenerator.generate(startYear, startMonth, startDay, endYear, endMonth, endDay, batchSize);
        //客户数
        int customerCount = Config.getIntValue("customerCount") == -1 ? 5000 : Config.getIntValue("customerCount");
        //销售数
        int salesStaffCount = Config.getIntValue("salesStaffCount") == -1 ? 2000 : Config.getIntValue("salesStaffCount");
        List<String> customers = CustomerGenerator.generate(areaCount, customerCount, batchSize);
        SalesStaffGenerator.generate(areaCount, salesStaffCount, batchSize, customers);
        PeopleNames.clear();
        customers.clear();
        //合同数
        int contractCount = Config.getIntValue("contractCount") == -1 ? 20000 : Config.getIntValue("contractCount");
        //商品数
        int itemCount = Config.getIntValue("itemCount") == -1 ? 10000 : Config.getIntValue("itemCount");
        //商品价格上限
        int priceLimit = Config.getIntValue("priceLimit") == -1 ? 1000 : Config.getIntValue("priceLimit");
        //商品类别数
        int categoryCount = CategoryGenerator.generate(batchSize);
        //商品品牌数
        int brandCount = BrandGenerator.generate(batchSize);
        Map<Integer, Float> items = ItemGenerator.generate(itemCount, batchSize, priceLimit, categoryCount, brandCount);
        ContractGenerator.generate(contractCount, dayStrs, customerCount, salesStaffCount, batchSize);
        //合同最大明细数
        int contractDetailLimit = Config.getIntValue("contractDetailLimit") == -1 ? 100 : Config.getIntValue("contractDetailLimit");
        //合同明细商品最大数量
        int itemQuantityLimit = Config.getIntValue("itemQuantityLimit") == -1 ? 100 : Config.getIntValue("itemQuantityLimit");
        //合同明细
        ContractDetailGenerator.generate(contractCount, contractDetailLimit, itemQuantityLimit, items, dayStrs, batchSize);
        dayStrs.clear();
        items.clear();
        LOGGER.info("数据生成耗时: {}", TimeUtils.getTimeDes(System.currentTimeMillis()-start));
    }

    public static void main(String[] args) {
        run();
    }
}
