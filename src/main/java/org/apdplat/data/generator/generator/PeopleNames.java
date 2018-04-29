package org.apdplat.data.generator.generator;

import org.apdplat.data.generator.utils.MultiResourcesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by ysc on 18/04/2018.
 */
public class PeopleNames {
    private static final Logger LOGGER = LoggerFactory.getLogger(PeopleNames.class);
    private static List<String> NAMES = new ArrayList<>();

    static {
        MultiResourcesUtils.load("names.txt").stream().filter(name->name.length()==3).distinct().forEach(NAMES::add);
        LOGGER.info("人名加载完毕, 人名个数: {}", NAMES.size());
    }

    public static void clear(){
        NAMES.clear();
    }

    public static List<String> getNames(int count){
        return getNames(count, null);
    }

    public static List<String> getNames(int count, Collection<String> exclude){
        if(count == NAMES.size()){
            return Collections.unmodifiableList(NAMES);
        }
        if(count > NAMES.size()){
            throw new RuntimeException("指定的人数过多, 不能大于: "+NAMES.size());
        }
        //如何在M个人名中随机地选择N个?
        long start = System.currentTimeMillis();
        Set<String> data = new HashSet<>();
        while(data.size()<count){
            long cost = System.currentTimeMillis() - start;
            if(cost > 60000){
                break;
            }
            int index = new Random(System.nanoTime()).nextInt(NAMES.size());
            String name = NAMES.get(index);
            if(exclude == null || !exclude.contains(name)){
                data.add(name);
            }
        }
        List<String> result = data.stream().collect(Collectors.toList());
        data.clear();
        return result;
    }

    public static void main(String[] args) {
        LOGGER.info(getNames(3).toString());
        LOGGER.info(getNames(3).toString());
        LOGGER.info(getNames(3).toString());
    }
}
