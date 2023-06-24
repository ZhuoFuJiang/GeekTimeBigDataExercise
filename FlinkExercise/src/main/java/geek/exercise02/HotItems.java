package geek.exercise02;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class HotItems {
    public static void main(String[] args) throws Exception {
        // 定义环境
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setParallelism(1);
        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 定义数据源
        DataStreamSource<String> dataStreamSource = sEnv.readTextFile(
                "D:\\BaiduNetdiskDownload\\极客时间-大数据项目训练营\\11、Flink\\2、数据资料\\UserBehavior.csv");

        // 数据转换成用户行为类
        dataStreamSource.map(line -> {
            String[] props = line.split(",");
            UserBehavior userBehavior = new UserBehavior();
            userBehavior.setUserId(Long.parseLong(props[0].trim()));
            userBehavior.setItemId(Long.parseLong(props[1].trim()));
            userBehavior.setCategoryId(Integer.parseInt(props[2].trim()));
            userBehavior.setBehavior(props[3].trim());
            userBehavior.setTimeStamp(Long.parseLong(props[4].trim()));
            return userBehavior;
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forMonotonousTimestamps().withTimestampAssigner(
                new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior userBehavior, long l) {
                        return userBehavior.getTimeStamp() * 1000;
                    }
                }
        )).filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior userBehavior) throws Exception {
                return userBehavior.getBehavior().equals("pv");
            }
        }).keyBy(new KeySelector<UserBehavior, Long>() {
            @Override
            public Long getKey(UserBehavior userBehavior) throws Exception {
                return userBehavior.getItemId();
            }
        }).window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new CountAgg(), new WindowResult())
                .keyBy(ItemViewCount::getWindowEnd)
                .process(new TopNHotItems(3))
                .print();

        sEnv.execute();
    }
}

class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(UserBehavior userBehavior, Long aLong) {
        return aLong + 1;
    }

    @Override
    public Long getResult(Long aLong) {
        return aLong;
    }

    @Override
    public Long merge(Long aLong, Long acc1) {
        return aLong + acc1;
    }
}


class WindowResult implements WindowFunction<Long, ItemViewCount, Long, TimeWindow> {

    @Override
    public void apply(Long key, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> collector) throws Exception {
        // 整理结果
        ItemViewCount itemViewCount = new ItemViewCount();
        long windowEnd = window.getEnd();
        Long count = input.iterator().next();
        itemViewCount.setItemId(key);
        itemViewCount.setWindowEnd(windowEnd);
        itemViewCount.setCount(count);
        collector.collect(itemViewCount);
    }
}


class TopNHotItems extends KeyedProcessFunction<Long, ItemViewCount, String> {
    private int n;
    private ListState<ItemViewCount> itemState;

    public TopNHotItems(int n) {
        this.n = n;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 定义状态变量
        itemState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item", ItemViewCount.class));
    }

    @Override
    public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) throws Exception {
        itemState.add(itemViewCount);
        context.timerService().registerEventTimeTimer(itemViewCount.getWindowEnd() + 100);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        ArrayList<ItemViewCount> allItems = new ArrayList<>();
        for(ItemViewCount itemViewCount: itemState.get()) {
            allItems.add(itemViewCount);
        }

        itemState.clear();

        allItems.sort(((o1, o2) -> {
            if(o1.getCount() > o2.getCount()) {
                return -1;
            } else if(o1.getCount() < o2.getCount()){
                return 1;
            } else {
                return 0;
            }
        }));

        List<ItemViewCount> sortedItems = allItems.subList(0, Math.min(allItems.size(), n));

        StringBuilder result = new StringBuilder();
        result.append("=======================================================\n");
        Date date = new Date(timestamp - 100);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        result.append("时间:").append(sdf.format(date)).append("\n");

        for(int i = 0; i < sortedItems.size(); i++) {
            ItemViewCount currentItem = sortedItems.get(i);
            result.append("No").append(i + 1).append(":")
                    .append("商品ID=").append(currentItem.getItemId()).append(" ")
                    .append("浏览量=").append(currentItem.getCount()).append(" ")
                    .append("\n");
        }

        result.append("=======================================================");

        Thread.sleep(1000);
        out.collect(result.toString());
    }
}

class UserBehavior {
    private long userId;
    private long itemId;
    private int categoryId;
    private String behavior;
    private long timeStamp;

    public UserBehavior() {}

    public long getUserId() {
        return userId;
    }

    public long getItemId() {
        return itemId;
    }

    public int getCategoryId() {
        return categoryId;
    }

    public String getBehavior() {
        return behavior;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    public void setCategoryId(int categoryId) {
        this.categoryId = categoryId;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }
}

class ItemViewCount {
    private long itemId;
    private long windowEnd;
    private long count;

    public long getItemId() {
        return itemId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "ItemViewCount{" +
                "itemId=" + itemId +
                ", windowEnd=" + windowEnd +
                ", count=" + count +
                '}';
    }
}


