package geek.exercise02;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class NetWorkFlow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setParallelism(1);
        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> dataStreamSource = sEnv.readTextFile(
                "D:\\BaiduNetdiskDownload\\极客时间-大数据项目训练营\\11、Flink\\2、数据资料\\apache.log");

        dataStreamSource.map(new MapFunction<String, ApacheLogEvent>() {
            @Override
            public ApacheLogEvent map(String s) throws Exception {
                String[] props = s.split(" ");
                ApacheLogEvent apacheLogEvent = new ApacheLogEvent();
                // 时间格式转换
                SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                Long timeStamp = sdf.parse(props[3].trim()).getTime();
                apacheLogEvent.setIp(props[0].trim());
                apacheLogEvent.setUserId(props[1].trim());
                apacheLogEvent.setEventTime(timeStamp);
                apacheLogEvent.setMethod(props[5].trim());
                apacheLogEvent.setUrl(props[6].trim());
                return apacheLogEvent;
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.seconds(60)) {
            @Override
            public long extractTimestamp(ApacheLogEvent apacheLogEvent) {
                return apacheLogEvent.getEventTime();
            }
        }).keyBy(ApacheLogEvent::getUrl)
                .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.seconds(5)))
                .aggregate(new LogCountAgg(), new LogWindowResult())
                .keyBy(UrlViewCount::getWindowEnd)
                .process(new LogTopNHotUrls(5))
                .print();

        sEnv.execute();
    }
}


class LogCountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(ApacheLogEvent apacheLogEvent, Long aLong) {
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


class LogWindowResult implements WindowFunction<Long, UrlViewCount, String, TimeWindow> {

    @Override
    public void apply(String key, TimeWindow window, Iterable<Long> input, Collector<UrlViewCount> collector) throws Exception {
        // 整理结果
        UrlViewCount urlViewCount = new UrlViewCount();
        long windowEnd = window.getEnd();
        Long count = input.iterator().next();
        urlViewCount.setUrl(key);
        urlViewCount.setWindowEnd(windowEnd);
        urlViewCount.setCount(count);
        collector.collect(urlViewCount);
    }
}


class LogTopNHotUrls extends KeyedProcessFunction<Long, UrlViewCount, String> {
    private int n;
    private ListState<UrlViewCount> itemState;

    public LogTopNHotUrls(int n) {
        this.n = n;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 定义状态变量
        itemState = getRuntimeContext().getListState(new ListStateDescriptor<UrlViewCount>("item", UrlViewCount.class));
    }

    @Override
    public void processElement(UrlViewCount urlViewCount, Context context, Collector<String> collector) throws Exception {
        itemState.add(urlViewCount);
        context.timerService().registerEventTimeTimer(urlViewCount.getWindowEnd() + 1);
    }


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        ArrayList<UrlViewCount> allItems = new ArrayList<>();
        for(UrlViewCount urlViewCount: itemState.get()) {
            allItems.add(urlViewCount);
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

        List<UrlViewCount> sortedItems = allItems.subList(0, Math.min(allItems.size(), n));

        StringBuilder result = new StringBuilder();
        result.append("=======================================================\n");
        Date date = new Date(timestamp - 1);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        result.append("时间:").append(sdf.format(date)).append("\n");

        for(int i = 0; i < sortedItems.size(); i++) {
            UrlViewCount currentUrl = sortedItems.get(i);
            result.append("No").append(i + 1).append(":")
                    .append("URL=").append(currentUrl.getUrl()).append(" ")
                    .append("流量=").append(currentUrl.getCount()).append(" ")
                    .append("\n");
        }

        result.append("=======================================================");

        Thread.sleep(1000);
        out.collect(result.toString());
    }
}


class ApacheLogEvent {
    private String ip;
    private String userId;
    private Long eventTime;
    private String method;
    private String url;

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public String toString() {
        return "ApacheLogEvent{" +
                "ip='" + ip + '\'' +
                ", userId='" + userId + '\'' +
                ", eventTime=" + eventTime +
                ", method='" + method + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}

class UrlViewCount {
    private String url;
    private Long windowEnd;
    private Long count;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "UrlViewCount{" +
                "url='" + url + '\'' +
                ", windowEnd=" + windowEnd +
                ", count=" + count +
                '}';
    }
}


