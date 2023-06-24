package geek.exercise02;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class OrderTimeout {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setParallelism(1);
        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<OrderResult> orderResultDataStream = sEnv.readTextFile("D:\\BaiduNetdiskDownload\\极客时间-大数据项目训练营\\11、Flink\\2、数据资料\\OrderLog.csv")
                .map(line -> {
                    String[] props = line.split(",");
                    OrderEvent orderEvent = new OrderEvent();
                    orderEvent.setOrderId(Long.parseLong(props[0].trim()));
                    orderEvent.setEventType(props[1].trim());
                    orderEvent.setEventTime(Long.parseLong(props[3].trim()));
                    return orderEvent;
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderEvent>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(OrderEvent orderEvent) {
                        return orderEvent.getEventTime() * 1000;
                    }
                }).keyBy(OrderEvent::getOrderId).process(new OrderPayMatchDetect());

        orderResultDataStream.print("payed");
        orderResultDataStream.getSideOutput(new OutputTag<OrderResult>("timeout"){}).print("timeout");

        sEnv.execute();

    }}

//实现自定义KeyedProcessFunction
class OrderPayMatchDetect extends KeyedProcessFunction<Long, OrderEvent, OrderResult> {
    //定义状态 保存之前订单是否已经create pay
    ValueState<Boolean> isPayedState;
    ValueState<Boolean> isCreatedState;
    //保存定时器时间戳
    ValueState<Long> timerTsState;

    //定义超时事件侧输出流标签
    private final static OutputTag<OrderResult> orderTimeoutTag = new OutputTag<OrderResult>("timeout") {};

    //注册状态
    @Override
    public void open(Configuration parameters) throws Exception {
        isPayedState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-payed", Boolean.class, false));
        isCreatedState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-created", Boolean.class, false));
        timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Long.class));

    }

    @Override
    public void processElement(OrderEvent orderEvent, Context context, Collector<OrderResult> collector) throws Exception {
        //取状态
        Boolean isPayed = isPayedState.value();
        Boolean isCreated = isCreatedState.value();
        Long timerTs = timerTsState.value();

        //判断当前时间类型
        if ("create".equals(orderEvent.getEventType())) {
            //如果是create 判断是否支付过
            if (isPayed) {
                //如果已经正常支付 输出正常匹配结果
                OrderResult orderResult = new OrderResult();
                orderResult.setOrderId(orderEvent.getOrderId());
                orderResult.setResultMsg("payed successfully");
                collector.collect(orderResult);
                //清空状态
                isCreatedState.clear();
                isPayedState.clear();
                timerTsState.clear();
                context.timerService().deleteEventTimeTimer(timerTs);
            } else {
                //如果没有支付过 注册15分钟之后的定时器 开始等待支付
                Long ts = (orderEvent.getEventTime() + 15 * 60) * 1000L;
                context.timerService().registerEventTimeTimer(ts);
                //更新状态
                timerTsState.update(ts);
                isCreatedState.update(true);
            }
        } else if ("pay".equals(orderEvent.getEventType())) {
            if (isCreated) {
                if (orderEvent.getEventTime() * 1000L < timerTs) {
                    OrderResult orderResult = new OrderResult();
                    orderResult.setOrderId(orderEvent.getOrderId());
                    orderResult.setResultMsg("payed successfully");
                    collector.collect(orderResult);
                } else {
                    OrderResult orderResult = new OrderResult();
                    orderResult.setOrderId(orderEvent.getOrderId());
                    orderResult.setResultMsg("payed but already timeout");
                    context.output(orderTimeoutTag, orderResult);
                }
                isCreatedState.clear();
                timerTsState.clear();
                context.timerService().deleteEventTimeTimer(timerTs);
            } else {
                context.timerService().registerEventTimeTimer(orderEvent.getEventTime() * 1000L);
                isPayedState.update(true);
                timerTsState.update(orderEvent.getEventTime() * 1000L);
            }
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx,
                        Collector<OrderResult> out) throws Exception {
        if (isPayedState.value()) {
            OrderResult orderResult = new OrderResult();
            orderResult.setOrderId(ctx.getCurrentKey());
            orderResult.setResultMsg("already payed but not found created log");
            ctx.output(orderTimeoutTag, orderResult);
        } else {
            OrderResult orderResult = new OrderResult();
            orderResult.setOrderId(ctx.getCurrentKey());
            orderResult.setResultMsg("order pay timeout");
            ctx.output(orderTimeoutTag, orderResult);
        }
        isPayedState.clear();
        isCreatedState.clear();
        timerTsState.clear();
    }


}

class OrderEvent {
    private Long orderId;
    private String eventType;
    private Long eventTime;

    @Override
    public String toString() {
        return "OrderEvent{" +
                "orderId=" + orderId +
                ", eventType='" + eventType + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }
}

class OrderResult {
    private Long orderId;
    private String resultMsg;

    @Override
    public String toString() {
        return "OrderResult{" +
                "orderId=" + orderId +
                ", resultMsg='" + resultMsg + '\'' +
                '}';
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getResultMsg() {
        return resultMsg;
    }

    public void setResultMsg(String resultMsg) {
        this.resultMsg = resultMsg;
    }
}
