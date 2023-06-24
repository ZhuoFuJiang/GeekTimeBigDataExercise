package geek.exercise02;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class LoginFailOne {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setParallelism(1);
        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        sEnv.readTextFile(
                "D:\\BaiduNetdiskDownload\\极客时间-大数据项目训练营\\11、Flink\\2、数据资料\\LoginLog.csv")
                .map(line -> {
                    String[] props = line.split(",");
                    LoginEvent loginEvent = new LoginEvent();
                    loginEvent.setUserId(Long.parseLong(props[0].trim()));
                    loginEvent.setIp(props[1].trim());
                    loginEvent.setEventType(props[2].trim());
                    loginEvent.setEventTime(Long.parseLong(props[3].trim()));
                    return loginEvent;
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(LoginEvent loginEvent) {
                return loginEvent.getEventTime() * 1000;
            }
        }).keyBy(LoginEvent::getUserId)
                .process(new LoginWarning(2)).print();

        sEnv.execute();
    }
}

class LoginWarning extends KeyedProcessFunction<Long, LoginEvent, Warning> {
    private int maxFailTimes;

    public LoginWarning(int maxFailTimes) {
        this.maxFailTimes = maxFailTimes;
    }

    // 保存登录失败事件的状态
    ListState<LoginEvent> loginFailState;

    @Override
    public void open(Configuration parameters) throws Exception {
        loginFailState =  getRuntimeContext().getListState(
                new ListStateDescriptor<LoginEvent>("loginfail-state", LoginEvent.class));
    }

    @Override
    public void processElement(LoginEvent loginEvent, Context context, Collector<Warning> collector) throws Exception {
        if(loginEvent.getEventType().equals("fail")) {
            Iterator<LoginEvent> iter = loginFailState.get().iterator();
            if(iter.hasNext()) {
                LoginEvent firstFailEvent = iter.next();
                if(loginEvent.getEventTime() < firstFailEvent.getEventTime() + 2) {
                    Warning warning = new Warning();
                    warning.setUserId(firstFailEvent.getUserId());
                    warning.setFirstFailTime(firstFailEvent.getEventTime());
                    warning.setLastFailTime(loginEvent.getEventTime());
                    warning.setWarningMsg("在2秒之内连续登录失败2次");
                    collector.collect(warning);
                }
                loginFailState.clear();
                loginFailState.add(loginEvent);
            } else {
                loginFailState.add(loginEvent);
            }
        } else {
            loginFailState.clear();
        }
    }

//    @Override
//    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Warning> out) throws Exception {
//        ArrayList<LoginEvent> allLoginFailEvents = new ArrayList<>();
//
//        Iterator<LoginEvent> iter = loginFailState.get().iterator();
//        while(iter.hasNext()) {
//            allLoginFailEvents.add(iter.next());
//        }
//
//        if(allLoginFailEvents.size() >= maxFailTimes) {
//            Warning warning = new Warning();
//            warning.setUserId(allLoginFailEvents.get(0).getUserId());
//            warning.setFirstFailTime(allLoginFailEvents.get(0).getEventTime());
//            warning.setLastFailTime(allLoginFailEvents.get(allLoginFailEvents.size() - 1).getEventTime());
//            warning.setWarningMsg("在2秒之内连续登录失败" + allLoginFailEvents.size() + "次");
//            out.collect(warning);
//        }
//
//        loginFailState.clear();
//    }
}

class LoginEvent {
    private Long userId;
    private String ip;
    private String eventType;
    private Long eventTime;

    @Override
    public String toString() {
        return "LoginEvent{" +
                "userId=" + userId +
                ", ip='" + ip + '\'' +
                ", eventType='" + eventType + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
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

class Warning {
    private Long userId;
    private Long firstFailTime;
    private Long lastFailTime;
    private String warningMsg;

    @Override
    public String toString() {
        return "Warning{" +
                "userId=" + userId +
                ", firstFailTime=" + firstFailTime +
                ", lastFailTime=" + lastFailTime +
                ", warningMsg='" + warningMsg + '\'' +
                '}';
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getFirstFailTime() {
        return firstFailTime;
    }

    public void setFirstFailTime(Long firstFailTime) {
        this.firstFailTime = firstFailTime;
    }

    public Long getLastFailTime() {
        return lastFailTime;
    }

    public void setLastFailTime(Long lastFailTime) {
        this.lastFailTime = lastFailTime;
    }

    public String getWarningMsg() {
        return warningMsg;
    }

    public void setWarningMsg(String warningMsg) {
        this.warningMsg = warningMsg;
    }
}