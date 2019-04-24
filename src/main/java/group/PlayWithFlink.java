package group;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PlayWithFlink {

    public static void main(String[] args) throws Exception {

    }
}

class MyStream {

    final static OutputTag<MyEvent> lateOutputTag = new OutputTag<MyEvent>("late-data") {
    };

    AssignerWithPunctuatedWatermarks eventTimeFunctionPuncuated = new AssignerWithPunctuatedWatermarks<MyEvent>() {
        long maxTs = 0;

        @Override
        public long extractTimestamp(MyEvent myEvent, long l) {
            long ts = myEvent.getEventTime();
            if (ts > maxTs) {
                maxTs = ts;
            }
            return ts;
        }

        @Override
        public Watermark checkAndGetNextWatermark(MyEvent event, long extractedTimestamp) {
            return new Watermark(maxTs - 10000);
        }
    };

    // TODO understand how BoundedOutOfOrderness is related to allowedLateness
    BoundedOutOfOrdernessTimestampExtractor<MyEvent> eventTimeFunction = new BoundedOutOfOrdernessTimestampExtractor<MyEvent>(Time.seconds(10)) {
        @Override
        public long extractTimestamp(MyEvent element) {
            return element.getEventTime();
        }
    };

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<MyEvent> events = env.fromCollection(MyEvent.examples())
            .assignTimestampsAndWatermarks(eventTimeFunctionPuncuated);

    private static AggregateFunction<MyEvent, MyAggregate, MyAggregate> aggregateFn = new AggregateFunction<MyEvent, MyAggregate, MyAggregate>() {
        @Override
        public MyAggregate createAccumulator() {
            return new MyAggregate();
        }

        @Override
        public MyAggregate add(MyEvent myEvent, MyAggregate myAggregate) {
            if (myEvent.getTracingId().equals("trace1")) {
                myAggregate.getTrace1().add(myEvent);
                return myAggregate;
            }
            myAggregate.getTrace2().add(myEvent);
            return myAggregate;
        }

        @Override
        public MyAggregate getResult(MyAggregate myAggregate) {
            return myAggregate;
        }

        @Override
        public MyAggregate merge(MyAggregate myAggregate, MyAggregate acc1) {
            acc1.getTrace1().addAll(myAggregate.getTrace1());
            acc1.getTrace2().addAll(myAggregate.getTrace2());
            return acc1;
        }
    };

    private static KeySelector<MyEvent, String> keyFn = new KeySelector<MyEvent, String>() {
        @Override
        public String getKey(MyEvent myEvent) throws Exception {
            return myEvent.getTracingId();
        }
    };

    public final SingleOutputStreamOperator<MyAggregate> result = events
            .keyBy(keyFn)
            .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
            .allowedLateness(Time.seconds(20))
            .sideOutputLateData(lateOutputTag)
            .aggregate(aggregateFn);

    public static DataStream<MyAggregate> base(DataStream<MyEvent> events) {
        return events
                .keyBy(keyFn)
                .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
                .allowedLateness(Time.seconds(20))
                .sideOutputLateData(lateOutputTag)
                .aggregate(aggregateFn);
    }


    public final DataStream lateStream;

    public MyStream() {

        lateStream = result.getSideOutput(lateOutputTag);
    }

    public void run() throws Exception {
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        result.print("SessionData");

        lateStream.print("LateData");

        env.execute();
    }
}

class MyEvent {
    private final String tracingId;
    private final Integer count;
    private final long eventTime;

    public MyEvent(String tracingId, Integer count, long eventTime) {
        this.tracingId = tracingId;
        this.count = count;
        this.eventTime = eventTime;
    }

    public String getTracingId() {
        return tracingId;
    }

    public Integer getCount() {
        return count;
    }

    public long getEventTime() {
        return eventTime;
    }

    public static List<MyEvent> examples() {
        long now = System.currentTimeMillis();
        MyEvent e1 = new MyEvent("trace1", 1, now);
        MyEvent e2 = new MyEvent("trace2", 1, now);
        MyEvent e3 = new MyEvent("trace2", 1, now - 1000);
        MyEvent e4 = new MyEvent("trace1", 1, now - 200);
        MyEvent e5 = new MyEvent("trace1", 1, now - 50000);
        return Arrays.asList(e1, e2, e3, e4, e5);
    }

    @Override
    public String toString() {
        return "MyEvent{" +
                "tracingId='" + tracingId + '\'' +
                ", count=" + count +
                ", eventTime=" + eventTime +
                '}';
    }
}

class MyAggregate {
    private final List<MyEvent> trace1 = new ArrayList<>();
    private final List<MyEvent> trace2 = new ArrayList<>();


    public List<MyEvent> getTrace1() {
        return trace1;
    }

    public List<MyEvent> getTrace2() {
        return trace2;
    }

    @Override
    public String toString() {
        return "MyAggregate{" +
                "trace1=" + trace1 +
                ", trace2=" + trace2 +
                '}';
    }
}
