package group;

import io.flinkspector.core.quantify.MatchTuples;
import io.flinkspector.core.quantify.OutputMatcher;
import io.flinkspector.datastream.DataStreamTestBase;
import io.flinkspector.datastream.input.time.InWindow;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.junit.Test;

import static org.hamcrest.Matchers.*;


public class MyAggregateTest extends DataStreamTestBase {

    @Test
    public void test() {

        // TODO what is this?
        setParallelism(2);

        DataStream<Tuple2<Integer, String>> testStream =
                createTimedTestStreamWith(Tuple2.of(1, "fritz"))
                        .emit(Tuple2.of(2, "fritz"))
                        //it's possible to generate unsorted input
                        .emit(Tuple2.of(2, "fritz"))
                        //emit the tuple multiple times, with the time span between:
                        .emit(Tuple2.of(1, "peter"), InWindow.to(20, seconds), times(2))
                        .close();

        OutputMatcher<Tuple2<Integer, String>> matcher =
                //name the values in your tuple with keys:
                new MatchTuples<Tuple2<Integer, String>>("value", "name")
                        //add an assertion using a value and hamcrest matchers
                        .assertThat("value", greaterThan(2))
                        .assertThat("name", either(is("fritz")).or(is("peter")))
                        //express how many matchers must return true for your test to pass:
                        .anyOfThem()
                        //define how many records need to fulfill the
                        .onEachRecord();

        assertStream(MyStream.base(testStream), matcher);

    }

}