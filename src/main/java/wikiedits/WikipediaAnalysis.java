package wikiedits;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;
import org.apache.flink.util.Collector;

public class WikipediaAnalysis {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());
		
		// 5 seconds tumbling window
		DataStream<Tuple2<String, Integer>> results = 
		edits.flatMap(new Extractor())
		//group by user name, and sum up the diff bytes
		.keyBy(0)
		.timeWindow(Time.seconds(5))
		.sum(1);
		
		results.print();

		see.execute();
	}
	

	/**
	 * Implements the extractor that extract user name and diff bytes from wiki edits. 
	 * The function takes a WikipediaEditEvent and extract user name and diff bytes into 
	 * multiple pairs in the form of "(user, bytes)".
	 */
	public static final class Extractor implements FlatMapFunction<WikipediaEditEvent, Tuple2<String, Integer>> {

		@Override
		public void flatMap(WikipediaEditEvent edit, Collector<Tuple2<String, Integer>> out) throws Exception {
			// TODO Auto-generated method stub
			out.collect(new Tuple2<String, Integer>(edit.getUser(), edit.getByteDiff()));
			
		}
	}

}
