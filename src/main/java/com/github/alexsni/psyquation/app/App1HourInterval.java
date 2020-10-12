package com.github.alexsni.psyquation.app;

import com.github.alexsni.psyquation.RDDTransformers.RDDTransformations;
import com.github.alexsni.psyquation.views.JsonView;
import com.github.alexsni.psyquation.views.OutView;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import scala.Tuple6;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;

public class App1HourInterval extends RDDTransformations<JsonView, OutView> {
    public App1HourInterval(String inputLocation, String outputLocation) {
        super("appName",
                "local[*]",
                "false",
                inputLocation, // "/Users/osnisar/alexsni/psyquation/first/15_min_out.json",
                outputLocation, // "/Users/osnisar/alexsni/psyquation/final_out.json",
                JsonView.class,
                OutView.class);
    }

    static JavaPairRDD<Tuple2<LocalDateTime, String>, Tuple6<Double, Double, Double, Double, Boolean, Double>> toPairRdd(JavaRDD<JsonView> input) {
        return input.keyBy(r -> new Tuple2<LocalDateTime, String>(r.getTimeSlotStart(), r.getLocation()))
                .mapValues(jv -> new Tuple6<Double, Double, Double, Double, Boolean, Double>(
                        jv.getTempMin(),
                        jv.getTempMax(),
                        jv.getTempAvg(),
                        jv.getTempCnt(),
                        jv.isPresence(),
                        jv.getPresenceCnt()
                ));
    }

    private JavaRDD<JsonView> truncateToHour(JavaRDD<JsonView> input) {
        return input.mapPartitions(rec -> {
            LinkedList<JsonView> n = new LinkedList<>();
            while (rec.hasNext()) {
                JsonView r = rec.next();
                n.add(new JsonView(
                        r.getTimeSlotStart().truncatedTo(ChronoUnit.HOURS),
                        r.getLocation(),
                        r.getTempMin(),
                        r.getTempMax(),
                        r.getTempAvg(),
                        r.getTempCnt(),
                        r.isPresence(),
                        r.getPresenceCnt()
                ));
            }
            return n.iterator();
        });
    }

    static JavaRDD<OutView> aggregateByNewWalue(JavaPairRDD<Tuple2<LocalDateTime, String>, Tuple6<Double, Double, Double, Double, Boolean, Double>> input){
        return input.reduceByKey((v1, v2) -> new Tuple6<>(
                Math.min(v1._1(), v2._1()),
                Math.max(v1._2(), v2._2()),
                (v1._3() + v2._3()) / 2,
                v1._4() + v2._4(),
                v1._5() ? v1._5() : v2._5(),
                v1._6() + v2._6()
        )).map(new Function<Tuple2<Tuple2<LocalDateTime, String>, Tuple6<Double, Double, Double, Double, Boolean, Double>>, OutView>() {

            @Override
            public OutView call(Tuple2<Tuple2<LocalDateTime, String>, Tuple6<Double, Double, Double, Double, Boolean, Double>> tuple2Tuple6Tuple2) throws Exception {
                return new OutView(
                        tuple2Tuple6Tuple2._1._1.toString(),
                        tuple2Tuple6Tuple2._1._2,
                        tuple2Tuple6Tuple2._2._1(),
                        tuple2Tuple6Tuple2._2._2(),
                        tuple2Tuple6Tuple2._2._3(),
                        tuple2Tuple6Tuple2._2._4(),
                        tuple2Tuple6Tuple2._2._5(),
                        tuple2Tuple6Tuple2._2._6()
                );
            }
        });
    }

    @Override
    public JavaRDD<OutView> transform(JavaRDD<JsonView> inputDf) {
        JavaRDD<JsonView> truncatedToFullHour = truncateToHour(inputDf);
        JavaPairRDD<Tuple2<LocalDateTime, String>, Tuple6<Double, Double, Double, Double, Boolean, Double>> kvPair = toPairRdd(truncatedToFullHour);
        return aggregateByNewWalue(kvPair).coalesce(1);
    }

    public static void main(String[] args) {
        String inputLocation = args[0];
        String outputLocation = args[1];
        App1HourInterval runner = new App1HourInterval(inputLocation, outputLocation);
        runner.runItAll();
    }
}
