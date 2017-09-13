package wyy;
import org.apache.ivy.util.StringUtils;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.Map;

public class App {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
//        JavaRDD<String> logData = sc.textFile("/home/wyy/spark.txt");
//        JavaRDD<String> pythonlines1 = logData.filter(new Function<String, Boolean>() {
//            public Boolean call(String s) { return s.contains("python"); }
//        });
//        //匿名内部类
//        JavaRDD<String> pythonlines2 = logData.filter(new Function<String, Boolean>() {
//            public Boolean call(String s) { return s.contains("good"); }
//        });
//
//
//        JavaRDD<String> pythonlines = pythonlines1.union(pythonlines2);
//        System.out.println("Lines with a: " + pythonlines );
//        System.out.println("input had " +pythonlines.count()+"concering lines");
//        System.out.println("Here are some examples:");
//        for (String line: pythonlines.take(5)){
//            System.out.println(line);
//        }
        JavaRDD<Integer> rdd=sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8));


        System.out.println(rdd.toDebugString());
        JavaRDD<Integer> result=rdd.map(new Function<Integer, Integer>() {
            public Integer call(Integer x)  {
                return x*x;
            }
        });
        JavaDoubleRDD result1=rdd.mapToDouble(new DoubleFunction<Integer>() {
            public double call(Integer x) {
                return (double)x*x;
            }
        });
        Integer sum =rdd.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer x, Integer y)  {
                return x+y;
            }
        });
        JavaRDD<Integer> mapresult=rdd.map(new Function<Integer, Integer>() {
            public Integer call(Integer x)  {
                return x*x;
            }
        });

        Map<Integer, Long> CbV=rdd.countByValue();
        System.out.println(result.takeSample(false,5));
        System.out.println(CbV);
        System.out.println(result1.take(10));
        System.out.println(mapresult.take(10));
        System.out.println(                                                                              sum);
        sc.stop();



    }
}
