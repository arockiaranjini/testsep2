import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object partitioning extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf =new SparkConf()
  sparkConf.set("spark.appname","myfirstapplication")
  sparkConf.set("spark.master","local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val ordersDf=spark.read
    .format("csv")
    .option("header",true)
    .option("path","C:/dataset/orders2.csv")
    .load

  print("orderdf has=" +ordersDf.rdd.getNumPartitions+"\n")
//inside the folder 4 files
  //  ordersRep.write
  ordersDf.write
    .format("csv")
    //partitionby //may be use two columns
    //.partitionBy("order_status")
    .option("maxRecordsPerFile",2000)
    .mode(SaveMode.Overwrite)
    .option("path","D:/dataset/partition")
    .save()

  //ordersDf.show()
  // ordersDf.printSchema()
  //scala.io.StdIn.readLine()
  // spark.stop()
}