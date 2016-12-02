
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liushilin on 2016/3/27.
  */
object rec_plist_als_rec {


  def main(args: Array[String]) {


    val conf = new SparkConf()
    val args = conf.get("spark.args")
    val array = args.split(",")

    if (array.length < 1) {
      println("args error !!!")
      System.exit(-1)
    }
    val argsMap = scala.collection.mutable.HashMap.empty[String, String]
    array.foreach(s => {
      val arr = s.split("#")
      if (arr.length != 2) {
        println("args error !!!")
        System.exit(-1)
      }
      argsMap(arr(0)) = arr(1)
    })


    val input = argsMap("input")
    val output = argsMap("output")
    val num = argsMap("num")

    val appName = s"${output}"

    conf.setAppName(appName)

    val input_url = s"hdfs://ns4/user/mart_wzyf/personal/model/$input/"
    val output_url = s"hdfs://ns4/user/mart_wzyf/personal/model/$output/"

    val sc = new SparkContext(conf)

    recommendProductsForUsers(sc, input_url, output_url, num.toInt)

    sc.stop()
  }

  def recommendProductsForUsers(sc: SparkContext, input_url: String, output_url: String, num: Int): Unit = {

    val fileSystem: FileSystem = FileSystem.get(new JobConf())
    if (fileSystem.exists(new Path(output_url))) {
      fileSystem.delete(new Path(output_url), true)
    }

    val model = MatrixFactorizationModel.load(sc, input_url)

    val userRec = model.recommendProductsForUsers(num)

    println("recommendProductsForUsers over")

    //    userRec.values.flatMap(x => x.toList).map(x => (x.user + 1.toChar.toString + x.product + 1.toChar.toString + x.rating)).saveAsTextFile(output_url)
    userRec.values.flatMap(x => x.toList)
      .map(x => (x.user + 1.toChar.toString + x.product + 1.toChar.toString + x.rating))
      .repartition(1000)
      .saveAsTextFile(output_url)


    println("saveAsTextFile over")

  }

}