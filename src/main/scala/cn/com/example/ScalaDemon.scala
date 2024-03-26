package cn.com.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConversions.{asScalaBuffer}
import scala.collection.mutable


object ScalaDemon {

  def getDemonResult(args: Int): Unit = {
    // 本地执行时需要设置这个
    System.setProperty("hadoop.home.dir", "G:\\workSpace\\hadoop-2.6.5");
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession
      .builder()
      .master("local[4]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //    val dataSource = Seq(
    //      ("ABC17969(AB)", "1", "ABC17969", 2022),
    //      ("ABC17969(AB)", "2", "CDC52533", 2022),
    //      ("ABC17969(AB)", "3", "DEC59161", 2023),
    //      ("ABC17969(AB)", "4", "F43874", 2022),
    //      ("ABC17969(AB)", "5", "MY06154", 2021),
    //      ("ABC17969(AB)", "6", "MY4387", 2022),
    //      ("AE686(AE)", "7", "AE686", 2023),
    //      ("AE686(AE)", "8", "BH2740", 2021),
    //      ("AE686(AE)", "9", "EG999", 2021),
    //      ("AE686(AE)", "10", "AE0908", 2021),
    //      ("AE686(AE)", "11", "QA402", 2022),
    //      ("AE686(AE)", "12", "OM691", 2022)
    //    )

    val dataSource = Seq(
      ("AE686(AE)", "7", "AE686", 2022),
      ("AE686(AE)", "8", "BH2740", 2021),
      ("AE686(AE)", "9", "EG999", 2021),
      ("AE686(AE)", "10", "AE0908", 2023),
      ("AE686(AE)", "11", "QA402", 2022),
      ("AE686(AE)", "12", "OA691", 2022),
      ("AE686(AE)", "12", "OB691", 2022),
      ("AE686(AE)", "12", "OC691", 2019),
      ("AE686(AE)", "12", "OD691", 2017)
    )


    //加载数据集并生成DF
    val ds = dataSource.map(x => {
      TestScehma(x._1, x._2, x._3, x._4)
    })
    val df = spark.createDataFrame(ds)


    //  1.	For each peer_id, get the year when peer_id contains id_2, for example for ‘ABC17969(AB)’ year is 2022.
    import spark.implicits._
    val step1 = df.map(x => {
      var value: Tuple2[String, Int] = null;
      if (x.getAs("peer_id").toString.contains(x.getAs("id_2"))) {
        value = Tuple2(x.getAs("peer_id"), x.getAs("year"))
      }
      value
    }).filter(_ != null)
    step1.show()


    // 2.	Given a size number, for example 3. For each peer_id count the number of each year (which is smaller or equal than the year in step1).
    // 2-1 对原始DF中peer_id 和year groupby 获取对应的年份次数
    val dfPeerIdCount = df.groupBy("peer_id", "year").count()
    //step1中的数据集合
    val stp1Map = step1.collect().toMap

    // 2-2 对生成的年份次数结果数据 针对Stp1中的年份 进行过滤处理得到结果DF
    val step2DF = dfPeerIdCount.filter(x => {
      val peer_id = x.getString(0)
      val year = x.getInt(1)
      val stepYe: Option[Int] = stp1Map.get(peer_id)
      year <= stepYe.getOrElse(0)
    })
    step2DF.show()




    //3.	Order the value in step 2 by year and check if the count number of the first year is bigger or equal than the given size number.
    // If yes, just return the year.
    //If not, plus the count number from the biggest year to next year until the count number is bigger or equal than the given number. For example,
    // for ‘AE686(AE)’, the year is 2023, and count are:

    //3.1 对2中的结果数据进行排序处理 针对year降序
    val sortDF = step2DF.sort($"peer_id", $"year".desc)
    sortDF.show()

    //3.2 对排序的结果DF进行处理，找出对应规则的数据。需要的临时
    val paramNumber = args
    //设置中间临时变量
    var countPeerIdTmp: String = ""
    var CountNumberTmp: Int = 0
    val result: mutable.MutableList[(String, Int)] = new mutable.MutableList[(String, Int)]()

    sortDF.collectAsList().foreach(x => {
      val peer_id = x.getAs[String]("peer_id")
      val year = x.getAs[Int]("year")
      val count = x.getAs[Long]("count").toInt
      //当countPeerIdTmp为空时，给countPeerIdTmp和CountNumberTmp 赋值
      if (countPeerIdTmp.isEmpty) {
        countPeerIdTmp = peer_id
        CountNumberTmp = count
        result += ((peer_id, year))
        //当countPeerIdTmp中的值和peer_id 判断，如果CountNumberTmp小于 paramNumber，则peer_id  year加入结果集，
        //并且CountNumberTmp重新赋值
      } else if (countPeerIdTmp.equalsIgnoreCase(peer_id)) {
        if (CountNumberTmp < paramNumber) {
          result += ((peer_id, year))
          CountNumberTmp += count
        }
      } else {
        countPeerIdTmp = peer_id
        CountNumberTmp = count
        result += ((peer_id, year))
      }
    })
    result.foreach(x => println(x))

    spark.stop()

  }

  case class TestScehma(peer_id: String, id_1: String, id_2: String, year: Int) extends Serializable
}
