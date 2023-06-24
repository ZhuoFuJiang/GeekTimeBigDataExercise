package geek.exercise01

import org.apache.spark.{SparkConf, SparkContext}

object exercise {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("scalaCount").setMaster("local[*]");
    val sc = new SparkContext(sparkConf);
    val file_data = sc.textFile("file:///D:\\BaiduNetdiskDownload\\极客时间-大数据项目训练营\\10、Spark\\2、数据资料\\score.txt")
    //    file_data.foreach(println)

    //    val numberOfExam = file_data.map(x => {
    //      val line = x.split(" ")
    //      line(0) + "," + line(1)
    //    }).distinct().count()
    //    System.out.println("考试人数:" + numberOfExam);

    //    一共有多少个小于20岁的人参加考试
    val numberOfExamLessThanTwenty = file_data.map(
      x => {
        val line = x.split(" ")
        line(0) + "," + line(1) + "," + line(2)
      }
    ).distinct().filter(_.split(",")(2).toInt < 20).count()
    System.out.println("小于20的考试人数:" + numberOfExamLessThanTwenty);

    //    一共有多少个等于20岁的人参加考试
    val numberOfExamEqualToTwenty = file_data.map(
      x => {
        val line = x.split(" ")
        line(0) + "," + line(1) + "," + line(2)
      }
    ).distinct().filter(_.split(",")(2).toInt == 20).count()
    System.out.println("等于20的考试人数:" + numberOfExamEqualToTwenty);

    //    一共有多少个大于20岁的人参加考试
    val numberOfExamBetterThanTwenty = file_data.map(
      x => {
        val line = x.split(" ")
        line(0) + "," + line(1) + "," + line(2)
      }
    ).distinct().filter(_.split(",")(2).toInt > 20).count()
    System.out.println("大于20的考试人数:" + numberOfExamBetterThanTwenty);

    //    一共有多个男生参加考试
    val numberOfMan = file_data.map(
      x => {
        val line = x.split(" ")
        line(0) + "," + line(1) + "," + line(3)
      }
    ).distinct().filter(_.split(",")(2) == "男").count()
    System.out.println("考试的男生人数:" + numberOfMan);

    //    一共有多个女生参加考试
    val numberOfWoman = file_data.map(
      x => {
        val line = x.split(" ")
        line(0) + "," + line(1) + "," + line(3)
      }
    ).distinct().filter(_.split(",")(2) == "女").count()
    System.out.println("考试的女生人数:" + numberOfWoman);

    //    12班有多少人参加考试
    val numberOfTwelveClass = file_data.map(
      x => {
        val line = x.split(" ")
        line(0) + "," + line(1)
      }
    ).distinct().filter(_.split(",")(0).toInt == 12).count()
    System.out.println("12班的人数:" + numberOfTwelveClass);

    //    13班有多少人参加考试
    val numberOfThirteenClass = file_data.map(
      x => {
        val line = x.split(" ")
        line(0) + "," + line(1)
      }
    ).distinct().filter(_.split(",")(0).toInt == 13).count()
    System.out.println("13班的人数:" + numberOfThirteenClass);

    //    语文科目的平均成绩是多少
    val chinese = file_data.map(x => {
      val line = x.split(" ")
      line(4) + "," + line(5)
    }).filter(_.split(",")(0) == "chinese")
    val chineseCount = chinese.count().toInt
    val chineseSum = chinese.map(_.split(",")(1).toInt).sum()
    val chineseAvg = chineseSum / chineseCount
    System.out.println("语文平均成绩:" + chineseAvg);

    //    数学科目的平均成绩是多少
    val math = file_data.map(x => {
      val line = x.split(" ")
      line(4) + "," + line(5)
    }).filter(_.split(",")(0) == "math")
    val mathCount = math.count().toInt
    val mathSum = math.map(_.split(",")(1).toInt).sum()
    val mathAvg = mathSum / mathCount
    System.out.println("数学平均成绩:" + mathAvg);

    //    英语科目的平均成绩是多少
    val english = file_data.map(x => {
      val line = x.split(" ")
      line(4) + "," + line(5)
    }).filter(_.split(",")(0) == "english")
    val englishCount = english.count().toInt
    val englishSum = english.map(_.split(",")(1).toInt).sum()
    val englishAvg = englishSum / englishCount
    System.out.println("英语平均成绩:" + englishAvg);

    //    每个人平均成绩是多少
    System.out.println("每个人的平均成绩:")
    file_data.map(x => {
      val line = x.split(" ")
      (line(0) + "," + line(1), (line(5).toInt, 1))
    }).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .map(x => (x._1, x._2._1 / x._2._2)).foreach(println)

    //    12班平均成绩是多少
    System.out.println("12班的平均成绩:")
    file_data.map(x => {
      val line = x.split(" ")
      (line(0), line(5).toInt)
    }).filter(x => x._1 == "12").map(x => (x._1, (x._2, 1)))
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .map(x => (x._1, x._2._1 / x._2._2)).foreach(println)

    //    12班男生平均总成绩是多少
    val BoyScore12 = file_data.map(x => {
      val line = x.split(" ")
      (line(0), line(3), line(5).toInt)
    }).filter(x => x._1 == "12").filter(x => x._2 == "男")
    val BoyScore12Num = BoyScore12.count()
    val BoyScore12Sum = BoyScore12.map(_._3).sum()
    val BoyScore12Avg = BoyScore12Sum / BoyScore12Num
    System.out.println("12班男生的平均成绩:" + BoyScore12Avg);

    //    12班女生平均总成绩是多少
    val GirlScore12 = file_data.map(x => {
      val line = x.split(" ")
      (line(0), line(3), line(5).toInt)
    }).filter(x => x._1 == "12").filter(x => x._2 == "女")
    val GirlScore12Num = GirlScore12.count()
    val GirlScore12Sum = GirlScore12.map(_._3).sum()
    val GirlScore12Avg = GirlScore12Sum / GirlScore12Num
    System.out.println("12班女生的平均成绩:" + GirlScore12Avg);

    //    13班平均成绩是多少
    System.out.println("13班的平均成绩:")
    file_data.map(x => {
      val line = x.split(" ")
      (line(0), line(5).toInt)
    }).filter(x => x._1 == "13").map(x => (x._1, (x._2, 1)))
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .map(x => (x._1, x._2._1 / x._2._2)).foreach(println)

    //    13班男生平均总成绩是多少
    val BoyScore13 = file_data.map(x => {
      val line = x.split(" ")
      (line(0), line(3), line(5).toInt)
    }).filter(x => x._1 == "13").filter(x => x._2 == "男")
    val BoyScore13Num = BoyScore13.count()
    val BoyScore13Sum = BoyScore13.map(_._3).sum()
    val BoyScore13Avg = BoyScore13Sum / BoyScore13Num
    System.out.println("13班男生的平均成绩:" + BoyScore13Avg);

    //    13班女生平均总成绩是多少
    val GirlScore13 = file_data.map(x => {
      val line = x.split(" ")
      (line(0), line(3), line(5).toInt)
    }).filter(x => x._1 == "13").filter(x => x._2 == "女")
    val GirlScore13Num = GirlScore13.count()
    val GirlScore13Sum = GirlScore13.map(_._3).sum()
    val GirlScore13Avg = GirlScore13Sum / GirlScore13Num
    System.out.println("12班女生的平均成绩:" + GirlScore13Avg);

    //    全校语文成绩最高分是多少
    val chineseMax = chinese.distinct().map(x => x.split(",")(1).toInt).max()
    System.out.println("全校语文最高分:" + chineseMax);

    //    12班语文成绩最低分是多少

    val chineseLine12 = file_data.map(x => {val line = x.split(" ");
      line(0)+ "," + line(4)+ "," + line(5)})
    val chineseMin12 = chineseLine12.distinct()
      .filter(_.split(",")(0).toInt == 12)
      .filter(_.split(",")(1) == "chinese")
      .map(x => x.split(",")(2).toInt).min()
    System.out.println("12班语文最低分:" + chineseMin12);

    //    13班数学最高成绩是多少
    val mathLine13 = file_data.map(x => {val line = x.split(" ");
      line(0)+ "," + line(4)+ "," + line(5)})
    val mathMax13 = mathLine13.distinct()
      .filter(_.split(",")(0).toInt == 13)
      .filter(_.split(",")(1) == "math")
      .map(x => x.split(",")(2).toInt).max()
    System.out.println("13班数学最高分:" + mathMax13);

    //    总成绩大于150分的12班的女生有几个
    val sumScore12Line = file_data.map(x => {val line = x.split(" "); (line(0)+","+line(1)+","+line(3),line(5).toInt)})
    val sumScore12Dayu150 = sumScore12Line.reduceByKey(_+_).filter(a => (a._2>150 &&
      a._1.split(",")(0).equals("12") && a._1.split(",")(2).equals("女"))).count()
    System.out.println("总成绩大于150分的12班的女生数量:" + sumScore12Dayu150);

    //    总成绩大于150分，且数学大于等于70，且年龄大于等于19岁的学生的平均成绩是多少
    System.out.println("筛选过后的平均成绩:")
    val complex1 = file_data.map(x => {val line = x.split(" "); (line(0)+","+line(1)+","+line(3),line(5).toInt)})
    val complex2 = file_data.map(x => {val line = x.split(" "); (line(0)+","+line(1)+","+line(3)+","+line(4),line(5).toInt)})

    val com1 = complex1.map(a => (a._1, (a._2, 1))).reduceByKey((a,b) => (a._1+b._1,a._2+b._2)).filter(a => (a._2._1>150))
      .map(t => (t._1,t._2._1/t._2._2))
    //过滤出 数学大于等于70，且年龄大于等于19岁的学生
    val com2 = complex2.filter(a => {val line = a._1.split(","); line(3).equals("math") && a._2>70})
      .map(a => {val line2 = a._1.split(","); (line2(0)+","+line2(1)+","+line2(2),a._2.toInt)})

    (com1).join(com2).map(a =>(a._1,a._2._1)).foreach(println)

  }

}

