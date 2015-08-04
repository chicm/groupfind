import java.io.{FileOutputStream, OutputStreamWriter}
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable
import scala.util.Random

/**
 * Created by chicm on 2015/8/4.
 */
object DataGen {
  def main (args: Array[String]) {
    if(args.length < 5) {
      println("usage: java DataGen fileDirectory numPerson numHotel numRecord year")
      return
    }
    println("generating candidates...")
    genPersonInfo(args(1).toInt*2)

    println("writing candidates...")
    val writerCand: OutputStreamWriter = new OutputStreamWriter(new FileOutputStream(args(0) + "/cand", false), "UTF-8")
    for (i <- 0 until args(1).toInt) {
          writerCand.write(String.format("%s,,,%s-%s-%s\n", personIDs(i), personIDs(i).substring(6, 10), personIDs(i).substring(10, 12), personIDs(i).substring(12, 14)))
    }
    writerCand.close

    println("writing generated groups...")
    genGroups(args(4).toInt, args(2).toInt)
    val writerGroups: OutputStreamWriter = new OutputStreamWriter(new FileOutputStream(args(0) + "/groups", false), "UTF-8")
    for(i <- 0 until groups.length) {
      writerGroups.write(groups(i) + "\n")
    }
    writerGroups.close

    println("writing hotel record...")
    val writerRecord: OutputStreamWriter = new OutputStreamWriter(new FileOutputStream(args(0) + "/hotel", false), "UTF-8")
    var groupIndex = 0
    for (i <- 0 until args(3).toInt) {
      if (i > 0 && groupIndex < groups.length && (i % 2 == 0)) {
        writerRecord.write(groups(groupIndex) + "\n")
        groupIndex += 1
      } else {
        writerRecord.write(genRegInfo(args(4).toInt, args(2).toInt) + "\n")
      }
    }
    writerRecord.close

  }

  val rand : Random = new Random
  def random(range: Int): Long = {
    Math.abs(rand.nextLong() % range)
  }
  val personMap = new mutable.HashMap[String,String]()
  var personIDs:Array[String] = null
  def genPersonInfo(numPerson: Int) {
    val f: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val DOBBase: Date = f.parse("1971-01-01")
    personIDs = new Array[String](numPerson)
    val s = new mutable.HashSet[String]()
    s.add("")
    var counter: Long = 0
    for (i <- 0 until numPerson) {
          val DOB: String = f.format(new Date(DOBBase.getTime + random(20 * 365) * 24 * 3600 * 1000))
          val sex: Boolean = rand.nextBoolean
          var id: String = ""
          if(i % 100000 == 0) {
              println("generating:" + i + ", dup:" + (counter-i))
          }
          while (s.contains(id)) {
            id = String.format("%06d%s%03d%s", new Integer(random(1000000).toInt), DOB, new Integer(random(1000).toInt), (if (sex) "1" else "2"))
            counter += 1
            if((counter-i) % 10000 == 0) {
              println("dup:" + (counter - i))
            }
          }
          s.add(id)
          personIDs(i) = id
          personMap.put(id, String.format("%s,%s,%s,%s", genName(sex), (if (sex) "男" else "女"), id, DOB))
    }
    println("dup:" + (counter-numPerson))
  }
  val lastName = Array("李", "王", "张", "刘", "陈", "杨", "赵", "黄", "周", "吴", "徐", "孙", "胡", "朱", "高", "林", "何", "郭", "马", "罗")
  val firstNameM = Array("宇逸", "辰栋", "星晨", "胤寅", "腾中", "嘉加", "梁锐", "锋龙", "远运", "柔帆", "骏运", "贤振", "子强", "栋逸", "文震", "吉骏", "成星", "盛骏", "畅博", "运运", "日骏", "远振", "翱骏", "海祥", "骞安", "晨骞", "延晨", "泽祥", "辰然", "辞腾", "辰树", "腾锦", "仕骏", "辰运", "树泽", "晨浩", "凯骏", "信吉", "强祜", "运凯", "骏鑫", "凯琛", "强晖", "星嘉", "家彬", "盛运", "澄柏", "沛栋", "睿烁", "振梓", "祥骏", "嘉腾", "骏初", "博腾", "祥嘉", "哲稷", "泽起", "谛佳", "允彬", "嘉逸")
  val firstNameF = Array("倩璇", "淑丽", "妍莲", "韵雪", "美萱", "格莉", "呈玲", "雨淑", "静姿", "萱慧", "俊楠", "颖薇", "婷初", "梓碧", "月彩", "雪萱", "琪萱", "晨采", "弦璐", "珠璇", "林雨", "雨欣", "梅帆", "涵美", "雪格", "优格", "帆月", "采惠", "柏静", "紫杉", "冰静", "彩慧", "彩韵", "采阳", "媛惠", "怡雨", "菲楠", "琪初", "月珊", "桐莲", "琪家", "彩漫", "洁锦", "楠呈", "初梦", "婷雪", "雯云", "楠冰", "彬薇", "雪雪", "琬莲", "蔚锦", "函淑", "妮玥", "妍梦", "橘楠", "漫琛", "美香", "雅梅", "露璐")

  def genName(male: Boolean): String = {
    return lastName(random(lastName.length).toInt) +
      (if (male) firstNameM(random(firstNameM.length).toInt)
      else firstNameF(random(firstNameF.length).toInt))
  }

  def genRegInfo(year: Int, numHotel: Int): String = {
    val id: String = personIDs(random(personIDs.length).toInt)
    val hotelID: Int = random(numHotel).toInt

    personMap.getOrElse(id, "") + "," + generateCheckinTime(year) +
      "," + (random(1000) + 100) + "," +  generateHotelInfo(hotelID)
  }

  def generateCheckinTime(year: Int): String = {
    val month: Int = random(12).toInt + 1
    var day: Int = 1
    if(List(1,3,5,7,8,10,12).contains(month)) {
      day = random(31).toInt + 1
    } else if(month == 2) {
      day = random(28).toInt + 1
    } else {
      day = random(30).toInt + 1
    }
    val checkinDate: String = String.format("%04d-%02d-%02d", new Integer(year), new Integer(month), new Integer(day))
    String.format("%s %02d:%02d:%02d", checkinDate, new Integer(random(24).toInt), new Integer(random(60).toInt), new Integer(random(60).toInt))
  }

  def generateHotelInfo(hotelID: Int): String = {
    val id = new Integer(hotelID)
    String.format("5301810011000%02d,招待所_%02d,虹山南路%02d号,530102,云南省昆明市五华区,530102410000,昆明市公安局五华分局虹山派出所", id, id, id)
  }

  val groups = new Array[String](24);
  def genGroups(year: Int, numHotel: Int) {
    if (numHotel < 5) {
      return;
    }
    var hotelID = 1;
    var personID = personMap.size / 4;
    if (personID < 3) {
      return;
    }
    var groupIndex = 0;
    for (i <- 0 to 3) {
      val time1 = generateCheckinTime(year)
      for(j <- 0 to 1) {
        groups(groupIndex) = personMap.getOrElse(personIDs(personID), "") + "," +
          time1 + "," + (random(1000) + 100) + "," +
          generateHotelInfo(hotelID)
        groupIndex += 1
        personID += 1
      }
      val time2 = generateCheckinTime(year)
      for (j <- 0 to 3) {
        groups(groupIndex) = personMap.getOrElse(personIDs(personID), "") + "," +
          time2 + "," + (random(1000) + 100) + "," +
          generateHotelInfo(hotelID)
        groupIndex += 1
        personID += 1
      }
      hotelID += 1;
      personID = personMap.size / 4;
    }
  }
}
