import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.{DefaultFormats, JValue}
import org.json4s.jackson.JsonMethods

case class ClientsAddress(street: String, city: String, state: String, postal_code: String)
case class ContactPerson(first_name: String, last_name: Option[String])
case class ClientsInfo(count: Int,
                       client_id: String,
                       client_name: String,
                       contact_person: ContactPerson,
                       email: Option[String],
                       phone: String,
                       address: ClientsAddress)

object workWithJson {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("json Data")
    val spark: SparkContext = new SparkContext(conf)

    val jsonRDD = spark.textFile("src/main/scala/fake_client_data.json")

    val jValueRDD: RDD[JValue] = jsonRDD.map(JsonMethods.parse(_))

    val clientsInfoRDD: RDD[ClientsInfo] = jValueRDD.map(jValue => {
      implicit val formats: DefaultFormats.type = DefaultFormats

      val count = (jValue \ "count").extract[Int]
      val client_id = (jValue \ "client_id").extract[String]
      val client_name = (jValue \ "client_name").extract[String]

      val contactPersonJValue = jValue \ "contact_person"
      val first_name = (contactPersonJValue \ "first_name").extract[String]
      val last_name = (contactPersonJValue \ "last_name").extractOpt[String]

      val email = (jValue \ "email").extractOpt[String]
      val phone = (jValue \ "phone").extract[String]

      val addressJValue = jValue \ "address"
      val street = (addressJValue \ "street").extract[String]
      val city = (addressJValue \ "city").extract[String]
      val state = (addressJValue \ "state").extract[String]
      val postal_code = (addressJValue \ "postal_code").extract[String]

      val contact_person = ContactPerson(first_name, last_name)
      val address = ClientsAddress(street, city, state, postal_code)

      ClientsInfo(count, client_id, client_name, contact_person, email, phone, address)
    })

    // Count clients in every state
    val clientsByState = clientsInfoRDD.map(info => (info.address.state, 1)).reduceByKey(_ + _)
    println("\nClients by State:")
    clientsByState.foreach(println)


    val techStack = Seq(
      (0, "AIML", 2000000),
      (1, "Web Designing", 1100000),
      (2, "Web Development", 1400000),
      (3, "UI/UX", 1300000)
    )

    // Broadcast techStacksRDD
    val techStackBroadcast: Broadcast[Seq[(Int, String, Int)]] = spark.broadcast(techStack)
    val joinedRDD = clientsInfoRDD.map { info =>
      val newInfo = techStackBroadcast.value.find(_._1 == info.count % 4).get
      (info, newInfo._2, newInfo._3)
    }
    println("\nJoined Table Data:")
    joinedRDD.foreach(println)


    // No. of companies working in same techStack
    val departmentNames = joinedRDD.map(dept => (dept._2, 1)).reduceByKey(_+_)
    println("\nTech Count:")
    departmentNames.foreach(println)


    // Name of companies having revenue more than 11
    val revenueAboveEleven = joinedRDD.filter(_._3 > 1100000).map(client => (client._1.client_name, client._3))
    println("\nClients having revenue above 11:")
    revenueAboveEleven.foreach(println)


    // Handling null values in email and last name
    val clientsWithNullValues = clientsInfoRDD.filter { client =>
      client.email.isEmpty && client.contact_person.last_name.isEmpty
    }
    println("\nClients with null values in email and last name:")
    clientsWithNullValues.foreach(println)


    // Names of Contact person for a revenue
    val contactPersonsForRevenue = joinedRDD.filter(_._3 == 1100000).map { case (client, _, _) =>
      val firstName = client.contact_person.first_name
      val lastName = client.contact_person.last_name
      val name = lastName match {
        case Some(ln) => s"$firstName $ln"
        case None => firstName
      }
      (name, client.phone)
    }
    println("\nNames of contact persons whose revenue is 1100000:")
    contactPersonsForRevenue.foreach(println)


    // Mean revenue
    val meanRevenue = joinedRDD.map(_._3).mean()
    println(s"\nMean Revenue: $meanRevenue")


    // Median revenue
    val sortedRevenue = joinedRDD.map(_._3).sortBy(identity)
    val count = sortedRevenue.count()
    val medianRevenue = if (count % 2 == 0) {
      val first = sortedRevenue.take(count.toInt / 2).last
      val second = sortedRevenue.take(count.toInt / 2 + 1).last
      (first + second) / 2
    } else {
      sortedRevenue.take(count.toInt / 2 + 1).last
    }
    println(s"\nMedian Revenue: $medianRevenue")


    // Revenue above mean value
    val clientsRevenueAboveMean = joinedRDD.filter(_._3 > meanRevenue).map { case (client, _, rev) =>
      (client.client_name, rev)
    }
    println("\nClients having revenue above mean value:")
    clientsRevenueAboveMean.foreach(println)

    while (true) {
      println("Spark application is running. Sleeping for 60 seconds...")
      Thread.sleep(60000)
    }

  }
}
