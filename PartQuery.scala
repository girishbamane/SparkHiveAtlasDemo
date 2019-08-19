import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object PartQueryDF {
  val LOG = Logger.getLogger(QueryDF.getClass.getName)

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.INFO)

    val spark = SparkSession.builder() .appName(args(0)).config(args(1), args(2))
      .config(args(3), args(4))
      .config(args(5), args(6))
      .enableHiveSupport()
      .getOrCreate()

    val sqlco = spark.sqlContext

    val orderSchema = StructType(List(StructField("OrderID", IntegerType, true), StructField("CustomerID", IntegerType, true), StructField("EmployeeID", IntegerType, true), StructField("OrderDate", StringType, true), StructField("ShipperID", IntegerType, true) ) )
    val orderDF = sqlco.read.format("csv").schema(orderSchema).load(args(7))
    sqlco.sql("drop table if exists ordersdb.orders")
    sqlco.sql("create database if not exists ordersdb LOCATION \'/apps/hive/warehouse/ordersdb\'")
    orderDF.write.mode(SaveMode.Overwrite).saveAsTable("ordersdb.orders")
    LOG.info("MYLOG Table is saved successfully : ordersdb.orders" )
    val orders = sqlco.sql("select * from ordersdb.orders" )

    val customerSchema = StructType(List(StructField("CustomerID", IntegerType, true), StructField("CustomerName", StringType, true), StructField("ContactName", StringType, true), StructField("Address", StringType, true), StructField("PostalCode", IntegerType, true) ) )
    val customersDF = sqlco.read.format("csv").schema(customerSchema).load(args(10))
    sqlco.sql("drop table if exists customersdb.customers")
    sqlco.sql("create database if not exists customersdb LOCATION \'/apps/hive/warehouse/customersdb\'")
    customersDF.write.mode(SaveMode.Overwrite).saveAsTable("customersdb.customers")
    LOG.info("MYLOG Table is saved successfully : customersdb.customers" )
    val customers = sqlco.sql("select * from ordersdb.orders" )

    val joindf = sqlco.sql("SELECT o1.OrderID, c1.CustomerName FROM ordersdb.orders o1  INNER JOIN customersdb.customers c1 ON o1.ShipperID = c1.CustomerID")
    sqlco.sql("create database if not exists "+args(13)+" LOCATION \'/apps/hive/warehouse/"+args(13)+"\'")
    joindf.write.mode(SaveMode.Overwrite).saveAsTable(args(13)+"."+args(14))

    val join = sqlco.sql("select * from "+args(13)+"."+args(14))
    join.foreach(row =>
        LOG.info("MYLOG "+args(13)+" :  "+ row.get(0)+" "+row.get(1))
    )
  }
}
