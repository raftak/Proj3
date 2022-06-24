Codigo Fonte:
//TP 6 - PB pt1
public class Q13 {
public static SparkSession spark;
public static JavaSparkContext sc;
public static void main(String[] args) throws AnalysisException {
spark =
SparkSession.builder().appName("ProjetBloco").master("local").getOrCreat
e();
sc = new JavaSparkContext(spark.sparkContext());
sc.setLogLevel("WARN");
runOctNovcsv(spark);
//runDeccsv(spark);
//runJancsv(spark);
//runOctNov_cosme(spark);
//runDecJan_cosme(spark);
spark.stop();
}
private static void runJancsv(SparkSession spark2) throws
AnalysisException {
//Dataset<Row> df = spark.read().option("header",
true).option("inferSchema", true)
//.option("timestampFormat", "yyyy-MM-dd
HH:mm:ss").csv("src/main/resources/2020-Jan.csv.gz");
//convert to parquet
//df.coalesce(1).write().mode(SaveMode.Overwrite).parquet("hdfs://localh
ost:9000/parquetJan");
Dataset<Row> df2 =
spark.read().parquet("hdfs://localhost:9000/parquetJan");
df2.printSchema();
df2.show();
System.out.println("Numero de linhas parquet: " +
df2.count());
// Criando uma coluna "hour" e "day"
df2 = df2.withColumn("hour",
hour(col("event_time")));
df2 = df2.withColumn("day_mth",
dayofmonth(col("event_time")));
df2 = df2.withColumn("day_wk",
dayofweek(col("event_time")));
df2 = df2.withColumn("month",
month(col("event_time")));
df2.show();
//Verificando vazios e outliers (coluna price)
System.out.println("Vazios no price: " +
df2.filter(col("price").isNull() ).count());
// Dataset<Row> sqlDF = spark.sql("SELECT month,
min(`price`), max(`price`), avg(`price`), stddev(`price`) FROM dados"
//+ " GROUP BY `month`");
//sqlDF.show();
df2.createTempView("dados");
//Queries com o tempo
Dataset<Row> sqlDF_time = spark.sql("SELECT
price, hour, day_mth, day_wk, month FROM dados"
+ " WHERE `event_type` = 'purchase' "
+ " ORDER BY hour DESC");
//criando outro temp view dados2 so com
purchases de cima, mth, dayweek, hour, month
sqlDF_time.createTempView("dados2");
Dataset<Row> sqlDF_time2 =
spark.sql("SELECT hour, count(*) as purchases FROM dados2 "
+ "GROUP BY `hour` ORDER BY
hour DESC");
sqlDF_time2.show(24);
sqlDF_time2.coalesce(1).write().option("header",
true).csv("src/main/Jan_time1");
Dataset<Row> sqlDF_time3 =
spark.sql("SELECT day_mth, count(*) as n_purchases, sum(`price`) as
revenue FROM dados2 "
+ "GROUP BY `day_mth` ORDER BY
day_mth DESC");
sqlDF_time3.show(31);
sqlDF_time3.coalesce(1).write().option("header",
true).csv("src/main/Jan_time2");
Dataset<Row> sqlDF_time4 =
spark.sql("SELECT day_wk, count(*) as purchases FROM dados2 "
+ "GROUP BY `day_wk` ORDER BY
day_wk DESC");
sqlDF_time4.show();
sqlDF_time4.coalesce(1).write().option("header",
true).csv("src/main/Jan_time3");
Dataset<Row> sqlDF_time5 =
spark.sql("SELECT month, count(*) as purchases, sum(`price`) as profit
FROM dados2 "
+ "GROUP BY `month`");
sqlDF_time5.show();
sqlDF_time5.coalesce(1).write().option("header",
true).csv("src/main/Jan_time4");
//Tipos de eventos, contagem
Dataset<Row> sqldf6 = spark.sql("SELECT
`event_type`, count(*) as count_event FROM dados "
+ "GROUP BY `event_type` ORDER BY
count_event DESC");
sqldf6.show();
sqldf6.coalesce(1).write().option("header",
true).csv("src/main/Jan_query1");
//Lucro total da loja no mes
Dataset<Row> sqldf7 = spark.sql("SELECT month,
sum(price) as mon_profit FROM dados"
+ " WHERE `event_type` = 'purchase'
GROUP BY `month`");
sqldf7.show();
sqldf7.coalesce(1).write().option("header",
true).csv("src/main/Jan_query2");
//Media, contagem, soma de vendas por brand
(filtrada por purchase)
Dataset<Row> sqldf2 = spark
.sql("SELECT `brand`, avg(`price`)
as avg_price, count(*) as count, sum(`price`) as profits"
+ " FROM dados"
+ " WHERE `event_type` =
'purchase' "
+ "GROUP BY `brand`
ORDER BY profits DESC")
.repartition(1);
sqldf2.show();
sqldf2.coalesce(1).write().option("header",
true).csv("src/main/Jan_query3");
//sqldf2.write().option("header",
true).csv("src/main/sqldf2_csv");
//Igualmente por Categoria
Dataset<Row> sqldf21 = spark
.sql("SELECT `category_code`,
avg(`price`) as avg_price, count(*) as count, sum(`price`) as profits"
+ " FROM dados"
+ " WHERE `event_type` =
'purchase' "
+ "GROUP BY
`category_code` ORDER BY profits DESC")
.repartition(1);
sqldf21.show();
sqldf21.coalesce(1).write().option("header",
true).csv("src/main/Jan_query4");
//contagem numero de usuarios distintos
Dataset<Row> sqldf3 = spark.sql("SELECT month,
COUNT(DISTINCT `user_id`) as Dist_users, COUNT(DISTINCT `user_session`)
as Dist_sessions FROM dados "
+ "GROUP BY `month` ");
sqldf3.show();
sqldf3.coalesce(1).write().option("header",
true).csv("src/main/Jan_query5");
//contagem sessoes distintas - visitas na loja
//Dataset<Row> sqldf31 = spark.sql("SELECT
month, COUNT(DISTINCT `user_session`) as SessionsDist FROM dados GROUP
BY `month` ");
//sqldf31.show();
//Produtos (ID) + vendidos, contagem
Dataset<Row> sqldf4 = spark.sql("SELECT
`product_id`, `category_code`, `brand`, count(*) as count_prod FROM
dados "
+ "WHERE `event_type` = 'purchase'"
+ "GROUP BY
`product_id`,`category_code`, `brand` ORDER BY count_prod DESC");
sqldf4.show();
sqldf4.coalesce(1).write().option("header",
true).csv("src/main/Jan_query6");
}
private static void runDeccsv(SparkSession spark2) throws
AnalysisException {
//Dataset<Row> df = spark.read().option("header",
true).option("inferSchema", true)
//.option("timestampFormat", "yyyy-MM-dd
HH:mm:ss").csv("src/main/resources/2019-Dec.csv.gz");
//convert to parquet
//
df.coalesce(1).write().mode(SaveMode.Overwrite).parquet("hdfs://localhos
t:9000/parquetDec");
Dataset<Row> df2 =
spark.read().parquet("hdfs://localhost:9000/parquetDec");
System.out.println("Esquema: ");
df2.printSchema();
df2.show();
System.out.println("Numero de linhas parquet: " + df2.count());
// Criando uma coluna "hour" e "day"
df2 = df2.withColumn("hour", hour(col("event_time")));
df2 = df2.withColumn("day_mth",
dayofmonth(col("event_time")));
df2 = df2.withColumn("day_wk",
dayofweek(col("event_time")));
df2 = df2.withColumn("month", month(col("event_time")));
df2.show();
//Verificando vazios e outliers (coluna price)
System.out.println("Vazios no price: " +
df2.filter(col("price").isNull() ).count());
df2.describe("price").show();
df2.createTempView("dados");
/*
//Queries com o tempo
Dataset<Row> sqlDF_time = spark.sql("SELECT price, hour,
day_mth, day_wk, month FROM dados"
+ " WHERE `event_type` = 'purchase' "
+ " ORDER BY hour DESC");
//criando outro temp view dados2 so com purchases de
cima, mth, dayweek, hour, month
sqlDF_time.createTempView("dados2");
Dataset<Row> sqlDF_time2 = spark.sql("SELECT hour,
count(*) as purchases FROM dados2 "
+ "GROUP BY `hour` ORDER BY hour DESC");
sqlDF_time2.show(24);
sqlDF_time2.coalesce(1).write().option("header",
true).csv("src/main/Dec_time1");
Dataset<Row> sqlDF_time3 = spark.sql("SELECT day_mth,
count(*) as n_purchases, sum(`price`) as revenue FROM dados2 "
+ "GROUP BY `day_mth` ORDER BY day_mth
DESC");
sqlDF_time3.show(31);
sqlDF_time3.coalesce(1).write().option("header",
true).csv("src/main/Dec_time2");
Dataset<Row> sqlDF_time4 = spark.sql("SELECT day_wk,
count(*) as purchases FROM dados2 "
+ "GROUP BY `day_wk` ORDER BY day_wk
DESC");
sqlDF_time4.show();
sqlDF_time4.coalesce(1).write().option("header",
true).csv("src/main/Dec_time3");
Dataset<Row> sqlDF_time5 = spark.sql("SELECT month,
count(*) as purchases, sum(`price`) as profit FROM dados2 "
+ "GROUP BY `month`");
sqlDF_time5.show();
sqlDF_time5.coalesce(1).write().option("header",
true).csv("src/main/Dec_time4");
*/
//Tipos de eventos, contagem
Dataset<Row> sqldf6 = spark.sql("SELECT `event_type`,
count(*) as count_event FROM dados "
+ "GROUP BY `event_type` ORDER BY count_event
DESC");
sqldf6.show();
sqldf6.coalesce(1).write().option("header",
true).csv("src/main/Dec_query1");
//Lucro total da loja no mes
Dataset<Row> sqldf7 = spark.sql("SELECT month, sum(price)
as mon_profit FROM dados"
+ " WHERE `event_type` = 'purchase' GROUP BY
`month`");
sqldf7.show();
sqldf7.coalesce(1).write().option("header",
true).csv("src/main/Dec_query2");
//Media, contagem, soma de vendas por brand (filtrada por
purchase)
Dataset<Row> sqldf2 = spark
.sql("SELECT `brand`, avg(`price`) as avg_price,
count(*) as count, sum(`price`) as profits"
+ " FROM dados"
+ " WHERE `event_type` = 'purchase'
"
+ "GROUP BY `brand` ORDER BY profits
DESC")
.repartition(1);
sqldf2.show();
sqldf2.coalesce(1).write().option("header",
true).csv("src/main/Dec_query3");
//sqldf2.write().option("header",
true).csv("src/main/sqldf2_csv");
//Igualmente por Categoria
Dataset<Row> sqldf21 = spark
.sql("SELECT `category_code`, avg(`price`) as
avg_price, count(*) as count, sum(`price`) as profits"
+ " FROM dados"
+ " WHERE `event_type` = 'purchase'
"
+ "GROUP BY `category_code` ORDER BY
profits DESC")
.repartition(1);
sqldf21.show();
sqldf21.coalesce(1).write().option("header",
true).csv("src/main/Dec_query4");
//contagem numero de usuarios distintos
Dataset<Row> sqldf3 = spark.sql("SELECT month,
COUNT(DISTINCT `user_id`) as Dist_users, COUNT(DISTINCT `user_session`)
as Dist_sessions FROM dados "
+ "GROUP BY `month` ");
sqldf3.show();
sqldf3.coalesce(1).write().option("header",
true).csv("src/main/Dec_query5");
//contagem sessoes distintas - visitas na loja
//Dataset<Row> sqldf31 = spark.sql("SELECT month,
COUNT(DISTINCT `user_session`) as SessionsDist FROM dados GROUP BY
`month` ");
//sqldf31.show();
//Produtos (ID) + vendidos, contagem
Dataset<Row> sqldf4 = spark.sql("SELECT `product_id`,
`category_code`, `brand`, count(*) as count_prod FROM dados "
+ "WHERE `event_type` = 'purchase'"
+ "GROUP BY `product_id`,`category_code`,
`brand` ORDER BY count_prod DESC");
sqldf4.show();
sqldf4.coalesce(1).write().option("header",
true).csv("src/main/Dec_query6");
//Usuarios que mais compram
Dataset<Row> sqldf8 = spark.sql("SELECT `user_id`, count(*)
from dados "
+ "WHERE `event_type` = 'purchase' "
+ "GROUP BY `user_id` ORDER BY count(*) DESC" );
}
private static void runOctNovcsv(SparkSession spark2) throws
AnalysisException {
//Dataset<Row> df = spark.read().option("header",
true).option("inferSchema", true)
//.option("timestampFormat", "yyyy-MM-dd
HH:mm:ss").csv("src/main/resources/2019-Dec.csv.gz");
//convert to parquet
//
df.coalesce(1).write().mode(SaveMode.Overwrite).parquet("hdfs://localhos
t:9000/parquetDec");
Dataset<Row> df2 =
spark.read().parquet("hdfs://localhost:9000/parquetOctNov");
df2.printSchema();
df2.show();
System.out.println("Numero de linhas parquet: " +
df2.count());
// Criando uma coluna "hour" e "day"
df2 = df2.withColumn("hour", hour(col("event_time")));
df2 = df2.withColumn("day_mth",
dayofmonth(col("event_time")));
df2 = df2.withColumn("day_wk",
dayofweek(col("event_time")));
df2 = df2.withColumn("month",
month(col("event_time")));
df2.show();
df2.createTempView("dados");
//Verificando vazios e outliers (coluna price)
System.out.println("Vazios no price: " +
df2.filter(col("price").isNull() ).count());
System.out.println("Vazios no category_code: " +
df2.filter(col("category_code").isNull() ).count());
System.out.println("Vazios no brand: " +
df2.filter(col("brand").isNull() ).count());
System.out.println("Vazios no event_type: " +
df2.filter(col("event_type").isNull() ).count());
//df2.describe().show();
df2.describe("price").show();
// Dataset<Row> sqlDF = spark.sql("SELECT month,
min(`price`), max(`price`), avg(`price`), stddev(`price`) FROM dados"
//+ " GROUP BY `month`");
//sqlDF.show();
/*
//Queries com o tempo
Dataset<Row> sqlDF_time = spark.sql("SELECT price,
hour, day_mth, day_wk, month FROM dados"
+ " WHERE `event_type` = 'purchase' "
+ " ORDER BY hour DESC");
//criando outro temp view dados2 so com purchases
de cima, mth, dayweek, hour, month
sqlDF_time.createTempView("dados2");
Dataset<Row> sqlDF_time2 = spark.sql("SELECT
hour, count(*) as purchases FROM dados2 "
+ "GROUP BY `hour` ORDER BY hour
DESC");
sqlDF_time2.show(24);
sqlDF_time2.coalesce(1).write().option("header",
true).csv("src/main/OctNov_time1");
Dataset<Row> sqlDF_time3 = spark.sql("SELECT
month, day_mth, count(*) as n_purchases, sum(`price`) as revenue FROM
dados2 "
+ "GROUP BY `day_mth`, `month` ORDER
BY day_mth DESC");
sqlDF_time3.show(31);
sqlDF_time3.coalesce(1).write().option("header",
true).csv("src/main/OctNov_time2");
Dataset<Row> sqlDF_time4 = spark.sql("SELECT
day_wk, count(*) as purchases FROM dados2 "
+ "GROUP BY `day_wk` ORDER BY day_wk
DESC");
sqlDF_time4.show();
sqlDF_time4.coalesce(1).write().option("header",
true).csv("src/main/OctNov_time3");
Dataset<Row> sqlDF_time5 = spark.sql("SELECT
month, count(*) as purchases, sum(`price`) as profit FROM dados2 "
+ "GROUP BY `month`");
sqlDF_time5.show();
sqlDF_time5.coalesce(1).write().option("header",
true).csv("src/main/OctNov_time4");
//Tipos de eventos, contagem
Dataset<Row> sqldf6 = spark.sql("SELECT `event_type`,
count(*) as count_event FROM dados "
+ "GROUP BY `event_type` ORDER BY
count_event DESC");
sqldf6.show();
sqldf6.coalesce(1).write().option("header",
true).csv("src/main/OctNov_query1");
//Lucro total da loja no mes
Dataset<Row> sqldf7 = spark.sql("SELECT month,
sum(price) as mon_profit FROM dados"
+ " WHERE `event_type` = 'purchase' GROUP
BY `month`");
sqldf7.show();
sqldf7.coalesce(1).write().option("header",
true).csv("src/main/OctNov_query2");
//Media, contagem, soma de vendas por brand
(filtrada por purchase)
Dataset<Row> sqldf2 = spark
.sql("SELECT `brand`, avg(`price`) as
avg_price, count(*) as count, sum(`price`) as profits"
+ " FROM dados"
+ " WHERE `event_type` =
'purchase' "
+ "GROUP BY `brand` ORDER BY
profits DESC")
.repartition(1);
sqldf2.show();
sqldf2.coalesce(1).write().option("header",
true).csv("src/main/OctNov_query3");
//sqldf2.write().option("header",
true).csv("src/main/sqldf2_csv");
//Igualmente por Categoria
Dataset<Row> sqldf21 = spark
.sql("SELECT `category_code`, avg(`price`)
as avg_price, count(*) as count, sum(`price`) as profits"
+ " FROM dados"
+ " WHERE `event_type` =
'purchase' "
+ "GROUP BY `category_code`
ORDER BY profits DESC")
.repartition(1);
sqldf21.show();
sqldf21.coalesce(1).write().option("header",
true).csv("src/main/OctNov_query4");
//contagem numero de usuarios distintos
Dataset<Row> sqldf3 = spark.sql("SELECT month,
COUNT(DISTINCT `user_id`) as Dist_users, COUNT(DISTINCT `user_session`)
as Dist_sessions FROM dados "
+ "GROUP BY `month` ");
sqldf3.show();
sqldf3.coalesce(1).write().option("header",
true).csv("src/main/OctNov_query5");
//contagem sessoes distintas - visitas na loja
//Dataset<Row> sqldf31 = spark.sql("SELECT month,
COUNT(DISTINCT `user_session`) as SessionsDist FROM dados GROUP BY
`month` ");
//sqldf31.show();
//Produtos (ID) + vendidos, contagem
Dataset<Row> sqldf4 = spark.sql("SELECT `product_id`,
`category_code`, `brand`, count(*) as count_prod FROM dados "
+ "WHERE `event_type` = 'purchase'"
+ "GROUP BY `product_id`,`category_code`,
`brand` ORDER BY count_prod DESC");
sqldf4.show();
sqldf4.coalesce(1).write().option("header",
true).csv("src/main/OctNov_query6");
*/
//Usuarios que mais compram
Dataset<Row> sqldf8 = spark.sql("SELECT `user_id`,
count(*) from dados "
+ "WHERE `event_type` = 'purchase' "
+ "GROUP BY `user_id` ORDER BY count(*)
DESC" );
sqldf8.show();
Dataset<Row> sqldf9 = spark.sql("SELECT
category_code, count(*) from dados "
+ "WHERE `user_id` = 564068124 OR
`user_id` = 512386086 OR `user_id` = 549109608 OR `user_id` = 513320236
OR `user_id` = 543312954 "
+ "GROUP BY `category_code` ORDER BY
count(*) DESC" );
sqldf9.show();
Dataset<Row> sqldf10 = spark.sql("SELECT user_id,
min(`price`), max(`price`), avg(`price`) from dados "
+ "WHERE `user_id` = 564068124 OR
`user_id` = 512386086 OR `user_id` = 549109608 OR `user_id` = 513320236
OR `user_id` = 543312954 "
+ "AND `event_type`='purchase'"
+ "GROUP BY `user_id`");
sqldf10.show();
}
private static void runNovcsv(SparkSession spark2) throws
AnalysisException {
/*
Dataset<Row> df = spark.read().option("header",
true).option("inferSchema", true)
.option("timestampFormat", "yyyy-MM-dd
HH:mm:ss").csv("hdfs://localhost:9000/2019-Nov.csv");
df.printSchema();
//convert to parquet
df.coalesce(1).write().mode(SaveMode.Overwrite).parquet("hdfs://localhos
t:9000/parquet2");
df.printSchema();
df.show();
*/
Dataset<Row> df2 =
spark.read().parquet("hdfs://localhost:9000/parquet2");
df2.printSchema();
// Criando uma coluna "hour" e "day"
df2 = df2.withColumn("hour", hour(col("event_time")));
df2 = df2.withColumn("day_mth",
dayofmonth(col("event_time")));
df2 = df2.withColumn("day_wk",
dayofweek(col("event_time")));
df2.printSchema();
df2.show();
df2.createTempView("dados");
// Mudando o tipo da coluna price para double
df2 = df2.withColumn("price",
df2.col("price").cast(DataTypes.DoubleType));
//Queries com o tempo
Dataset<Row> sqlDF_time = spark.sql("SELECT price, hour,
day_mth, day_wk FROM dados"
+ " WHERE `event_type` = 'purchase' "
+ " ORDER BY hour DESC");
//criando outro temp view dados2 so com purchases de
cima, mth, dayweek, hour
sqlDF_time.createTempView("dados2");
Dataset<Row> sqlDF_time2 = spark.sql("SELECT hour,
count(*) as purchases FROM dados2 "
+ "GROUP BY `hour` ORDER BY hour DESC");
sqlDF_time2.show();
Dataset<Row> sqlDF_time3 = spark.sql("SELECT day_mth,
count(*) as purchases FROM dados2 "
+ "GROUP BY `day_mth` ORDER BY day_mth
DESC");
sqlDF_time3.show();
Dataset<Row> sqlDF_time4 = spark.sql("SELECT day_wk,
count(*) as purchases FROM dados2 "
+ "GROUP BY `day_wk` ORDER BY day_wk
DESC");
sqlDF_time4.show();
//Media, contagem, soma de vendas por marca (filtrada por
purchase)
Dataset<Row> sqldf2 = spark
.sql("SELECT `brand`, avg(`price`) as avg_price,
count(*) as count, sum(`price`) as profits"
+ " FROM dados"
+ " WHERE `event_type` = 'purchase'
"
+ "GROUP BY `brand` ORDER BY profits
DESC")
.repartition(1);
sqldf2.show();
//por Categoria
Dataset<Row> sqldf21 = spark
.sql("SELECT `category_code`, avg(`price`) as
avg_price, count(*) as count, sum(`price`) as profits"
+ " FROM dados"
+ " WHERE `event_type` = 'purchase'
"
+ "GROUP BY `category_code` ORDER BY
profits DESC")
.repartition(1);
sqldf21.show();
//contagem numero de usuarios distintos
Dataset<Row> sqldf3 = spark.sql("SELECT COUNT(DISTINCT
`user_id`) as usersDist FROM dados ");
//contagem sessoes distintas - visitas na loja
Dataset<Row> sqldf31 = spark.sql("SELECT COUNT(DISTINCT
`user_session`) as SessionsDist FROM dados ");
sqldf31.show();
sqldf3.show();
//Produtos (ID) + vendidos, contagem
Dataset<Row> sqldf4 = spark.sql("SELECT `product_id`,
`category_code`, `brand`, count(*) as count_prod FROM dados "
+ "WHERE `event_type` = 'purchase'"
+ "GROUP BY `product_id`,`category_code`,
`brand` ORDER BY count_prod DESC");
sqldf4.show();
//Marca + vendida, contagem
Dataset<Row> sqldf5 = spark.sql("SELECT `brand`, count(*)
as count_brand FROM dados "
+ "WHERE `event_type` = 'purchase'"
+ "GROUP BY `brand` ORDER BY count_brand DESC");
sqldf5.show();
//Tipos de eventos, contagem
Dataset<Row> sqldf6 = spark.sql("SELECT `event_type`,
count(*) as count_event FROM dados "
+ "GROUP BY `event_type` ORDER BY count_event
DESC");
sqldf6.show();
//Lucro total da loja no mes
Dataset<Row> sqldf7 = spark.sql("SELECT sum(price) as lucro
FROM dados WHERE `event_type` = 'purchase'");
sqldf7.show();
}
private static void runOctcsv(SparkSession spark) throws
AnalysisException {
/*
Dataset<Row> df = spark.read().option("header",
true).option("inferSchema", true)
.option("timestampFormat", "yyyy-MM-dd
HH:mm:ss").csv("hdfs://localhost:9000/2019-Oct.csv");
// df.show();
df.printSchema();
//convert to parquet
df.coalesce(1).write().mode(SaveMode.Overwrite).parquet("parquet1");
*/
Dataset<Row> df2 = spark.read().parquet("parquet1");
df2.printSchema();
// Criando uma coluna "hour" e "day"
df2 = df2.withColumn("hour", hour(col("event_time")));
df2 = df2.withColumn("day_mth", dayofmonth(col("event_time")));
df2 = df2.withColumn("day_wk", dayofweek(col("event_time")));
df2.printSchema();
df2.show();
df2.createTempView("dados");
// Mudando o tipo da coluna price para double
// df = df.withColumn("price",
df.col("price").cast(DataTypes.DoubleType));
//Queries com o tempo
Dataset<Row> sqlDF_time = spark.sql("SELECT price, hour, day_mth,
day_wk FROM dados"
+ " WHERE `event_type` = 'purchase' "
+ " ORDER BY hour DESC");
//criando outro temp view dados2 so com purchases de cima,
mth, dayweek, hour
sqlDF_time.createTempView("dados2");
Dataset<Row> sqlDF_time2 = spark.sql("SELECT hour, count(*)
as purchases FROM dados2 "
+ "GROUP BY `hour` ORDER BY hour DESC");
sqlDF_time2.show();
Dataset<Row> sqlDF_time3 = spark.sql("SELECT day_mth,
count(*) as purchases FROM dados2 "
+ "GROUP BY `day_mth` ORDER BY day_mth DESC");
sqlDF_time3.show();
Dataset<Row> sqlDF_time4 = spark.sql("SELECT day_wk,
count(*) as purchases FROM dados2 "
+ "GROUP BY `day_wk` ORDER BY day_wk DESC");
sqlDF_time4.show();
//Num de linhas
Dataset<Row> sqldf = spark.sql("SELECT count(*) as lines FROM
dados").repartition(1);
sqldf.show();
//Media, contagem, soma de vendas por marca (filtrada por
purchase)
Dataset<Row> sqldf2 = spark
.sql("SELECT `brand`, avg(`price`) as avg_price,
count(*) as count, sum(`price`) as profits"
+ " FROM dados"
+ " WHERE `event_type` = 'purchase' "
+ "GROUP BY `brand` ORDER BY profits
DESC")
.repartition(1);
sqldf2.show();
//por Categoria
Dataset<Row> sqldf21 = spark
.sql("SELECT `category_code`, avg(`price`) as
avg_price, count(*) as count, sum(`price`) as profits"
+ " FROM dados"
+ " WHERE `event_type` = 'purchase' "
+ "GROUP BY `category_code` ORDER BY
profits DESC")
.repartition(1);
sqldf21.show();
//contagem numero de usuarios distintos
Dataset<Row> sqldf3 = spark.sql("SELECT COUNT(DISTINCT `user_id`)
as usersDist FROM dados ");
//contagem sessoes distintas - visitas na loja
Dataset<Row> sqldf31 = spark.sql("SELECT COUNT(DISTINCT
`user_session`) as SessionsDist FROM dados ");
sqldf31.show();
sqldf3.show();
//Produtos (ID) + vendidos, contagem
Dataset<Row> sqldf4 = spark.sql("SELECT `product_id`, count(*) as
count_prod FROM dados "
+ "WHERE `event_type` = 'purchase'"
+ "GROUP BY `product_id` ORDER BY count_prod DESC");
sqldf4.show();
//Marca + vendida, contagem
Dataset<Row> sqldf5 = spark.sql("SELECT `brand`, count(*) as
count_brand FROM dados "
+ "WHERE `event_type` = 'purchase'"
+ "GROUP BY `brand` ORDER BY count_brand DESC");
sqldf5.show();
//Tipos de eventos, contagem
Dataset<Row> sqldf6 = spark.sql("SELECT `event_type`, count(*) as
count_event FROM dados "
+ "GROUP BY `event_type` ORDER BY count_event DESC");
sqldf6.show();
//Lucro total da loja no mes
Dataset<Row> sqldf7 = spark.sql("SELECT sum(price) as lucro FROM
dados WHERE `event_type` = 'purchase'");
sqldf7.show();
}
}
/*
* import org.apache.spark.api.java.JavaSparkContext; import
* org.apache.spark.sql.AnalysisException; import
org.apache.spark.sql.Dataset;
* import org.apache.spark.sql.Row; import
org.apache.spark.sql.SparkSession;
* import org.apache.spark.sql.functions;
*
*
* public class Q13 {
*
* public static SparkSession spark; public static JavaSparkContext sc;
*
* public static void main(String[] args) throws AnalysisException {
*
* spark =
*
SparkSession.builder().appName("crimesCountSql").master("local").getOrCr
eate(
* );
*
* sc = new JavaSparkContext(spark.sparkContext());
sc.setLogLevel("WARN");
*
* Dataset<Row> dfCrimes = spark.read().option("header",
* true).csv("hdfs://spark01:9000/Crimes_-_2001_to_present.csv.gz");
*
* dfCrimes.printSchema();
*
* // dfCrimes.show();
*
* //Dataset<Row> crimesCount = dfCrimes.groupBy("Primary
Type").count(); //
* crimesCount.orderBy(functions.desc("count")).show();
*
* dfCrimes.createTempView("crimes");
*
* Dataset<Row> crimesCount2 = spark.
* sql("SELECT `Primary Type` as ptype, count(*) as count FROM crimes
GROUP BY ptype ORDER BY count DESC"
* ).repartition(1);
*
* crimesCount2.write().option("header",
* true).csv("file:///home/felipefg/Infnet/contagem_de_crimes_csv");
* crimesCount2.write().json(
* "file:///home/felipefg/Infnet/contagem_de_crimes_json");
*
* }
*
* }
*/
package pb_classification;
public class Classification {
public static void main(String[] args) throws AnalysisException {
// Instanciando um contexto Spark
SparkSession spark = SparkSession
.builder()
.master("local[*]")
.appName("Pca Classification")
.getOrCreate();
Logger rootLogger = Logger.getRootLogger();
rootLogger.setLevel(Level.ERROR);
// clusteringtest(spark);
// Pca_classification_decisiontree(spark);
// association(spark);
colabfiltering(spark);
}
private static void colabfiltering(SparkSession spark) {
//Testando para Filtragem Colaborativa (ALS)
// Vamos criar um Dataframe df a aprtir de dados em nosso arquivo
de dados.
Dataset<Row> df =
spark.read().parquet("src/main/resources/parquetOct2019");
df.createOrReplaceTempView("cosme");
//Filtrando por `purchases`
Dataset<Row> sqlDF0 = spark.sql("SELECT * from cosme
WHERE event_type='purchase' " );
sqlDF0.createOrReplaceTempView("store");
Dataset<Row> ratings = spark.sql("SELECT `user_id`,
`product_id`, count(*) as qty from store"
+ " GROUP BY `user_id`, `product_id` ORDER BY qty
DESC");
ratings.createOrReplaceTempView("store2");
Dataset<Row> ratings2 = spark.sql("SELECT `user_id`,
`product_id`, qty from store2 WHERE qty>0"
+ " ORDER BY qty DESC");
//Dataset<Row> ratings = spark.read().option("header",
true).option("inferSchema", true)
// .csv("src/main/ALS/*.csv"); // Base OctNov2019
System.out.println("Base original 2 linhas:" + ratings2.count());
ratings2 = ratings2.sample(1.0);
// Imprime o schema da tabela em formto de arvore
ratings2.printSchema();
ratings2.show();
//Dataset<Row> ratings = spark.createDataFrame(ratingsRDD,
Rating.class);
Dataset<Row>[] splits = ratings2.randomSplit(new double[]{0.8,
0.2});
Dataset<Row> training = splits[0];
Dataset<Row> test = splits[1];
// Build the recommendation model using ALS on the training data
ALS als = new ALS()
.setMaxIter(10)
.setRegParam(0.01)
.setImplicitPrefs(true)
.setUserCol("user_id")
.setItemCol("product_id")
.setRatingCol("qty");
ALSModel model = als.fit(training);
// Evaluate the model by computing the RMSE on the test data
// Note we set cold start strategy to 'drop' to ensure we don't
get NaN evaluation metrics
model.setColdStartStrategy("drop");
Dataset<Row> predictions = model.transform(test);
RegressionEvaluator evaluator = new RegressionEvaluator()
.setMetricName("rmse")
.setLabelCol("qty")
.setPredictionCol("prediction");
Double rmse = evaluator.evaluate(predictions);
System.out.println("Root-mean-square error = " + rmse);
// Generate top 5 movie recommendations for each user
Dataset<Row> userRecs = model.recommendForAllUsers(5);
userRecs.printSchema();
userRecs.show();
//userRecs.coalesce(1).write().json("ALSresult1json");
/*
// Generate top 5 user recommendations for each movie
Dataset<Row> movieRecs = model.recommendForAllItems(5);
movieRecs.show();
//movieRecs.write().csv("ALSresult2csv");
*/
// Generate top 10 movie recommendations for a specified set of
users
Dataset<Row> users =
ratings.select(als.getUserCol()).distinct().limit(3);
Dataset<Row> userSubsetRecs = model.recommendForUserSubset(users,
3);
//userSubsetRecs.coalesce(1).write().json("3recs");
// Generate top 10 user recommendations for a specified set of
movies
//Dataset<Row> movies =
ratings.select(als.getItemCol()).distinct().limit(3);
//Dataset<Row> movieSubSetRecs =
model.recommendForItemSubset(movies, 10);
//Teste para precisao e recall
//Dataset<Row> json1 =
spark.read().json("ALSresult1json/*.json");
userRecs.createOrReplaceTempView("json2");
Dataset<Row> sqlDF = spark.sql("SELECT user_id,
inline(recommendations) from json2");
sqlDF.createOrReplaceTempView("json3");
Dataset<Row> sqlDF2 = spark.sql("SELECT `user_id`,
COLLECT_SET(product_id) as items from json3"
+ " GROUP BY `user_id`");
sqlDF2.printSchema();
//sqlDF2.show();
/*
Dataset<Row> ratings = spark.read().option("header",
true).option("inferSchema", true)
.csv("src/main/ALS/*.csv"); // Base OctNov2019
System.out.println("Ratings Lines:" + ratings.count());
ratings = ratings.sample(0.1);
*/
ratings = ratings.select(col("user_id"),col("product_id"));
ratings.printSchema();
//ratings.show();
Dataset<Row> joinratings = ratings.join(sqlDF2,"user_id");
joinratings.printSchema();
//joinratings.show();
joinratings.createOrReplaceTempView("join");
Dataset<Row> sqlDFjoin = spark.sql("SELECT user_id, product_id,
items,"
+ " array_contains(`items`, `product_id`) as
array_contains from join");
sqlDFjoin.printSchema();
//sqlDFjoin.show();
sqlDFjoin.groupBy("array_contains").count().show();
sqlDFjoin.createOrReplaceTempView("join2");
Dataset<Row> sqlDFjoin2 = spark.sql("SELECT user_id, count(*) as
n_count, "
+ "SUM(CASE WHEN array_contains THEN 1 ELSE 0 END) as
sum_true, "
+ "SUM(CASE WHEN array_contains THEN 1 ELSE 0
END)/count(*) as recall,"
+ "SUM(CASE WHEN array_contains THEN 1 ELSE 0 END)/5
as precision" // 10 recs items
+ " from join2 GROUP BY `user_id`");
sqlDFjoin2.show();
//sqlDFjoin2.coalesce(1).write().csv("src/main/resources/sqlDfjoin5");
sqlDFjoin2.createOrReplaceTempView("join3");
Dataset<Row> sqlDFjoin3 = spark.sql("SELECT avg(`precision`),
avg(`recall`) from join3");
sqlDFjoin3.show();
//SELECT array_contains(array(1, 2, 3), 2);*/
}
private static void association(SparkSession spark) {
// Vamos criar um Dataframe df a aprtir de dados em nosso arquivo
de dados.
Dataset<Row> df =
spark.read().parquet("hdfs://localhost:9000/parquetOctNov");
// Imprime o schema da tabela em formto de arvore
df.printSchema();
//Contando os tipos de eventos
Dataset<Row> typesCount = df.groupBy("event_type").count();
typesCount.orderBy(functions.desc("count")).show();
df.describe("price").show();
df.show();
// Registra o Dataframe como um SQL view temporario
df.createOrReplaceTempView("cosme");
//Filtrando por `purchases`
Dataset<Row> sqlDF0 = spark.sql("SELECT * from cosme WHERE
event_type='view' " );
sqlDF0.show();
System.out.println("Tamanho de linhas: "+sqlDF0.count());
sqlDF0.createOrReplaceTempView("store");
//Dataset<Row> sqlDF = spark.sql("SELECT *,YEAR(event_time) as
year,MONTH(event_time) as month, DAY(event_time) as day,
HOUR(event_time) as hour FROM cosme ORDER BY price");
//sqlDF.show();
Dataset<Row> sqlDF = spark.sql("SELECT `user_session`,
COLLECT_SET(product_id) as items, SIZE(COLLECT_SET(product_id)) as size
from store"
+ " GROUP BY `user_session` ORDER BY size DESC");
sqlDF = sqlDF.filter(col("size").gt(1));
sqlDF.show();
//Applying FPgrowth model
FPGrowthModel model = new FPGrowth()
.setItemsCol("items")
.setMinSupport(0.0001)
.setMinConfidence(0.5)
.fit(sqlDF);
// Display frequent itemsets.
model.freqItemsets().orderBy((functions.desc("freq"))).show();
// Display generated association rules.
model.associationRules().orderBy((functions.desc("confidence"))).show();
// transform examines the input items against all the
association rules and summarize the
// consequents as prediction
//model.transform(sqlDF).show();
}
private static void Pca_classification_decisiontree(SparkSession
spark) throws AnalysisException {
//Dataset<Row> df = spark.read().option("header",
true).option("inferSchema", true)
//.option("timestampFormat", "yyyy-MM-dd
HH:mm:ss").csv("src/main/resources/2019-Oct.csv");
//convert to parquet
//df.coalesce(1).write().mode(SaveMode.Overwrite).parquet("src/main/reso
urces/parquetOct2019");
Dataset<Row> df_bruto =
spark.read().parquet("src/main/resources/parquetJan2020");
// Dataset<Row> df_bruto =
spark.read().parquet("hdfs://localhost:9000/parquetJan");
df_bruto.printSchema();
df_bruto.show();
df_bruto = df_bruto.sample(0.03); //500k lines ou 1M
//Contando os tipos de eventos
Dataset<Row> typesCount =
df_bruto.groupBy("event_type").count();
typesCount.orderBy(functions.desc("count")).show();
System.out.println("Numero de linhas parquet: " +
df_bruto.count());
df_bruto.createTempView("data1");
Dataset<Row> df_bruto2 = spark.sql("SELECT * from
data1 WHERE `event_type`='cart' OR `event_type`='purchase'");
df_bruto2.show();
//df_bruto.describe().show();
// Criando uma coluna "hour" e "day"
df_bruto2 = df_bruto2.withColumn("hour",
hour(col("event_time")));
df_bruto2 = df_bruto2.withColumn("day_mth",
dayofmonth(col("event_time")));
df_bruto2 = df_bruto2.withColumn("day_wk",
dayofweek(col("event_time")));
//df2 = df2.withColumn("month",
month(col("event_time")));
df_bruto2.show();
//String Indexer stage
StringIndexer categoryIndexer = new
StringIndexer().setHandleInvalid("keep")
.setInputCol("category_code")
.setOutputCol("categoryIndex");
StringIndexer brandIndexer = new
StringIndexer().setHandleInvalid("keep")
.setInputCol("brand")
.setOutputCol("brandIndex");
StringIndexer labelIndexer = new
StringIndexer().setHandleInvalid("keep")
.setInputCol("event_type")
.setOutputCol("labelIndex");
OneHotEncoderEstimator encoder = new
OneHotEncoderEstimator()
.setInputCols(new String[]
{"categoryIndex"
,
"brandIndex"
})
.setOutputCols(new String[]
{"categoryVec"
, "brandVec"
});
VectorAssembler assembler = (new VectorAssembler()
.setInputCols(new
String[]{"price","hour","day_wk","day_mth","categoryVec"
,"brandVec"}))
.setOutputCol("features");
StandardScaler scaler = new StandardScaler()
.setInputCol("features")
.setOutputCol("scaledFeatures")
.setWithMean(true)
.setWithStd(true);
PCA pca = (new PCA()
.setInputCol("scaledFeatures")
.setOutputCol("pcaFeatures")
.setK(4));
//Pipeline
Pipeline pipeline = new Pipeline()
.setStages(new
PipelineStage[]{categoryIndexer
,brandIndexer
,labelIndexer
,encoder
,assembler
,scaler
,pca});
//Fit the pipeline to training documents.
PipelineModel model = pipeline.fit(df_bruto2);
//Get Results on Test Set
Dataset<Row> df_tratado = model.transform(df_bruto2);
System.out.println("Dataframe transformado:");
df_tratado.show();
Dataset<Row> df_model =
df_tratado.select(col("labelIndex"), col("pcaFeatures"));
System.out.println("Dataframe modelado:");
df_model.show();
//Treino e Teste set
Dataset<Row>[] split = df_model.randomSplit(new
double[] {0.7, 0.3});
Dataset<Row> train = split[0];
Dataset<Row> test = split[1];
//Log Regression
LogisticRegression lr = new LogisticRegression()
.setLabelCol("labelIndex")
.setFeaturesCol("pcaFeatures");
LogisticRegressionModel lr_model = lr.fit(train);
Dataset<Row> predictions = lr_model.transform(test);
System.out.println("Base de teste tem " +
predictions.count() + " linhas");
predictions.groupBy(col("labelIndex")).count().show();
/*
//Decision Tree
DecisionTreeClassifier dt = new
DecisionTreeClassifier()
.setLabelCol("labelIndex")
.setFeaturesCol("pcaFeatures");
DecisionTreeClassificationModel dt_model =
dt.fit(train);
Dataset<Row> predictions = dt_model.transform(test);
System.out.println("Base de teste tem " +
predictions.count() + " linhas");
predictions.groupBy(col("labelIndex")).count().show();
System.out.println(dt_model.toDebugString());
*/
//////////////////////////////////
////MODEL EVALUATION /////////////
//////////////////////////////////
//View results
System.out.println("Logistic Regression Result sample
:");
predictions.select("labelIndex", "prediction",
"pcaFeatures").show(5);
//View confusion matrix
System.out.println("Logistic Regression Confusion
Matrix :");
predictions.groupBy(col("labelIndex"),
col("prediction")).count().show();
predictions.groupBy(col("prediction")).count().show();
//Accuracy computation
MulticlassClassificationEvaluator evaluator = new
MulticlassClassificationEvaluator()
.setLabelCol("labelIndex")
.setPredictionCol("prediction")
.setMetricName("accuracy");
double accuracy = evaluator.evaluate(predictions);
System.out.println("Logistic Regression Accuracy = "
+ Math.round( accuracy * 100) + " %" );
}
}