# README.md
ETL-project


# import section
from pyspark.sql import SparkSession  
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from pyspark.sql.functions import col, count, when, floor, rand, split, expr, to_date
from pyspark.sql import functions as F
import random


# create a spark session 
spark = SparkSession.builder \
    .appName("Facebook") \
    .getOrCreate()


# assign schema for each column
schema = StructType([
    StructField("sr_no", IntegerType(), True),
    StructField("Page_total_likes", IntegerType(), True),
    StructField("Type", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Post_Month", StringType(), True),
    StructField("Post_Weekday", StringType(), True),
    StructField("Post_Hour", IntegerType(), True),
    StructField("Paid", BooleanType(), True),
    StructField("Post_Total_Reach", IntegerType(), True),
    StructField("Post_Total_Impressions", IntegerType(), True),
    StructField("Engaged_Users", IntegerType(), True),
    StructField("Post_Consumers", IntegerType(), True),
    StructField("Post_Consumptions", IntegerType(), True),
    StructField("who_liked_your_Page", StringType(), True),
    StructField("Post_reach_people_like_your_Page", IntegerType(), True),
    StructField("liked_your_Page_and_engaged_your_post", IntegerType(), True),
    StructField("comment", IntegerType(), True),
    StructField("like", IntegerType(), True),
    StructField("share", IntegerType(), True),
    StructField("Total_Interactions", IntegerType(), True)
])


# reading data from facebook file
fb_data = spark.read.format("csv").option("header", True).schema(schema).load("/FileStore/tables/Facebook_Metrics_of_Cosmetic_Brand-1.csv")


# display schema of fb_data
fb_data.printSchema()

# display first six(6) rows
fb_data.show(6)

# total rows in this dataframe
fb_data.count()

# delete this three column "like", "share", "paid" which have null values occurs
fb_data1 = fb_data.drop("like", "share","paid")

# filtering for those records which have not occur NULL values
Facebook_final_datas= fb_data1.filter(col("Total_Interactions").isNotNull())

# displaying this dataframe 👉 Facebook_final_datas
Facebook_final_datas.show()

# after handling missing value or incorrect value
Facebook_final_datas.count() 




# schema assigning with another method for email_campaign
schema_for_email="ad_id int,xyz_campaign_id int,fb_campaign_id int,age string,gender string,interest integer,Impressions integer,Open integer,Spent Double,Total_Conversion 
integer,Approved_Converion integer"

# reading data from email file
email_campaign = spark.read.format("csv").option("header",True).schema(schema_for_email).load("/FileStore/tables/email_campaign_data.csv")  

# displaying  first(Top) 10 rows from email-campaign
email_campaign.show(10)

# Displaying schema for email-campaign
email_campaign.printSchema() 

# counting total rows in email-campaign
email_campaign.count()

# storing total null values occur in each column of email_campaign in null-count
null_count= email_campaign.select([ count(when(col(c).isNull(), c)).alias(c) for c in email_campaign.columns ])

# displaying Count the total null values in each column 
null_count.show()

# remove rows where "interest" is null
email_campaign1= email_campaign.dropna(subset="interest")

# displaying records after remove null value from interest
email_campaign1.show()

# count total records after delete the rows where "interest" is null
email_campaign1.count()


# add new column "Approved_Conversion"
# if "Approved_Converion" is null then fill with 0 or 1 randomaly, then delete the column "Approved_Converion"
email_data_filled = email_campaign1.withColumn("Approved_Conversion", 
    when(col("Approved_Converion").isNull(), random.choice([0, 1])).otherwise(col("Approved_Converion")) ).drop("Approved_Converion")

# displaying email-data-filled
email_data_filled.show()

# counting total rows of email_data_filled
email_data_filled.count()

# add new column "general_id" with random floating numbers, then converting to integer
email_campaign_with_general_id = email_data_filled.withColumn(
    "general_id", 
    (floor(rand() * 700) + 1).cast("int"))

# splitting age from range e.x.= given age is 30-40 then 30, then cast string to integer
email_age_Casted= email_campaign_with_general_id.withColumn("Age",  
    ((split(col("Age"), "-").getItem(0).cast("int") ) ))

# after preforming all transform
email_age_Casted.show()



# schema for web-data
schema_web = "Page_Views Integer,Session_Duration Double,Bounce_Rate Double,Traffic_Source String,Time_on_Page Double,Previous_Visits Integer,Conversion_Rate Double"

# reading website_data file
web_data = spark.read.format("csv").option("header", True).schema(schema_web).load("/FileStore/tables/website_data.csv")

# displying whole dataframe
web_data.show()

# counting total rows of web-data dataframe
web_data.count()

# drona() is used for removing all rows where null occur
web_data1= web_data.dropna()

# counting total rows from web-data1
web_data1.count()

# calculate difference between Start-date & End-date
start_date = "2022-01-01"
end_dat e=   "2024-01-01"
date_diff= F.datediff(F.lit(end_date), F.lit(start_date))

# adding "random number" of days between[0 & date_diff]  to starting-date
df_random_date= web_data1.withColumn("random_dates",
        F.date_add(F.lit(start_date), (rand() * date_diff).cast("int") ))  


# displaying only "random_dates" which is [yyyy-dd-mm]
df_random_date.select(col("random_dates")).show()

# displaying whole df_random_date(dataframe)
df_random_date.show()

# adding new column "new-date", then converting to this 👉 format[yyyy-mm-dd], then delete the column "random_dates"
after_normalize_random_dates= df_random_date.withColumn("new-date",
        to_date(col("random_dates"), "yyyy-dd-mm"  )).drop("random_dates")

# displaying whole dataframe[after_normalize_random_dates]
after_normalize_random_dates.show()


# add new column 👉"id", It have 400 to 1100, then conerting to integer format
final_web_data=  after_normalize_random_dates.withColumn("id",
        (rand() * 700 + 400 ).cast("int") )


# display dataframe after adding "id" column
final_web_data.show()

# counting total rows of 👉 final_web_data
final_web_data.count()


# join the [Facebook_final_datas] with [email_age_Casted]
fb_and_email=  Facebook_final_datas.join(email_age_Casted, Facebook_final_datas.sr_no == email_age_Casted.general_id, how="inner")

# displaying 👉 fb_and_email
fb_and_email.show()

# counting total rows 👉 fb_and_email
fb_and_email.count()

# Join [fb_and_email] with [final_fb_email_web]
final_fb_email_web= fb_and_email.join(final_web_data, fb_and_email.sr_no == final_web_data.id, how="inner")  

# counting total rows of [final_fb_email_web]
final_fb_email_web.count()

# delete 👉 [general_id], [id]
fb_amail_web=  final_fb_email_web.drop("general_id", "id")

# printing schema of [fb_amail_web]
fb_amail_web.printSchema()

# delete some rows which is useless
final_data= fb_amail_web.drop("Category", "who_liked_your_Page", "Post_reach_people_like_your_Page", "liked_your_Page_and_engaged_your_post", "comment", "interest", "Impressions", "open", "spent")


# displaying after all(3) dataframe joined
final_data.show()


# for connecting with snowflake
spark = SparkSession.builder \
    .appName("Load Data to Snowflake") \
    .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:8.38.3,net.snowflake:snowflake-jdbc:8.38.3") \
    .getOrCreate()

# I have created Database:- [ETL_DB] & schema:- [ETL_SCHEMA] in this warehouse [COMPUTE_WH] on Snowflake, then created table:- [Project]
# define for snowflake connection
option_for_snowflake= {
    "sfUrl": "https://sh42165.ap-southeast-1.snowflakecomputing.com",
    "sfUser": "SOHAM",
    "sfPassword": " ",
    "sfDatabase": "ETL_DB",
    "sfSchema": "ETL_SCHEMA",
    "sfWarehouse": "COMPUTE_WH"
}


# writing final_data  into  snowflake, [overwrite]  is used for updating data, when we want to appending the all file then used [append] mode
final_data.write.format("snowflake").options(**option_for_snowflake).option("dbtable", "project").mode("overwrite") \
.save()





# displaying all data 
select * from project;

# configuration for Connect to databricks with snowflake
SELECT CURRENT_VERSION();

# rename the column name from [sr_no] to [user_id]
alter table project
rename column sr_no to user_id;


# (1) return the sum of total_reach, engaged_user, interaction by each user on each day
select  
    user_id,
    "new-date" as post_date,
    sum(Post_Total_Reach) as total_reach,
    sum(Engaged_Users) as total_engaged_users,
    sum(Total_Interactions) as total_interactions
from 
    project 
group by 
    user_id, "new-date"
order by 
    user_id, post_date ;


# (2) counting opening-rate & ctr 
select
    xyz_campaign_id as campaign_id,
    sum(impressions) as total_impressions,
    sum(open) as total_opens,
    sum(total_conversion) as total_conversions,
    case 
        when sum(impression)>0 then (sum(open)* 100)/ sum(impressions) else 0 end as opening_rate,
    case 
        when sum(impression)>0 then (sum(total_conversion)* 100)/ sum(impressions) else 0 end as ctr
from  
    project
group by xyz_campaign_id
order by xyz_campaign_id ;



# (2) counting Web traffic metrics
select
    "new-date" as date,
    count(distinct user_id) as unique_session,
    avg(session_duration)as avg_session_duration,
    avg(Bounce_Rate) as avg_bounce_rate,   
    sum(Total_Conversion) as total_conversions,
    avg(Conversion_Rate) as avg_conversion_rate 
from project
group by "new-date"
order by "new-date" ;


# (3) adding new campaigns for future
create table campaign (
    campaign_id INTEGER AUTOINCREMENT PRIMARY KEY,
    campaign_name VARCHAR(100) not null,
    created_at date DEFAULT current_date,
    updated_at date
);
        

# using CTAS concept to making new-table[SocialMediaPostMetrics] from existing[project] table for future 
create table SocialMediaPostMetrics as
select * from project;

# want to see the discription
DESC table SocialMediaPostMetrics;

# adding constraint that foreign key that references that campaign's [campaign_id]
alter table SocialMediaPostMetrics
add constraint fk_campaign FOREIGN KEY (FB_CAMPAIGN_ID) REFERENCES campaign(campaign_id);

# remove columns [ad_id],[xyz_campaign_id]
alter table project                                        
drop column ad_id, xyz_campaign_id ;


# weekly trends
select 
    DATE_TRUNC('week', "new-date") as week_start,  -- group by week
    count(distinct user_id) as total_users,        -- total unique users
    count(type) AS total_comments,              -- total comments
    sum(Engaged_Users) AS total_engaged_users,   -- sum of engaged users
    avg(Conversion_Rate) AS avg_conversion_rate, -- average conversion rate
from
    project  
group by 
    DATE_TRUNC('week', "new-date")  -- grouping by week based on the date column
order by
    week_start desc;                     -- Order by the most recent week first


# top 3 email[user_id]  by click-through rate.
select  
    user_id, 
    (sum(total_conversion) / sum(total_interactions)) * 100 as click_through_rate
from 
    project
where 
    total_interactions > 0  -- Ensure that Impressions are non-zero to avoid division by zero
group by 
    user_id
order by 
    click_through_rate desc
limit 3;


# web traffic by traffic
with cte_traffic as (
select 
    traffic_source,
    sum(page_views) as total_page_views,                 
    avg(session_duration) as avg_session_duration,
    avg(bounce_rate) as avg_bounce_rate,
    avg(conversion_rate) as avg_conversion_rate,
    sum(total_conversion) as total_conversions,
    count(distinct user_id) as unique_users
from 
    project 
group by 
    traffic_source
)
select * from cte_traffic 
order by 
    total_page_views desc;





# Building dashboard on using Power BI tool
1. download the Power BI from Microsoft store
2. Home-> Get data-> more...-> snowflake-> connect-> I given my server of snowflake->  given my warehouse-name of snowflake->  ok-> given User-name of snowflake-> given password of sonwflake's account
3. I selected my database 👉[ETL_DB], schema 👉[ETL_SCHEMA], then select my table 👉[project], then import in to Power BI. 


My dashboard's Title is:- "Social Media Performance"
(1) My first visualization using: Slicer
    put [traffic_source] in to the field of Visualizations Section

(2) My second visulization using: Line Chart
    put [new-date] & [Year] in to the X-axis of Visualizations Section
    put [post_total_impressions] & [post_total_reach] in to the Y-axis Visualizations Section

(3) My third Visualizations using: Clustered column Chart
    put [post_weekday]  in to the X-axis of Visualizations Section
    put [engaged_users] in to the Y-axis of Visualizations Section

(4) My four Visualizations using: stacked column Chart
    put [type] in to the X-axis of Visualizations Section
    put [post_total_impressions] in to the Y-axis of Visualizations Section


# I have attached a photo and PDF of the dashboard I created.
![Dashboard of Project](https://github.com/user-attachments/assets/e6aa5799-4d00-45fa-a8f3-967a92f0d313)

[Dashboard  .pdf](https://github.com/user-attachments/files/17349808/Dashboard.pdf)

