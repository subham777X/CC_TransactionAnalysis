create database CCTransactions; --creating the database--

Create schema bronze;           --creating the schemas--
create schema silver;
create schema gold;

drop table if exists bronze.cctrans;  --creating the table ---
create table bronze.cctrans (
	sr_number int,
	trans_date_trans_time timestamp,
	cc_num bigint,
	merchant text,
	category varchar(20),
	amount numeric(10,2),
	first_name text,
	last_name text,
	gender varchar(10),
	street text,
	city varchar(30),
	state varchar(10),
	zip varchar(10),
	latitude double precision,
	longitude double precision,
	city_population int,
	job text,
	dob date,
	trans_num varchar(40),
	unix int,
	merch_lat double precision,
	merch_lon double precision,
	is_fraud int,
	merch_zip varchar(10)
);

truncate table bronze.cctrans;  --loading the data--
copy bronze.cctrans 
from 'C:/Users/Public/sql project data/credit_card_transactions.csv'
with (
 format CSV,
 header true,
 delimiter ','
);


--select * from bronze.cctrans limit 20; --Checking the data ---
--select count(*) from bronze.cctrans;


select           --dropped the sr.no
concat(first_name,' ',last_name) as full_name,  --merged the first name and last name as full name
age(dob) as age,  --got age from the DOB
cc_num,
trans_date_trans_time,
replace(lower(merchant),'fraud_','') as merchant,  -- we had fraud_  like a prefix in this data set so removed it
category,
amount,
gender,
street,
city,
state,
zip,
latitude,
longitude,
city_population,
job,
trans_num,       --dropped unix
merch_lat,
merch_lon,
is_fraud,
merch_zip
from bronze.cctrans 
limit (100)


drop table if exists silver.cctrans;   --creating silver layer table
create table silver.cctrans(
full_name text,
age int ,
cc_num bigint,
trans_date_trans_time timestamp,
merchant text,
category varchar(20),
amount numeric(10,2),
gender varchar(2),
street text,
city varchar(30),
state varchar(10),
zip varchar(10),
latitude double precision,
longitude double precision,
city_population int,
job text,
trans_id varchar(40),
merch_lon double precision,
merch_lat double precision,
is_fraud varchar(2),
merch_zip varchar(10)
);


truncate table silver.cctrans;
insert into silver.cctrans(
	full_name ,
	age  ,
	cc_num ,
	trans_date_trans_time ,
	merchant ,
	category ,
	amount ,
	gender ,
	street ,
	city ,
	state ,
	zip ,
	latitude ,
	longitude ,
	city_population ,
	job,
	trans_id ,
	merch_lon ,
	merch_lat ,
	is_fraud ,
	merch_zip 
) select           --dropped the sr.no
concat(first_name,' ',last_name) as full_name,  --merged the first name and last name as full name
extract(year from age(dob))::int as age,  --got age from the DOB
cc_num,
trans_date_trans_time,
replace(lower(merchant),'fraud_','') as merchant,  -- we had "fraud_" prefix in this data set so removed it
category,
amount,
gender,
street,
city,
state,
zip,
latitude,
longitude,
city_population,
job,
trans_num,       --dropped unix time as it doesn't align with the transaction time
merch_lat,
merch_lon,
is_fraud,
merch_zip
from bronze.cctrans 
;

select * from silver.cctrans
where merch_zip = '01001'


update silver.cctrans c1               --to replace the null value with the avialable merch_zip code from the dataset
set merch_zip = c2.merch_zip           ---executed at the end
from (select merchant , min(merch_zip) as merch_zip
from silver.cctrans where merch_zip is not null
group by merchant) c2
where c1.merch_zip is null
and c1.merchant = c2.merchant

select c1.merchant , c1.merch_zip as old_zip, c2.merch_zip as new_zip  ---just to check how the results pan out
from silver.cctrans c1 join 
(select merchant, min(merch_zip) as merch_zip 
from silver.cctrans 
where merch_zip is not null
group by 1) c2
on c1.merchant = c2.merchant
where c1.merch_zip is null

select count(*) from silver.cctrans 
where merch_zip is null


create temp table temp_cctrans as 
select * from silver.cctrans;

update temp_cctrans c1               --to replace the null value with the avialable merch_zip code from the dataset
set merch_zip = c2.merch_zip         ---checking it in the test run using a temp table
from (select merchant , min(merch_zip) as merch_zip
 from silver.cctrans where merch_zip is not null
 group by merchant) c2
where c1.merch_zip is null
and c1.merchant = c2.merchant

select * from temp_cctrans   
where merch_zip is null

drop table temp_cctrans;     --deleted the temp table

select * from silver.cctrans    
where merch_zip is null    

----now loading the cleaned data to the gold layer----

drop table if exists gold.cctrans;
create table gold.cctrans(
	full_name text,
	age int ,
	cc_num bigint,
	trans_date_trans_time timestamp,
	merchant text,
	category varchar(20),
	amount numeric(10,2),
	gender varchar(2),
	street text,
	city varchar(30),
	state varchar(10),
	zip varchar(10),
	latitude double precision,
	longitude double precision,
	city_population int,
	job text,
	trans_id varchar(40),
	merch_lon double precision,
	merch_lat double precision,
	is_fraud varchar(2),
	merch_zip varchar(10)
);

SELECT column_name, data_type         --checking the metadata
FROM information_schema.columns
WHERE table_name = 'cctrans'
  AND table_schema = 'gold';

truncate table gold.cctrans;    ---just copying the table as all the data is cleaned and feature engineered 
insert into gold.cctrans(
select *
from silver.cctrans
);

----now connecting  the data to tableau