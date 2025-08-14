select merchant, merch_zip from bronze.cctrans 
where merchant in (select distinct(merchant) from bronze.cctrans where merch_zip is null) 
limit(10000)
--this is to check where merchant codes are null but the merchants have a zip code--


--so now we have to get rid of sr_nuber(as thats of no use to us), concat the first name and last name to full name , get rid of 
--"fraud_" from the merchant name , drop dob and get age instead, replace null with 0 as they can't be dropped as they
--have some transactional value wich could result in changes in the average transactions etc
--also dropped the unix time as we didnt need it we already have transaction time 


select * from bronze.cctrans limit 10;

select * from silver.cctrans
limit 100;

select merchant , merch_zip      --checked for the matching names for the merch_zip to where it is null in the other records
from silver.cctrans c1 where exists
(select 1 from silver.cctrans c2
where c1.merchant = c2.merchant
and merch_zip is null)

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

select full_name , cc_num from silver.cctrans  
group by 2,1
   


select * from silver.cctrans
 

