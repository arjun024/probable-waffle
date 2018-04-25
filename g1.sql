create view adultg as 
select *, case when marital_status like '%Married%' then 0 else 1 end as g1 from adult;