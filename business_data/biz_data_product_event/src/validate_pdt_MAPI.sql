WITH pdt as(
select cast (array_join(array [year,month, day], '-') as date) dt , cardinality(array_agg(distinct  hour  ) )as hours_list_ct, count(1) as ct
from cnpd_mapi_pdt.mapi   
 where year = '2017'
 -- and month = '11'
 -- and day = '12'
-- and hour = '01'
group by 1
  ), ts as(
select  fulldate 
from domain_review.date_dim
    )
select fulldate, hours_list_ct, ct
from ts 
left outer join pdt on (ts.fulldate = pdt.dt)
where cast(fulldate as varchar) like '2017-1%'
and fulldate < date_add('day', -1, current_date)
and hours_list_ct < 24
order by fulldate desc

