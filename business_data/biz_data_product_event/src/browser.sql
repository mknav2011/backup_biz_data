WITH data as(
select distinct    
 concat(regexp_split(split(reverse(split(reverse("$PATH") ,'/')[1]), '.')[1] , '-|_')[2]
,regexp_split(split(reverse(split(reverse("$PATH") ,'/')[1]), '.')[1] , '-|_')[3]
        ) as created_seq
, id, name
from move_dl.browser_raw
), ranked as(
select *, row_number() over(partition by id order by created_seq desc) as rnk
from data
)
select distinct id, name, created_seq
from ranked
where rnk = 1
