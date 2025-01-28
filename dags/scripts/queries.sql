SELECT * FROM public.new_releases;



select spotify_available_date, count(distinct id)
from new_releases
group by spotify_available_date;


select spotify_available_date, release_date, count(distinct id)
from new_releases
group by spotify_available_date, release_date
order by spotify_available_date, release_date, count(distinct id)