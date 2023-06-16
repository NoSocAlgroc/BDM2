-- VISTAS

-- vistes per district

create or replace view idealista_district as
select "districtID" as districtID, district_name, sum(bathrooms) as bathrooms, sum("numPhotos") as numPhotos, sum(price) as price, sum("priceByArea") as priceByArea, sum(latitude) as latitude, sum(longitude) as longitude, sum(distance) as distance, sum(exterior) as exterior, sum(has360) as has360, sum("has3DTour") as has3DTour, sum("hasLift") as hasLift, sum("hasPlan") as hasPlan, sum("hasStaging") as hasStaging, sum("hasVideo") as hasVideo, sum("newDevelopment") as newDevelopment, sum("showAddress") as showAddress, sum("topNewDevelopment") as topNewDevelopment, sum("count") as count   
from idealista
where "districtID" is not null  
group by "districtID", district_name


create or replace view income_district as
select "districtID" as districtID, district_name, sum(rfd) as rfd, sum(population) as population 
from income
where "districtID" is not null
group by "districtID" , district_name

create or replace view price_district as
select "districtID" as districtID, sum(amount) as amount, sum("perMeter") as perMeter, sum("diffAmount") as diffAmount, sum("diffPerMeter") as diffPerMeter, sum("usedAmount") as "usedAmount", sum("usedPerMeter") as "usedPerMeter"  
from price
where "districtID" is not null
group by "districtID", district_name





-- vistes per neighborhood

create or replace view idealista_neighborhood as
select "neighborhoodID"  , sum(bathrooms) as bathrooms, sum("numPhotos") as numPhotos, sum(price) as price, sum("priceByArea") as priceByArea, sum(latitude) as latitude, sum(longitude) as longitude, sum(distance) as distance, sum(exterior) as exterior, sum(has360) as has360, sum("has3DTour") as has3DTour, sum("hasLift") as hasLift, sum("hasPlan") as hasPlan, sum("hasStaging") as hasStaging, sum("hasVideo") as hasVideo, sum("newDevelopment") as newDevelopment, sum("showAddress") as showAddress, sum("topNewDevelopment") as topNewDevelopment, sum("count") as count   
from idealista
where "neighborhoodID" is not null  
group by "neighborhoodID"

create or replace view income_neighborhood as
select "neighborhoodID"  , sum(rfd) as rfd, sum(population) as population 
from income
where "neighborhoodID" is not null
group by "neighborhoodID" 

create or replace view price_neighborhood as
select "neighborhoodID"  , sum(amount) as amount, sum("perMeter") as perMeter, sum("diffAmount") as diffAmount, sum("diffPerMeter") as diffPerMeter, sum("usedAmount") as "usedAmount", sum("usedPerMeter") as "usedPerMeter"  
from price
where "neighborhoodID" is not null
group by "neighborhoodID"




-- vistes per año

create or replace view income_year as
select "year", sum(rfd) as rfd, sum(population) as population 
from income
group by "year" 

create or replace view price_year as
select "year", sum(amount) as amount, sum("perMeter") as perMeter, sum("diffAmount") as diffAmount, sum("diffPerMeter") as diffPerMeter, sum("usedAmount") as "usedAmount", sum("usedPerMeter") as "usedPerMeter"  
from price
group by "year"

create or replace view idealista_year as
select "year"  , sum(bathrooms) as bathrooms, sum("numPhotos") as numPhotos, sum(price) as price, sum("priceByArea") as priceByArea, sum(latitude) as latitude, sum(longitude) as longitude, sum(distance) as distance, sum(exterior) as exterior, sum(has360) as has360, sum("has3DTour") as has3DTour, sum("hasLift") as hasLift, sum("hasPlan") as hasPlan, sum("hasStaging") as hasStaging, sum("hasVideo") as hasVideo, sum("newDevelopment") as newDevelopment, sum("showAddress") as showAddress, sum("topNewDevelopment") as topNewDevelopment, sum("count") as count   
from idealista 
group by "year"


-- vistes per año y neighborhood

create or replace view idealista_year_neighborhood as
select "year", "neighborhoodID"  , sum(bathrooms) as bathrooms, sum("numPhotos") as numPhotos, sum(price) as price, sum("priceByArea") as priceByArea, sum(latitude) as latitude, sum(longitude) as longitude, sum(distance) as distance, sum(exterior) as exterior, sum(has360) as has360, sum("has3DTour") as has3DTour, sum("hasLift") as hasLift, sum("hasPlan") as hasPlan, sum("hasStaging") as hasStaging, sum("hasVideo") as hasVideo, sum("newDevelopment") as newDevelopment, sum("showAddress") as showAddress, sum("topNewDevelopment") as topNewDevelopment, sum("count") as count   
from idealista
where "neighborhoodID" is not null  
group by "neighborhoodID", "year"

create or replace view income_year_neighborhood as
select "year", "neighborhoodID"  , sum(rfd) as rfd, sum(population) as population 
from income
where "neighborhoodID" is not null
group by "neighborhoodID", "year"

create or replace view price_year_neighborhood as
select "year", "neighborhoodID"  , sum(amount) as amount, sum("perMeter") as perMeter, sum("diffAmount") as diffAmount, sum("diffPerMeter") as diffPerMeter, sum("usedAmount") as "usedAmount", sum("usedPerMeter") as "usedPerMeter"  
from price
where "neighborhoodID" is not null
group by "neighborhoodID", "year"


-- vistes per año y district

create or replace view  idealista_year_district as
select "year", "districtID" as districtID, district_name, sum(bathrooms) as bathrooms, sum("numPhotos") as numPhotos, sum(price) as price, sum("priceByArea") as priceByArea, sum(latitude) as latitude, sum(longitude) as longitude, sum(distance) as distance, sum(exterior) as exterior, sum(has360) as has360, sum("has3DTour") as has3DTour, sum("hasLift") as hasLift, sum("hasPlan") as hasPlan, sum("hasStaging") as hasStaging, sum("hasVideo") as hasVideo, sum("newDevelopment") as newDevelopment, sum("showAddress") as showAddress, sum("topNewDevelopment") as topNewDevelopment, sum("count") as count   
from idealista
where "districtID" is not null  
group by "districtID", district_name, "year"


create or replace view income_year_district as
select "year", "districtID" as districtID, district_name, sum(rfd) as rfd, sum(population) as population 
from income
where "districtID" is not null
group by "districtID" , district_name, "year"

create or replace view price_year_district as
select "year", "districtID" as districtID, sum(amount) as amount, sum("perMeter") as perMeter, sum("diffAmount") as diffAmount, sum("diffPerMeter") as diffPerMeter, sum("usedAmount") as "usedAmount", sum("usedPerMeter") as "usedPerMeter"  
from price
where "districtID" is not null
group by "districtID", district_name, "year"



-- district information

create or replace view district_information as
select i.districtID, i.district_name, i.rfd as rfd, i.population as population, p.amount as amount, p.perMeter as perMeter, p.diffAmount as diffAmount, p.diffPerMeter as diffPerMeter, p."usedAmount" as usedAmount, p."usedPerMeter" as usedPerMeter,
i2.bathrooms as bathrooms, i2.numPhotos as numPhotos, i2.price as price, i2.priceByArea as priceByArea, i2.latitude as latitude, i2.longitude as longitude,
i2.distance as distance, i2.exterior as exterior, i2.has360 as has360, i2.has3DTour as has3DTour, i2.hasLift as hasLift, i2.hasPlan as hasPlan, i2.hasStaging as hasStaging,
i2.hasVideo as hasVideo, i2.newDevelopment as newDevelopment, i2.showAddress as showAddress, i2.topNewDevelopment as topNewDevelopment, i2.count as count
from income_district i
join price_district p on i.districtID = p.districtID
join idealista_district i2 on i.districtID = i2.districtID;


-- district information per year

create or replace view district_information_per_year as
select i.districtID, i.year as year, i.district_name, i.rfd as rfd, i.population as population, p.amount as amount, p.perMeter as perMeter, p.diffAmount as diffAmount, p.diffPerMeter as diffPerMeter, p."usedAmount" as usedAmount, p."usedPerMeter" as usedPerMeter
from income_year_district i
join price_year_district p on i.districtID = p.districtID;


--View with features for model training
create or replace view idealista_model as
select i.bathrooms,i."numPhotos" ,i.latitude ,i.longitude,i.distance ,i.exterior,i.has360,i."hasLift",i."has3DTour" ,i."hasPlan" ,i."hasStaging" , i."hasVideo" ,i."newDevelopment" , i."showAddress" ,i."topNewDevelopment" , pd.amount ,pd.permeter,pd."usedAmount" ,pd."usedPerMeter",id.rfd ,id.population ,i.price
from idealista i join price_district pd on i."districtID" = pd.districtid join income_district id on i."districtID" = id.districtid ;




