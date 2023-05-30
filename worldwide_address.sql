// GET Starschema Worldwide Address Data: https://app.snowflake.com/marketplace/listing/GZSNZ7F5UT/starschema-worldwide-address-data
// Name mounted database as WORLDWIDE_ADDRESS_DATA
select count(*) from WORLDWIDE_ADDRESS_DATA.address.openaddress;

// Eiffel Tower (48.85837°, 2.294481°), Leaning Tower of Pisa (43.722839°, 10.401689°)
select * from worldwide_address_data.address.openaddress
where postcode is not null and postcode != ''
-- order by pow(-33.87 - lat, 2) + pow(151.2 - lon, 2)
-- order by pow(48.85 - lat, 2) + pow(2.29 - lon, 2)
order by pow(43.72 - lat, 2) + pow(10.4 - lon, 2)
limit 3;

// Show all available addresses for Rio
select * from WORLDWIDE_ADDRESS_DATA.address.openaddress
where country='br'
and city ilike 'rio de janeiro';

// Create Local Address Database and Share Schema
create or replace database local_worldwide_address_data;
create schema basedata;
create schema sharedata;
grant usage on database local_worldwide_address_data to public;
grant usage on all schemas in database local_worldwide_address_data to public;

create or replace table basedata.openaddress as select * from WORLDWIDE_ADDRESS_DATA.ADDRESS.OPENADDRESS; -- 560M rows

grant select on table basedata.openaddress to public;

create tag basedata.country_code;

// use this to populate the country_entitlement table below with consumer accounts
select current_account(), current_region();

create or replace table basedata.country_entitlement copy grants (country_code varchar, consumer_account varchar, consumer_region varchar);
insert into basedata.country_entitlement values ('fr', 'CQ84479', 'PUBLIC.GCP_EUROPE_WEST2'),
                                       ('it', 'CQ84479', 'PUBLIC.GCP_EUROPE_WEST2'),
                                       ('us', 'AT45871', 'PUBLIC.AZURE_EASTUS2'),
                                       ('*', 'NSA04695', 'PUBLIC.AWS_US_WEST_2'),
                                       ('br', 'AT45871', 'PUBLIC.AWS_US_WEST_2'),
                                       ('it', 'AT45871', 'PUBLIC.AWS_US_WEST_2')
                                       ;
alter table basedata.country_entitlement set tag country_code = 'multiple';

// reader account                                       
update basedata.country_entitlement set country_code = 'mx' where consumer_account = 'NSA04695';
update basedata.country_entitlement set country_code = '*' where consumer_account = 'NSA04695';

delete from basedata.country_entitlement where consumer_account = 'PM';
insert into basedata.country_entitlement values('ca', 'PM', 'AWS_US_EAST_1'),
                                               ('us', 'PM', 'AWS_US_EAST_1'),
                                               ('mx', 'PM', 'AWS_US_EAST_1');

select * from basedata.country_entitlement order by consumer_account;
                                       
alter table basedata.openaddress drop row access policy basedata.rap_country;                                       
create or replace row access policy basedata.rap_country as (country_cd varchar) returns boolean ->
    current_role() in ('ACCOUNTADMIN','SYSADMIN')
    or exists (
        select 1 from basedata.country_entitlement
        where consumer_account = current_account() and consumer_region = current_region()
        and (country_code = '*' or country_cd = country_code)
        )
;

alter table basedata.openaddress add row access policy basedata.rap_country on (country);

create or replace secure view sharedata.country_entries copy grants as
select country, count(*) entries 
from basedata.openaddress
group by country
having entries > 5
order by entries desc;

select * from sharedata.country_entries;

create or replace secure function sharedata.find_closest_address(latitude float, longitude float)
returns table (lat float, lon float, distance float, number varchar, street varchar, city varchar, region varchar, postcode varchar(50), country varchar(16))
as 
' 
    select lat, lon, sqrt(pow(latitude - lat, 2) + pow(longitude - lon, 2)) as dist, 
    number, street, city, region, postcode, country 
    from basedata.openaddress
    where postcode is not null and postcode != ''''
    order by 3
    limit 1
'    
;

describe function local_worldwide_address_data.sharedata.find_closest_address(float, float);

grant select on all views in schema sharedata to public;
grant usage on all functions in schema sharedata to public;

create or replace share worldwide_address_share;
grant usage on database local_worldwide_address_data to share worldwide_address_share;
grant usage on all schemas in database local_worldwide_address_data to share worldwide_address_share;
grant select on view sharedata.country_entries to share worldwide_address_share;
grant usage on function sharedata.find_closest_address(float, float) to share worldwide_address_share;

show shares like 'worldwide_address_share';
describe share worldwide_address_share;


// Put this into the SQL Usage Examples for the Listing

select * from table(local_worldwide_address_data.sharedata.find_closest_address(-33.87::float, 151.2::float));
select * from table(local_worldwide_address_data.sharedata.find_closest_address(43.72::float, 10.4::float));
select * from table(local_worldwide_address_data.sharedata.find_closest_address(100::float, 140::float));

select * from sharedata.country_entries;



