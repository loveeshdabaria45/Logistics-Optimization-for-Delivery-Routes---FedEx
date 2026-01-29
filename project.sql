create database project;

use project_work;

DELETE FROM fedex_orders
WHERE Order_ID IN (
    SELECT Order_ID
    FROM (
        SELECT Order_ID
        FROM fedex_orders
        GROUP BY Order_ID
        HAVING COUNT(*) > 1
    ) AS t
);
rollback;

select distinct * from fedex_orders;

select * from fedex_routes;

select * from fedex_shipments 
where delivery_date<pickup_date;

#Delivery_Delay_Analysis
select shipment_id, transit_time
from fedex_shipments;

Alter table fedex_shipments add transit_time double;
 
Update fedex_shipments 
set transit_time
= timestampdiff(hour, pickup_date, delivery_date);

Select s.route_id,r.source_city,r.destination_city,avg(s.transit_time) as avg_delay 
from fedex_shipments s left join fedex_routes r 
on s.route_id=r.route_id
group by s.route_id,r.source_city, r.destination_city
order by avg(s.delay_hours) desc
LIMIT 10;

select shipment_id, warehouse_id, transit_time, dense_rank() 
over(partition by warehouse_id 
     order by transit_time desc) as delay_rank 
from fedex_shipments;

select o.delivery_type, avg(s.transit_time) as avg_transit 
from fedex_shipments s 
join fedex_orders o 
on s.order_id=o.order_id group by delivery_type;

#Route Optimization Insights

select route_id, avg(transit_time) as avg_transit_time 
from fedex_shipments group by route_id;

select route_id, avg(delay_hours) as avg_delay 
from fedex_shipments 
group by route_id 
order by avg_delay desc;

select route_id, 
round(distance_km/avg_transit_time_hours,2) as efficiency 
from fedex_routes 
order by efficiency ;

select d.route_id, d.delayed_shipments, t.total_shipments, d.delayed_shipments/t.total_shipments as delay_rate 
from (select route_id,count(*) as delayed_shipments from fedex_shipments where delay_hours>0 group by route_id) d
join (select route_id,count(*) as total_shipments from fedex_shipments group by route_id) t
on d.route_id=t.route_id
where d.delayed_shipments/t.total_shipments>0.2
group by route_id;

#Warehouse Performance
 
select warehouse_id, 
round(avg(delay_hours),2) as avg_delay 
from fedex_shipments 
group by warehouse_id 
order by avg_delay desc 
limit 3;

select d.warehouse_id, d.delayed_shipments, t.total_shipments, 
d.delayed_shipments/t.total_shipments as delay_rate 
from (select warehouse_id,count(*) as delayed_shipments 
      from fedex_shipments where delay_hours>0 
      group by warehouse_id) d
join (select warehouse_id,count(*) as total_shipments 
      from fedex_shipments 
      group by warehouse_id) t
on d.warehouse_id=t.warehouse_id
order by delay_Rate desc;

with w as 
(select warehouse_id,avg(delay_hours) as avg_delay_w 
from fedex_shipments group by warehouse_id),
avg_delay as 
(select avg(delay_hours) as g_avg_delay from fedex_shipments)
select w.warehouse_id,w.avg_delay_w,g.g_avg_delay 
from w cross join avg_delay g 
where w.avg_delay_w>g.g_avg_delay
order by w.avg_delay_w;

select d.warehouse_id, d.on_time_shipments, t.total_shipments, 
d.on_time_shipments/t.total_shipments*100 as on_time_perc, rank() over( 
order by d.on_time_shipments/t.total_shipments*100 desc) as rnk
from (select warehouse_id,count(*) as on_time_shipments 
      from fedex_shipments where delay_hours=0 
      group by warehouse_id) d
join (select warehouse_id,count(*) as total_shipments 
      from fedex_shipments 
      group by warehouse_id) t
on d.warehouse_id=t.warehouse_id;

#Delivery agent Analysis

select route_id,on_time_percentage,
rank() over( order by on_time_percentage desc) as route_rank
from ( select route_id, round( 100 * sum(case when delay_hours <= 0 then 1 else 0 end)
/count(*),2) as on_time_percentage from fedex_shipments 
group by route_id) t;

select route_id, agent_id, on_time_percentage, rank()over 
(partition by route_id order by on_time_percentage desc) as route_rank
from (select route_id,agent_id, round(100* sum(case when delay_hours=0 then 1 else 0 end)
      /count(*),2) as on_time_percentage
      from fedex_shipments group by route_id, agent_id) t
where on_time_percentage<85
group by route_id,agent_id;

select agent_id, avg_rating, experience_years from fedex_delivery_agents
where agent_id in (select agent_id from ( select agent_id, 
round(100*sum( case when delay_hours=0 then 1 else 0 end)/count(*),2) as on_time_percentage
from fedex_shipments group by agent_id order by on_time_percentage desc limit 5) t);

select agent_id, avg_rating, experience_years from fedex_delivery_agents
where agent_id in (select agent_id from ( select agent_id, 
round(100*sum( case when delay_hours=0 then 1 else 0 end)/count(*),2) as on_time_percentage
from fedex_shipments group by agent_id order by on_time_percentage asc limit 5) t);


#Shipment Tracking Analytics

select shipment_id, delivery_status, delivery_date 
from fedex_shipments;

select route_id, count(*) as no_shipments 
from fedex_shipments where delivery_status!='Delivered' 
group by route_id
order by no_shipments desc;

select delay_reason,count(*) 
from fedex_shipments 
group by delay_reason;

select order_id,delay_hours 
from fedex_shipments 
where delay_hours>120 
order by delay_hours desc;

#Advanced KPI Reporting

select s.route_id,r.source_country,
round(avg(s.delay_hours),2) as avg_delay
from fedex_shipments s 
join fedex_routes r
on s.route_id=r.route_id
group by s.route_id,r.source_country
order by avg_delay desc;

select o.on_time/t.total*100 as on_time_perc 
from (select count(*) as on_time 
      from fedex_shipments where delay_hours=0) o
join (select count(*) as total 
      from fedex_shipments) t;

select route_id, round(avg(delay_hours),2) as avg_delay 
from fedex_shipments group by route_id;

select s.warehouse_id,
s.shipments_handled/w.capacity_per_day*100 as warehouse_utilization
from (select warehouse_id,count(*) as shipments_handled 
      from fedex_shipments 
      group by warehouse_id) s
join fedex_warehouses w
on s.warehouse_id=w.warehouse_id;










