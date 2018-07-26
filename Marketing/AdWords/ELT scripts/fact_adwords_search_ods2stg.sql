BEGIN;

truncate stg.adwords.fact_adwords_search;

set start_time_range = (select nvl(max(snapshot_timestamp),to_date('1900-01-01'))
						from dwh.adwords.fact_adwords_search);
set finish_time_range = current_timestamp;


insert into stg.adwords.fact_adwords_search	
select  current_timestamp ,
        nvl(t1.day ,t2.day),
        nvl(t1.customerid ,t2.customerid),
        nvl(t1.account,t2.account),
        nvl(t1.currency ,t2.currency),
        nvl(t1.campaignid ,t2.campaignid ),
        nvl(t1.campaign ,t2.campaign ),
        nvl(t1.campaignstate,t2.campaignstate),
        'Search',
        t1.bidstrategyid ,
        nvl(t1.bidstrategyname,'DSA Campaign'),
        nvl(t1.bidstrategytype,'DSA Campaign'),
        nvl(t1.adgroupid ,t2.adgroupid),
        nvl(t1.adgroup,t2.adgroup),
        nvl(t1.adgroupstate,t2.adgroupstate),
        t3.adgrouptype,
        t4.adid,
        t4.adtype ,
        t4.adstate ,
        t4.approvalstatus ,
        t4.autoappliedadsuggestion ,
        t4.businessname,
        t2.device,
        nvl(t1.keywordid,t2.keywordid),
        nvl(t1.keyword,'DSA Campaign'),
        nvl(t1.keywordstate,'DSA Campaign'),
        nvl(t1.approvalStatus,'DSA Campaign'),
        nvl(t1.criterionServingStatus,'DSA Campaign'),
        case when t1.maxcpc  regexp '[0-9]*' then t1.maxcpc::integer/1000000 end  as max_cpc_bid,
        case when t1.maxcpm  regexp '[0-9]*' then t1.maxcpm::integer/1000000 end as max_cpm_bid,
        nvl(t1.labelids,'DSA Campaign'::variant),
        nvl(t1.labels,'DSA Campaign'::variant),
        nvl(t1.matchtype,'DSA Campaign'),
        t2.searchterm,
        t2.addedexcluded,
        t2.matchtype,
        t1.qualityscore,
        t2.cost/1000000 ,
        t2.clicks ,
        t2.impressions,
        t2.views,
        t2.conversions,
        t2.totalconvvalue,
        case when t2.clicks>0 then (t2.cost/1000000)/ t2.clicks end as avg_cpc,
        case when t2.impressions>0 then t2.cost/(1000 * t2.impressions) end as avg_cpm,
        case when  t2.views>0 then (t2.cost)/1000000/ t2.views end as avg_cpv,
        case when t2.impressions>0 then (t2.clicks/ t2.impressions)*100 end as ctr,
        case when t2.impressions>0 then (t2.cost/1000000)/ t2.impressions end as cpa,
        case when t2.impressions>0 then (t2.conversions/ t2.impressions)*100 end as conversion_rate,
        case when t2.cost>0 then (t2.totalconvvalue/ (t2.cost/1000000))-1 end  as roi,
        case when t2.conversions>0 then t2.totalconvvalue/ t2.conversions end as aov,
        0 as atg_conversions,
        0 as atg_order_val,
		$finish_time_range  as snapshot_timestamp
from 
	(
		 select *, row_number() over(partition by adgroupid,keywordid, day,device order by create_timestamp desc) batch_rank
		 from ods.adwords.keywords_performance_report 
		 where create_timestamp between $start_time_range and $finish_time_range
	 
    )t1 right join 

 	(select *
     from(
		 select *, row_number() over(partition by  adgroupid,adid,keywordid,searchterm ,day,device,matchtype order by create_timestamp desc) batch_rank
		 from ods.adwords.search_query_performance_report 
		 where create_timestamp between $start_time_range and $finish_time_range
         )
     where batch_rank=1
	 )t2        on t1.adgroupid = t2.adgroupid and
				   t1.device = t2.device and
				   t1.day = t2.day and
                   t1.batch_rank=1 and
				   t1.keywordid =t2.keywordid and
                   t1.customerid=t2.customerid
      left join (
                    select distinct customerid,campaignid,adgroupid, adgrouptype
                    from ods.adwords.adgroup_performance_report 
                    where create_timestamp = (select max(create_timestamp)
                                              from ods.adwords.adgroup_performance_report)
        
                ) t3 on t2.customerid=t3.customerid and
                        t2.campaignid = t3.campaignid and
                        t2.adgroupid = t3.adgroupid
      left join (
                    select distinct customerid,
                                    campaignid,
                                    adgroupid, 
                                    adid,
                                    adtype ,
                                    adstate ,
                                    approvalstatus ,
                                    autoappliedadsuggestion ,
                                    businessname
                    from ods.adwords.ad_performance_report 
                    where create_timestamp = (select max(create_timestamp)
                                              from ods.adwords.ad_performance_report)
        
                ) t4 on t2.customerid=t4.customerid and
                        t2.campaignid = t4.campaignid and
                        t2.adgroupid = t4.adgroupid and
                        t2.adid = t4.adid; 
   
COMMIT;
