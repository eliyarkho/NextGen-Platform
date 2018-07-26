
BEGIN;

truncate stg.adwords.fact_adwords_performance;

set start_time_range = (select nvl(max(snapshot_timestamp),to_date('1900-01-01'))
						from dwh.adwords.fact_adwords_performance);
set finish_time_range = current_timestamp;


insert into stg.adwords.fact_adwords_performance	
select  current_timestamp as create_timestamp,
        t1.day as business_date,
        t1.customerid as account_id,
        t1.account as account_name,
        t1.currency as currency,
        t1.campaignid as campaign_id,
        t1.campaign as campaign_name,
        t1.campaignstate as campaign_status,
        t1.advertisingChannel as campaign_type,
        t1.bidstrategyid as bidding_strategy_id,
        t1.bidstrategyname as bidding_strategy_name,
        t1.bidstrategytype as bidding_strategy_type,
        t1.labelids as labels_id_campaign,
        t1.labels as labels_campaign,
        case when t1.budget  regexp '[0-9]*' then t1.budget::integer/1000000 end  as budget,
        t4.totalbudgetamount as total_budget,
        t4.budgetname as budget_name,
        t2.adgroupid as ad_group_id,
        t2.adgroup as ad_group_name,
        t2.adgroupstate as ad_group_status,
        t2.adgrouptype as ad_group_type,
        t2.labelids as labels_id_ad_group,
        t2.labels as labels_ad_group,
        case when t2.defaultmaxcpc  regexp '[0-9]*' then t2.defaultmaxcpc::integer/1000000 end  as max_cpc_bid,
        case when t2.maxcpm  regexp '[0-9]*' then t2.maxcpm::integer/1000000 end as max_cpm_bid,
        case when t2.maxcpv  regexp '[0-9]*' then t2.maxcpv::integer/1000000 end  as max_cpv_bid,
        t3.adid as ad_id,
        t3.adtype as ad_type,
		t3.adstate as ad_status,
        t3.approvalstatus as ad_approval_status,
        t3.autoappliedadsuggestion as is_automated_ad,
        t3.businessname as ad_business_name,
        t3.devicepreference as device_preference,
        t3.finalurl as final_url,
        t3.mobilefinalurl as mobile_final_url,
        t3.displayurl as ad_display_url,
        t3.ad as headline,
        t3.headline1 as headline_part_1,
        t3.headline2 as headline_part_2,
        t3.description as description,
        t3.descriptionline1 as description_part_1,
        t3.descriptionline2 as description_part_2,
        t3.longheadline as long_headline,
        t3.promotiontextresponsive as promotion_text,
        t3.priceprefixresponsive as price_prefix,
        t3.imageidresponsive as image_id,
        t3.squareimageidresponsive as square_image_id,
        t3.logosmultiassetresponsivedisplay as logo_id,
        t3.landscapelogoidresponsive as landscape_logo_id,
        t3.calltoactiontextresponsive as call_to_action_text,
        nvl(nvl(t3.trackingtemplate,t2.trackingtemplate),t1.trackingtemplate) as tracking_template,
        t3.customparameter as custom_parameter,
        t3.path1 as path_1,
        t3.path2 as path_2,
        t3.callonlyadphonenumber as phone_number,
        t3.shortheadline as short_headline,
        t3.gmailadmarketingimageheadline as image_headline,
        t3.gmailadmarketingimagedescription as image_description,
        t3.labelids as labels_ad_id,
        t3.labels as labels_ad,
        t3.device,
        metrics.agg_cost/1000000 as cost,
        metrics.agg_clicks as clicks,
        metrics.agg_impressions as impressions,
        metrics.agg_views as views,
        metrics.agg_conversions as conversions,
        metrics.agg_totalconvvalue as total_conv_value,
        case when metrics.agg_clicks>0 then (metrics.agg_cost/1000000)/ metrics.agg_clicks end as avg_cpc,
        case when metrics.agg_impressions>0 then metrics.agg_cost/(1000 * metrics.agg_impressions) end as avg_cpm,
        case when metrics.agg_views>0 then (metrics.agg_cost)/1000000/ metrics.agg_views end as avg_cpv,
        case when metrics.agg_impressions>0 then (metrics.agg_clicks/ metrics.agg_impressions)*100 end as ctr,
        case when metrics.agg_impressions>0 then (metrics.agg_cost/1000000)/ metrics.agg_impressions end as cpa,
        case when metrics.agg_impressions>0 then (metrics.agg_conversions/ metrics.agg_impressions)*100 end as conversion_rate,
        case when metrics.agg_cost>0 then (metrics.agg_totalconvvalue/ (metrics.agg_cost/1000000))-1 end  as roi,
        case when metrics.agg_conversions>0 then metrics.agg_totalconvvalue/ metrics.agg_conversions end as aov,
        0 as atg_conversions,
        0 as atg_order_val,
		$finish_time_range  as snapshot_timestamp
from 
	(
		 select *, row_number() over(partition by campaignid, day,device order by create_timestamp desc) batch_rank
		 from ods.adwords.campaign_performance_report 
		 where create_timestamp between $start_time_range and $finish_time_range
	 )t1 inner join 
	
	(
		 select *,  row_number() over(partition by adgroupid, day,device order by create_timestamp desc) batch_rank 
		 from ods.adwords.adgroup_performance_report 
		 where create_timestamp between $start_time_range and $finish_time_range

	 )t2 on t1.campaignid = t2.campaignid and
				   t1.device = t2.device and
				   t1.day = t2.day and
                   t1.batch_rank=1 and
				   t2.batch_rank=1 and
				   t1.customerid =t2.customerid 

		inner join 
	(
		select *,  row_number() over(partition by adid, day,device order by create_timestamp desc) batch_rank
		from ods.adwords.ad_performance_report
		where create_timestamp between $start_time_range and $finish_time_range
	
		) t3 on t1.campaignid = t3.campaignid and
				   t2.adgroupid = t3.adgroupid and
				   t1.device = t3.device and
				   t1.day = t3.day and
				   t3.batch_rank=1 and
				   t1.customerid =t3.customerid 
		inner join
	(
		select  create_timestamp, adid,day,device,  sum(cost) as agg_cost, 
                                                    sum(clicks) as agg_clicks, 
                                                    sum(impressions) as agg_impressions,
                                                    sum(views) as agg_views,
                                                    sum(conversions) as agg_conversions,
                                                    sum(totalconvvalue) as agg_totalconvvalue
        from ods.adwords.ad_performance_report
        group by 1,2,3,4
		
	) metrics on t3.adid = metrics.adid and
				   t3.day = metrics.day and
				   t3.device = metrics.device and
				   t3.create_timestamp = metrics.create_timestamp 
				   
		left join 

    (	
		select *, row_number() over(partition by budgetid order by (create_timestamp) desc) batch_rank
		from ods.adwords.budget_performance_report

		
	)t4 on  t1.budgetid = t4.budgetid and
             t4.batch_rank=1 ;

COMMIT;