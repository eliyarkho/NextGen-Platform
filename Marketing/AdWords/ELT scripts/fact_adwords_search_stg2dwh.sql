
------merge stg.adwords.fact_adwords_performance to dwh 		  
		  
		  
merge into dwh.adwords.fact_adwords_search t1 using stg.adwords.fact_adwords_search t2 on t1.business_date = t2.business_date and
                                                                                                    t1.account_id = t2.account_id and
                                                                                                    t1.campaign_id = t2.campaign_id and
                                                                                                    t1.ad_group_id = t2.ad_group_id and
                                                                                                    t1.keyword_id = t2.keyword_id and
                                                                                                    t1.device = t2.device and
                                                                                                    t1.search_term = t2.search_term and
                                                                                                    t1.SEARCH_TERM_MATCH_TYPE =t2.SEARCH_TERM_MATCH_TYPE and
                                                                                                    t1.ad_id = t2.ad_id

    when matched  then update set t1.conversions = t2.conversions, 
                                  t1.total_conv_value = t2.total_conv_value , 
                                  t1.conversion_rate=t2.conversion_rate,
                                  t1.roi=t2.roi,
                                  t1.aov=t2.aov,
								  t1.update_timestamp = current_timestamp,
								  t1.snapshot_timestamp = t2.snapshot_timestamp
    when not matched then insert (CREATE_TIMESTAMP ,
                                  BUSINESS_DATE ,
                                  ACCOUNT_ID ,
                                  ACCOUNT_NAME ,
                                  CURRENCY ,
                                  CAMPAIGN_ID ,
                                  CAMPAIGN_NAME ,
                                  CAMPAIGN_STATUS ,
                                  CAMPAIGN_TYPE ,
                                  BIDDING_STRATEGY_ID ,
                                  BIDDING_STRATEGY_NAME ,
                                  BIDDING_STRATEGY_TYPE ,
                                  AD_GROUP_ID ,
                                  AD_GROUP_NAME ,
                                  AD_GROUP_STATUS,
                                  AD_GROUP_TYPE ,
                                  AD_ID ,
                                  AD_TYPE ,
                                  AD_STATUS,
                                  AD_APPROVAL_STATUS ,
                                  AUTOMATED,
                                  AD_BUSINESS_NAME,
                                  DEVICE ,
                                  KEYWORD_ID ,
                                  KEYWORD_NAME ,
                                  KEYWORD_STATUS,
                                  APPROVAL_STATUS ,
                                  CRITERION_SERVING_STATUS ,
                                  MAX_CPC_BID ,
                                  MAX_CPM_BID ,
                                  KEYWORDS_LABLELS_ID ,
                                  KEYWORDS_LABLELS ,
                                  KEYWORD_MATCH_TYPE ,
                                  SEARCH_TERM ,
                                  ADDED_OR_EXCLUDED ,
                                  SEARCH_TERM_MATCH_TYPE ,
                                  QUALITY_SCORE ,
                                  COST ,
                                  CLICKS ,
                                  IMPRESSIONS ,
                                  VIEWS ,
                                  CONVERSIONS,
                                  TOTAL_CONV_VALUE ,
                                  AVG_CPC ,
                                  AVG_CPM ,
                                  AVG_CPV ,
                                  CTR ,
                                  CPA ,
                                  CONVERSION_RATE ,
                                  ROI ,
                                  AOV ,
                                  ATG_CONVERSIONS ,
                                  ATG_ORDER_VAL ,
                                  SNAPSHOT_TIMESTAMP ) values ( current_timestamp,
																BUSINESS_DATE ,
                                                                ACCOUNT_ID ,
                                                                ACCOUNT_NAME ,
                                                                CURRENCY ,
                                                                CAMPAIGN_ID ,
                                                                CAMPAIGN_NAME ,
                                                                CAMPAIGN_STATUS ,
                                                                CAMPAIGN_TYPE ,
                                                                BIDDING_STRATEGY_ID ,
                                                                BIDDING_STRATEGY_NAME ,
                                                                BIDDING_STRATEGY_TYPE ,
                                                                AD_GROUP_ID ,
                                                                AD_GROUP_NAME ,
                                                                AD_GROUP_STATUS,
                                                                AD_GROUP_TYPE ,
                                                                AD_ID ,
                                                                AD_TYPE ,
                                                                AD_STATUS,
                                                                AD_APPROVAL_STATUS ,
                                                                AUTOMATED,
                                                                AD_BUSINESS_NAME,
                                                                DEVICE ,
                                                                KEYWORD_ID ,
                                                                KEYWORD_NAME ,
                                                                KEYWORD_STATUS,
                                                                APPROVAL_STATUS ,
                                                                CRITERION_SERVING_STATUS ,
                                                                MAX_CPC_BID ,
                                                                MAX_CPM_BID ,
                                                                KEYWORDS_LABLELS_ID ,
                                                                KEYWORDS_LABLELS ,
                                                                KEYWORD_MATCH_TYPE ,
                                                                SEARCH_TERM ,
                                                                ADDED_OR_EXCLUDED ,
                                                                SEARCH_TERM_MATCH_TYPE ,
                                                                QUALITY_SCORE ,
                                                                COST ,
                                                                CLICKS ,
                                                                IMPRESSIONS ,
                                                                VIEWS ,
                                                                CONVERSIONS,
                                                                TOTAL_CONV_VALUE ,
                                                                AVG_CPC ,
                                                                AVG_CPM ,
                                                                AVG_CPV ,
                                                                CTR ,
                                                                CPA ,
                                                                CONVERSION_RATE ,
                                                                ROI ,
                                                                AOV ,
                                                                ATG_CONVERSIONS ,
                                                                ATG_ORDER_VAL ,
                                                                SNAPSHOT_TIMESTAMP);