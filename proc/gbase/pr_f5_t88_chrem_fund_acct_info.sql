DELIMITER |

CREATE DEFINER="cq_sjpt"@"%" PROCEDURE "pr_f5_t88_chrem_fund_acct_info"(
	OUT	OUT_RES_MSG		VARCHAR(200),
	IN	IN_TX_DATE		VARCHAR(8) 
	)
lable:BEGIN
 
/**********************************
 * dg 20211009 新建
 * 理财基金账户信息
 *********************************/
	
	DECLARE ETL_T_TAB_ENG_NAME 		VARCHAR(50)		DEFAULT 't88_chrem_fund_acct_info';
	DECLARE ETL_STEP_NO				INTEGER			DEFAULT 1;
	DECLARE ETL_TX_DATE 			VARCHAR(8)		DEFAULT IN_TX_DATE;
	DECLARE PK_COUNT				BIGINT			DEFAULT 0;


	/*定义临时表*/
-- ETL_STEP_NO = 1 
	SET @SQL_STR = 'DROP TEMPORARY TABLE IF EXISTS VT_t88_chrem_fund_acct_info';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	
	SET @SQL_STR = 'DROP TEMPORARY TABLE IF EXISTS AT_t88_chrem_fund_acct_info';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

-- ETL_STEP_NO = 2 
	SET @SQL_STR = 'CREATE TEMPORARY  TABLE VT_t88_chrem_fund_acct_info LIKE pdm.t88_chrem_fund_acct_info';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	SET @SQL_STR = 'CREATE TEMPORARY  TABLE AT_t88_chrem_fund_acct_info LIKE pdm.t88_chrem_fund_acct_info';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

	/*创建昨日临时表*/
	-- ETL_STEP_NO = 3 
	SET @SQL_STR = 'DROP TEMPORARY TABLE IF EXISTS VT_pre_t88_chrem_fund_acct_info';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	-- ETL_STEP_NO = 4
	SET @SQL_STR = '
	CREATE TEMPORARY TABLE VT_pre_t88_chrem_fund_acct_info(
		Agmt_ID	VARCHAR(80)	NOT	NULL	COMMENT	\'协议编号\',			
		Cust_ID	VARCHAR(60)	NOT	NULL	COMMENT	\'客户编号\',			
		Acct_Nm	VARCHAR(200)		COMMENT	\'账户名称\',			
		Cur_Cd	VARCHAR(30)		COMMENT	\'币种代码\',			
		Prod_ID	VARCHAR(30)	NOT	NULL	COMMENT	\'产品编号\',	
		Prod_Ctgy_Cd VARCHAR(10)	NOT	NULL	COMMENT	\'产品类别代码\',	
		Lot_Totl_Cnt	DECIMAL(22,0)	NOT	NULL	COMMENT	\'当前份额\',			
        Curr_Bal decimal(22,6) DEFAULT NULL COMMENT \'当前余额\',
        Mth_Total_Bal decimal(22,6) DEFAULT NULL COMMENT \'月累计余额\',
        Quar_Tota_Bal decimal(22,6) DEFAULT NULL COMMENT \'季累计余额\',
        Yr_Tota_Bal decimal(22,6) DEFAULT NULL COMMENT \'年累计余额\',
        Mth_DAvg decimal(22,6) DEFAULT NULL COMMENT \'月日均余额\',
        Quar_DAvg decimal(22,6) DEFAULT NULL COMMENT \'季日均余额\',
        Yr_DAvg decimal(22,6) DEFAULT NULL COMMENT \'年日均余额\',
        In_Trans_Purch_Amt decimal(22,6) DEFAULT NULL COMMENT \'在途申购金额\',
        Mth_Total_Amt decimal(22,6) DEFAULT NULL COMMENT \'月累计在途申购金额\',
        Quar_Tota_Amt decimal(22,6) DEFAULT NULL COMMENT \'季累计在途申购金额\',
        Yr_Tota_Amt decimal(22,6) DEFAULT NULL COMMENT \'年累计在途申购金额\',
        Mth_DAvg_Amt decimal(22,6) DEFAULT NULL COMMENT \'月日均在途申购金额\',
        Quar_DAvg_Amt decimal(22,6) DEFAULT NULL COMMENT \'季日均在途申购金额\',
        Yr_DAvg_Amt decimal(22,6) DEFAULT NULL COMMENT \'年日均在途申购金额\',
		Data_Src_Cd	VARCHAR(30)	NOT	NULL	COMMENT	\'数据来源表名\',
		Totl_In_Trans_Amt decimal(22,6) DEFAULT NULL COMMENT \'总在途金额\',
		Mth_Accm_Totl_In_Trans_Amt decimal(22,6) DEFAULT NULL COMMENT \'月累计总在途金额\',
        Quar_Accm_Totl_In_Trans_Amt decimal(22,6) DEFAULT NULL COMMENT \'季累计总在途金额\',
        Yr_Accm_Totl_In_Trans_Amt decimal(22,6) DEFAULT NULL COMMENT \'年累计总在途金额\',
        Mth_DAvg_Totl_In_Trans_Amt decimal(22,6) DEFAULT NULL COMMENT \'月日均总在途金额\',
        Quar_DAvg_Totl_In_Trans_Amt decimal(22,6) DEFAULT NULL COMMENT \'季日均总在途金额\',
        Yr_DAvg_Totl_In_Trans_Amt decimal(22,6) DEFAULT NULL COMMENT \'年日均总在途金额\',
		Redem_Prep_Inacct_Amt decimal(22,6) DEFAULT NULL COMMENT \'赎回待上帐在途金额\',
		Mth_Accm_Redem_Prep_Inacct_Amt  decimal(22,6) DEFAULT NULL COMMENT \'月累计赎回待上帐在途金额\',
        Quar_Accm_Redem_Prep_Inacct_Amt decimal(22,6) DEFAULT NULL COMMENT \'季累计赎回待上帐在途金额\',
        Yr_Accm_Redem_Prep_Inacct_Amt decimal(22,6) DEFAULT NULL COMMENT \'年累计赎回待上帐在途金额\',
        Mth_DAvg_Redem_Prep_Inacct_Amt  decimal(22,6) DEFAULT NULL COMMENT \'月日均赎回待上帐在途金额\',
        Quar_DAvg_Redem_Prep_Inacct_Amt decimal(22,6) DEFAULT NULL COMMENT \'季日均赎回待上帐在途金额\',
        Yr_DAvg_Redem_Prep_Inacct_Amt decimal(22,6) DEFAULT NULL COMMENT \'年日均赎回待上帐在途金额\'
	)';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

	/*昨日金额数据插入昨日临时表*/
	-- ETL_STEP_NO = 5
		SET @SQL_STR = '
	INSERT INTO VT_pre_t88_chrem_fund_acct_info(
		Agmt_ID,
			Cust_ID,
			Acct_Nm,
			Cur_Cd,
			Prod_ID,
			Prod_Ctgy_Cd,
			Lot_Totl_Cnt,
			Curr_Bal,
			Mth_Total_Bal,
			Quar_Tota_Bal,
			Yr_Tota_Bal,
			Mth_DAvg,
			Quar_DAvg,
			Yr_DAvg,
            In_Trans_Purch_Amt,
            Mth_Total_Amt,
            Quar_Tota_Amt,
            Yr_Tota_Amt,
            Mth_DAvg_Amt,
            Quar_DAvg_Amt,
            Yr_DAvg_Amt,
			Data_Src_Cd,
			Totl_In_Trans_Amt,
			Mth_Accm_Totl_In_Trans_Amt,
			Quar_Accm_Totl_In_Trans_Amt,
			Yr_Accm_Totl_In_Trans_Amt,
			Mth_DAvg_Totl_In_Trans_Amt,
			Quar_DAvg_Totl_In_Trans_Amt,
			Yr_DAvg_Totl_In_Trans_Amt,
			Redem_Prep_Inacct_Amt,
			Mth_Accm_Redem_Prep_Inacct_Amt,
			Quar_Accm_Redem_Prep_Inacct_Amt,
			Yr_Accm_Redem_Prep_Inacct_Amt,
			Mth_DAvg_Redem_Prep_Inacct_Amt,
			Quar_DAvg_Redem_Prep_Inacct_Amt,
			Yr_DAvg_Redem_Prep_Inacct_Amt			
	)
	SELECT 	Agmt_ID,
			Cust_ID,
			Acct_Nm,
			Cur_Cd,
			Prod_ID,
			Prod_Ctgy_Cd,
			Lot_Totl_Cnt,
			Curr_Bal,
			Mth_Total_Bal,
			Quar_Tota_Bal,
			Yr_Tota_Bal,
			Mth_DAvg,
			Quar_DAvg,
			Yr_DAvg,
            In_Trans_Purch_Amt,
            Mth_Total_Amt,
            Quar_Tota_Amt,
            Yr_Tota_Amt,
            Mth_DAvg_Amt,
            Quar_DAvg_Amt,
            Yr_DAvg_Amt,
			Data_Src_Cd,
			Totl_In_Trans_Amt,
			Mth_Accm_Totl_In_Trans_Amt,
			Quar_Accm_Totl_In_Trans_Amt,
			Yr_Accm_Totl_In_Trans_Amt,
			Mth_DAvg_Totl_In_Trans_Amt,
			Quar_DAvg_Totl_In_Trans_Amt,
			Yr_DAvg_Totl_In_Trans_Amt,
			Redem_Prep_Inacct_Amt,
			Mth_Accm_Redem_Prep_Inacct_Amt,
			Quar_Accm_Redem_Prep_Inacct_Amt,
			Yr_Accm_Redem_Prep_Inacct_Amt,
			Mth_DAvg_Redem_Prep_Inacct_Amt,
			Quar_DAvg_Redem_Prep_Inacct_Amt,
			Yr_DAvg_Redem_Prep_Inacct_Amt	
	FROM PDM.t88_chrem_fund_acct_info WHERE Statt_Dt = ${LAST_TX_DATE}';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	/*今日数据插入到临时表*/
	-- ETL_STEP_NO = 6
		SET @SQL_STR = 	'
	insert into AT_t88_chrem_fund_acct_info(
		Statt_Dt,
			Agmt_ID,
			Cust_ID,
			Acct_Nm,
			Cur_Cd,
			Prod_ID,
			Prod_Ctgy_Cd,
			Lot_Totl_Cnt,
			Curr_Bal,
			Mth_Total_Bal,
			Quar_Tota_Bal,
			Yr_Tota_Bal,
			Mth_DAvg,
			Quar_DAvg,
			Yr_DAvg,
            In_Trans_Purch_Amt,
            Mth_Total_Amt,
            Quar_Tota_Amt,
            Yr_Tota_Amt,
            Mth_DAvg_Amt,
            Quar_DAvg_Amt,
            Yr_DAvg_Amt,
			Data_Src_Cd,
			Totl_In_Trans_Amt,
			Mth_Accm_Totl_In_Trans_Amt,
			Quar_Accm_Totl_In_Trans_Amt,
			Yr_Accm_Totl_In_Trans_Amt,
			Mth_DAvg_Totl_In_Trans_Amt,
			Quar_DAvg_Totl_In_Trans_Amt,
			Yr_DAvg_Totl_In_Trans_Amt,
			Redem_Prep_Inacct_Amt,
			Mth_Accm_Redem_Prep_Inacct_Amt,
			Quar_Accm_Redem_Prep_Inacct_Amt,
			Yr_Accm_Redem_Prep_Inacct_Amt,
			Mth_DAvg_Redem_Prep_Inacct_Amt,
			Quar_DAvg_Redem_Prep_Inacct_Amt,
			Yr_DAvg_Redem_Prep_Inacct_Amt	
	)
		select 
	    ${TX_DATE} ,-- 1统计日期
			t.Agmt_ID,
			t.Cust_ID,
			t2.Cust_Nm,
			t1.Cur_Cd,
			t.Prod_Id,
			t1.Prod_Ctgy_Cd,
			COALESCE(t.Lot_Totl_Cnt,0),
		    COALESCE(t.Lot_Totl_Cnt,0)*COALESCE(t1.Prod_Net_Val,0),
			case when ${TX_DATE} = ${THIS_MONTH_BEGIN} then COALESCE(t.Lot_Totl_Cnt,0)*COALESCE(t1.Prod_Net_Val,0)
 		      else COALESCE(t.Lot_Totl_Cnt,0)*COALESCE(t1.Prod_Net_Val,0) + COALESCE(b.Mth_Total_Bal,0) 
			end as Mth_Total_Bal,	-- 月累计余额
			case when ${TX_DATE} = ${THIS_QUART_BEGIN} then COALESCE(t.Lot_Totl_Cnt,0)*COALESCE(t1.Prod_Net_Val,0) 
 		      else COALESCE(t.Lot_Totl_Cnt,0)*COALESCE(t1.Prod_Net_Val,0) + COALESCE(b.Quar_Tota_Bal, 0) 
			END as Quar_Tota_Bal,  -- 	季累计余额
			case when ${TX_DATE} = ${THIS_YEAR_BEGIN} then COALESCE(t.Lot_Totl_Cnt,0)*COALESCE(t1.Prod_Net_Val,0)
 		      else COALESCE(t.Lot_Totl_Cnt,0)*COALESCE(t1.Prod_Net_Val,0) + COALESCE(b.Yr_Tota_Bal, 0) 
			END as Yr_Tota_Bal  ,		-- 年累计余额
			0 as Mth_DAvg	,-- 	月日均余额
			0 as Quar_DAvg	,-- 	季日均余额
			0 as Yr_DAvg		,-- 	年日均余额
            COALESCE(t.In_Trans_Purch_Amt,0), -- 在途申购金额
			case when ${TX_DATE} = ${THIS_MONTH_BEGIN} then COALESCE(t.In_Trans_Purch_Amt,0)
 		      else COALESCE(t.In_Trans_Purch_Amt,0) + COALESCE(b.Mth_Total_Amt,0) 
			end as Mth_Total_Amt,	-- 月累计在途申购金额
			case when ${TX_DATE} = ${THIS_QUART_BEGIN} then COALESCE(t.In_Trans_Purch_Amt,0)
 		      else COALESCE(t.In_Trans_Purch_Amt,0) + COALESCE(b.Quar_Tota_Amt, 0) 
			END as Quar_Tota_Amt,  -- 	季累计在途申购金额
			case when ${TX_DATE} = ${THIS_YEAR_BEGIN} then COALESCE(t.In_Trans_Purch_Amt,0)
 		      else COALESCE(t.In_Trans_Purch_Amt,0) + COALESCE(b.Yr_Tota_Amt, 0) 
			END as Yr_Tota_Amt,		-- 年累计在途申购金额
			0 as Mth_DAvg_Amt,-- 	月日均在途申购金额
			0 as Quar_DAvg_Amt,-- 	季日均在途申购金额
			0 as Yr_DAvg_Amt,-- 	年日均在途申购金额
			t.Data_Src_Cd,
			 COALESCE(t.Totl_In_Trans_Amt,0), -- 总在途金额
			case when ${TX_DATE} = ${THIS_MONTH_BEGIN} then COALESCE(t.Totl_In_Trans_Amt,0)
 		      else COALESCE(t.Totl_In_Trans_Amt,0) + COALESCE(b.Mth_Accm_Totl_In_Trans_Amt,0) 
			end as Mth_Accm_Totl_In_Trans_Amt,	-- 月累计总在途金额
			case when ${TX_DATE} = ${THIS_QUART_BEGIN} then COALESCE(t.Totl_In_Trans_Amt,0)
 		      else COALESCE(t.Totl_In_Trans_Amt,0) + COALESCE(b.Quar_Accm_Totl_In_Trans_Amt, 0) 
			END as Quar_Accm_Totl_In_Trans_Amt,  -- 	季累计总在途金额
			case when ${TX_DATE} = ${THIS_YEAR_BEGIN} then COALESCE(t.Totl_In_Trans_Amt,0)
 		      else COALESCE(t.Totl_In_Trans_Amt,0) + COALESCE(b.Yr_Accm_Totl_In_Trans_Amt, 0) 
			END as Yr_Accm_Totl_In_Trans_Amt,		-- 年累计总在途金额
			0 as Mth_DAvg_Totl_In_Trans_Amt,-- 	月日均总在途金额
			0 as Quar_DAvg_Totl_In_Trans_Amt,-- 	季日均总在途金额
			0 as Yr_DAvg_Totl_In_Trans_Amt,-- 	年日均总在途金额
			 COALESCE(t.Redem_Prep_Inacct_Amt,0), -- 赎回待上帐在途金额
			case when ${TX_DATE} = ${THIS_MONTH_BEGIN} then COALESCE(t.Redem_Prep_Inacct_Amt,0)
 		      else COALESCE(t.Redem_Prep_Inacct_Amt,0) + COALESCE(b.Mth_Accm_Redem_Prep_Inacct_Amt,0) 
			end as Mth_Accm_Redem_Prep_Inacct_Amt,	-- 月累计赎回待上帐在途金额
			case when ${TX_DATE} = ${THIS_QUART_BEGIN} then COALESCE(t.Redem_Prep_Inacct_Amt,0)
 		      else COALESCE(t.Redem_Prep_Inacct_Amt,0) + COALESCE(b.Quar_Accm_Redem_Prep_Inacct_Amt, 0) 
			END as Quar_Accm_Redem_Prep_Inacct_Amt,  -- 	季累计赎回待上帐在途金额
			case when ${TX_DATE} = ${THIS_YEAR_BEGIN} then COALESCE(t.Redem_Prep_Inacct_Amt,0)
 		      else COALESCE(t.Redem_Prep_Inacct_Amt,0) + COALESCE(b.Yr_Accm_Redem_Prep_Inacct_Amt, 0) 
			END as Yr_Accm_Redem_Prep_Inacct_Amt,		-- 年累计赎回待上帐在途金额
			0 as Mth_DAvg_Redem_Prep_Inacct_Amt,-- 	月日均赎回待上帐在途金额
			0 as Quar_DAvg_Redem_Prep_Inacct_Amt,-- 	季日均赎回待上帐在途金额
			0 as Yr_DAvg_Redem_Prep_Inacct_Amt -- 	年日均赎回待上帐在途金额
	from pdm.t03_chrem_sell_lot_h t -- 理财销售份额历史
	left join pdm.t02_chrem_sell_prod_h t1 -- 理财产品信息表
	  on t.Prod_Id = t1.Src_Sys_Prod_Id 
      and t.Data_Src_Cd = t1.Data_Src_Cd
      and t1.Start_Dt <= ${TX_DATE}  and t1.End_Dt >= ${TX_DATE}
    left join pdm.t01_cust_h t2
      on t.Cust_ID=t2.Cust_Id
      and t2.Start_Dt <= ${TX_DATE}  and t2.End_Dt >= ${TX_DATE}
    left join VT_pre_t88_chrem_fund_acct_info b
      on t.agmt_id=b.agmt_id
      and t.Data_Src_Cd=b.Data_Src_Cd
      and t.Cust_ID=b.Cust_ID
    where
      ${TX_DATE} between t.Start_Dt and t.end_dt'
	;
  
    CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	
	/*算累计*/
	-- ETL_STEP_NO = 6
		SET @SQL_STR = 	'
	insert into VT_t88_chrem_fund_acct_info(
		    Statt_Dt,
			Agmt_ID,
			Cust_ID,
			Acct_Nm,
			Cur_Cd,
			Prod_ID,
			Prod_Ctgy_Cd,
			Lot_Totl_Cnt,
			Curr_Bal,
			Mth_Total_Bal,
			Quar_Tota_Bal,
			Yr_Tota_Bal,
			Mth_DAvg,
			Quar_DAvg,
			Yr_DAvg,
			In_Trans_Purch_Amt,
            Mth_Total_Amt,
            Quar_Tota_Amt,
            Yr_Tota_Amt,
            Mth_DAvg_Amt,
            Quar_DAvg_Amt,
            Yr_DAvg_Amt,
			Data_Src_Cd,
			Totl_In_Trans_Amt,
			Mth_Accm_Totl_In_Trans_Amt,
			Quar_Accm_Totl_In_Trans_Amt,
			Yr_Accm_Totl_In_Trans_Amt,
			Mth_DAvg_Totl_In_Trans_Amt,
			Quar_DAvg_Totl_In_Trans_Amt,
			Yr_DAvg_Totl_In_Trans_Amt,
			Redem_Prep_Inacct_Amt,
			Mth_Accm_Redem_Prep_Inacct_Amt,
			Quar_Accm_Redem_Prep_Inacct_Amt,
			Yr_Accm_Redem_Prep_Inacct_Amt,
			Mth_DAvg_Redem_Prep_Inacct_Amt,
			Quar_DAvg_Redem_Prep_Inacct_Amt,
			Yr_DAvg_Redem_Prep_Inacct_Amt	
	)
		select 
	    ${TX_DATE} ,-- 1统计日期
			COALESCE(t.Agmt_ID,b.Agmt_ID),
			COALESCE(t.Cust_ID,b.Cust_ID),
			COALESCE(t.Acct_Nm,b.Acct_Nm),
			COALESCE(t.Cur_Cd,b.Cur_Cd),
			COALESCE(t.Prod_ID,b.Prod_ID),
			COALESCE(t.Prod_Ctgy_Cd,b.Prod_Ctgy_Cd),
			COALESCE(t.Lot_Totl_Cnt,0),
			COALESCE(t.Curr_Bal,0),
			case when ${TX_DATE} = ${THIS_MONTH_BEGIN} then COALESCE(t.Curr_Bal,0) 
              else COALESCE(b.Mth_Total_Bal,0)+COALESCE(t.Curr_Bal,0)
			end as Mth_Total_Bal,	-- 月累计余额
			case when ${TX_DATE} = ${THIS_QUART_BEGIN} then COALESCE(t.Curr_Bal,0) 
 		      else COALESCE(b.Quar_Tota_Bal,0)+COALESCE(t.Curr_Bal,0) 
			END as Quar_Tota_Bal,  -- 	季累计余额
			case when ${TX_DATE} = ${THIS_YEAR_BEGIN} then COALESCE(t.Curr_Bal,0) 
 		      else COALESCE(b.Yr_Tota_Bal,0)+COALESCE(t.Curr_Bal,0) 
			END as Yr_Tota_Bal  ,		-- 年累计余额
			case when ${TX_DATE} = ${THIS_MONTH_BEGIN} then COALESCE(t.Curr_Bal,0) / (${TX_DATE}-${THIS_MONTH_BEGIN} + 1) 
			  else (COALESCE(b.Mth_Total_Bal,0)+COALESCE(t.Curr_Bal,0)) / (${TX_DATE}-${THIS_MONTH_BEGIN} + 1)
            end	as Mth_DAvg	,-- 	月日均余额
			case when ${TX_DATE} = ${THIS_QUART_BEGIN} then COALESCE(t.Curr_Bal,0) / (${TX_DATE}-${THIS_QUART_BEGIN} + 1) 
              else (COALESCE(b.Quar_Tota_Bal,0)+COALESCE(t.Curr_Bal,0) ) / (${TX_DATE}-${THIS_QUART_BEGIN} + 1)
			END as Quar_DAvg	,-- 	季日均余额
			case when ${TX_DATE} = ${THIS_YEAR_BEGIN} then COALESCE(t.Curr_Bal,0) / (${TX_DATE}-${THIS_YEAR_BEGIN} + 1) 
              else (COALESCE(b.Yr_Tota_Bal,0)+COALESCE(t.Curr_Bal,0)) / (${TX_DATE}-${THIS_YEAR_BEGIN} + 1)
			END as Yr_DAvg		,-- 	年日均余额
            COALESCE(t.In_Trans_Purch_Amt,0),
			case when ${TX_DATE} = ${THIS_MONTH_BEGIN} then COALESCE(t.In_Trans_Purch_Amt,0) 
              else COALESCE(b.Mth_Total_Amt,0)+COALESCE(t.In_Trans_Purch_Amt,0)
			end as Mth_Total_Amt,	-- 月累计在途申购金额
			case when ${TX_DATE} = ${THIS_QUART_BEGIN} then COALESCE(t.In_Trans_Purch_Amt,0) 
 		      else COALESCE(b.Quar_Tota_Amt,0)+COALESCE(t.In_Trans_Purch_Amt,0) 
			END as Quar_Tota_Amt,  -- 	季累计在途申购金额
			case when ${TX_DATE} = ${THIS_YEAR_BEGIN} then COALESCE(t.In_Trans_Purch_Amt,0) 
 		      else COALESCE(b.Yr_Tota_Amt,0)+COALESCE(t.In_Trans_Purch_Amt,0) 
			END as Yr_Tota_Amt,		-- 年累计在途申购金额
			case when ${TX_DATE} = ${THIS_MONTH_BEGIN} then COALESCE(t.In_Trans_Purch_Amt,0) / (${TX_DATE}-${THIS_MONTH_BEGIN} + 1) 
			  else (COALESCE(b.Mth_Total_Amt,0)+COALESCE(t.In_Trans_Purch_Amt,0)) / (${TX_DATE}-${THIS_MONTH_BEGIN} + 1)
            end	as Mth_DAvg_Amt,-- 	月日均在途申购金额
			case when ${TX_DATE} = ${THIS_QUART_BEGIN} then COALESCE(t.In_Trans_Purch_Amt,0) / (${TX_DATE}-${THIS_QUART_BEGIN} + 1) 
              else (COALESCE(b.Quar_Tota_Amt,0)+COALESCE(t.In_Trans_Purch_Amt,0) ) / (${TX_DATE}-${THIS_QUART_BEGIN} + 1)
			END as Quar_DAvg_Amt,-- 	季日均在途申购金额
			case when ${TX_DATE} = ${THIS_YEAR_BEGIN} then COALESCE(t.In_Trans_Purch_Amt,0) / (${TX_DATE}-${THIS_YEAR_BEGIN} + 1) 
              else (COALESCE(b.Yr_Tota_Amt,0)+COALESCE(t.In_Trans_Purch_Amt,0)) / (${TX_DATE}-${THIS_YEAR_BEGIN} + 1)
			END as Yr_DAvg_Amt,-- 	年日均在途申购金额
			COALESCE(t.Data_Src_Cd,b.Data_Src_Cd) ,
			 COALESCE(t.Totl_In_Trans_Amt,0),  -- 总在途金额
			case when ${TX_DATE} = ${THIS_MONTH_BEGIN} then COALESCE(t.Totl_In_Trans_Amt,0) 
              else COALESCE(b.Mth_Accm_Totl_In_Trans_Amt,0)+COALESCE(t.Totl_In_Trans_Amt,0)
			end as Mth_Accm_Totl_In_Trans_Amt,	-- 月累计总在途金额
			case when ${TX_DATE} = ${THIS_QUART_BEGIN} then COALESCE(t.Totl_In_Trans_Amt,0) 
 		      else COALESCE(b.Quar_Accm_Totl_In_Trans_Amt,0)+COALESCE(t.Totl_In_Trans_Amt,0) 
			END as Quar_Accm_Totl_In_Trans_Amt,  -- 	季累计总在途金额
			case when ${TX_DATE} = ${THIS_YEAR_BEGIN} then COALESCE(t.Totl_In_Trans_Amt,0) 
 		      else COALESCE(b.Yr_Accm_Totl_In_Trans_Amt,0)+COALESCE(t.Totl_In_Trans_Amt,0) 
			END as Yr_Accm_Totl_In_Trans_Amt,		-- 年累计总在途金额
			case when ${TX_DATE} = ${THIS_MONTH_BEGIN} then COALESCE(t.Totl_In_Trans_Amt,0) / (${TX_DATE}-${THIS_MONTH_BEGIN} + 1) 
			  else (COALESCE(b.Mth_Accm_Totl_In_Trans_Amt,0)+COALESCE(t.Totl_In_Trans_Amt,0)) / (${TX_DATE}-${THIS_MONTH_BEGIN} + 1)
            end	as Mth_DAvg_Totl_In_Trans_Amt,-- 	月日均总在途金额
			case when ${TX_DATE} = ${THIS_QUART_BEGIN} then COALESCE(t.Totl_In_Trans_Amt,0) / (${TX_DATE}-${THIS_QUART_BEGIN} + 1) 
              else (COALESCE(b.Quar_Accm_Totl_In_Trans_Amt,0)+COALESCE(t.Totl_In_Trans_Amt,0) ) / (${TX_DATE}-${THIS_QUART_BEGIN} + 1)
			END as Quar_DAvg_Totl_In_Trans_Amt,-- 	季日均总在途金额
			case when ${TX_DATE} = ${THIS_YEAR_BEGIN} then COALESCE(t.Totl_In_Trans_Amt,0) / (${TX_DATE}-${THIS_YEAR_BEGIN} + 1) 
              else (COALESCE(b.Yr_Accm_Totl_In_Trans_Amt,0)+COALESCE(t.Totl_In_Trans_Amt,0)) / (${TX_DATE}-${THIS_YEAR_BEGIN} + 1)
			END as Yr_DAvg_Totl_In_Trans_Amt,-- 	年日均总在途金额
			 COALESCE(t.Redem_Prep_Inacct_Amt,0),  -- 赎回待上帐在途金额
			case when ${TX_DATE} = ${THIS_MONTH_BEGIN} then COALESCE(t.Redem_Prep_Inacct_Amt,0) 
              else COALESCE(b.Mth_Accm_Redem_Prep_Inacct_Amt,0)+COALESCE(t.Redem_Prep_Inacct_Amt,0)
			end as Mth_Accm_Redem_Prep_Inacct_Amt,	-- 月累计赎回待上帐在途金额
			case when ${TX_DATE} = ${THIS_QUART_BEGIN} then COALESCE(t.Redem_Prep_Inacct_Amt,0) 
 		      else COALESCE(b.Quar_Accm_Redem_Prep_Inacct_Amt,0)+COALESCE(t.Redem_Prep_Inacct_Amt,0) 
			END as Quar_Accm_Redem_Prep_Inacct_Amt,  -- 	季累计赎回待上帐在途金额
			case when ${TX_DATE} = ${THIS_YEAR_BEGIN} then COALESCE(t.Redem_Prep_Inacct_Amt,0) 
 		      else COALESCE(b.Yr_Accm_Redem_Prep_Inacct_Amt,0)+COALESCE(t.Redem_Prep_Inacct_Amt,0) 
			END as Yr_Accm_Redem_Prep_Inacct_Amt,		-- 年累计赎回待上帐在途金额
			case when ${TX_DATE} = ${THIS_MONTH_BEGIN} then COALESCE(t.Redem_Prep_Inacct_Amt,0) / (${TX_DATE}-${THIS_MONTH_BEGIN} + 1) 
			  else (COALESCE(b.Mth_Accm_Redem_Prep_Inacct_Amt,0)+COALESCE(t.Redem_Prep_Inacct_Amt,0)) / (${TX_DATE}-${THIS_MONTH_BEGIN} + 1)
            end	as Mth_DAvg_Redem_Prep_Inacct_Amt,-- 	月日均赎回待上帐在途金额
			case when ${TX_DATE} = ${THIS_QUART_BEGIN} then COALESCE(t.Redem_Prep_Inacct_Amt,0) / (${TX_DATE}-${THIS_QUART_BEGIN} + 1) 
              else (COALESCE(b.Quar_Accm_Redem_Prep_Inacct_Amt,0)+COALESCE(t.Redem_Prep_Inacct_Amt,0) ) / (${TX_DATE}-${THIS_QUART_BEGIN} + 1)
			END as Quar_DAvg_Redem_Prep_Inacct_Amt,-- 	季日均赎回待上帐在途金额
			case when ${TX_DATE} = ${THIS_YEAR_BEGIN} then COALESCE(t.Redem_Prep_Inacct_Amt,0) / (${TX_DATE}-${THIS_YEAR_BEGIN} + 1) 
              else (COALESCE(b.Yr_Accm_Redem_Prep_Inacct_Amt,0)+COALESCE(t.Redem_Prep_Inacct_Amt,0)) / (${TX_DATE}-${THIS_YEAR_BEGIN} + 1)
			END as Yr_DAvg_Redem_Prep_Inacct_Amt-- 	年日均赎回待上帐在途金额
	from AT_t88_chrem_fund_acct_info t
    FULL JOIN (
	SELECT * FROM PDM.t88_chrem_fund_acct_info 
		WHERE STATT_DT = ${LAST_TX_DATE} 
		AND (
			  CASE WHEN ${TX_DATE} = ${THIS_YEAR_BEGIN}
				THEN 0
				ELSE 1
			  END
			) = 1       
          ) b -- 昨日数据

	ON t.Agmt_ID = b.Agmt_ID  -- 以主键作on条件 
    '
	;
  
    CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
  
	/*检查插入的临时表数据是否有主键错误*/
	-- ETL_STEP_NO = 7	
	DELETE FROM ETL.ETL_JOB_STATUS_EDW 	WHERE tx_date = ETL_TX_DATE   AND step_no = ETL_STEP_NO	AND sql_unit = ETL_T_TAB_ENG_NAME;
	INSERT INTO ETL.ETL_JOB_STATUS_EDW VALUES ('',SESSION_USER(),ETL_T_TAB_ENG_NAME,ETL_TX_DATE,ETL_STEP_NO,'主键是否重复验证',0,'Running','',CURRENT_TIMESTAMP,'');
  
	SELECT COUNT(*) INTO PK_COUNT
	FROM 
	(
		SELECT Agmt_ID FROM VT_t88_chrem_fund_acct_info
		GROUP BY 1
		HAVING COUNT(*) > 1
	) A ;
	IF PK_COUNT > 0
	THEN
		SET OUT_RES_MSG = '9999';
			update ETL.ETL_JOB_STATUS_EDW 
				set step_status = 'Failed',
					step_err_log = '主键重复',
					last_end_time = current_timestamp
			  WHERE user_id = SESSION_USER() 
				AND tx_date = ETL_TX_DATE 
				AND step_no = ETL_STEP_NO
				AND sql_unit = ETL_T_TAB_ENG_NAME
				AND step_status = 'Running';		
		leave lable;
	END IF;
	
	update ETL.ETL_JOB_STATUS_EDW 
				set step_status = 'Done',
					step_err_log = '验证通过主键无重复',
					last_end_time = current_timestamp
			  WHERE user_id = SESSION_USER() 
				AND tx_date = ETL_TX_DATE 
				AND step_no = ETL_STEP_NO
				AND sql_unit = ETL_T_TAB_ENG_NAME
				AND step_status = 'Running';
				
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
 	
	/* 支持数据重跑*/
	-- ETL_STEP_NO = 8
	SET @SQL_STR = '
	DELETE FROM pdm.t88_chrem_fund_acct_info WHERE Statt_Dt >= ${TX_DATE}';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	/*通过主键检查的数据插入正式表中*/
-- ETL_STEP_NO = 9	
	SET @SQL_STR = '
	INSERT INTO pdm.t88_chrem_fund_acct_info SELECT * FROM VT_t88_chrem_fund_acct_info where Statt_Dt = ${TX_DATE}';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	/*删除今日临时表*/
	-- ETL_STEP_NO = 10
	SET @SQL_STR = 'DROP TEMPORARY TABLE VT_t88_chrem_fund_acct_info';
	
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	
	SET @SQL_STR = 'DROP TEMPORARY TABLE AT_t88_chrem_fund_acct_info';
	
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
		/*删除昨日临时表*/
	-- ETL_STEP_NO = 11
	SET @SQL_STR = 'DROP TEMPORARY TABLE VT_pre_t88_chrem_fund_acct_info';
	
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	SET OUT_RES_MSG = 'SUCCESSFUL';

END |