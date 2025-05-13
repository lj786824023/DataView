DELIMITER |

CREATE DEFINER="gbase"@"%" PROCEDURE "pr_f5_t88_insu_acct_info"(
	OUT	OUT_RES_MSG		VARCHAR(200),
	IN	IN_TX_DATE		VARCHAR(8) 
	)
lable:BEGIN
 
/**********************************
 * yj 20220109 新建
 * 保险账户信息
 * 
 *********************************/
	
	DECLARE ETL_T_TAB_ENG_NAME 		VARCHAR(50)		DEFAULT 't88_insu_acct_info';
	DECLARE ETL_STEP_NO				INTEGER			DEFAULT 1;
	DECLARE ETL_TX_DATE 			VARCHAR(8)		DEFAULT IN_TX_DATE;
	DECLARE PK_COUNT				BIGINT			DEFAULT 0;


	/*定义临时表*/
-- ETL_STEP_NO = 1 
	SET @SQL_STR = '
	DROP TEMPORARY TABLE IF EXISTS pdm.VT_t88_insu_acct_info';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

-- ETL_STEP_NO = 2 
	SET @SQL_STR = '
	CREATE TEMPORARY  TABLE pdm.VT_t88_insu_acct_info LIKE pdm.t88_insu_acct_info';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

	/*创建昨日临时表*/
	-- ETL_STEP_NO = 3 
	SET @SQL_STR = '
	DROP TEMPORARY TABLE IF EXISTS pdm.VT_pre_t88_insu_acct_info';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

	-- ETL_STEP_NO = 4
	SET @SQL_STR = '
	CREATE TEMPORARY TABLE pdm.VT_pre_t88_insu_acct_info(
		Agmt_ID	VARCHAR(80)	NOT	NULL	COMMENT	\'协议编号\',			
		Cust_ID	VARCHAR(60)	NOT	NULL	COMMENT	\'客户编号\',			
		Cust_Nm	VARCHAR(200)		COMMENT	\'客户名称\',			
		Bank_Acct_Num	VARCHAR(64)		COMMENT	\'银行账号\',			
		Cur_Cd	VARCHAR(30)		COMMENT	\'币种代码\',			
		Prod_ID	VARCHAR(30)	NOT	NULL	COMMENT	\'产品编号\',
		Prod_Ctgy_Cd	VARCHAR(10)	NOT	NULL	COMMENT	\'产品类别代码\',		
		Curr_Bal	DECIMAL(22,6)	NOT	NULL	COMMENT	\'当前余额\',			
		Mth_Total_Bal	DECIMAL(22,6)	NOT	NULL	COMMENT	\'月累积余额\',			
		Quar_Tota_Bal	DECIMAL(22,6)	NOT	NULL	COMMENT	\'季累积余额\',			
		Yr_Tota_Bal	DECIMAL(22,6)	NOT	NULL	COMMENT	\'年累积余额\',			
		Mth_davg_bal	DECIMAL(22,6)	NOT	NULL	COMMENT	\'月日均余额\',			
		Quar_DAvg	DECIMAL(22,6)	NOT	NULL	COMMENT	\'季日均余额\',			
		yr_davg_bal	DECIMAL(22,6)	NOT	NULL	COMMENT	\'年日均余额\',			
		Data_Src_Cd	VARCHAR(30)	NOT	NULL	COMMENT	\'数据来源表名\'	
	)';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

	/*昨日金额数据插入昨日临时表*/
	-- ETL_STEP_NO = 5
		SET @SQL_STR = '
	INSERT INTO pdm.VT_pre_t88_insu_acct_info(
		Agmt_ID,
			Cust_ID,
			Cust_Nm,
			Bank_Acct_Num,
			Cur_Cd,
			Prod_ID,
			Prod_Ctgy_Cd,
			Curr_Bal,
			Mth_Total_Bal,
			Quar_Tota_Bal,
			Yr_Tota_Bal,
			Mth_davg_bal,
			Quar_DAvg,
			yr_davg_bal,
			Data_Src_Cd
	)
	SELECT 	Agmt_ID,
			Cust_ID,
			Cust_Nm,
			Bank_Acct_Num,
			Cur_Cd,
			Prod_ID,
			Prod_Ctgy_Cd,
			Curr_Bal,
			Mth_Total_Bal,
			Quar_Tota_Bal,
			Yr_Tota_Bal,
			Mth_davg_bal,
			Quar_DAvg,
			yr_davg_bal,
			Data_Src_Cd
	FROM pdm.t88_insu_acct_info WHERE Statt_Dt = ${LAST_TX_DATE}';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	/*今日数据插入到临时表*/
	-- ETL_STEP_NO = 6
		SET @SQL_STR = 	'
	insert into pdm.VT_t88_insu_acct_info(
		Statt_Dt,
			Agmt_ID,
			Cust_ID,
			Cust_Nm,
			Bank_Acct_Num,
			Cur_Cd,
			Prod_ID,
			Prod_Ctgy_Cd,
			Curr_Bal,
			Mth_Total_Bal,
			Quar_Tota_Bal,
			Yr_Tota_Bal,
			Mth_davg_bal,
			Quar_DAvg,
			yr_davg_bal,
			Data_Src_Cd
	)
select ${TX_DATE} as Statt_Dt	-- 统计日期
  ,t1.Agmt_ID AS Agmt_ID	-- 协议编号
  ,t1.Cust_ID AS Cust_ID	-- 客户编号
  ,t2.Cust_Nm AS Cust_Nm	-- 客户名称
  ,t1.TX_Acct_Num AS Bank_Acct_Num	-- 银行账号
  ,''CNY'' AS Cur_Cd	-- 币种代码
  ,t1.Prod_Cd AS Prod_ID	-- 产品编号
  ,t3.Prod_Sub_Cls AS Prod_Ctgy_Cd	-- 产品类别代码
  ,t1.Insu_Amt AS Curr_Bal	-- 当前余额
  	,case when ${TX_DATE} = ${THIS_MONTH_BEGIN} then COALESCE(t1.Insu_Amt,0)
 		      else COALESCE(t1.Insu_Amt,0) + COALESCE(t4.Mth_Total_Bal,0) 
			end as Mth_Total_Bal	-- 月累积余额
			,case when ${TX_DATE} = ${THIS_QUART_BEGIN} then COALESCE(t1.Insu_Amt,0) 
 		      else COALESCE(t1.Insu_Amt,0) + COALESCE(t4.Quar_Tota_Bal, 0) 
			END as Quar_Tota_Bal  -- 	季累积余额
			,case when ${TX_DATE} = ${THIS_YEAR_BEGIN} then COALESCE(t1.Insu_Amt,0)
 		      else COALESCE(t1.Insu_Amt,0) + COALESCE(t4.Yr_Tota_Bal, 0) 
			END as Yr_Tota_Bal  		-- 年累积余额
			,case when ${TX_DATE} = ${THIS_MONTH_BEGIN} then COALESCE(t1.Insu_Amt,0)
 		      else (COALESCE(t1.Insu_Amt,0) + COALESCE(t4.Mth_Total_Bal,0)) / (${TX_DATE}-${THIS_MONTH_BEGIN} + 1)
			end as Mth_DAvg	-- 	月日均余额
			,case when ${TX_DATE} = ${THIS_QUART_BEGIN} then COALESCE(t1.Insu_Amt,0)
 		      else (COALESCE(t1.Insu_Amt,0) + COALESCE(t4.Quar_Tota_Bal,0)) / (${TX_DATE}-${THIS_QUART_BEGIN} + 1)
			end as Quar_DAvg	-- 	季日均余额
			,case when ${TX_DATE} = ${THIS_YEAR_BEGIN} then COALESCE(t1.Insu_Amt,0)
 		      else (COALESCE(t1.Insu_Amt,0) + COALESCE(t4.Yr_Tota_Bal,0)) / (${TX_DATE}-${THIS_YEAR_BEGIN} + 1)
			end as Yr_DAvg		-- 	年日均余额
			,t1.Data_Src_Cd    -- 来源系统
       FROM pdm.t03_guar_slip_info_h t1	-- 保单信息历史
        left join pdm.t01_cust_h t2 -- 客户历史
        	on t1.Cust_ID=t2.Cust_Id
        	and t2.Start_Dt <= ${TX_DATE}  
        	and t2.End_Dt >= ${TX_DATE}
        left join pdm.t02_ybt_prod_h t3 -- 银保通产品表
        	on t1.Prod_Cd = t3.Prod_Id 
        	and t3.Start_Dt <= ${TX_DATE}  
        	and t3.End_Dt >= ${TX_DATE}
        left join pdm.VT_pre_t88_insu_acct_info t4    
        	on t1.agmt_id=t4.agmt_id
        	and t1.Data_Src_Cd=t4.Data_Src_Cd
        	and t1.Cust_ID=t4.Cust_ID
        where t1.Start_Dt <= ${TX_DATE}  
        	and t1.End_Dt >= ${TX_DATE}
        	and t1.Guar_Slip_Stat = ''0''-- 保单状态  0:正常
            and ${TX_DATE} between t1.Proc_Dt and add_months(t1.Proc_Dt,12*t1.Prot_Yr_Term) -- added by LJZ on 2023-03-14
'
	;
  
    CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
  
	/*检查插入的临时表数据是否有主键错误*/
	-- ETL_STEP_NO = 7	
	DELETE FROM etl.ETL_JOB_STATUS_EDW 	WHERE tx_date = ETL_TX_DATE   AND step_no = ETL_STEP_NO	AND sql_unit = ETL_T_TAB_ENG_NAME;
	INSERT INTO etl.ETL_JOB_STATUS_EDW VALUES ('',SESSION_USER(),ETL_T_TAB_ENG_NAME,ETL_TX_DATE,ETL_STEP_NO,'主键是否重复验证',0,'Running','',CURRENT_TIMESTAMP,'');
  
	SELECT COUNT(*) INTO PK_COUNT
	FROM 
	(
		SELECT Agmt_ID FROM pdm.VT_t88_insu_acct_info
		GROUP BY 1
		HAVING COUNT(*) > 1
	) A ;
	IF PK_COUNT > 0
	THEN
		SET OUT_RES_MSG = '9999';
			update etl.ETL_JOB_STATUS_EDW 
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
	
	update etl.ETL_JOB_STATUS_EDW 
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
	DELETE FROM pdm.t88_insu_acct_info WHERE Statt_Dt >= ${TX_DATE}';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	/*通过主键检查的数据插入正式表中*/
-- ETL_STEP_NO = 9	
	SET @SQL_STR = '
	INSERT INTO pdm.t88_insu_acct_info SELECT * FROM pdm.VT_t88_insu_acct_info where Statt_Dt = ${TX_DATE}';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	/*删除今日临时表*/
	-- ETL_STEP_NO = 10
	SET @SQL_STR = '
	DROP TEMPORARY TABLE pdm.VT_t88_insu_acct_info';
	
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
		/*删除昨日临时表*/
	-- ETL_STEP_NO = 11
	SET @SQL_STR = '
	DROP TEMPORARY TABLE pdm.VT_pre_t88_insu_acct_info';
	
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	SET OUT_RES_MSG = '0';

END |