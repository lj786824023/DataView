DELIMITER |

CREATE DEFINER="cq_sjpt"@"%" PROCEDURE "pr_f5_t88_subj_sum_dtl"(
	OUT	OUT_RES_MSG		VARCHAR(200),
	IN	IN_TX_DATE		VARCHAR(8)
	)
lable:BEGIN
/**********************************
 * whd 20210518 新建
 * whd 20210525 增加折人民币字段
 * whd 20210701 增加累计数、平均数处理逻辑
 * 科目汇总明细
 *******************************/
	
	DECLARE ETL_T_TAB_ENG_NAME 		VARCHAR(50)		DEFAULT 't88_subj_sum_dtl';
	DECLARE ETL_STEP_NO				INTEGER			DEFAULT 1;
	DECLARE ETL_TX_DATE 			VARCHAR(8)		DEFAULT IN_TX_DATE;
	DECLARE PK_COUNT				BIGINT			DEFAULT 0;

	/*定义临时表*/
-- ETL_STEP_NO = 1 
	SET @SQL_STR = '
DROP TEMPORARY TABLE IF EXISTS pdm.VT_t88_subj_sum_dtl';
	
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
-- ETL_STEP_NO = 2 	
	SET @SQL_STR = '
CREATE TEMPORARY TABLE pdm.VT_t88_subj_sum_dtl LIKE pdm.t88_subj_sum_dtl';
	
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

-- ETL_STEP_NO = 3 
	SET @SQL_STR = '
DROP TEMPORARY TABLE IF EXISTS pdm.AT_t88_subj_sum_dtl';
	
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
-- ETL_STEP_NO = 4 	
	SET @SQL_STR = '
CREATE TEMPORARY TABLE pdm.AT_t88_subj_sum_dtl LIKE pdm.t88_subj_sum_dtl';
	
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	

	/*今日数据先插入临时表*/
-- ETL_STEP_NO = 5 	
	SET @SQL_STR ='
	INSERT INTO pdm.VT_t88_subj_sum_dtl(
		Statt_Dt					,-- 1	统计日期
		Org_ID						,-- 2	机构编号
		Cur_Cd						,-- 3	币种代码
		Subj_ID						,-- 4	科目编号
		Subj_Nm						,-- 5	科目名称
		Subj_Bal_Drct_Cd			,-- 6	科目余额方向代码
		Up_Subj_ID					,-- 7	上级科目编号
		Exchg_Rate					,-- 8	汇率
		Yr_Bgn_Bal					,-- 9	年初余额
		Yr_Bgn_Debt_Bal				,-- 10	年初借方余额
		Yr_Bgn_Crdt_Bal				,-- 11	年初贷方余额
		Rmb_Yr_Bgn_Bal				,-- 12	人民币年初余额
		Rmb_Yr_Bgn_Debt_Bal			,-- 13	人民币年初借方余额
		Rmb_Yr_Bgn_Crdt_Bal			,-- 14	人民币年初贷方余额
		Curr_Bal					,-- 15	当前余额
		Curr_Debt_Bal				,-- 16	当前借方余额 
		Curr_Crdt_Bal				,-- 17	当前贷方余额
		Curr_Rmb_Bal				,-- 18	当前人民币余额
		Curr_Rmb_Debt_Bal			,-- 19	当前人民币借方余额
		Curr_Rmb_Crdt_Bal			,-- 20	当前人民币贷方余额
		CurDay_Debt_Amt				,-- 21	本日借方发生额 t.Today_Dmt_Cur_Debt_Amt	,-- 今日本币借方金额
		CurDay_Crdt_Amt				,-- 22	本日贷方发生额 t.Today_Dmt_Cur_Crdt_Amt	,-- 今日本币贷方金额
		CurDay_Rmb_Debt_Amt			,-- 23 	本日人民币借方发生额
		CurDay_Rmb_Crdt_Amt			,-- 24 	本日人民币贷方发生额
		CurMth_Total_Bal			,-- 25	本月累积余额
		CurMth_Total_Debt_Bal		,-- 26	本月累积借方余额
		CurMth_Total_Crdt_Bal		,-- 27	本月累积贷方余额
		CurMth_Rmb_Total_Bal		,-- 28	本月人民币累积余额
		CurMth_Rmb_Total_Debt_Bal	,-- 29	本月人民币累积借方余额
		CurMth_Rmb_Total_Crdt_Bal	,-- 30	本月人民币累积贷方余额
		Curr_Quar_Total_Bal			,-- 31	本季累积余额
		Curr_Quar_Total_Debt_Bal	,-- 32	本季累积借方余额
		Curr_Quar_Total_Crdt_Bal	,-- 33	本季累积贷方余额
		Curr_Quar_Rmb_Total_Bal		,-- 34	本季人民币累积余额
		Curr_Quar_Rmb_Total_Debt_Bal,-- 35	本季人民币累积借方余额
		Curr_Quar_Rmb_Total_Crdt_Bal,-- 36	本季人民币累积贷方余额
		Annl_Total_Bal				,-- 37	本年累积余额
		Annl_Total_Debt_Bal			,-- 38	本年累积借方余额
		Annl_Total_Crdt_Bal			,-- 39	本年累积贷方余额
		Annl_Rmb_Total_Bal			,-- 40	本年人民币累积余额
		Annl_Rmb_Total_Debt_Bal		,-- 41	本年人民币累积借方余额
		Annl_Rmb_Total_Crdt_Bal		,-- 42	本年人民币累积贷方余额
		CurMth_DAvg					,-- 43	本月日均余额
		CurMth_DAvg_Debt_Bal		,-- 44	本月日均借方余额
		CurMth_DAvg_Crdt_Bal		,-- 45	本月日均贷方余额
		CurMth_Rmb_DAvg				,-- 46	本月人民币日均余额
		CurMth_Rmb_DAvg_Debt_Bal	,-- 47	本月人民币日均借方余额
		CurMth_Rmb_DAvg_Crdt_Bal	,-- 48	本月人民币日均贷方余额
		Curr_Quar_DAvg				,-- 49	本季日均余额
		Curr_Quar_DAvg_Debt_Bal		,-- 50	本季日均借方余额
		Curr_Quar_DAvg_Crdt_Bal		,-- 51	本季日均贷方余额
		Curr_Quar_Rmb_DAvg			,-- 52	本季人民币日均余额
		Curr_Quar_Rmb_DAvg_Debt_Bal	,-- 53	本季人民币日均借方余额
		Curr_Quar_Rmb_DAvg_Crdt_Bal	,-- 54	本季人民币日均贷方余额
		Annl_DAvg					,-- 55	本年日均余额
		Annl_DAvg_Debt_Bal			,-- 56	本年日均借方余额
		Annl_DAvg_Crdt_Bal			,-- 57	本年日均贷方余额
		Annl_Rmb_DAvg				,-- 58	本年人民币日均余额
		Annl_Rmb_DAvg_Debt_Bal		,-- 59	本年人民币日均借方余额
		Annl_Rmb_DAvg_Crdt_Bal		,-- 60	本年人民币日均贷方余额
		Data_Src_Cd					 -- 61      数据来源代码
	)
SELECT ${TX_DATE}						,-- 1统计日期
		t.Org_Id						,-- 2机构编号
		t.Cur_Cd						,-- 3币种代码
		t.Subj_ID						,-- 4科目编号
		nvl(t1.Subj_Nm,${NULL_STR})		,-- 5科目名称
		t.Debt_Crdt_Drct_Cd				,-- 6科目余额方向代码
		nvl(t1.Up_Subj_Id,${NULL_STR})	,-- 7上级科目编号
		nvl(t2.Mdl_Prc,0)				,-- 8汇率
		0								,-- 9年初余额
		0								,-- 10年初借方余额
		0								,-- 11年初贷方余额
		0								,-- 12人民币年初余额
		0								,-- 13人民币年初借方余额
		0								,-- 14人民币年初贷方余额
		t.Bal							,-- 15当前余额
		t.Debt_Bal						,-- 16当前借方余额 
		t.Crdt_Bal						,-- 17当前贷方余额
		nvl(t.Bal * t2.Mdl_Prc,0)		,-- 18当前人民币余额
		nvl(t.Debt_Bal * t2.Mdl_Prc,0)	,-- 19当前人民币借方余额
		nvl(t.Crdt_Bal * t2.Mdl_Prc,0)	,-- 20当前人民币贷方余额
		t.Today_Dmt_Cur_Debt_Amt		,-- 21本日借方发生额	,-- 今日本币借方金额
		t.Today_Dmt_Cur_Crdt_Amt		,-- 22本日贷方发生额	,-- 今日本币贷方金额
		nvl(t.Today_Dmt_Cur_Debt_Amt * t2.Mdl_Prc,0)	,-- 23 本日人民币借方发生额
		nvl(t.Today_Dmt_Cur_Crdt_Amt * t2.Mdl_Prc,0)	,-- 24 本日人民币贷方发生额
		0  ,-- 25	本月累积余额
		0  ,-- 26	本月累积借方余额
		0  ,-- 27	本月累积贷方余额
		0  ,-- 28	本月人民币累积余额
		0  ,-- 29	本月人民币累积借方余额
		0  ,-- 30	本月人民币累积贷方余额
		0  ,-- 31	本季累积余额
		0  ,-- 32	本季累积借方余额
		0  ,-- 33	本季累积贷方余额
		0  ,-- 34	本季人民币累积余额
		0  ,-- 35	本季人民币累积借方余额
		0  ,-- 36	本季人民币累积贷方余额
		0  ,-- 37	本年累积余额
		0  ,-- 38	本年累积借方余额
		0  ,-- 39	本年累积贷方余额
		0  ,-- 40	本年人民币累积余额
		0  ,-- 41	本年人民币累积借方余额
		0  ,-- 42	本年人民币累积贷方余额
		0  ,-- 43	本月日均余额
		0  ,-- 44	本月日均借方余额
		0  ,-- 45	本月日均贷方余额
		0  ,-- 46	本月人民币日均余额
		0  ,-- 47	本月人民币日均借方余额
		0  ,-- 48	本月人民币日均贷方余额
		0  ,-- 49	本季日均余额
		0  ,-- 50	本季日均借方余额
		0  ,-- 51	本季日均贷方余额
		0  ,-- 52	本季人民币日均余额
		0  ,-- 53	本季人民币日均借方余额
		0  ,-- 54	本季人民币日均贷方余额
		0  ,-- 55	本年日均余额
		0  ,-- 56	本年日均借方余额
		0  ,-- 57	本年日均贷方余额
		0  ,-- 58	本年人民币日均余额
		0  ,-- 59	本年人民币日均借方余额
		0  ,-- 60	本年人民币日均贷方余额
 		t.Data_Src_Cd 		-- 61 数据来源代码	 
  FROM pdm.t10_subj_bal_h t -- 科目余额历史

 LEFT JOIN pdm.t10_gl_bas_info_h t1 -- 会计科目基本信息历史
 	ON t.Subj_Id = t1.Subj_Id 
   AND t1.Start_Dt <= ${TX_DATE}  
   AND t1.End_Dt >= ${TX_DATE}

 LEFT JOIN pdm.t88_exchg_rate t2 -- 汇率牌价表	
 	ON t.Cur_Cd = t2.Init_Cur 
   AND t2.statt_dt = ${TX_DATE}

WHERE t.Start_Dt <= ${TX_DATE}  
  AND t.End_Dt >= ${TX_DATE}'
  ;
  
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

 -- 计算汇总金额和平均数
	-- ETL_STEP_NO = 6 	
SET @SQL_STR ='
	INSERT INTO pdm.AT_t88_subj_sum_dtl(
		Statt_Dt					,-- 1	统计日期
		Org_ID						,-- 2	机构编号
		Cur_Cd						,-- 3	币种代码
		Subj_ID						,-- 4	科目编号
		Subj_Nm						,-- 5	科目名称
		Subj_Bal_Drct_Cd			,-- 6	科目余额方向代码
		Up_Subj_ID					,-- 7	上级科目编号
		Exchg_Rate					,-- 8	汇率
		Yr_Bgn_Bal					,-- 9	年初余额
		Yr_Bgn_Debt_Bal				,-- 10	年初借方余额
		Yr_Bgn_Crdt_Bal				,-- 11	年初贷方余额
		Rmb_Yr_Bgn_Bal				,-- 12	人民币年初余额
		Rmb_Yr_Bgn_Debt_Bal			,-- 13	人民币年初借方余额
		Rmb_Yr_Bgn_Crdt_Bal			,-- 14	人民币年初贷方余额
		Curr_Bal					,-- 15	当前余额
		Curr_Debt_Bal				,-- 16	当前借方余额 
		Curr_Crdt_Bal				,-- 17	当前贷方余额
		Curr_Rmb_Bal				,-- 18	当前人民币余额
		Curr_Rmb_Debt_Bal			,-- 19	当前人民币借方余额
		Curr_Rmb_Crdt_Bal			,-- 20	当前人民币贷方余额
		CurDay_Debt_Amt				,-- 21	本日借方发生额 t.Today_Dmt_Cur_Debt_Amt	,-- 今日本币借方金额
		CurDay_Crdt_Amt				,-- 22	本日贷方发生额 t.Today_Dmt_Cur_Crdt_Amt	,-- 今日本币贷方金额
		CurDay_Rmb_Debt_Amt			,-- 23 	本日人民币借方发生额
		CurDay_Rmb_Crdt_Amt			,-- 24 	本日人民币贷方发生额
		CurMth_Total_Bal			,-- 25	本月累积余额
		CurMth_Total_Debt_Bal		,-- 26	本月累积借方余额
		CurMth_Total_Crdt_Bal		,-- 27	本月累积贷方余额
		CurMth_Rmb_Total_Bal		,-- 28	本月人民币累积余额
		CurMth_Rmb_Total_Debt_Bal	,-- 29	本月人民币累积借方余额
		CurMth_Rmb_Total_Crdt_Bal	,-- 30	本月人民币累积贷方余额
		Curr_Quar_Total_Bal			,-- 31	本季累积余额
		Curr_Quar_Total_Debt_Bal	,-- 32	本季累积借方余额
		Curr_Quar_Total_Crdt_Bal	,-- 33	本季累积贷方余额
		Curr_Quar_Rmb_Total_Bal		,-- 34	本季人民币累积余额
		Curr_Quar_Rmb_Total_Debt_Bal,-- 35	本季人民币累积借方余额
		Curr_Quar_Rmb_Total_Crdt_Bal,-- 36	本季人民币累积贷方余额
		Annl_Total_Bal				,-- 37	本年累积余额
		Annl_Total_Debt_Bal			,-- 38	本年累积借方余额
		Annl_Total_Crdt_Bal			,-- 39	本年累积贷方余额
		Annl_Rmb_Total_Bal			,-- 40	本年人民币累积余额
		Annl_Rmb_Total_Debt_Bal		,-- 41	本年人民币累积借方余额
		Annl_Rmb_Total_Crdt_Bal		,-- 42	本年人民币累积贷方余额
		CurMth_DAvg					,-- 43	本月日均余额
		CurMth_DAvg_Debt_Bal		,-- 44	本月日均借方余额
		CurMth_DAvg_Crdt_Bal		,-- 45	本月日均贷方余额
		CurMth_Rmb_DAvg				,-- 46	本月人民币日均余额
		CurMth_Rmb_DAvg_Debt_Bal	,-- 47	本月人民币日均借方余额
		CurMth_Rmb_DAvg_Crdt_Bal	,-- 48	本月人民币日均贷方余额
		Curr_Quar_DAvg				,-- 49	本季日均余额
		Curr_Quar_DAvg_Debt_Bal		,-- 50	本季日均借方余额
		Curr_Quar_DAvg_Crdt_Bal		,-- 51	本季日均贷方余额
		Curr_Quar_Rmb_DAvg			,-- 52	本季人民币日均余额
		Curr_Quar_Rmb_DAvg_Debt_Bal	,-- 53	本季人民币日均借方余额
		Curr_Quar_Rmb_DAvg_Crdt_Bal	,-- 54	本季人民币日均贷方余额
		Annl_DAvg					,-- 55	本年日均余额
		Annl_DAvg_Debt_Bal			,-- 56	本年日均借方余额
		Annl_DAvg_Crdt_Bal			,-- 57	本年日均贷方余额
		Annl_Rmb_DAvg				,-- 58	本年人民币日均余额
		Annl_Rmb_DAvg_Debt_Bal		,-- 59	本年人民币日均借方余额
		Annl_Rmb_DAvg_Crdt_Bal		,-- 60	本年人民币日均贷方余额
		Data_Src_Cd					 -- 61      数据来源代码
	)
SELECT ${TX_DATE}										,-- 1	统计日期
		COALESCE(t.Org_ID, b.Org_ID)					,-- 2	机构编号
		COALESCE(t.Cur_Cd, b.Cur_Cd)					,-- 3	币种代码
		COALESCE(t.Subj_ID, b.Subj_ID)					,-- 4	科目编号
		COALESCE(t.Subj_Nm, b.Subj_Nm)					,-- 5	科目名称
		COALESCE(t.Subj_Bal_Drct_Cd, b.Subj_Bal_Drct_Cd),-- 6	科目余额方向代码
		COALESCE(t.Up_Subj_ID, b.Up_Subj_ID)			,-- 7	上级科目编号
		COALESCE(t.Exchg_Rate, 1)						,-- 8	汇率
		0												,-- 9	年初余额
		0												,-- 10	年初借方余额
		0												,-- 11	年初贷方余额
		0												,-- 12	人民币年初余额
		0												,-- 13	人民币年初借方余额
		0												,-- 14	人民币年初贷方余额
		COALESCE(t.Curr_Bal,0)							,-- 15	当前余额
		COALESCE(t.Curr_Debt_Bal,0)						,-- 16	当前借方余额 
		COALESCE(t.Curr_Crdt_Bal,0)						,-- 17	当前贷方余额
		COALESCE(t.Curr_Rmb_Bal,0)						,-- 18	当前人民币余额
		COALESCE(t.Curr_Rmb_Debt_Bal,0)					,-- 19	当前人民币借方余额
		COALESCE(t.Curr_Rmb_Crdt_Bal,0)					,-- 20	当前人民币贷方余额
		COALESCE(t.CurDay_Debt_Amt,0)					,-- 21	本日借方发生额  今日本币借方金额
		COALESCE(t.CurDay_Crdt_Amt,0)					,-- 22	本日贷方发生额  今日本币贷方金额
		COALESCE(t.CurDay_Rmb_Debt_Amt,0)				,-- 23 	本日人民币借方发生额
		COALESCE(t.CurDay_Rmb_Crdt_Amt,0)				,-- 24 	本日人民币贷方发生额
		-- --------------------------- 月累计 ------------------------------------------
 	   	CASE WHEN 
			${TX_DATE} = ${THIS_MONTH_BEGIN} THEN COALESCE(t.Curr_Bal,0)
 		 ELSE 
			COALESCE(t.Curr_Bal,0) + COALESCE(b.CurMth_Total_Bal,0) 
	   	END 															AS CurMth_Total_Bal,  				-- 25 本月累积余额

 		CASE WHEN 
			${TX_DATE} = ${THIS_MONTH_BEGIN} THEN COALESCE(t.Curr_Debt_Bal, 0) 
 		 ELSE 
			COALESCE(t.Curr_Debt_Bal, 0) + COALESCE(b.CurMth_Total_Debt_Bal, 0) 
		END 															AS CurMth_Total_Debt_Bal,  			-- 26 本月累积借方余额

 		CASE WHEN 
			${TX_DATE} = ${THIS_MONTH_BEGIN} THEN COALESCE(t.Curr_Crdt_Bal, 0) 
 		 ELSE 
			COALESCE(t.Curr_Crdt_Bal, 0) + COALESCE(b.CurMth_Total_Crdt_Bal, 0) 
		END 															AS CurMth_Total_Crdt_Bal,  			-- 27 本月累积贷方余额

    	CASE WHEN 
			${TX_DATE} = ${THIS_MONTH_BEGIN} THEN COALESCE(t.Curr_Rmb_Bal, 0) 
 		 ELSE 
			COALESCE(t.Curr_Rmb_Bal, 0) + COALESCE(b.CurMth_Rmb_Total_Bal, 0) 
		END 															AS CurMth_Rmb_Total_Bal,  			-- 28 本月人民币累积余额

    	CASE WHEN 
			${TX_DATE} = ${THIS_MONTH_BEGIN} THEN COALESCE(t.Curr_Rmb_Debt_Bal, 0) 
 		 ELSE 
			COALESCE(t.Curr_Rmb_Debt_Bal, 0) + COALESCE(b.CurMth_Rmb_Total_Debt_Bal, 0) 
		END 															AS CurMth_Rmb_Total_Debt_Bal,  		-- 29 本月人民币累积借方余额

 		CASE WHEN 
			${TX_DATE} = ${THIS_MONTH_BEGIN} THEN COALESCE(t.Curr_Rmb_Crdt_Bal, 0) 
 		 ELSE 
			COALESCE(t.Curr_Rmb_Crdt_Bal, 0) + COALESCE(b.CurMth_Rmb_Total_Crdt_Bal, 0) 
		END 															AS CurMth_Rmb_Total_Crdt_Bal,  		-- 30 本月人民币累积贷方余额
 	
 		 -- --------------------------------季累计-------------------------------------------------------
 		CASE WHEN 
			${TX_DATE} = ${THIS_QUART_BEGIN} THEN COALESCE(t.Curr_Bal, 0) 
 		 ELSE 
			COALESCE(t.Curr_Bal, 0) + COALESCE(b.Curr_Quar_Total_Bal, 0) 
		END 															AS Curr_Quar_Total_Bal, 			-- 31 本季累积余额

 		CASE WHEN 
			${TX_DATE} = ${THIS_QUART_BEGIN} THEN COALESCE(t.Curr_Debt_Bal, 0) 
 		 ELSE 
			COALESCE(t.Curr_Debt_Bal, 0) + COALESCE(b.Curr_Quar_Total_Debt_Bal, 0) 
		END 															AS Curr_Quar_Total_Debt_Bal,  		-- 32 本季累积借方余额

 		CASE WHEN 
			${TX_DATE} = ${THIS_QUART_BEGIN} THEN COALESCE(t.Curr_Crdt_Bal, 0) 
 		 ELSE 
			COALESCE(t.Curr_Crdt_Bal, 0) + COALESCE(b.Curr_Quar_Total_Crdt_Bal, 0) 
		END 															AS Curr_Quar_Total_Crdt_Bal,  		-- 33 本季累积贷方余额

 		CASE WHEN 
			${TX_DATE} = ${THIS_QUART_BEGIN} THEN COALESCE(t.Curr_Rmb_Bal, 0) 
 		 ELSE 
			COALESCE(t.Curr_Rmb_Bal, 0) + COALESCE(b.Curr_Quar_Rmb_Total_Bal, 0) 
		END 															AS Curr_Quar_Rmb_Total_Bal,  		-- 34 本季人民币累积余额

 		CASE WHEN 
			${TX_DATE} = ${THIS_QUART_BEGIN} THEN COALESCE(t.Curr_Rmb_Debt_Bal, 0) 
 		 ELSE 
			COALESCE(t.Curr_Rmb_Debt_Bal, 0) + COALESCE(b.Curr_Quar_Rmb_Total_Debt_Bal, 0) 
		END 															AS Curr_Quar_Rmb_Total_Debt_Bal,	-- 35 本季人民币累积借方余额

 		CASE WHEN 
			${TX_DATE} = ${THIS_QUART_BEGIN} THEN COALESCE(t.Curr_Rmb_Crdt_Bal, 0) 
 		 ELSE 
			COALESCE(t.Curr_Rmb_Crdt_Bal, 0) + COALESCE(b.Curr_Quar_Rmb_Total_Crdt_Bal, 0) 
		END 															AS Curr_Quar_Rmb_Total_Crdt_Bal,  	-- 36 本季人民币累积贷方余额
 	
 		 -- --------------------------------------年累计---------------------------------------------------------
 		CASE WHEN 
			${TX_DATE} = ${THIS_YEAR_BEGIN} THEN COALESCE(t.Curr_Bal, 0) 
 		 ELSE 
			COALESCE(t.Curr_Bal, 0) + COALESCE(b.Annl_Total_Bal, 0) 
		END 															AS Annl_Total_Bal,  				-- 37 本年累积余额

 		CASE WHEN 
			${TX_DATE} = ${THIS_YEAR_BEGIN} THEN COALESCE(t.Curr_Debt_Bal, 0) 
 		 ELSE 
			COALESCE(t.Curr_Debt_Bal, 0) + COALESCE(b.Annl_Total_Debt_Bal, 0) 
		END 															AS Annl_Total_Debt_Bal,  			-- 38 本年累积借方余额

 		CASE WHEN 
			${TX_DATE} = ${THIS_YEAR_BEGIN} THEN COALESCE(t.Curr_Crdt_Bal, 0) 
 		 ELSE 
			COALESCE(t.Curr_Crdt_Bal, 0) + COALESCE(b.Annl_Total_Crdt_Bal, 0) 
		END 															AS Annl_Total_Crdt_Bal,  			-- 39 本年累积贷方余额

 		CASE WHEN 
			${TX_DATE} = ${THIS_YEAR_BEGIN} THEN COALESCE(t.Curr_Rmb_Bal, 0) 
 		 ELSE 
			COALESCE(t.Curr_Rmb_Bal, 0) + COALESCE(b.Annl_Rmb_Total_Bal, 0) 
		END 															AS Annl_Rmb_Total_Bal,  			-- 40 本年人民币累积余额

 		CASE WHEN 
			${TX_DATE} = ${THIS_YEAR_BEGIN} THEN COALESCE(t.Curr_Rmb_Debt_Bal, 0) 
 		 ELSE 
			COALESCE(t.Curr_Rmb_Debt_Bal, 0) + COALESCE(b.Annl_Rmb_Total_Debt_Bal, 0) 
		END 															AS Annl_Rmb_Total_Debt_Bal, 	 	-- 41 本年人民币累积借方余额

 		CASE WHEN 
			${TX_DATE} = ${THIS_YEAR_BEGIN} THEN COALESCE(t.Curr_Rmb_Crdt_Bal, 0) 
 		 ELSE 
			COALESCE(t.Curr_Rmb_Crdt_Bal, 0) + COALESCE(b.Annl_Rmb_Total_Crdt_Bal, 0) 
		END 															AS Annl_Rmb_Total_Crdt_Bal,  		-- 42 本年人民币累积贷方余额

 		 -- ----------------------------------------月日均------------------------------------------------------------
 		CASE WHEN 
			${TX_DATE} = ${THIS_MONTH_BEGIN} THEN COALESCE(t.Curr_Bal, 0) 
 		 ELSE 
			(COALESCE(t.Curr_Bal, 0) + COALESCE(b.CurMth_Total_Bal, 0)) / (${TX_DATE}-${THIS_MONTH_BEGIN} + 1)
 		END 															AS CurMth_DAvg,  					-- 43 本月日均余额

 		CASE WHEN 
			${TX_DATE} = ${THIS_MONTH_BEGIN} THEN COALESCE(t.Curr_Debt_Bal, 0) 
 		 ELSE 
			(COALESCE(t.Curr_Debt_Bal, 0) + COALESCE(b.CurMth_Total_Debt_Bal, 0)) / (${TX_DATE}-${THIS_MONTH_BEGIN} + 1)
 		END 															AS CurMth_DAvg_Debt_Bal,			-- 44 本月日均借方余额

 		CASE WHEN 
			${TX_DATE} = ${THIS_MONTH_BEGIN} THEN COALESCE(t.Curr_Crdt_Bal, 0) 
 		 ELSE 
			(COALESCE(t.Curr_Crdt_Bal, 0) + COALESCE(b.CurMth_Total_Crdt_Bal, 0)) / (${TX_DATE}-${THIS_MONTH_BEGIN} + 1)
 		END 															AS CurMth_DAvg_Crdt_Bal,			-- 45 本月日均贷方余额

    	CASE WHEN 
			${TX_DATE} = ${THIS_MONTH_BEGIN} THEN COALESCE(t.Curr_Rmb_Bal, 0) 
 		 ELSE 
			(COALESCE(t.Curr_Rmb_Bal, 0) + COALESCE(b.CurMth_Rmb_Total_Bal, 0)) / (${TX_DATE}-${THIS_MONTH_BEGIN} + 1)
 		END 															AS CurMth_Rmb_DAvg,					-- 46 本月人民币日均余额

    	CASE WHEN 
			${TX_DATE} = ${THIS_MONTH_BEGIN} THEN COALESCE(t.Curr_Rmb_Debt_Bal, 0) 
 		 ELSE 
			(COALESCE(t.Curr_Rmb_Debt_Bal, 0) + COALESCE(b.CurMth_Rmb_Total_Debt_Bal, 0)) / (${TX_DATE}-${THIS_MONTH_BEGIN} + 1)
 		END 															AS CurMth_Rmb_DAvg_Debt_Bal,		-- 47 本月人民币日均借方余额

 		CASE WHEN 
			${TX_DATE} = ${THIS_MONTH_BEGIN} THEN COALESCE(t.Curr_Rmb_Crdt_Bal, 0) 
 		 ELSE 
			(COALESCE(t.Curr_Rmb_Crdt_Bal, 0) + COALESCE(b.CurMth_Rmb_Total_Crdt_Bal, 0)) / (${TX_DATE}-${THIS_MONTH_BEGIN} + 1)
 		END 															AS CurMth_Rmb_DAvg_Crdt_Bal,		-- 48 本月人民币日均贷方余额	 
    
 		 -- ----------------------------------------季日均--------------------------------------------------------------
 		CASE WHEN 
			${TX_DATE} = ${THIS_QUART_BEGIN} THEN COALESCE(t.Curr_Bal, 0) 
 		 ELSE 
			(COALESCE(t.Curr_Bal, 0) + COALESCE(b.Curr_Quar_Total_Bal, 0)) / (${TX_DATE}-${THIS_QUART_BEGIN} + 1)
 		END 															AS Curr_Quar_DAvg, 					-- 49 本季日均余额

 		CASE WHEN 
			${TX_DATE} = ${THIS_QUART_BEGIN} THEN COALESCE(t.Curr_Debt_Bal, 0) 
 		 ELSE 
			(COALESCE(t.Curr_Debt_Bal, 0) + COALESCE(b.Curr_Quar_Total_Debt_Bal, 0)) / (${TX_DATE}-${THIS_QUART_BEGIN} + 1)
 		END 															AS Curr_Quar_DAvg_Debt_Bal,			-- 50 本季日均借方余额

 		CASE WHEN 
			${TX_DATE} = ${THIS_QUART_BEGIN} THEN COALESCE(t.Curr_Crdt_Bal, 0) 
 		 ELSE 
			(COALESCE(t.Curr_Crdt_Bal, 0) + COALESCE(b.Curr_Quar_Total_Crdt_Bal, 0)) / (${TX_DATE}-${THIS_QUART_BEGIN} + 1)
 		END 															AS Curr_Quar_DAvg_Crdt_Bal,			-- 51 本季日均贷方余额

    	CASE WHEN 
			${TX_DATE} = ${THIS_QUART_BEGIN} THEN COALESCE(t.Curr_Rmb_Bal, 0) 
 		 ELSE 
			(COALESCE(t.Curr_Rmb_Bal, 0) + COALESCE(b.Curr_Quar_Rmb_Total_Bal, 0)) / (${TX_DATE}-${THIS_QUART_BEGIN} + 1)
 		END 															AS Curr_Quar_Rmb_DAvg,				-- 52 本季人民币日均余额

    	CASE WHEN 
			${TX_DATE} = ${THIS_QUART_BEGIN} THEN COALESCE(t.Curr_Rmb_Debt_Bal, 0) 
 		 ELSE 
			(COALESCE(t.Curr_Rmb_Debt_Bal, 0) + COALESCE(b.Curr_Quar_Rmb_Total_Debt_Bal, 0)) / (${TX_DATE}-${THIS_QUART_BEGIN} + 1)
 		END 															AS Curr_Quar_Rmb_DAvg_Debt_Bal,		-- 53 本季人民币日均借方余额

 		CASE WHEN 
			${TX_DATE} = ${THIS_QUART_BEGIN} THEN COALESCE(t.Curr_Rmb_Crdt_Bal, 0) 
 		 ELSE 
			(COALESCE(t.Curr_Rmb_Crdt_Bal, 0) + COALESCE(b.Curr_Quar_Rmb_Total_Crdt_Bal, 0)) / (${TX_DATE}-${THIS_QUART_BEGIN} + 1)
 		END 															AS Curr_Quar_Rmb_DAvg_Crdt_Bal,		-- 54 本季人民币日均贷方余额
 	
 		-- ---------------------------------------------年日均------------------------------------------------------------
 		CASE WHEN 
			${TX_DATE} = ${THIS_YEAR_BEGIN} THEN COALESCE(t.Curr_Bal, 0) 
 		 ELSE 
			(COALESCE(t.Curr_Bal, 0) + COALESCE(b.Annl_Total_Bal, 0)) / (${TX_DATE}-${THIS_YEAR_BEGIN} + 1)
 		END 															AS Annl_DAvg,						-- 55 本年日均余额
 		CASE WHEN 
			${TX_DATE} = ${THIS_YEAR_BEGIN} THEN COALESCE(t.Curr_Debt_Bal, 0) 
 		 ELSE 
			(COALESCE(t.Curr_Debt_Bal, 0) + COALESCE(b.Annl_Total_Debt_Bal, 0)) / (${TX_DATE}-${THIS_YEAR_BEGIN} + 1)
 		END 															AS Annl_DAvg_Debt_Bal,				-- 56 本年日均借方余额

 		CASE WHEN 
			${TX_DATE} = ${THIS_YEAR_BEGIN} THEN COALESCE(t.Curr_Crdt_Bal, 0) 
 		 ELSE 
			(COALESCE(t.Curr_Crdt_Bal, 0) + COALESCE(b.Annl_Total_Crdt_Bal, 0)) / (${TX_DATE}-${THIS_YEAR_BEGIN} + 1)
 		END 															AS Annl_DAvg_Crdt_Bal, 				-- 57 本年日均贷方余额

    	CASE WHEN 
			${TX_DATE} = ${THIS_YEAR_BEGIN} THEN COALESCE(t.Curr_Rmb_Bal, 0) 
 		 ELSE 
			(COALESCE(t.Curr_Rmb_Bal, 0) + COALESCE(b.Annl_Rmb_Total_Bal, 0)) / (${TX_DATE}-${THIS_YEAR_BEGIN} + 1)
 		END 															AS Annl_Rmb_DAvg,					-- 58 本年人民币日均余额

    	CASE WHEN 
			${TX_DATE} = ${THIS_YEAR_BEGIN} THEN COALESCE(t.Curr_Rmb_Debt_Bal, 0) 
 		 ELSE 
			(COALESCE(t.Curr_Rmb_Debt_Bal, 0) + COALESCE(b.Annl_Rmb_Total_Debt_Bal, 0)) / (${TX_DATE}-${THIS_YEAR_BEGIN} + 1)
 		END 															AS Annl_Rmb_DAvg_Debt_Bal,			-- 59 本年人民币日均借方余额

 		CASE WHEN 
			${TX_DATE} = ${THIS_YEAR_BEGIN} THEN COALESCE(t.Curr_Rmb_Crdt_Bal, 0) 
 		 ELSE 
			(COALESCE(t.Curr_Rmb_Crdt_Bal, 0) + COALESCE(b.Annl_Rmb_Total_Crdt_Bal, 0)) / (${TX_DATE}-${THIS_YEAR_BEGIN} + 1)
 		END 															AS Annl_Rmb_DAvg_Crdt_Bal,		 	-- 60 本年人民币日均贷方余额	

 		COALESCE(t.Data_Src_Cd ,b.Data_Src_Cd)															 	-- 61 数据来源代码	 
  FROM pdm.VT_t88_subj_sum_dtl t 

FULL JOIN 
	( SELECT * FROM PDM.t88_subj_sum_dtl
		WHERE STATT_DT = ${LAST_TX_DATE} 
		AND (
			  CASE WHEN ${TX_DATE} = ${THIS_YEAR_BEGIN}
				THEN 0
				ELSE 1
			  END
			) = 1       
          ) b -- 昨日数据
	ON t.Org_ID = b.Org_ID
   AND t.Cur_Cd = b.Cur_Cd 
   AND t.Subj_ID = b.Subj_ID'
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
		SELECT Org_ID,Cur_Cd,Subj_ID FROM pdm.AT_t88_subj_sum_dtl
		GROUP BY 1,2,3
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
DELETE FROM pdm.t88_subj_sum_dtl WHERE Statt_Dt >= ${TX_DATE}';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
		
	
	/*通过主键检查的数据插入正式表中*/
-- ETL_STEP_NO = 9	
	SET @SQL_STR = '
INSERT INTO pdm.t88_subj_sum_dtl SELECT * FROM pdm.AT_t88_subj_sum_dtl where Statt_Dt = ${TX_DATE}';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

	
	/*删除今日临时表*/
-- ETL_STEP_NO = 10
	SET @SQL_STR = '
DROP TEMPORARY TABLE pdm.VT_t88_subj_sum_dtl';
	
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	/*删除昨日临时表*/
	-- ETL_STEP_NO = 11
	SET @SQL_STR = '
DROP TEMPORARY TABLE pdm.AT_t88_subj_sum_dtl';
	
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	SET OUT_RES_MSG = 'SUCCESSFUL';

END |