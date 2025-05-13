DELIMITER |

CREATE DEFINER="cq_sjpt"@"%" PROCEDURE "pr_f5_t88_ccard_info_form"(
	OUT	OUT_RES_MSG		VARCHAR(200),
	IN	IN_TX_DATE		VARCHAR(8)
	)
LABLE:BEGIN
/**********************************
 * LJZ 20210726 新建
 * 信用卡卡信息
 *********************************/
	
	DECLARE ETL_T_TAB_ENG_NAME 		VARCHAR(100)	DEFAULT 't88_ccard_info_form';
	DECLARE ETL_STEP_NO				INTEGER			DEFAULT 1;
	DECLARE ETL_TX_DATE 			VARCHAR(8)		DEFAULT IN_TX_DATE;
	SET OUT_RES_MSG = 'FAILED';
	
	
	/*支持数据重跑*/
	SET @SQL_STR = 'DELETE FROM PDM.'||ETL_T_TAB_ENG_NAME||' WHERE STATT_DT >= ${TX_DATE}';
	CALL ETL.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	/*创建临时表*/
	SET @SQL_STR = 'DROP TEMPORARY TABLE IF EXISTS PDM.VT_'||ETL_T_TAB_ENG_NAME;
	CALL ETL.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	SET @SQL_STR = 'CREATE TEMPORARY TABLE PDM.VT_'||ETL_T_TAB_ENG_NAME||' LIKE PDM.'||ETL_T_TAB_ENG_NAME;
	CALL ETL.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	/*数据首先插入临时表VT_*/

	-- 第一组： ${NULL_STR}
	SET @SQL_STR = '
	INSERT INTO PDM.VT_t88_ccard_info_form(
		Statt_Dt				,-- 1统计日期
		Card_Num				,-- 2卡号
		Pri_Card_Num			,-- 3主卡号
		Agmt_ID					,-- 4协议编号
		Org_ID					,-- 5机构编号
		Actv_Org_ID				,-- 6激活机构编号
		Cust_ID					,-- 7客户编号
		Appl_ID					,-- 8申请编号
		Apprv_Dt				,-- 9核准日期
		Issu_Card_Dt			,-- 10卡日期
		Actv_Dt					,-- 11激活日期
		Firs_Used_Dt			,-- 12首刷日期
		Pin_Card_Dt				,-- 13销卡日期
		Actv_Ind				,-- 14激活标志
		Repl_Card_Ind			,-- 15换卡标志
		Main_Sec_Card_Ind		,-- 16主副卡标志
		Card_Typ_Cd				,-- 17卡片种类代码
		Card_Stat_Cd			,-- 18卡状态代码
		Card_Lvl_Cd				,-- 19卡片等级代码
		Crdt_Card_Appl_Stat_Cd	,-- 20信用卡申请状态代码
		Crdt_Card_Chk_Reasn_Cd	,-- 21信用卡审核原因代码
		Crdt_Lmt				,-- 22授信额度
		Usabl_Lmt				,-- 23可用额度
		CurMth_Cash_Amt			,-- 24当月取现金额
		CurMth_Consm_Amt		,-- 25当月消费金额
		Amort_Amt				,-- 26分期金额
		Ovrd_Amt				,-- 27逾期金额
		Data_Src_Cd,			 -- 28数据来源表名
		Main_Src_Task
	)
	SELECT ${TX_DATE}						,-- 1统计日期
		t.Card_Num						,-- 2卡号
		t.Pri_Card_Num					,-- 3主卡号
		t.Agmt_Id						,-- 4贷记卡账户协议编号
		NVL(t1.Org_Id,${NULL_STR})		,-- 5机构编号
		${NULL_STR}						,-- 6激活机构编号  t03_ccard_appl 增加CCS_INTR推荐人信息表中ACT_BRNCH激活机构字段
		t.Cust_Id						,-- 7客户编号
		${NULL_STR},-- NVL(t2.Sys_Appl_Id,${NULL_STR})	,-- 8系统申编号
		\'0001-01-01\',-- NVL(t2.Apprv_Dt,\'0001-01-01\')	,-- 9核准日期 t03_ccard_appl 增加ccs_apma表的APPDEC_DAY字段核准日期
		t.Issu_Card_Dt					,-- 10发卡日期
		t.Card_Actv_Dt					,-- 11卡激活日期
		t.First_TX_Dt					,-- 12首刷日期 t03_ccard_h 增加 ccs_card表MAILER_1ST字段第一次产生交易的日期
		t.Pin_Card_Dt					,-- 13销卡日期
		CASE WHEN nvl(t.Card_Actv_Dt, ${NULL_STR}) =  ${NULL_STR} THEN 0 
			 ELSE 1 
		END 									,-- 14激活标志  1:是 0：否
		 ${NULL_STR}							,-- 15换卡标志  t03_ccard_h表增加ccs_card表的issue_nbr字段 issue_nbr<>0 then 1 else 0 1:是 0：否
		DECODE(t.Card_Hldr_Ordr_Num,1,1,0)		,-- 16主副卡标志  1:主卡 0:副卡 
		t.Prod_Id								,-- 17卡片所属产品代码
		t.Card_Wrtoff_Cd 						,-- 18卡片注销代码
		NVL(t3.Card_Kind_Lvl_Cd,${NULL_STR})	,-- 19卡种级别代码
		${NULL_STR}, -- NVL(t2.Appl_Stat_Cd,${NULL_STR})		,-- 20申请状态代码
		${NULL_STR},-- NVL(t2.Appl_Chk_Cd,${NULL_STR})			,-- 21申请审核代码
		t.Crdt_Lmt								,-- 22信用额度
		0										,-- 23可用额度
		CASE WHEN t4.Curr_Cash_Amt + t4.Proph_Cash_Amt >= 0 -- AND t1.mths_odue = 0 
			 THEN t4.Curr_Cash_Amt + t4.Proph_Cash_Amt
			 ELSE 0
		END										,-- 24当月取现金额  
		CASE WHEN T4.Curr_Cash_Amt + T4.Proph_Cash_Amt >= 0 -- AND T4.mths_odue = 0 
			 THEN T4.Curr_Consm_Amt -- + T4.STM_BALFRE
			 ELSE 0
		END										,-- 25当月消费金额  t03_ccard_acct_h需要增加 ccs_acct表的 mths_odue和STM_BALFRE字段
		-- CASE WHEN t4.mths_odue = 0 THEN
				 (CASE
					WHEN t4.Curr_Cash_Amt + t4.Proph_Cash_Amt >= 0 THEN
					t4.Curr_Amort_Amt + t4.Proph_Amort_Amt + t4.Amort_Defandpre +
					decode(t5.Larg_Amt_Amort_Remn_Prin, NULL, 0, t5.Larg_Amt_Amort_Remn_Prin)
					ELSE
					nvl(t4.Amort_Defandpre,0) +
					decode(t5.Larg_Amt_Amort_Remn_Prin, NULL, 0, t5.Larg_Amt_Amort_Remn_Prin)
					END),
				-- ELSE 0 END					,-- 26分期金额
				(CASE
					WHEN t4.Curr_Cash_Amt + t4.Proph_Cash_Amt >= 0 THEN
					t4.Curr_Consm_Amt + /*t1.stm_balfre*/ + t4.Curr_Cash_Amt + t4.Proph_Cash_Amt +
					t4.Curr_Amort_Amt + t4.Proph_Amort_Amt + t4.Amort_Defandpre +
					decode(t5.Larg_Amt_Amort_Remn_Prin, NULL, 0, t5.Larg_Amt_Amort_Remn_Prin)
					ELSE
					nvl(t4.Amort_Defandpre,0) +
					decode(t5.Larg_Amt_Amort_Remn_Prin, NULL, 0, t5.Larg_Amt_Amort_Remn_Prin)
					END),
				-- ELSE 0 END					,-- 27逾期金额
		\'t03_ccard_h\'							,-- 28数据来源表名
		${NULL_STR}
 FROM pdm.t03_ccard_h t  -- 贷记卡历史

	LEFT JOIN pdm.t03_card_h t1 -- 卡历史
		ON t.Card_Num = t1.Card_Num
		AND t1.Start_Dt <= ${TX_DATE}
		AND t1.End_Dt >= ${TX_DATE}

	/*LEFT JOIN pdm.t03_ccard_appl t2 -- 贷记卡申请
		ON substr(t.Agmt_Id,4) = t2.Acct_Num
		AND t2.Tx_Dt = ${TX_DATE}*/

	LEFT JOIN pdm.t02_ccard_card_prod_h t3 -- 信用卡卡产品历史
		ON t.Prod_Id = t3.Prod_Id
		AND t3.Start_Dt <= ${TX_DATE}
		AND t3.End_Dt >= ${TX_DATE}

	LEFT JOIN pdm.t03_ccard_acct_h t4 -- 贷记卡账户历史
		ON t.Agmt_Id = t4.Agmt_Id
		AND t4.Start_Dt <= ${TX_DATE}
		AND t4.End_Dt >= ${TX_DATE}

	LEFT JOIN pdm.t03_ccard_sms_serv_h t5 -- 贷记卡短信服务
		ON t.Agmt_Id = t5.Agmt_Id
		AND t5.Start_Dt <= ${TX_DATE}
		AND t5.End_Dt >= ${TX_DATE}

WHERE t.Start_Dt <= ${TX_DATE}
  AND t.End_Dt >= ${TX_DATE}
	';
	CALL ETL.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	/*检查插入的临时表数据是否有主键错误*/
	/*
	-- 获取主键字段
	SELECT PHYSICAL_PRI_KEY INTO @PK_COLUMN FROM DATAMAPPING_TASK WHERE T_TAB_ENG_NAME=ETL_T_TAB_ENG_NAME;
	-- 0 正常
	SET @SQL_STR = 'SELECT COUNT(1) INTO @PK_COUNT FROM(SELECT 1 FROM PDM.VT_'||ETL_T_TAB_ENG_NAME||' GROUP BY '||@PK_COLUMN||' HAVING COUNT(1)>1) T';
	CALL ETL.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	IF @PK_COUNT <> 0
	THEN
		SET OUT_RES_MSG = '9999';
		UPDATE ETL.ETL_JOB_STATUS_EDW SET STEP_STATUS='Failed',STEP_ERR_LOG='主键重复', LAST_END_TIME=CURRENT_TIMESTAMP WHERE SQL_UNIT=ETL_T_TAB_ENG_NAME AND TX_DATE=TX_DATE AND STEP_NO=ETL_STEP_NO-1;
		LEAVE LABLE;
	END IF;
	*/
	
	/*通过主键检查的数据插入正式表中*/
	SET @SQL_STR='INSERT INTO PDM.'||ETL_T_TAB_ENG_NAME||' SELECT * FROM PDM.VT_'||ETL_T_TAB_ENG_NAME||' WHERE STATT_DT=${TX_DATE}'; 
	CALL ETL.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

	SET OUT_RES_MSG='SUCCESSFUL';
	

END |