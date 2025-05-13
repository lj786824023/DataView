DELIMITER |

CREATE DEFINER="cq_sjpt"@"%" PROCEDURE "pr_f5_t88_inn_acct_info"(
	OUT	OUT_RES_MSG		VARCHAR(200),
	IN	IN_TX_DATE		VARCHAR(8)
	)
lable:BEGIN
/**********************************
 * whd 20210518 新建
 * whd 20210525 增加汇率，折人民币金额信息
 * whd 20210728 增加累计数、平均数计算逻辑
 * whd 20230109 关联协议类主表更改为子表，使不依赖所有系统
 * 内部帐信息
 *******************************/
	
	DECLARE ETL_T_TAB_ENG_NAME 		VARCHAR(50)		DEFAULT 't88_inn_acct_info';
	DECLARE ETL_STEP_NO				INTEGER			DEFAULT 1;
	DECLARE ETL_TX_DATE 			VARCHAR(8)		DEFAULT IN_TX_DATE;
	DECLARE PK_COUNT				BIGINT			DEFAULT 0;
	DECLARE PK_ERR_CNT				BIGINT			DEFAULT 0;
	DECLARE RET_CODE				INTEGER			DEFAULT 0;


	/*定义临时表*/
-- ETL_STEP_NO = 1 
	SET @SQL_STR = 'DROP TEMPORARY TABLE IF EXISTS ${AUTO_PDM}.VT_t88_inn_acct_info';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

-- ETL_STEP_NO = 2 
	SET @SQL_STR = 'CREATE TEMPORARY TABLE ${AUTO_PDM}.VT_t88_inn_acct_info LIKE ${AUTO_PDM}.t88_inn_acct_info';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
-- ETL_STEP_NO = 3
	SET @SQL_STR = 'DROP TEMPORARY TABLE IF EXISTS ${AUTO_PDM}.AT_t88_inn_acct_info';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

-- ETL_STEP_NO = 4 
	SET @SQL_STR = 'CREATE TEMPORARY TABLE ${AUTO_PDM}.AT_t88_inn_acct_info LIKE ${AUTO_PDM}.t88_inn_acct_info';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

	
-- ETL_STEP_NO = 5 
	SET @SQL_STR ='INSERT INTO ${AUTO_PDM}.VT_t88_inn_acct_info(
		Statt_Dt				,--  1 统计日期,
		Agmt_Id					,--  2 协议编号,
		Cust_Id					,--  3 客户编号,
		Acct_Nm					,--  4 账户名称,
		Cur_Cd					,--  5 币种代码,
		OpenAcct_Org_Id			,--  6 开户机构编号,
		Acct_Org_Id				,--  7 账务机构编号,
		Prod_Id					,--  8 产品编号,
		Subj_Id					,--  9 科目编号,
		Int_Ind					,--  10 计息标志,
		OpenAcct_Dt				,--  11 开户日期,
		ClosAcct_Dt				,--  12 销户日期,
		St_Int_Dt				,--  13 起息日期,
		Final_TX_Dt				,--  14 最后交易日期,
		Base_Int_Rate			,--  15 基准利率,
		Exec_Int_Rate			,--  16 执行利率,
		Exchg_Rate				,--  17 汇率,
		Curr_Bal				,--  18 当前余额,
		To_Rmb_Bal				,--  19 折人民币余额,
		Mth_Total_Bal			,--  20 月累积余额,
		Quar_Tota_Bal			,--  21 季累积余额,
		Yr_Tota_Bal				,--  22 年累积余额,
		Rmb_Mth_Total_Bal		,--  23 人民币月累积余额,
		Rmb_Quar_Total_Bal		,--  24 人民币季累积余额,
		Rmb_Yr_Total_Bal		,--  25 人民币年累积余额,
		Mth_DAvg				,--  26 月日均余额,
		Quar_DAvg				,--  27 季日均余额,
		Yr_DAvg					,--  28 年日均余额,
		Rmb_Mth_DAvg			,--  29 人民币月日均余额,
		Rmb_Quar_DAvg			,--  30 人民币季日均余额,
		Rmb_Yr_DAvg				 --  31人民币年日均余额
)
SELECT
	${TX_DATE}
	,T1.Agmt_Id										AS	Agmt_Id				--	1 协议编号
	,T1.Cust_Id										AS	Cust_Id				--	2 客户编号
	,T1.Acct_Nm										AS	Acct_Nm				--	3 账户名称
	,T1.Agmt_Cur_Cd									AS	Cur_Cd				--	4 币种代码
	,T1.Acct_Open_Org_Id							AS	OpenAcct_Org_ID		--	5 开户机构编号
	,T1.Acct_Acct_Org_Id							AS	Acct_Org_ID			--	6 账务机构编号
	,T1.Prod_Id										AS	Prod_ID				--	7 产品编号
	,T2.Subj_Id										AS	Subj_Id				--	8 科目编号
	,T3.Int_Ind										AS	Int_Ind				--	9 计息标志
	,T1.Acct_Open_Dt								AS	OpenAcct_Dt			--	10 开户日期
	,T1.Acct_Closacct_Dt							AS	ClosAcct_Dt			--	11 销户日期
	,T1.Acct_St_Int_Dt								AS	St_Int_Dt			--	12 起息日期
	,${NULL_DATE}									AS	Final_TX_Dt			--	13 最后交易日期
	,COALESCE(T4.Base_Int_Rate,0)					AS	Base_Int_Rate		--	14 基准利率
	,COALESCE(T4.Exec_Int_Rate,0)					AS	Exec_Int_Rate		--	15 执行利率
	,COALESCE(T5.Mdl_Prc,1)							AS	Exchg_Rate			--	16 汇率
	,COALESCE(T6.Bal,0)								AS	Curr_Bal			--	17 当前余额
	,COALESCE(T6.Bal,0) * COALESCE(T5.Mdl_Prc,1)	AS	To_Rmb_Bal			--	18 折人民币余额
	,0												AS	Mth_Total_Bal		--	19 月累积余额
	,0												AS	Quar_Tota_Bal		--	20 季累积余额
	,0												AS	Yr_Tota_Bal			--	21 年累积余额
	,0												AS	Rmb_Mth_Total_Bal	--	22 人民币月累积余额
	,0												AS	Rmb_Quar_Total_Bal	--	23 人民币季累积余额
	,0												AS	Rmb_Yr_Total_Bal	--	24 人民币年累积余额
	,0												AS	Mth_DAvg			--	25 月日均余额
	,0												AS	Quar_DAvg			--	26 季日均余额
	,0												AS	Yr_DAvg				--	27 年日均余额
	,0 												AS	Rmb_Mth_DAvg		--	28 人民币月日均余额
	,0												AS	Rmb_Quar_DAvg		--	29 人民币季日均余额
	,0												AS	Rmb_Yr_DAvg			--	30 人民币年日均余额
	FROM ${AUTO_PDM}.t03_inn_acct_h T1
	
	LEFT JOIN -- ${AUTO_PDM}.t03_agmt_subj_rel_h 
				${AUTO_PDM}.t03_agmt_subj_rel_ncs T2
		ON T1.Agmt_Id			= T2.Agmt_Id
		AND T1.Agmt_Cate_Cd 	= T2.Agmt_Cate_Cd
		AND T2.Start_Dt			<= ${TX_DATE}
		AND T2.End_Dt			>= ${TX_DATE}
	
	LEFT JOIN ${AUTO_PDM}.t03_acct_extinfo_h T3
		ON T1.Agmt_Id			= T3.Agmt_Id
		AND T1.Agmt_Cate_Cd 	= T3.Agmt_Cate_Cd
		AND T3.Start_Dt			<= ${TX_DATE}
		AND T3.End_Dt			>= ${TX_DATE}
	
	LEFT JOIN -- ${AUTO_PDM}.t03_agmt_rate_h 
			 	${AUTO_PDM}.t03_agmt_rate_ncs_h T4
		ON T1.Agmt_Id			= T4.Agmt_Id
		AND T1.Agmt_Cate_Cd 	= T4.Agmt_Cate_Cd
		AND T4.Agmt_Rate_Typ_Cd = ''01''
		AND T4.Start_Dt			<= ${TX_DATE}
		AND T4.End_Dt			>= ${TX_DATE}
	
	LEFT JOIN ${AUTO_PDM}.t88_exchg_rate T5
		ON T1.Agmt_Cur_Cd		= T5.Init_Cur
		AND T5.Statt_Dt			= ${TX_DATE}
	
	LEFT JOIN -- ${AUTO_PDM}.t03_agmt_bal_h T6
				${AUTO_PDM}.t03_agmt_bal_ncs_h T6
		ON T1.Agmt_Id			= T6.Agmt_Id
		AND T1.Agmt_Cate_Cd		= T6.Agmt_Cate_Cd
		AND T6.Agmt_Bal_Typ_Cd	= ''06''
		AND T6.Start_Dt			<= ${TX_DATE}
		AND T6.End_Dt			>= ${TX_DATE}
	
	WHERE T1.Start_Dt		<= ${TX_DATE}
	AND T1.End_Dt			>= ${TX_DATE}
	AND T2.Subj_Id IS NOT NULL
';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
-- ETL_STEP_NO = 6 
	SET @SQL_STR ='INSERT INTO ${AUTO_PDM}.AT_t88_inn_acct_info(
		Statt_Dt				,--  1 统计日期,
		Agmt_Id					,--  2 协议编号,
		Cust_Id					,--  3 客户编号,
		Acct_Nm					,--  4 账户名称,
		Cur_Cd					,--  5 币种代码,
		OpenAcct_Org_Id			,--  6 开户机构编号,
		Acct_Org_Id				,--  7 账务机构编号,
		Prod_Id					,--  8 产品编号,
		Subj_Id					,--  9 科目编号,
		Int_Ind					,--  10 计息标志,
		OpenAcct_Dt				,--  11 开户日期,
		ClosAcct_Dt				,--  12 销户日期,
		St_Int_Dt				,--  13 起息日期,
		Final_TX_Dt				,--  14 最后交易日期,
		Base_Int_Rate			,--  15 基准利率,
		Exec_Int_Rate			,--  16 执行利率,
		Exchg_Rate				,--  17 汇率,
		Curr_Bal				,--  18 当前余额,
		To_Rmb_Bal				,--  19 折人民币余额,
		Mth_Total_Bal			,--  20 月累积余额,
		Quar_Tota_Bal			,--  21 季累积余额,
		Yr_Tota_Bal				,--  22 年累积余额,
		Rmb_Mth_Total_Bal		,--  23 人民币月累积余额,
		Rmb_Quar_Total_Bal		,--  24 人民币季累积余额,
		Rmb_Yr_Total_Bal		,--  25 人民币年累积余额,
		Mth_DAvg				,--  26 月日均余额,
		Quar_DAvg				,--  27 季日均余额,
		Yr_DAvg					,--  28 年日均余额,
		Rmb_Mth_DAvg			,--  29 人民币月日均余额,
		Rmb_Quar_DAvg			,--  30 人民币季日均余额,
		Rmb_Yr_DAvg				 --  31人民币年日均余额
)
SELECT
		${TX_DATE}											AS  Statt_Dt			--  1 统计日期
		,COALESCE(t1.Agmt_ID, b.Agmt_ID)					AS	Agmt_Id				--	2协议编号
		,COALESCE(T1.Cust_Id, b.Cust_Id)					AS	Cust_Id				--	3客户编号
		,COALESCE(T1.Acct_Nm, b.Acct_Nm)					AS	Acct_Nm				--	4账户名称
		,COALESCE(T1.Cur_Cd, b.Cur_Cd)						AS	Cur_Cd				--	5币种代码
		,COALESCE(T1.OpenAcct_Org_ID, b.OpenAcct_Org_ID)	AS	OpenAcct_Org_ID		--	6开户机构编号
		,COALESCE(T1.Acct_Org_ID, b.Acct_Org_ID)			AS	Acct_Org_ID			--	7账务机构编号
		,COALESCE(T1.Prod_ID, b.Prod_ID)					AS	Prod_ID				--	8产品编号
		,COALESCE(T1.Subj_Id, b.Subj_Id)					AS	Subj_Id				--	9科目编号
		,COALESCE(T1.Int_Ind, b.Int_Ind)					AS	Int_Ind				--	10计息标志
		,COALESCE(T1.OpenAcct_Dt, b.OpenAcct_Dt)			AS	OpenAcct_Dt			--	11开户日期
		,COALESCE(T1.ClosAcct_Dt, b.ClosAcct_Dt)			AS	ClosAcct_Dt			--	12销户日期
		,COALESCE(T1.St_Int_Dt, b.St_Int_Dt)				AS	St_Int_Dt			--	13起息日期
		,${NULL_DATE}										AS	Final_TX_Dt			--	14最后交易日期
		,COALESCE(T1.Base_Int_Rate, b.Base_Int_Rate)		AS	Base_Int_Rate		--	15基准利率
		,COALESCE(T1.Exec_Int_Rate, b.Exec_Int_Rate)		AS	Exec_Int_Rate		--	16执行利率
		,COALESCE(T1.Exchg_Rate, 1)							AS	Exchg_Rate			--	17汇率
		,COALESCE(T1.Curr_Bal, 0)							AS	Curr_Bal			--	18当前余额
		,COALESCE(T1.To_Rmb_Bal ,0)							AS	To_Rmb_Bal			--	19折人民币余额
		,CASE 
			WHEN ${TX_DATE} = ${THIS_MONTH_BEGIN} 
			THEN COALESCE(T1.Curr_Bal,0)
			ELSE COALESCE(T1.Curr_Bal,0) + COALESCE(b.Mth_Total_Bal,0) 
		END												AS	Mth_Total_Bal		--	20月累积余额
		,CASE 
			WHEN ${TX_DATE} = ${THIS_QUART_BEGIN} 
			THEN COALESCE(T1.Curr_Bal,0)
			ELSE COALESCE(T1.Curr_Bal,0) + COALESCE(b.Quar_Tota_Bal,0)
		END												AS	Quar_Tota_Bal		--	21季累积余额
		,CASE
			WHEN ${TX_DATE} = ${THIS_YEAR_BEGIN} 
			THEN COALESCE(T1.Curr_Bal,0)
			ELSE COALESCE(T1.Curr_Bal,0) + COALESCE(b.Yr_Tota_Bal,0)
		END												AS	Yr_Tota_Bal			--	22年累积余额
		,CASE
			WHEN ${TX_DATE} = ${THIS_MONTH_BEGIN}
			THEN COALESCE(T1.To_Rmb_Bal,0)
			ELSE COALESCE(T1.To_Rmb_Bal,0) + COALESCE(b.Rmb_Mth_Total_Bal,0)
		END												AS	Rmb_Mth_Total_Bal	--	23人民币月累积余额
		,CASE
			WHEN ${TX_DATE} = ${THIS_QUART_BEGIN}
			THEN COALESCE(T1.To_Rmb_Bal,0)
			ELSE COALESCE(T1.To_Rmb_Bal,0) + COALESCE(b.Rmb_Quar_Total_Bal,0)
		END												AS	Rmb_Quar_Total_Bal	--	24人民币季累积余额
		,CASE
			WHEN ${TX_DATE} = ${THIS_YEAR_BEGIN} 
			THEN COALESCE(T1.To_Rmb_Bal,0)
			ELSE COALESCE(T1.To_Rmb_Bal,0) + COALESCE(b.Rmb_Yr_Total_Bal, 0)
		END												AS	Rmb_Yr_Total_Bal	--	25人民币年累积余额
		,CASE 
			WHEN ${TX_DATE} = ${THIS_MONTH_BEGIN} 
			THEN COALESCE(T1.Curr_Bal,0)
			ELSE (COALESCE(T1.Curr_Bal,0) + COALESCE(b.Mth_Total_Bal,0)) / (${TX_DATE}-${THIS_MONTH_BEGIN} + 1)
		END												AS	Mth_DAvg			--	26月日均余额
		,CASE
			WHEN ${TX_DATE} = ${THIS_QUART_BEGIN}
			THEN COALESCE(T1.Curr_Bal,0)
			ELSE (COALESCE(T1.Curr_Bal,0) + COALESCE(b.Quar_Tota_Bal,0)) / (${TX_DATE}-${THIS_QUART_BEGIN} + 1)
		END												AS	Quar_DAvg			--	27季日均余额
		,CASE
			WHEN ${TX_DATE} = ${THIS_YEAR_BEGIN} 
			THEN COALESCE(T1.Curr_Bal,0)
			ELSE (COALESCE(T1.Curr_Bal,0) + COALESCE(b.Yr_Tota_Bal,0)) / (${TX_DATE}-${THIS_YEAR_BEGIN} + 1)
		END												AS	Yr_DAvg				--	28年日均余额
		,CASE
			WHEN ${TX_DATE} = ${THIS_MONTH_BEGIN} 
			THEN COALESCE(T1.To_Rmb_Bal,0)
			ELSE (COALESCE(T1.To_Rmb_Bal,0) + COALESCE(b.Rmb_Mth_Total_Bal,0)) / (${TX_DATE}-${THIS_MONTH_BEGIN} + 1)
		END 											AS	Rmb_Mth_DAvg		--	29人民币月日均余额
		,CASE
			WHEN ${TX_DATE} = ${THIS_QUART_BEGIN}
			THEN COALESCE(T1.To_Rmb_Bal,0)
			ELSE (COALESCE(T1.To_Rmb_Bal,0) + COALESCE(b.Rmb_Quar_Total_Bal,0)) / (${TX_DATE}-${THIS_QUART_BEGIN} + 1)
		END												AS	Rmb_Quar_DAvg		--	30人民币季日均余额
		,CASE
			WHEN ${TX_DATE} = ${THIS_YEAR_BEGIN} 
			THEN COALESCE(T1.To_Rmb_Bal,0)
			ELSE (COALESCE(T1.To_Rmb_Bal,0) + COALESCE(b.Rmb_Yr_Total_Bal,0)) / (${TX_DATE}-${THIS_YEAR_BEGIN} + 1)
		END												AS	Rmb_Yr_DAvg			--	31人民币年日均余额
	FROM ${AUTO_PDM}.VT_t88_inn_acct_info T1

FULL JOIN 
	( SELECT * FROM ${AUTO_PDM}.t88_inn_acct_info
		WHERE STATT_DT = ${LAST_TX_DATE} 
		AND (
			  CASE WHEN ${TX_DATE} = ${THIS_YEAR_BEGIN}
				THEN 0
				ELSE 1
			  END
			) = 1       
          ) b -- 昨日数据
	ON T1.Agmt_ID = b.Agmt_ID
';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
  
  /*检查插入的临时表数据是否有主键错误*/
-- ETL_STEP_NO = 9999
SELECT COUNT(*) INTO PK_ERR_CNT 
FROM (
	SELECT Agmt_Id FROM pdm.AT_t88_inn_acct_info
	GROUP BY Agmt_Id HAVING COUNT(*) > 1
) I;
IF PK_ERR_CNT <> 0 THEN
	INSERT INTO ETL.ETL_JOB_STATUS_EDW VALUES ('',ETL_USER_ID,ETL_T_TAB_ENG_NAME,ETL_TX_DATE,9999,'',PK_ERR_CNT,'Failed','主键验证失败',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);	
END IF;
 	
	/* 支持数据重跑*/
-- ETL_STEP_NO = 7
	SET @SQL_STR = 'DELETE FROM ${AUTO_PDM}.t88_inn_acct_info WHERE Statt_Dt >= ${TX_DATE}';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	/*通过主键检查的数据插入正式表中*/
-- ETL_STEP_NO = 8	
	SET @SQL_STR = 'INSERT INTO ${AUTO_PDM}.t88_inn_acct_info SELECT * FROM ${AUTO_PDM}.AT_t88_inn_acct_info';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	/*删除临时表*/
	-- ETL_STEP_NO = 9
	SET @SQL_STR = 'DROP TEMPORARY TABLE ${AUTO_PDM}.VT_t88_inn_acct_info';
	
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	-- ETL_STEP_NO = 10
	SET @SQL_STR = 'DROP TEMPORARY TABLE ${AUTO_PDM}.AT_t88_inn_acct_info';
	
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	

	SET OUT_RES_MSG = 'SUCCESSFUL';

END |