DELIMITER |

CREATE DEFINER="gbase"@"%" PROCEDURE "pr_f5_t88_dpst_acct_bas_info"(
	OUT	OUT_RES_MSG		VARCHAR(200),
	IN	IN_TX_DATE		VARCHAR(8)
)
LABLE:BEGIN
	/********************
	 * 
	 * Lip	2021-08-12 New
	 * 
	 *********************/
	
	DECLARE ETL_T_TAB_ENG_NAME 		VARCHAR(50)		default 't88_dpst_acct_bas_info';
	DECLARE ETL_STEP_NO				INTEGER			DEFAULT 1;
	DECLARE ETL_TX_DATE 			VARCHAR(8)		DEFAULT IN_TX_DATE;
	DECLARE	ETL_USER_ID				VARCHAR(30)		DEFAULT SESSION_USER();
	DECLARE PK_ERR_CNT				BIGINT			DEFAULT 0;
	DECLARE RET_CODE				INTEGER			DEFAULT 0;
	
	-- TEMPORARY
	
 
	/*定义临时表 */

-- ETL_STEP_NO = 4 
SET @SQL_STR = 'DROP TEMPORARY TABLE IF EXISTS VT_t88_dpst_acct_bas_info';

CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
IF @RTC <> 0 THEN LEAVE LABLE;END IF;
SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;	
 	
-- ETL_STEP_NO = 5
SET @SQL_STR = 'CREATE TEMPORARY TABLE VT_t88_dpst_acct_bas_info LIKE ${AUTO_PDM}.t88_dpst_acct_bas_info';

CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
IF @RTC <> 0 THEN LEAVE LABLE;END IF;
SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

-- ETL_STEP_NO = 6 
SET @SQL_STR = 'DROP TEMPORARY TABLE IF EXISTS AT_t88_dpst_acct_bas_info';

CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
IF @RTC <> 0 THEN LEAVE LABLE;END IF;
SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;	
 	
-- ETL_STEP_NO = 7
SET @SQL_STR = 'CREATE TEMPORARY TABLE AT_t88_dpst_acct_bas_info LIKE ${AUTO_PDM}.t88_dpst_acct_bas_info';

CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
IF @RTC <> 0 THEN LEAVE LABLE;END IF;
SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

-- ETL_STEP_NO = 8
SET @SQL_STR = 'INSERT INTO VT_t88_dpst_acct_bas_info
 SELECT
	${TX_DATE}									AS Statt_Dt						-- 统计日期
	,T1.Agmt_Id									AS Agmt_Id						-- 协议编号
	,T1.Card_Num								AS Card_Num						-- 卡号
	,T1.Cust_Id									AS Cust_ID						-- 客户编号 
	,T1.Acct_Nm									AS Acct_Nm						-- 账户名称
	,T1.Agmt_Cur_Cd								AS Cur_Cd						-- 币种代码
	,T1.Acct_Open_Org_Id						AS OpenAcct_Org_ID				-- 开户机构编号
	,T1.Acct_Acct_Org_Id						AS Acct_Org_ID					-- 账务机构编号
	,T1.Prod_ID									AS Prod_ID						-- 产品编号
	,COALESCE(T2.Subj_Id,'''')					AS Subj_Id						-- 科目编号
	-- ,T3.Cust_Cate_Cd							AS Cust_Cate_Cd					-- 客户类型代码  -- 等待ECIF上线
	,''''											AS Cust_Cate_Cd					-- 客户类型代码
	,T1.Acct_Ctgy_Cd							AS Acct_Ctgy_Cd					-- 账户类别代码
	,T1.Acct_Stat_Cd							AS Agmt_Stat_Cd					-- 协议状态代码
	,T1.Tm_Dpst_Acct_Subdv_Cls_Cd				AS Tm_Dpst_Acct_Sub_Cls_Cd		-- 定期账户细类代码
	,T1.Dpst_Term_Cate_Cd						AS Dpst_Term_Cate_Cd			-- 存期类型代码
	,T1.Dpst_Term								AS Dpst_Term_Cd					-- 存款期限代码
	,T1.Acct_Cate_Cd							AS Dept_Tm_Curr_Cate_Cd			-- 定活类型代码 S-活期 T-定期账户
	,CASE
		WHEN T4.Agmt_Id IS NOT NULL
		THEN ''1''
		ELSE ''0''
	END 										AS Agmt_Ind						-- 协定协议标志
	,CASE 
		WHEN T1.AutoRnw_Ind = ''''
		THEN ''Z''
		ELSE T1.AutoRnw_Ind
	END											AS AutoRnw_Cd					-- 自动转存代码
	,T5.Int_Ind									AS Int_Ind						-- 计息标志
	,T1.Cross_Dep_Ind							AS Cross_Dep_Ind				-- 通存标志
	,T1.Cir_Exch_Ind 							AS Cir_Exch_Ind					-- 通兑标志
	,T1.Acct_Open_Dt							AS OpenAcct_Dt					-- 开户日期
	,T1.Acct_Closacct_Dt						AS ClosAcct_Dt					-- 销户日期
	,T1.Acct_St_Int_Dt							AS St_Int_Dt					-- 起息日期
	,T1.Acct_Matr_Dt							AS Matr_Dt						-- 到期日期
	,COALESCE(T6.Int_Rate_Cate_Cd,''1'')			AS Int_Rate_Cate_Cd				-- 利率类型代码 1-固定  2-浮动
	,COALESCE(T6.Int_Rate_Flt_Mod_Cd,''Z'')		AS Int_Rate_Flt_Mod_Cd			-- 利率浮动方式代码
	,COALESCE(T6.Int_Rate_Rprc_Prd_Cate_Cd,''Z'')	AS Int_Rate_Rprc_Prd_Cate_Cd	-- 利率重定价周期类型代码
	,COALESCE(T6.Int_Rate_Rprc_Prd,'''')			AS Int_Rate_Rprc_Prd			-- 利率重定价周期
	,COALESCE(T6.Base_Int_Rate,0)				AS Base_Int_Rate				-- 基准利率
	,COALESCE(T6.Exec_Int_Rate,0)				AS Exec_Int_Rate				-- 执行利率
	,COALESCE(T6.Flt_Point,0)					AS Flt_Point					-- 浮动点数
	,COALESCE(T6.Flt_Ratio,0)					AS Flt_Ratio					-- 浮动比例
	,COALESCE(T20.Agmt_Int,0)					AS Cur_Prd_Provs_Int			-- 当期计提利息
	,COALESCE(T30.Agmt_Int,0)					AS Accm_Provs_Int_Amt			-- 累计计提利息金额
	,COALESCE(T40.Agmt_Int,0)					AS Accm_Int_Amt					-- 累计利息金额
	,COALESCE(T50.Agmt_Int,0)					AS Int_Stl_Int_Amt				-- 结息利息金额
	,COALESCE(T8.Mdl_Prc,1)						AS Exchg_Rate					-- 汇率
	,COALESCE(T9.Bal,0)								AS	Curr_Bal			--	当前余额
	,COALESCE(T9.Bal,0) * COALESCE(T8.Mdl_Prc,1)	AS	To_Rmb_Bal			--	折人民币余额
	,0												AS	Mth_Total_Bal		--	月累积余额
	,0												AS	Quar_Tota_Bal		--	季累积余额
	,0												AS	Yr_Tota_Bal			--	年累积余额
	,0												AS	Rmb_Mth_Total_Bal	--	人民币月累积余额
	,0												AS	Rmb_Quar_Total_Bal	--	人民币季累积余额
	,0												AS	Rmb_Yr_Total_Bal	--	人民币年累积余额
	,0												AS	Mth_DAvg			--	月日均余额
	,0												AS	Quar_DAvg			--	季日均余额
	,0												AS	Yr_DAvg				--	年日均余额
	,0 												AS	Rmb_Mth_DAvg		--	人民币月日均余额
	,0												AS	Rmb_Quar_DAvg		--	人民币季日均余额
	,0												AS  Rmb_Yr_DAvg			--	人民币年日均余额
FROM ${AUTO_PDM}.t03_dpst_acct_h T1 --  存款账户历史

LEFT JOIN ${AUTO_PDM}.t03_agmt_subj_rel_h T2 --  协议科目关系历史
	ON T1.Agmt_Id		= T2.Agmt_Id
	AND T1.Agmt_Cate_Cd = T2.Agmt_Cate_Cd
	AND T2.Subj_Typ_Cd = ''01''
	AND T2.Data_Src_Cd = ''NCS''
	AND T2.Start_Dt		<= ${TX_DATE}
	AND T2.End_Dt 		>= ${TX_DATE}

LEFT JOIN ${AUTO_PDM}.t03_acct_extinfo_h T5 --  账户扩展信息历史
	ON T1.Agmt_Id = T5.Agmt_Id
	AND T1.Agmt_Cate_Cd = T5.Agmt_Cate_Cd
	AND T5.Agmt_Cate_Cd = ''0201''
	AND T5.Start_Dt		<= ${TX_DATE}
	AND T5.End_Dt 		>= ${TX_DATE}

LEFT JOIN ${AUTO_PDM}.t03_agmt_rate_h T6 --  协议利率历史	
	ON T1.Agmt_Id = T6.Agmt_Id
	AND T1.Agmt_Cate_Cd = T6.Agmt_Cate_Cd
	AND T6.Agmt_Cate_Cd = ''0201''
	AND T6.Agmt_Rate_Typ_Cd = ''01''
	AND T6.Start_Dt		<= ${TX_DATE}
	AND T6.End_Dt 		>= ${TX_DATE}

LEFT JOIN ${AUTO_PDM}.t03_agmt_int_h T20 --  协议利息历史
	ON T1.Agmt_Id			= T20.Agmt_Id
	AND T1.Agmt_Cate_Cd		= T20.Agmt_Cate_Cd
	AND T20.Agmt_Cate_Cd	= ''0201''
	AND T20.Agmt_Int_Typ_Cd	= ''0104'' 
	AND T20.Start_Dt		<= ${TX_DATE}
	AND T20.End_Dt			>= ${TX_DATE}

LEFT JOIN ${AUTO_PDM}.t03_agmt_int_h T30 --  协议利息历史
	ON T1.Agmt_Id			= T30.Agmt_Id
	AND T1.Agmt_Cate_Cd		= T30.Agmt_Cate_Cd
	AND T30.Agmt_Cate_Cd	= ''0201''
	AND T30.Agmt_Int_Typ_Cd	= ''0103'' 
	AND T30.Start_Dt		<= ${TX_DATE}
	AND T30.End_Dt			>= ${TX_DATE}

LEFT JOIN ${AUTO_PDM}.t03_agmt_int_h T40 --  协议利息历史
	ON T1.Agmt_Id			= T40.Agmt_Id
	AND T1.Agmt_Cate_Cd		= T40.Agmt_Cate_Cd
	AND T40.Agmt_Cate_Cd	= ''0201''
	AND T40.Agmt_Int_Typ_Cd	= ''0101'' 
	AND T40.Start_Dt		<= ${TX_DATE}
	AND T40.End_Dt			>= ${TX_DATE}

LEFT JOIN ${AUTO_PDM}.t03_agmt_int_h T50 --  协议利息历史
	ON T1.Agmt_Id			= T50.Agmt_Id
	AND T1.Agmt_Cate_Cd		= T50.Agmt_Cate_Cd
	AND T50.Agmt_Cate_Cd	= ''0201''
	AND T50.Agmt_Int_Typ_Cd	= ''0102'' 
	AND T50.Start_Dt		<= ${TX_DATE}
	AND T50.End_Dt			>= ${TX_DATE}

LEFT JOIN ${AUTO_PDM}.t03_agmt_bal_h T9 --  协议利息历史
	ON T1.Agmt_Id		= T9.Agmt_Id
	AND T1.Agmt_Cate_Cd = T9.Agmt_Cate_Cd
	AND T9.Agmt_Cate_Cd = ''0201''
	AND T9.Agmt_Bal_Typ_Cd = ''06''
	-- AND T9.Statt_Dt		= ${TX_DATE}
	AND T9.Start_Dt		<= ${TX_DATE}
	AND T9.End_Dt 		>= ${TX_DATE}

LEFT JOIN ${AUTO_PDM}.t88_exchg_rate T8 --  汇率牌价信息
	ON T1.Agmt_Cur_Cd	= T8.Init_Cur
	AND T8.Statt_Dt		= ${TX_DATE}

LEFT JOIN ${AUTO_PDM}.t01_cust_h T3 --  客户历史
	ON T1.Cust_Id = T3.Cust_Id
	AND T3.Start_Dt		<= ${TX_DATE}
	AND T3.End_Dt 		>= ${TX_DATE}

LEFT JOIN 
(
	SELECT Agmt_Id , ''0201'' AS Agmt_Cate_Cd
	FROM ${AUTO_PDM}.t03_agmt_accord_book 
	WHERE DATA_DT	<= ${TX_DATE}
	GROUP BY Agmt_Id
) T4 --  协定协议登记簿
	ON T1.Agmt_Id		= T4.Agmt_Id
	AND T1.Agmt_Cate_Cd	= T4.Agmt_Cate_Cd

WHERE T1.Start_Dt 	<= ${TX_DATE}
  AND T1.End_Dt 	>= ${TX_DATE}
  AND T1.real_acct_ind = ''1''
/*
AND (
	T1.Loan_Fiv_Cls_Cd like ''1%'' 
	OR T1.Loan_Fiv_Cls_Cd like ''2%'' 
	OR T1.Loan_Fiv_Cls_Cd = ''Z''
)
AND T1.Prod_Id NOT IN 
(''NCS10020204''
,''NCS02030206''
,''NCS02030106''
,''NCS02030108''
,''NCS02030101''
,''NCS02030203''
,''NCS02030104''
,''NCS02030207'') 
*/
';

CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
IF @RTC <> 0 THEN LEAVE LABLE;END IF;
SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

 /*计算汇总数、平均数*/
-- ETL_STEP_NO = 9
	SET @SQL_STR = 'INSERT INTO AT_t88_dpst_acct_bas_info(
		Statt_Dt			   		,--	统计日期
		Agmt_Id				   		,--	协议编号
		Card_Num			   		,--	卡号
		Cust_Id				   		,--	客户编号
		Acct_Nm				   		,--	账户名称
		Cur_Cd				   		,--	币种代码
		OpenAcct_Org_Id		   		,--	开户机构编号
		Acct_Org_Id			   		,--	账务机构编号
		Prod_Id				   		,--	产品编号
		Subj_Id				   		,--	科目编号
		Cust_Cate_Cd		   		,--	客户类型代码
		Acct_Ctgy_Cd		   		,--	账户类别代码
		Agmt_Stat_Cd		   		,--	协议状态代码
		Tm_Dpst_Acct_Sub_Cls_Cd		,--	定期账户细类代码
		Dpst_Term_Cate_Cd			,--	存期类型代码
		Dpst_Term_Cd				,--	存款期限代码
		Dept_Tm_Curr_Cate_Cd		,--	定活类型代码
		Agmt_Ind					,--	协定协议标志
		AutoRnw_Cd					,--	自动转存代码
		Int_Ind						,--	计息标志
		Cross_Dep_Ind				,--	通存标志
		Cir_Exch_Ind				,--	通兑标志
		OpenAcct_Dt					,--	开户日期
		ClosAcct_Dt					,--	销户日期
		St_Int_Dt					,--	起息日期
		Matr_Dt						,--	到期日期
		Int_Rate_Cate_Cd			,--	利率类型代码
		Int_Rate_Flt_Mod_Cd			,--	利率浮动方式代码
		Int_Rate_Rprc_Prd_Cate_Cd	,--	利率重定价周期类型代码
		Int_Rate_Rprc_Prd			,--	利率重定价周期
		Base_Int_Rate				,--	基准利率
		Exec_Int_Rate				,--	执行利率
		Flt_Point					,--	浮动点数
		Flt_Ratio					,--	浮动比例
		Cur_Prd_Provs_Int			,--	当期计提利息
		Accm_Provs_Int_Amt			,--	累计计提利息金额
		Accm_Int_Amt				,--	累计利息金额
		Int_Stl_Int_Amt				,--	结息利息金额
		Exchg_Rate					,--	汇率
		Curr_Bal					,--	当前余额
		To_Rmb_Bal					,--	折人民币余额
		Mth_Total_Bal				,--	月累积余额
		Quar_Tota_Bal				,--	季累积余额
		Yr_Tota_Bal					,--	年累积余额
		Rmb_Mth_Total_Bal			,--	人民币月累积余额
		Rmb_Quar_Total_Bal			,--	人民币季累积余额
		Rmb_Yr_Total_Bal			,--	人民币年累积余额
		Mth_DAvg					,--	月日均余额
		Quar_DAvg					,--	季日均余额
		Yr_DAvg						,--	年日均余额
		Rmb_Mth_DAvg				,--	人民币月日均余额
		Rmb_Quar_DAvg				,--	人民币季日均余额
		Rmb_Yr_DAvg					 --	人民币年日均余额
)
SELECT 	${TX_DATE}											AS  Statt_Dt,			--  统计日期
	   	COALESCE(t.Agmt_ID, b.Agmt_ID)						AS	Agmt_Id,			--	协议编号
		COALESCE(t.Card_Num, b.Card_Num)			   		AS 	Card_Num,			--	卡号
		COALESCE(t.Cust_Id, b.Cust_Id)				   		AS	Cust_Id,			--	客户编号
		COALESCE(t.Acct_Nm, b.Acct_Nm)				   		AS 	Acct_Nm,			--	账户名称
		COALESCE(t.Cur_Cd, b.Cur_Cd)				   		AS	Cur_Cd,				--	币种代码
		COALESCE(t.OpenAcct_Org_Id, b.OpenAcct_Org_Id)		AS	OpenAcct_Org_Id,	--	开户机构编号
		COALESCE(t.Acct_Org_Id, b.Acct_Org_Id)			   	AS	Acct_Org_Id,		--	账务机构编号
		COALESCE(t.Prod_Id, b.Prod_Id)				   		AS	Prod_Id,			--	产品编号
		COALESCE(t.Subj_Id, b.Subj_Id)				   		AS	Subj_Id,			--	科目编号
		COALESCE(t.Cust_Cate_Cd, b.Cust_Cate_Cd)		   	AS	Cust_Cate_Cd,		--	客户类型代码
		COALESCE(t.Acct_Ctgy_Cd, b.Acct_Ctgy_Cd)		   	AS	Acct_Ctgy_Cd,		--	账户类别代码
		COALESCE(t.Agmt_Stat_Cd, b.Agmt_Stat_Cd)		   	AS	Agmt_Stat_Cd,		--	协议状态代码
		COALESCE(t.Tm_Dpst_Acct_Sub_Cls_Cd, b.Tm_Dpst_Acct_Sub_Cls_Cd),		 		--	定期账户细类代码
		COALESCE(t.Dpst_Term_Cate_Cd, b.Dpst_Term_Cate_Cd)	AS	Dpst_Term_Cate_Cd,	--	存期类型代码
		COALESCE(t.Dpst_Term_Cd, b.Dpst_Term_Cd)			AS	Dpst_Term_Cd,		--	存款期限代码
		COALESCE(t.Dept_Tm_Curr_Cate_Cd, b.Dept_Tm_Curr_Cate_Cd),					--	定活类型代码
		COALESCE(t.Agmt_Ind, b.Agmt_Ind)					AS	Agmt_Ind,			--	协定协议标志
		COALESCE(t.AutoRnw_Cd, b.AutoRnw_Cd)				AS	AutoRnw_Cd,			--	自动转存代码
		COALESCE(t.Int_Ind, b.Int_Ind)						AS	Int_Ind,			--	计息标志 
		COALESCE(t.Cross_Dep_Ind, b.Cross_Dep_Ind)			AS	Cross_Dep_Ind,		--	通存标志
		COALESCE(t.Cir_Exch_Ind, b.Cir_Exch_Ind)			AS	Cir_Exch_Ind,		--	通兑标志
		COALESCE(t.OpenAcct_Dt, b.OpenAcct_Dt)				AS	OpenAcct_Dt,		--	开户日期
		COALESCE(t.ClosAcct_Dt, b.ClosAcct_Dt)				AS	ClosAcct_Dt,		--	销户日期
		COALESCE(t.St_Int_Dt, b.St_Int_Dt)					AS	St_Int_Dt,			--	起息日期
		COALESCE(t.Matr_Dt, b.Matr_Dt)						AS	Matr_Dt,			--	到期日期
		COALESCE(t.Int_Rate_Cate_Cd, b.Int_Rate_Cate_Cd)	AS	Int_Rate_Cate_Cd,	--	利率类型代码
		COALESCE(t.Int_Rate_Flt_Mod_Cd, b.Int_Rate_Flt_Mod_Cd),						--	利率浮动方式代码
		COALESCE(t.Int_Rate_Rprc_Prd_Cate_Cd, b.Int_Rate_Rprc_Prd_Cate_Cd),			--	利率重定价周期类型代码
		COALESCE(t.Int_Rate_Rprc_Prd, b.Int_Rate_Rprc_Prd)	AS	Int_Rate_Rprc_Prd,	--	利率重定价周期
		COALESCE(t.Base_Int_Rate, b.Base_Int_Rate)			AS	Base_Int_Rate,		--	基准利率
		COALESCE(t.Exec_Int_Rate, b.Exec_Int_Rate)			AS	Exec_Int_Rate,		--	执行利率
		COALESCE(t.Flt_Point, b.Flt_Point)					AS	Flt_Point,			--	浮动点数
		COALESCE(t.Flt_Ratio, b.Flt_Ratio)					AS	Flt_Ratio,			--	浮动比例
		COALESCE(t.Cur_Prd_Provs_Int, 0)					AS	Cur_Prd_Provs_Int,	--	当期计提利息
		COALESCE(t.Accm_Provs_Int_Amt, b.Accm_Provs_Int_Amt),						--	累计计提利息金额
		COALESCE(t.Accm_Int_Amt, b.Accm_Int_Amt)			AS	Accm_Int_Amt,		--	累计利息金额
		COALESCE(t.Int_Stl_Int_Amt, 0)						AS	Int_Stl_Int_Amt,	--	结息利息金额
		COALESCE(t.Exchg_Rate, 1)							AS	Exchg_Rate,			--	汇率
		COALESCE(t.Curr_Bal, 0)								AS	Curr_Bal,			--	当前余额
		COALESCE(t.To_Rmb_Bal, 0)							AS	To_Rmb_Bal,			--	折人民币余额	   
		CASE 
			WHEN ${TX_DATE} = ${THIS_MONTH_BEGIN} 
			THEN COALESCE(T.Curr_Bal,0)
			ELSE COALESCE(T.Curr_Bal,0) + COALESCE(b.Mth_Total_Bal,0) 
		END												AS	Mth_Total_Bal		--	月累积余额
		,CASE 
			WHEN ${TX_DATE} = ${THIS_QUART_BEGIN} 
			THEN COALESCE(T.Curr_Bal,0)
			ELSE COALESCE(T.Curr_Bal,0) + COALESCE(b.Quar_Tota_Bal,0)
		END												AS	Quar_Tota_Bal		--	季累积余额
		,CASE
			WHEN ${TX_DATE} = ${THIS_YEAR_BEGIN} 
			THEN COALESCE(T.Curr_Bal,0)
			ELSE COALESCE(T.Curr_Bal,0) + COALESCE(b.Yr_Tota_Bal,0)
		END												AS	Yr_Tota_Bal			--	年累积余额
		,CASE
			WHEN ${TX_DATE} = ${THIS_MONTH_BEGIN}
			THEN COALESCE(T.To_Rmb_Bal,0)
			ELSE COALESCE(T.To_Rmb_Bal,0) + COALESCE(b.Rmb_Mth_Total_Bal,0)
		END												AS	Rmb_Mth_Total_Bal	--	人民币月累积余额
		,CASE
			WHEN ${TX_DATE} = ${THIS_QUART_BEGIN}
			THEN COALESCE(T.To_Rmb_Bal,0) 
			ELSE COALESCE(T.To_Rmb_Bal,0) + COALESCE(b.Rmb_Quar_Total_Bal,0)
		END												AS	Rmb_Quar_Total_Bal	--	人民币季累积余额
		,CASE
			WHEN ${TX_DATE} = ${THIS_YEAR_BEGIN} 
			THEN COALESCE(T.To_Rmb_Bal,0)
			ELSE COALESCE(T.To_Rmb_Bal,0) + COALESCE(b.Rmb_Yr_Total_Bal, 0)
		END												AS	Rmb_Yr_Total_Bal	--	人民币年累积余额
		,CASE 
			WHEN ${TX_DATE} = ${THIS_MONTH_BEGIN} 
			THEN COALESCE(T.Curr_Bal,0)
			ELSE (COALESCE(T.Curr_Bal,0) + COALESCE(b.Mth_Total_Bal,0)) / (${TX_DATE}-${THIS_MONTH_BEGIN} + 1)
		END												AS	Mth_DAvg			--	月日均余额
		,CASE
			WHEN ${TX_DATE} = ${THIS_QUART_BEGIN}
			THEN COALESCE(T.Curr_Bal,0)
			ELSE (COALESCE(T.Curr_Bal,0) + COALESCE(b.Quar_Tota_Bal,0)) / (${TX_DATE}-${THIS_QUART_BEGIN} + 1)
		END												AS	Quar_DAvg			--	季日均余额
		,CASE
			WHEN ${TX_DATE} = ${THIS_YEAR_BEGIN} 
			THEN COALESCE(T.Curr_Bal,0)
			ELSE (COALESCE(T.Curr_Bal,0) + COALESCE(b.Yr_Tota_Bal,0)) / (${TX_DATE}-${THIS_YEAR_BEGIN} + 1)
		END												AS	Yr_DAvg				--	年日均余额
		,CASE
			WHEN ${TX_DATE} = ${THIS_MONTH_BEGIN} 
			THEN COALESCE(T.To_Rmb_Bal,0)
			ELSE (COALESCE(T.To_Rmb_Bal,0) + COALESCE(b.Rmb_Mth_Total_Bal,0)) / (${TX_DATE}-${THIS_MONTH_BEGIN} + 1)
		END 											AS	Rmb_Mth_DAvg		--	人民币月日均余额
		,CASE
			WHEN ${TX_DATE} = ${THIS_QUART_BEGIN}
			THEN COALESCE(T.To_Rmb_Bal,0)
			ELSE (COALESCE(T.To_Rmb_Bal,0) + COALESCE(b.Rmb_Quar_Total_Bal,0)) / (${TX_DATE}-${THIS_QUART_BEGIN} + 1)
		END												AS	Rmb_Quar_DAvg		--	人民币季日均余额
		,CASE
			WHEN ${TX_DATE} = ${THIS_YEAR_BEGIN} 
			THEN COALESCE(T.To_Rmb_Bal,0)
			ELSE (COALESCE(T.To_Rmb_Bal,0) + COALESCE(b.Rmb_Yr_Total_Bal,0)) / (${TX_DATE}-${THIS_YEAR_BEGIN} + 1)
		END												AS	Rmb_Yr_DAvg			--	人民币年日均余额
FROM VT_t88_dpst_acct_bas_info T

FULL JOIN 
	( SELECT * FROM ${AUTO_PDM}.t88_dpst_acct_bas_info
		WHERE STATT_DT = ${LAST_TX_DATE} 
		AND (
			  CASE WHEN ${TX_DATE} = ${THIS_YEAR_BEGIN}
				THEN 0
				ELSE 1
			  END
			) = 1       
          ) b -- 昨日数据
	ON T.Agmt_ID = b.Agmt_ID
';

CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
IF @RTC <> 0 THEN LEAVE LABLE;END IF;
SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

-- ETL_STEP_NO = 9999
SELECT COUNT(*) INTO PK_ERR_CNT 
FROM (
	SELECT Agmt_Id FROM AT_t88_dpst_acct_bas_info
	GROUP BY Agmt_Id HAVING COUNT(*) > 1
) I;
IF PK_ERR_CNT <> 0 THEN
	INSERT INTO ETL.ETL_JOB_STATUS_EDW VALUES ('',ETL_USER_ID,ETL_T_TAB_ENG_NAME,ETL_TX_DATE,9999,'',PK_ERR_CNT,'Failed','主键验证失败',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);	
END IF;


-- ETL_STEP_NO = 10	
	SET @SQL_STR = 'DELETE FROM ${AUTO_PDM}.t88_dpst_acct_bas_info WHERE Statt_Dt >= ${TX_DATE}';
CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
IF @RTC <> 0 THEN LEAVE LABLE;END IF;
SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

-- ETL_STEP_NO = 11	
	SET @SQL_STR = 'INSERT INTO ${AUTO_PDM}.t88_dpst_acct_bas_info SELECT * FROM AT_t88_dpst_acct_bas_info';
CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
IF @RTC <> 0 THEN LEAVE LABLE;END IF;
SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

/*删除临时表*/
-- ETL_STEP_NO = 12 
	SET @SQL_STR = 'DROP TEMPORARY TABLE IF EXISTS VT_t88_dpst_acct_bas_info';

CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
IF @RTC <> 0 THEN LEAVE LABLE;END IF;
SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;	

-- ETL_STEP_NO = 13 
	SET @SQL_STR = 'DROP TEMPORARY TABLE IF EXISTS AT_t88_dpst_acct_bas_info';

CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
IF @RTC <> 0 THEN LEAVE LABLE;END IF;
SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;	

SET OUT_RES_MSG='SUCCESSFUL';


END |