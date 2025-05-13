DELIMITER |

CREATE DEFINER="cqbank_sj"@"%" PROCEDURE "pr_f5_t88_product"(
	out OUT_RES_MSG VARCHAR(200),
	in IN_TX_DATE VARCHAR(8)
)
lable:BEGIN
/**
 * 产品信息汇总
 * 20221018 whd 新建 
 *  
 * */
	DECLARE ETL_T_TAB_ENG_NAME 		VARCHAR(50)		DEFAULT 't88_product';
	DECLARE ETL_STEP_NO				INTEGER			DEFAULT 1;
	DECLARE ETL_TX_DATE 			VARCHAR(8)		DEFAULT IN_TX_DATE;
	DECLARE PK_COUNT				BIGINT			DEFAULT 0;
	DECLARE PK_ERR_CNT				BIGINT			DEFAULT 0;
	DECLARE RET_CODE				INTEGER			DEFAULT 0;
	
/* 支持数据重跑*/
	
	/*定义临时表*/
	SET @SQL_STR = '
DROP TEMPORARY  TABLE IF EXISTS  ${AUTO_PDM}.VT_t88_product';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	SET @SQL_STR = '
CREATE TEMPORARY TABLE ${AUTO_PDM}.VT_t88_product LIKE ${AUTO_PDM}.t88_product';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

-- 1 贷款产品历史 t02_loan_prod_h
	SET @SQL_STR ='INSERT INTO ${AUTO_PDM}.VT_t88_product(
	Prod_Id						,-- 1	产品编号
	Src_Sys_Prod_Id				,-- 2	源系统产品编号
	Prod_Nm						,-- 3	产品名称
	Valid_Ind					,-- 4	有效标志
	Inout_Bal_Ind				,-- 5	表内外代码
	Auto_Ent_Acct_Ind			,-- 6	自动上账标志
	Is_Net_Loan_Biz				,-- 7	网贷业务标志
	Online_Offline_Prod_Ind_Cd	,-- 8	线上线下产品分类代码
	Setup_Dt					,-- 9	创建日期
	Upd_Dt						,-- 10	更新日期
	Rgst_Org					,-- 11	登记机构编号
	Main_Src_Task				,-- 82	主要源系统任务
	Statt_Dt					 -- 83	统计日期
	)
SELECT  a.Prod_Id						,--	1 产品编号
		a.Src_Sys_Prod_Id				,-- 2 源系统产品编号
		a.Prod_Nm						,-- 3  产品名称
		a.Valid_Ind						,-- 4 有效标志
		a.Inout_Bal_Ind					,-- 5 表内外标志
		a.Auto_Ent_Acct_Ind				,-- 6 自动上账标志
		a.Is_Net_Loan_Biz				,-- 7 网贷业务标志
		a.Online_Offline_Prod_Ind_Cd	,-- 8 线上线下产品分类代码
		substr(replace(a.Setup_Dt,''/'',''''),1,8)						,-- 9 创建日期
		substr(replace(a.Upd_Dt,''/'',''''),1,8)						,-- 10 更新日期
		a.Rgst_Org						,-- 11 登记机构
		''T02_LOAN_PROD_H''				,-- 82 主要源系统任务
		${TX_DATE} 						 -- 83 统计日期
 FROM ${AUTO_PDM}.t02_loan_prod_h A WHERE start_dt <= ${TX_DATE} and end_dt >= ${TX_DATE}
';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
-- 2  直接融资工具历史 t02_drct_fin_tool_h
SET @SQL_STR ='INSERT INTO ${AUTO_PDM}.VT_t88_product(
	Prod_Id						,-- 1	产品编号
	Src_Sys_Prod_Id				,-- 2	源系统产品编号
	Prod_Nm						,-- 3	产品名称
	Setup_Dt					,-- 9	创建日期
	Rgst_Org					,-- 11	登记机构编号
	Tx_Site_Cd					,-- 12	交易所代码
	Int_Base_Cd					,-- 13	计息基准代码
	Int_Prd						,-- 14	计息周期
	Int_Pay_Prd					,-- 15	付息周期
	Int_Rate_Cate_Cd			,-- 16	利率类型代码
	Prosp_Prft_Rate				,-- 17	预期收益率
	Asst_Liab_Ind				,-- 18	资产负债标志
	Int_Mod_Cd					,-- 19	计息方式代码
	Int_Pay_Mod_Cd				,-- 20	付息方式代码
	St_Int_Dt					,-- 21	起息日期
	Matr_Dt						,-- 22	到期日期
	Issu_Prc					,-- 23	发行价格
	Issu_Mod_Cd					,-- 24	发行方式代码
	Actl_Issu_Totl_Amt			,-- 25	实际发行总额
	Fin_Pers_Nm					,-- 26	融资人名称
	Fin_Trust_Corp_Nm			,-- 27	信托公司名称
	Fin_Trust_Plan_Charc_Cd		,-- 28	信托计划性质代码
	Int_Post_Mod_Cd				,-- 29	计息顺延方式代码
	Int_Pay_Post_Mod_Cd			,-- 30	付息顺延方式代码
	Matr_Post_Mod_Cd			,-- 31	到期顺延方式代码
	Fin_Pers_Main_Grade_Cd		,-- 50	融资人主体评级代码
	Guar_Mod_Cd					,-- 52	担保方式代码
	Main_Src_Task				,-- 82	主要源系统任务
	Statt_Dt					 -- 83	统计日期
	)
SELECT  a.Prod_Id					,-- 1 产品编号
		a.Src_Sys_Tool_Id			,-- 2 源系统工具编号
		a.Prod_Nm					,-- 3 产品名称
		a.Set_Up_Dt					,-- 9 成立日期
		a.Input_Org_Id				,-- 11 录入机构编号
		a.Tx_Site_Cd				,-- 12 交易所代码
		a.Int_Base_Cd				,-- 13 计息基准代码
		a.Int_Prd					,-- 14 计息周期
		a.Int_Pay_Prd				,-- 15 付息周期
		a.Int_Rate_Cate_Cd			,-- 16 利率类型代码
		a.Prosp_Prft_Rate			,-- 17 预期收益率
		a.Asst_Liab_Ind				,-- 18 资产负债标志
		a.Int_Mod_Cd				,-- 19 计息方式代码
		a.Int_Pay_Mod_Cd			,-- 20 付息方式代码
		a.St_Int_Dt					,-- 21 起息日期
		a.Matr_Dt					,-- 22 到期日期
		a.Issu_Prc					,-- 23 发行价格
		a.Issu_Mod_Cd				,-- 24 发行方式代码
		a.Actl_Issu_Totl_Amt		,-- 25 实际发行总额
		a.Fin_Pers_Nm				,-- 26 融资人名称
		a.Fin_Trust_Corp_Nm			,-- 27 信托公司名称
		a.Fin_Trust_Plan_Charc_Cd	,-- 28 信托计划性质代码
		a.Int_Post_Mod_Cd			,-- 29 计息顺延方式代码
		a.Int_Pay_Post_Mod_Cd		,-- 30 付息顺延方式代码
		a.Matr_Post_Mod_Cd			,-- 31 到期顺延方式代码
		a.Main_Grade				,-- 50 主体评级
		a.Guar_Mod_Cd				,-- 52 担保方式代码
		''T02_DRCT_FIN_TOOL_H''		,-- 82 主要源系统任务
		${TX_DATE} 					-- 823统计日期
 FROM ${AUTO_PDM}.t02_drct_fin_tool_h A WHERE start_dt <= ${TX_DATE} and end_dt >= ${TX_DATE}
'; 

	-- 3 资管债券历史 t02_asset_mgmt_bond_h
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	SET @SQL_STR ='INSERT INTO ${AUTO_PDM}.VT_t88_product(
	Prod_Id						,-- 1	产品编号
	Src_Sys_Prod_Id				,-- 2	源系统产品编号
	Prod_Nm						,-- 3	产品名称
	Tx_Site_Cd					,-- 12	交易所代码
	Int_Base_Cd					,-- 13	计息基准代码
	Int_Prd						,-- 14	计息周期
	Int_Rate_Cate_Cd			,-- 16	利率类型代码
	Int_Pay_Mod_Cd				,-- 20	付息方式代码
	St_Int_Dt					,-- 21	起息日期
	Matr_Dt						,-- 22	到期日期
	Issu_Prc					,-- 23	发行价格
	Int_Post_Mod_Cd				,-- 29	计息顺延方式代码
	Int_Pay_Post_Mod_Cd			,-- 30	付息顺延方式代码
	Matr_Post_Mod_Cd			,-- 31	到期顺延方式代码
	Bond_Int_Rate				,-- 32	债券利率
	Bond_Charc_Cd				,-- 33	债券性质代码
	Bond_Cate_Cd				,-- 34	债券类型代码
	Int_Pay_Freq_Cd				,-- 35	付息频率代码
	Input_Dt					,-- 36	入池日期
	Issu_Org_Cate_Cd			,-- 37	发行机构类型代码
	Bond_Crdt_Grade				,-- 38	债券信用评级
	Main_Crdt_Grade				,-- 39	主体信用评级
	Bond_Crdt_Grade_Org_Id		,-- 40	债券信用评级机构编号
	Main_Crdt_Grade_Org_Id		,-- 41	主体信用评级机构编号
	Trust_Bank_Id				,-- 42	托管银行编号
	Issu_Qty					,-- 47	发行量
	Bond_Par					,-- 62	债券面值
	Main_Src_Task				,-- 82	主要源系统任务
	Statt_Dt					 -- 83	统计日期
	)
SELECT  a.Prod_Id						,-- 1 产品编号
		a.Src_Sys_Bond_Id				,-- 2 源系统产品编号
		a.Bond_Nm						,-- 3  产品名称
		a.Tx_Site_Cd					,-- 12 交易所代码
		a.Int_Base_Cd					,-- 13 计息基准代码
		a.Int_Prd						,-- 14 计息周期
		a.Int_Rate_Cate_Cd				,-- 16 利率类型代码
		a.Int_Pay_Mod_Cd				,-- 20 付息方式代码
		a.St_Int_Dt						,-- 21 起息日期
		a.Matr_Dt						,-- 22 到期日期
		a.Issu_Prc						,-- 23 发行价格
		a.Int_Post_Mod_Cd				,-- 29计息顺延方式代码
		a.Int_Pay_Post_Mod_Cd			,-- 30 付息顺延方式代码
		a.Matr_Post_Mod_Cd				,-- 31 到期顺延方式代码
		a.Bond_Int_Rate					,-- 32 债券利率
		a.Bond_Charc_Cd					,-- 33债券性质代码
		a.Bond_Cate_Cd					,-- 34 债券类型代码
		a.Int_Pay_Freq_Cd				,-- 35 付息频率代码
		a.Input_Dt						,-- 36 入池日期
		a.Issu_Org_Cate_Cd				,-- 37 发行机构类型代码
		a.Bond_Crdt_Grade				,-- 38 债券信用评级
		a.Main_Crdt_Grade				,-- 39 主体信用评级
		a.Bond_Crdt_Grade_Org_Id		,-- 40 债券信用评级机构编号
		a.Main_Crdt_Grade_Org_Id		,-- 41主体信用评级机构编号
		a.Trust_Bank_Id					,-- 42 托管银行编号
		a.Issu_Qty						,-- 47 发行量
		a.Bond_Par						,-- 62 债券面值
		''T02_ASSET_MGMT_BOND_H''		,-- 82 主要源系统任务
		${TX_DATE} 						 -- 83 统计日期
 FROM ${AUTO_PDM}.t02_asset_mgmt_bond_h A WHERE start_dt <= ${TX_DATE} and end_dt >= ${TX_DATE}
 '
; 

	-- 4 t02_fund_h 基金历史
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	SET @SQL_STR ='INSERT INTO ${AUTO_PDM}.VT_t88_product(
	Prod_Id						,-- 1	产品编号
	Src_Sys_Prod_Id				,-- 2	源系统产品编号
	Prod_Nm						,-- 3	产品名称
	Setup_Dt					,-- 9	创建日期
	Rgst_Org					,-- 11	登记机构编号
	Tx_Site_Cd					,-- 12	交易所代码
	Issu_Mod_Cd					,-- 24	发行方式代码
	Trust_Bank_Id				,-- 42	托管银行编号
	Fund_Mgr_Id					,-- 43	基金经理编号
	Fund_Mgr_Nm					,-- 44	基金经理名称
	Cur_Cd						,-- 45	币种代码
	Accti_Mod_Cd				,-- 46	核算方式代码
	Custodian_Nm				,-- 48	托管银行名称
	Main_Src_Task				,-- 82	主要源系统任务
	Statt_Dt					 -- 83	统计日期
	)
SELECT  a.Prod_Id			,-- 1 产品编号
		a.Src_Sys_Fund_Id	,-- 2 源系统基金编号
		a.Fund_Nm			,-- 3 基金名称
		a.Set_Up_Dt			,-- 9 成立日期
		a.Rgst_Org_Id		,-- 11 登记机构编号
		a.Tx_Site_Cd		,-- 12 交易所代码
 		a.Issu_Mod_Cd		,-- 24 发行方式代码
		a.Trust_Bank_Id		,-- 42 托管银行编号
		a.Fund_Mgr_Id		,-- 43 基金经理编号
		a.Fund_Mgr_Nm		,-- 44 基金经理名称
		a.Cur_Cd			,-- 45 币种代码
		a.Accti_Mod_Cd		,-- 46 核算方式代码
		a.Trust_Pers_Nm		,-- 48 托管人名称
		''T02_FUND_H''		,-- 82 主要源系统任务
		${TX_DATE} 			 -- 83 统计日期
 FROM ${AUTO_PDM}.t02_fund_h A WHERE start_dt <= ${TX_DATE} and end_dt >= ${TX_DATE}
'; 

	
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	-- 5 资产管理计划 t02_asst_mgmt_plan
	SET @SQL_STR ='INSERT INTO ${AUTO_PDM}.VT_t88_product(
	Prod_Id						,-- 1	产品编号
	Src_Sys_Prod_Id				,-- 2	源系统产品编号
	Prod_Nm						,-- 3	产品名称
	Setup_Dt					,-- 9	创建日期
	Tx_Site_Cd					,-- 12	交易所代码
	Int_Base_Cd					,-- 13	计息基准代码
	Int_Prd						,-- 14	计息周期
	Int_Rate_Cate_Cd			,-- 16	利率类型代码
	Prosp_Prft_Rate				,-- 17	预期收益率
	St_Int_Dt					,-- 21	起息日期
	Matr_Dt						,-- 22	到期日期
	Issu_Prc					,-- 23	发行价格
	Fin_Pers_Nm					,-- 26	融资人名称
	Fin_Trust_Corp_Nm			,-- 27	信托公司名称
	Fin_Trust_Plan_Charc_Cd		,-- 28	信托计划性质代码
	Int_Post_Mod_Cd				,-- 29	计息顺延方式代码
	Int_Pay_Post_Mod_Cd			,-- 30	付息顺延方式代码
	Matr_Post_Mod_Cd			,-- 31	到期顺延方式代码
	Int_Pay_Freq_Cd				,-- 35	付息频率代码
	Issu_Qty					,-- 47	发行量
	Custodian_Nm				,-- 48	托管银行名称
	Fin_Pers_Main_Grade_Cd		,-- 50	融资人主体评级代码
	Belg_Cty_And_Zone			,-- 51	所属国家和地区
	Guar_Mod_Cd					,-- 52	担保方式代码
	Guar_Cur_Cd					,-- 53	担保币种代码
	Guar_Amt					,-- 54	担保金额
	Nonnorm_Asst_Ind			,-- 55	非标资产标志
	Main_Src_Task				,-- 82	主要源系统任务
	Statt_Dt					 -- 83	统计日期
	)
SELECT  a.Prod_Id					,-- 1 产品编号
		a.Src_Sys_Fin_Trust_Id		,-- 2 源系统信托编号
		a.Fin_Trust_Plan_Full_Nm	,-- 3 信托计划全称
		a.Set_Up_Dt					,-- 9 成立日期
		a.Tx_Site_Cd				,-- 12 交易所代码
		a.Int_Base_Cd				,-- 13 计息基准代码
		a.Int_Prd					,-- 14 计息周期
		a.Int_Rate_Cate_Cd			,-- 16 利率类型代码
		a.Prosp_Prft_Rate	 		,-- 17 预期收益率
		a.St_Int_Dt					,-- 21 起息日期
		a.Matr_Dt					,-- 22 到期日期
		a.Issu_Prc					,-- 23 发行价格
		a.Fin_Pers_Nm				,-- 26 融资人名称
		a.Fin_Trust_Corp_Nm			,-- 27 信托公司名称
		a.Fin_Trust_Plan_Charc_Cd	,-- 28 信托计划性质代码
		a.Int_Post_Mod_Cd			,-- 29 计息顺延方式代码
		a.Int_Pay_Post_Mod_Cd		,-- 30 付息顺延方式代码
		a.Matr_Post_Mod_Cd			,-- 31 到期顺延方式代码
		a.Int_Pay_Freq_Cd			,-- 35 付息频率代码
		a.Issu_Qty					,-- 47 发行量
		a.Custodian_Nm				,-- 48 托管行名称
		a.Fin_Pers_Main_Grade_Cd	,-- 50 融资人主体评级代码
		a.Belg_Cty_And_Zone			,-- 51 所属国家和地区
		a.Guar_Mod_Cd				,-- 52 担保方式代码
		a.Guar_Cur_Cd				,-- 53 担保币种代码
		a.Guar_Amt					,-- 54 担保金额
		a.Nonnorm_Asst_Ind			,-- 55 非标资产标志
		''T02_ASST_MGMT_PLAN''		,-- 82 主要源系统任务
		${TX_DATE} 					 -- 83 统计日期
 FROM ${AUTO_PDM}.t02_asst_mgmt_plan A WHERE Statt_Dt = ${TX_DATE} 
'; 

	
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	-- 6 ,16 理财产品历史 t02_chrem_prod_h
	SET @SQL_STR ='INSERT INTO ${AUTO_PDM}.VT_t88_product(
	Prod_Id						,-- 1	产品编号
	Src_Sys_Prod_Id				,-- 2	源系统产品编号
	Prod_Nm						,-- 3	产品名称
	Prosp_Prft_Rate				,-- 17	预期收益率
	St_Int_Dt					,-- 21	起息日期
	Matr_Dt						,-- 22	到期日期
	Issu_Prc					,-- 23	发行价格
	Actl_Issu_Totl_Amt			,-- 25	实际发行总额
	Cur_Cd						,-- 45	币种代码
	Custodian_Nm				,-- 48	托管银行名称
	Chrem_Term					,-- 56	理财期限
	Coll_Start_Dt				,-- 57	募集起始日期
	Coll_End_Dt					,-- 58	募集结束日期
	Max_Coll_Amt				,-- 59	最高募集金额
	Prod_Matr_Ind				,-- 60	产品到期标志
	Brk_Ev_Income_Cls_Cd		,-- 61	保本收益类代码
	Prod_Cate_Cd				,-- 75	产品类型代码
	Main_Src_Task				,-- 82	主要源系统任务
	Statt_Dt					 -- 83	统计日期
	)
SELECT  a.Prod_Id					,-- 1 产品编号
		a.Src_Sys_Prod_Id			,-- 2 源系统产品编号
		a.Prod_Nm					,-- 3 产品名称
		a.Prosp_Prft_Rate			,-- 17 预期收益率
		a.St_Int_Dt					,-- 21 起息日期
		a.Matr_Dt					,-- 22 到期日期
		a.Issu_Prc					,-- 23 发行价格
		a.Coll_Amt					,-- 25 募集金额
		a.Issu_Cur_Cd				,-- 45 发行币种代码
		a.Custodian_Nm				,-- 48 托管行名称
		a.Chrem_Term				,-- 56 理财期限
		a.Coll_Start_Dt				,-- 57 募集起始日期
		a.Coll_End_Dt				,-- 58 募集结束日期
		a.Max_Coll_Amt				,-- 59 最高募集金额
		a.Prod_Matr_Ind				,-- 60 产品到期标志
		a.Brk_Ev_Income_Cls_Cd		,-- 61 保本收益类代码
		a.Prod_Cate_Cd				,-- 75 产品类型代码
		''T02_CHREM_PROD_H''		,-- 82主要源系统任务
		${TX_DATE} 					 -- 83 统计日期
 FROM ${AUTO_PDM}.t02_chrem_prod_h A WHERE start_dt <= ${TX_DATE} and end_dt >= ${TX_DATE}
'; 

	-- 7 外汇债券历史 t02_fx_bond_h
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
 
	SET @SQL_STR ='INSERT INTO ${AUTO_PDM}.VT_t88_product(
	Prod_Id						,-- 1	产品编号
	Src_Sys_Prod_Id				,-- 2	源系统产品编号
	Prod_Nm						,-- 3	产品名称
	Int_Base_Cd					,-- 13	计息基准代码
	St_Int_Dt					,-- 21	起息日期
	Matr_Dt						,-- 22	到期日期
	Bond_Int_Rate				,-- 32	债券利率
	Bond_Cate_Cd				,-- 34	债券类型代码
	Int_Pay_Freq_Cd				,-- 35	付息频率代码
	Cur_Cd						,-- 45	币种代码
	Bond_Par					,-- 62	债券面值
	Term_Corp_Cd				,-- 63	期限单位代码
	Issur						,-- 64	发行人编号
	Issu_Dt						,-- 65	发行日期
	Int_Stl_Freq_Cd				,-- 80	结息频率代码
	Main_Src_Task				,-- 82	主要源系统任务
	Statt_Dt					 -- 83	统计日期
	)
SELECT  a.Prod_Id 				,-- 1 产品编号
		a.Src_Sys_Bond_Id		,-- 2 源系统债券编号
		a.Bond_Nm				,-- 3 债券名称
		a.Int_Base_Cd			,-- 13 计息基准代码
		a.St_Int_Dt				,-- 21 起息日期
		a.Matr_Dt				,-- 22 到期日期
		a.Par_Int_Rate			,-- 32 票面利率
		a.Bond_Cate_Cd			,-- 34 债券类型代码
		a.Int_Pay_Freq_Cd		,-- 35 付息频率代码
		a.Stl_Cur_Cd			,-- 45 结算币种代码
		a.Bond_Par				,-- 62 债券面值
		a.Term_Corp_Cd			,-- 63 期限单位代码
		a.Issur					,-- 64 发行人
		a.Issu_Dt				,-- 65 发行日期
		a.Int_Stl_Freq_Cd		,-- 80 结息频率代码
		''T02_FX_BOND_H''		,-- 82主要源系统任务
		${TX_DATE} 				 -- 83 统计日期
 FROM ${AUTO_PDM}.t02_fx_bond_h A WHERE start_dt <= ${TX_DATE} and end_dt >= ${TX_DATE}
'; 

	-- 8 资金债券历史 t02_cap_bond_h
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	SET @SQL_STR ='INSERT INTO ${AUTO_PDM}.VT_t88_product(
	Prod_Id						,-- 1	产品编号
	Src_Sys_Prod_Id				,-- 2	源系统产品编号
	Prod_Nm						,-- 3	产品名称
	Valid_Ind					,-- 4	有效标志
	Int_Base_Cd					,-- 13	计息基准代码
	Int_Rate_Cate_Cd			,-- 16	利率类型代码
	St_Int_Dt					,-- 21	起息日期
	Matr_Dt						,-- 22	到期日期
	Issu_Prc					,-- 23	发行价格
	Actl_Issu_Totl_Amt			,-- 25	实际发行总额
	Bond_Int_Rate				,-- 32	债券利率
	Bond_Charc_Cd				,-- 33	债券性质代码
	Bond_Cate_Cd				,-- 34	债券类型代码
	Int_Pay_Freq_Cd				,-- 35	付息频率代码
	Cur_Cd						,-- 45	币种代码
	Bond_Par					,-- 62	债券面值
	Prod_Cate_Cd				,-- 75	产品类型代码
	Main_Src_Task				,-- 82	主要源系统任务
	Statt_Dt					 -- 83	统计日期
	)
SELECT  a.Prod_Id				,-- 1 产品编号
		a.Src_Sys_Bond_Id		,-- 2 源系统债券编号
		a.Bond_Full_Nm			,-- 3 债券全称
		a.Data_Valid_Ind		,-- 4 数据有效标志
		a.Int_Base_Cd			,-- 13 计息基准代码
		a.Int_Rate_Cate_Cd		,-- 16 利率类型代码
		a.Bond_St_Int_Dt		,-- 21 债券起息日期
		a.Bond_Matr_Dt			,-- 22 债券到期日期
		a.Issu_Prc				,-- 23 发行价格
		a.Bond_Issu_Totl_Amt	,-- 25 债券发行总额
		a.Par_Int_Rate			,-- 32 票面利率
		a.Bond_Charc_Cd			,-- 33 债券性质代码
		a.Bond_Cate_Cd			,-- 34 债券类型代码
		a.Int_Pay_Freq_Cd		,-- 35 付息频率代码
		a.Stl_Cur_Cd			,-- 45 结算币种代码
		a.Bond_Par				,-- 62 债券面额
		a.Prod_Cate_Cd			,-- 75 产品类型代码
		''T02_CAP_BOND_H''		,-- 82主要源系统任务
		${TX_DATE} 				 -- 83 统计日期
 FROM ${AUTO_PDM}.t02_cap_bond_h A WHERE start_dt <= ${TX_DATE} and end_dt >= ${TX_DATE}
'; 

	
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	-- 9/12 存款产品历史 t02_dpst_prod_h
	SET @SQL_STR ='INSERT INTO ${AUTO_PDM}.VT_t88_product(
	Prod_Id						,-- 1	产品编号
	Src_Sys_Prod_Id				,-- 2	源系统产品编号
	Prod_Nm						,-- 3	产品名称
	Tm_Dpst_Cls_Cd				,-- 66	定期存款分类代码
	Efft_Dt						,-- 67	生效日期
	Invld_Dt					,-- 68	失效日期
	Dpst_Amt					,-- 69	起存金额
	Rnw_Freq					,-- 70	转存频率
	Rnw_Ind						,-- 71	转存标志
	Cross_Dep_Ind				,-- 72	通存标志
	Unexp_Draw_Ind				,-- 73	提前支取标志
	Unexp_Draw_Cnt				,-- 74	提前支取次数
	Prod_Cate_Cd				,-- 75	产品类型代码
	Main_Src_Task				,-- 82	主要源系统任务
	Statt_Dt					 -- 83	统计日期
	)
SELECT  a.Prod_Id				,-- 1 产品编号
		a.Src_Sys_Prod_Id		,-- 2 源系统产品编号
		a.Prod_Nm				,-- 3 产品名称
		a.Tm_Dpst_Cls_Cd		,-- 66 定期存款分类代码
		a.Prod_Efft_Dt			,-- 67 产品生效日期
		a.Prod_Invld_Dt			,-- 68 产品失效日期
		a.Dpst_Amt				,-- 69 起存金额
		a.Rnw_Freq				,-- 70 转存频率
		a.Rnw_Ind				,-- 71 转存标志
		a.Cross_Dep_Ind			,-- 72 通存标志
		a.Unexp_Draw_Ind		,-- 73 提前支取标志
		a.Unexp_Draw_Cnt		,-- 74 提前支取次数
		a.Prod_Cate_Cd			,-- 75 产品类型代码
		''T02_DPST_PROD_H''		,-- 82主要源系统任务
		${TX_DATE} 				 -- 83 统计日期
 FROM ${AUTO_PDM}.t02_dpst_prod_h A WHERE start_dt <= ${TX_DATE} and end_dt >= ${TX_DATE}
'; 

	
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	-- 11（10组无子表）信用卡卡产品历史 t02_ccard_card_prod_h
	SET @SQL_STR ='INSERT INTO ${AUTO_PDM}.VT_t88_product(
	Prod_Id						,-- 1	产品编号
	Src_Sys_Prod_Id				,-- 2	源系统产品编号
	Prod_Nm						,-- 3	产品名称
	Prod_Cate_Cd				,-- 75	产品类型代码
	PLN_Mgmt_Cd					,-- 76	贷后管理代码
	Insu_Corp_Prod_Cd			,-- 78	保险公司产品代码
	Hesit_Term_Days				,-- 79	犹豫期天数
	Main_Src_Task				,-- 82	主要源系统任务
	Statt_Dt					 -- 83	统计日期
	)
SELECT 	a.Prod_Id					,-- 1  产品编号
		a.Src_Sys_Prod_Id			,-- 2  源系统产品编号
		a.Prod_Nm					,-- 3  产品名称
		a.Card_Brand_Cd				,-- 75 卡品牌代码
		a.Corp_Card_Cate_Cd			,-- 76 公司卡类型代码
		a.Fst_Issu_Card_Valid_Yrs	,-- 78 首次发卡有效年数
		a.Stop_Issu_Card_Ind		,-- 79 停止发卡标志
		''T02_CCARD_CARD_PROD_H''	,-- 82主要源系统任务
		${TX_DATE} 					 -- 83 统计日期
 FROM ${AUTO_PDM}.t02_ccard_card_prod_h A WHERE start_dt <= ${TX_DATE} and end_dt >= ${TX_DATE}
'; 

	
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	-- 14、（13组无子表）IC卡产品信息 t02_ic_card_prod_info
	SET @SQL_STR ='INSERT INTO ${AUTO_PDM}.VT_t88_product(
	Prod_Id						,-- 1	产品编号
	Invld_Dt					,-- 68	失效日期
	Main_Src_Task				,-- 82	主要源系统任务
	Statt_Dt					 -- 83	统计日期
	)
SELECT  a.Card_Prod_Cd			,-- 1 卡产品编号
		a.Invld_Dt				,-- 68  失效日期
		''T02_IC_CARD_PROD_INFO''	,-- 82主要源系统任务
		${TX_DATE} 					-- 83 统计日期
 FROM ${AUTO_PDM}.t02_ic_card_prod_info A WHERE statt_dt = ${TX_DATE} 
'; 

	
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	-- 15  网贷产品历史 t02_cfs_loan_prod_h
	SET @SQL_STR ='INSERT INTO ${AUTO_PDM}.VT_t88_product(
	Prod_Id						,-- 1	产品编号
	Src_Sys_Prod_Id				,-- 2	源系统产品编号
	Prod_Nm						,-- 3	产品名称
	Valid_Ind					,-- 4	有效标志
	Inout_Bal_Ind				,-- 5	表内外代码
	Rgst_Org					,-- 11	登记机构编号
	Cur_Cd						,-- 45	币种代码
	Efft_Dt						,-- 67	生效日期
	Invld_Dt					,-- 68	失效日期
	Prod_Cate_Cd				,-- 75	产品类型代码
	PLN_Mgmt_Cd					,-- 76	贷后管理代码
	Is_Can_Cir					,-- 77	可循环标志                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
	Main_Src_Task				,-- 82	主要源系统任务
	Statt_Dt					 -- 83	统计日期
	)
SELECT  a.Prod_ID				,-- 1 产品编号
		a.Src_Sys_Prod_ID		,-- 2 源系统产品编号
		a.Prod_Nm				,-- 3 产品名称
		a.Valid_Ind				,-- 4 有效标识
		a.In_Bal_Off_Bal		,-- 5 表内表外
		a.Rgst_Org				,-- 11 登记机构
		a.Cur_Cd				,-- 45 币种代码
		a.Prod_Efft_Dt			,-- 67 产品生效日期
		a.Prod_Invld_Dt			,-- 68 产品失效日期
		a.Prod_Cate_Cd			,-- 75 产品类型代码
		a.PLN_Mgmt_Cd			,-- 76 贷后管理代码
		a.Is_Can_Cir			,-- 77 是否可循环
		''T02_CFS_LOAN_PROD_H''	,-- 82主要源系统任务
		${TX_DATE} 				 -- 83 统计日期
 FROM ${AUTO_PDM}.t02_cfs_loan_prod_h A WHERE start_dt <= ${TX_DATE} and end_dt >= ${TX_DATE}
'; 

	
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	-- 17 银保通产品表  t02_ybt_prod_h
	SET @SQL_STR ='INSERT INTO ${AUTO_PDM}.VT_t88_product(
	Prod_Id						,-- 1	产品编号
	Src_Sys_Prod_Id				,-- 2	源系统产品编号
	Prod_Nm						,-- 3	产品名称
	Cur_Cd						,-- 45	币种代码
	Efft_Dt						,-- 67	生效日期
	Invld_Dt					,-- 68	失效日期
	Prod_Cate_Cd				,-- 75	产品类型代码
	Insu_Corp_Prod_Cd			,-- 78	保险公司产品代码
	Hesit_Term_Days				,-- 79	犹豫期天数
	Main_Src_Task				,-- 82	主要源系统任务
	Statt_Dt					 -- 83	统计日期
	)
SELECT  a.Prod_ID				,-- 1 产品编号
		a.Src_Sys_Prod_ID		,-- 2 源系统产品编号
		a.Prod_Nm				,-- 3 产品名称
		a.TX_Cur				,-- 45 交易币种
		a.Contr_Start_Dt		,-- 67 合约开始日期
		a.Contr_End_Dt			,-- 68 合约结束日期
		a.Prod_Cate				,-- 75 产品类型
		a.Insu_Corp_Prod_Cd		,-- 78 保险公司产品代码
		a.Hesit_Term_Days		,-- 79 犹豫期天数
		''T02_YBT_PROD_H''		,-- 82主要源系统任务
		${TX_DATE} 				 -- 83 统计日期
 FROM ${AUTO_PDM}.t02_ybt_prod_h A WHERE start_dt <= ${TX_DATE} and end_dt >= ${TX_DATE}
'; 

	
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

-- 18 理财销售产品历史  t02_chrem_sell_prod_h
	SET @SQL_STR ='INSERT INTO ${AUTO_PDM}.VT_t88_product(
	Prod_Id						,-- 1	产品编号
	Src_Sys_Prod_Id				,-- 2	源系统产品编号
	Prod_Nm						,-- 3	产品名称
	Setup_Dt					,-- 9	创建日期
	St_Int_Dt					,-- 21	起息日期
	Matr_Dt						,-- 22	到期日期
	Bond_Crdt_Grade				,-- 38	债券信用评级
	Fund_Mgr_Id					,-- 43	基金经理编号
	Cur_Cd						,-- 45	币种代码
	Coll_Start_Dt				,-- 57	募集起始日期
	Coll_End_Dt					,-- 58	募集结束日期
	Prod_Cate_Cd				,-- 75	产品类型代码
	Main_Src_Task				,-- 82	主要源系统任务
	Statt_Dt					 -- 83	统计日期
	)
SELECT  a.Prod_Id					,-- 1 产品编号
		a.Src_Sys_Prod_Id			,-- 2 源系统产品编号
		a.Prod_Nm					,-- 3 产品名称
		a.Prod_Set_Up_Dt			,-- 9 产品成立日期
		a.Prod_St_Int_Dt			,-- 21 产品起息日期
		a.Prod_End_Dt				,-- 22 产品结束日期
		a.Estim_Lvl_Cd				,-- 38 评估等级代码
		a.Prod_Mgmt_Pers_Cd			,-- 43 产品管理人代码
		a.Cur_Cd					,-- 45 币种代码
		a.Coll_Start_Dt				,-- 57 募集起始日期
		a.Coll_End_Dt				,-- 58 募集结束日期
		a.Prod_Ctgy_Cd				,-- 75 产品类别代码
		''T02_CHREM_SELL_PROD_H''	,-- 82 主要源系统任务
		${TX_DATE} 				 	 -- 83 统计日期
 FROM ${AUTO_PDM}.t02_chrem_sell_prod_h A WHERE start_dt <= ${TX_DATE} and end_dt >= ${TX_DATE}
'; 

	
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	/*SET @SQL_STR ='INSERT INTO ${AUTO_PDM}.VT_t88_product(
	Prod_Id						,-- 1	产品编号
	Src_Sys_Prod_Id				,-- 2	源系统产品编号
	Prod_Nm						,-- 3	产品名称
	Valid_Ind					,-- 4	有效标志
	Inout_Bal_Ind				,-- 5	表内外代码
	Auto_Ent_Acct_Ind			,-- 6	自动上账标志
	Is_Net_Loan_Biz				,-- 7	网贷业务标志
	Online_Offline_Prod_Ind_Cd	,-- 8	线上线下产品分类代码
	Setup_Dt					,-- 9	创建日期
	Upd_Dt						,-- 10	更新日期
	Rgst_Org					,-- 11	登记机构编号
	Tx_Site_Cd					,-- 12	交易所代码
	Int_Base_Cd					,-- 13	计息基准代码
	Int_Prd						,-- 14	计息周期
	Int_Pay_Prd					,-- 15	付息周期
	Int_Rate_Cate_Cd			,-- 16	利率类型代码
	Prosp_Prft_Rate				,-- 17	预期收益率
	Asst_Liab_Ind				,-- 18	资产负债标志
	Int_Mod_Cd					,-- 19	计息方式代码
	Int_Pay_Mod_Cd				,-- 20	付息方式代码
	St_Int_Dt					,-- 21	起息日期
	Matr_Dt						,-- 22	到期日期
	Issu_Prc					,-- 23	发行价格
	Issu_Mod_Cd					,-- 24	发行方式代码
	Actl_Issu_Totl_Amt			,-- 25	实际发行总额
	Fin_Pers_Nm					,-- 26	融资人名称
	Fin_Trust_Corp_Nm			,-- 27	信托公司名称
	Fin_Trust_Plan_Charc_Cd		,-- 28	信托计划性质代码
	Int_Post_Mod_Cd				,-- 29	计息顺延方式代码
	Int_Pay_Post_Mod_Cd			,-- 30	付息顺延方式代码
	Matr_Post_Mod_Cd			,-- 31	到期顺延方式代码
	Bond_Int_Rate				,-- 32	债券利率
	Bond_Charc_Cd				,-- 33	债券性质代码
	Bond_Cate_Cd				,-- 34	债券类型代码
	Int_Pay_Freq_Cd				,-- 35	付息频率代码
	Input_Dt					,-- 36	入池日期
	Issu_Org_Cate_Cd			,-- 37	发行机构类型代码
	Bond_Crdt_Grade				,-- 38	债券信用评级
	Main_Crdt_Grade				,-- 39	主体信用评级
	Bond_Crdt_Grade_Org_Id		,-- 40	债券信用评级机构编号
	Main_Crdt_Grade_Org_Id		,-- 41	主体信用评级机构编号
	Trust_Bank_Id				,-- 42	托管银行编号
	Fund_Mgr_Id					,-- 43	基金经理编号
	Fund_Mgr_Nm					,-- 44	基金经理名称
	Cur_Cd						,-- 45	币种代码
	Accti_Mod_Cd				,-- 46	核算方式代码
	Issu_Qty					,-- 47	发行量
	Custodian_Nm				,-- 48	托管银行名称
	Fin_Pers_Main_Grade_Cd		,-- 50	融资人主体评级代码
	Belg_Cty_And_Zone			,-- 51	所属国家和地区
	Guar_Mod_Cd					,-- 52	担保方式代码
	Guar_Cur_Cd					,-- 53	担保币种代码
	Guar_Amt					,-- 54	担保金额
	Nonnorm_Asst_Ind			,-- 55	非标资产标志
	Chrem_Term					,-- 56	理财期限
	Coll_Start_Dt				,-- 57	募集起始日期
	Coll_End_Dt					,-- 58	募集结束日期
	Max_Coll_Amt				,-- 59	最高募集金额
	Prod_Matr_Ind				,-- 60	产品到期标志
	Brk_Ev_Income_Cls_Cd		,-- 61	保本收益类代码
	Bond_Par					,-- 62	债券面值
	Term_Corp_Cd				,-- 63	期限单位代码
	Issur						,-- 64	发行人编号
	Issu_Dt						,-- 65	发行日期
	Tm_Dpst_Cls_Cd				,-- 66	定期存款分类代码
	Efft_Dt						,-- 67	生效日期
	Invld_Dt					,-- 68	失效日期
	Dpst_Amt					,-- 69	起存金额
	Rnw_Freq					,-- 70	转存频率
	Rnw_Ind						,-- 71	转存标志
	Cross_Dep_Ind				,-- 72	通存标志
	Unexp_Draw_Ind				,-- 73	提前支取标志
	Unexp_Draw_Cnt				,-- 74	提前支取次数
	Prod_Cate_Cd				,-- 75	产品类型代码
	PLN_Mgmt_Cd					,-- 76	贷后管理代码
	Is_Can_Cir					,-- 77	可循环标志
	Insu_Corp_Prod_Cd			,-- 78	保险公司产品代码
	Hesit_Term_Days				,-- 79	犹豫期天数
	Int_Stl_Freq_Cd				,-- 80	结息频率代码
	Data_Src_Cd					,-- 81	数据来源系统代码
	Main_Src_Task				,-- 82	主要源系统任务
	Statt_Dt					 -- 83	统计日期
	)
SELECT  a.Prod_ID				,-- 1 产品编号
		a.Src_Sys_Prod_ID		,-- 2 源系统产品编号
		a.Prod_Nm				,-- 3 产品名称
		a.TX_Cur				,-- 45 交易币种
		a.Contr_Start_Dt		,-- 67 合约开始日期
		a.Contr_End_Dt			,-- 68 合约结束日期
		a.Prod_Cate				,-- 75 产品类型
		a.Insu_Corp_Prod_Cd		,-- 78 保险公司产品代码
		a.Hesit_Term_Days		,-- 79 犹豫期天数
		''T02_YBT_PROD_H''		,-- 82主要源系统任务
		${TX_DATE} 				 -- 83 统计日期
 FROM ${AUTO_PDM}.t02_ybt_prod_h A WHERE start_dt <= ${TX_DATE} and end_dt >= ${TX_DATE}
'; 
*/
	
		
	-- 重跑删数
	SET @SQL_STR = 'DELETE FROM ${AUTO_PDM}.t88_product WHERE Statt_Dt = ${TX_DATE}';

	CALL etl.pr_exec_sql(@RTC, '', ETL_T_TAB_ENG_NAME, ETL_STEP_NO, @SQL_STR, ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
			
	SET @SQL_STR = 'INSERT INTO ${AUTO_PDM}.t88_product SELECT * FROM ${AUTO_PDM}.VT_t88_product';

	CALL etl.pr_exec_sql(@RTC, '', ETL_T_TAB_ENG_NAME, ETL_STEP_NO, @SQL_STR, ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	SET OUT_RES_MSG = '0';

END |