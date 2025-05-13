DELIMITER |

CREATE DEFINER="cq_sjpt"@"%" PROCEDURE "pr_f5_t88_cap_bond_invst_biz_info"(
	OUT	OUT_RES_MSG		VARCHAR(200),
	IN	IN_TX_DATE		VARCHAR(8)
	)
LABLE:BEGIN
/**********************************
 * LJZ 20210726 新建
 * WHD 20220701 增加应收利息、利息调整、公允价值变动相关科目和余额信息
 * 债券投资业务信息
 *********************************/
	
	DECLARE ETL_T_TAB_ENG_NAME 		VARCHAR(100)	DEFAULT 't88_cap_bond_invst_biz_info';
	DECLARE ETL_STEP_NO				INTEGER			DEFAULT 1;
	DECLARE ETL_TX_DATE 			VARCHAR(8)		DEFAULT IN_TX_DATE;
	SET OUT_RES_MSG = 'FAILED';
	
	
	/*支持数据重跑*/
	SET @SQL_STR = 'DELETE FROM PDM.'||ETL_T_TAB_ENG_NAME||' WHERE STATT_DT >= ${TX_DATE}';
	CALL ETL.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	/*创建临时表*/
	SET @SQL_STR = 'DROP TEMPORARY TABLE IF EXISTS VT_'||ETL_T_TAB_ENG_NAME;
	CALL ETL.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	SET @SQL_STR = 'CREATE TEMPORARY TABLE ETL.VT_'||ETL_T_TAB_ENG_NAME||' LIKE PDM.'||ETL_T_TAB_ENG_NAME;
	CALL ETL.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	/*获取昨日数据计算累计VT_PRE_*/
	SET @SQL_STR = 'DROP TEMPORARY TABLE IF EXISTS VT_PRE_'||ETL_T_TAB_ENG_NAME;
	CALL ETL.PR_EXEC_SQL(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SET @SQL_STR = 'CREATE TEMPORARY TABLE VT_PRE_'||ETL_T_TAB_ENG_NAME||' AS SELECT * FROM PDM.'||ETL_T_TAB_ENG_NAME||' WHERE STATT_DT = ${LAST_TX_DATE}';
	CALL ETL.PR_EXEC_SQL(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	/*数据首先插入临时表VT_*/

	-- 第一组：资金系统-人民币债券 ${NULL_STR}
	SET @SQL_STR = '
	INSERT INTO VT_t88_cap_bond_invst_biz_info(
		STATT_DT              , -- 1统计日期 
		AGMT_ID               , -- 2协议编号
		ORG_ID                , -- 3机构编号
		ACCTI_ORG_ID          , -- 4核算机构编号
		AGENT_ORG_ID          , -- 5代理机构编号
		CUR_CD                , -- 6币种代码
		BOND_ID               , -- 7债券编号
		BOND_NM               , -- 8债券名称
		BOND_CATE_CD          , -- 9债券类型代码
		BOND_CATE_NM          , -- 10债券类型名称
		TX_DT                 , -- 11交易日期
		ST_INT_DT             , -- 12起息日期
		MATR_DT               , -- 13到期日期
		STL_DT                , -- 14结算日期
		PRIN_AMT              , -- 15券面金额
		PAR_INT_RATE          , -- 16票面利率
		INT_PAY_FREQ          , -- 17付息频率
		NET_PRC               , -- 18净价
		FULL_PRC              , -- 19全价
		TX_AMT                , -- 20交易金额
		REMN_MAK_POS_QTY      , -- 21剩余持仓量
		MATR_PRFT_RATE        , -- 22到期收益率
		TX_CATE_CD            , -- 23交易类型代码
		STL_MOD_CD            , -- 24结算方式代码
		PRFT_RATE_STL_MOD_CD  , -- 25收益率结算方式代码
		INVST_MAIN_CATE_CD    , -- 26投资主体类型代码
		REPAY_MOD_CD          , -- 27还本方式代码
		INT_BASE_CD           , -- 28计息基础代码
		TX_CHNL_CD            , -- 29交易渠道代码
		CUST_MGR              , -- 30客户经理
		LINE_CD               , -- 31条线代码
		INVST_CATE_CD         , -- 32投资类型代码
		VALID_IND             , -- 33有效标志
		MBANK_CAP_ACCT_NUM    , -- 34本方资金账号
		MBANK_PAY_BANK_NUM    , -- 35本方付款行行号
		MBANK_TRUST_ORG_ID    , -- 36本方托管机构编号
		CNTPTY_CAP_ACCT_NUM   , -- 37对方资金账号
		CNTPTY_PAY_BANK_NUM   , -- 38对方付款行行号
		CNTPTY_TRUST_ORG_ID   , -- 39对方托管机构编号
		EXCHG_RATE            , -- 40汇率
		PRIN_SUBJ_ID          , -- 41本金科目编号
		BOOK_BAL              , -- 42账面余额
		TO_RMB_BAL            , -- 43折人民币账面余额
		MTH_TOTAL_BAL         , -- 44月累积余额
		QUAR_TOTAL_BAL        , -- 45季累积余额
		YR_TOTAL_BAL          , -- 46年累积余额
		RMB_MTH_TOTAL_BAL     , -- 47人民币月累积余额
		RMB_QUAR_TOTAL_BAL    , -- 48人民币季累积余额
		RMB_YR_TOTAL_BAL      , -- 49人民币年累积余额
		MTH_DAVG              , -- 50月日均余额
		QUAR_DAVG             , -- 51季日均余额
		YR_DAVG               , -- 52年日均余额
		RMB_MTH_DAVG          , -- 53人民币月日均余额
		RMB_QUAR_DAVG         , -- 54人民币季日均余额
		RMB_YR_DAVG           , -- 55人民币年日均余额
		ACCRD_INT_SUBJ_ID     , -- 56应计利息科目编号
		ACCRD_INT_BAL         , -- 57应计利息余额
		Recvbl_Int_Subj_Id	  , -- 58应收利息科目编号 20220701 ADD
		Recvbl_Int_Bal		  , -- 59应收利息余额 20220701 ADD
		FVTPL_SUBJ            , -- 60公允价值变动科目
		FVTPL_AMT             , -- 61公允价值变动金额
		INT_ADJ_SUBJ_ID       , -- 62利息调整科目编号
		INT_ADJ_AMT           , -- 63利息调整金额
		COST_AMT              , -- 64成本金额
		STL_BAL                 -- 65结算金余额
	)
	SELECT 
		${TX_DATE}               						, -- 1统计日期
		T2.Bond_Agmt_Id               					, -- 2债券协议编号
		T2.Org_Id                						, -- 3机构编号
		NVL(T1.ACCTI_ORG_ID,T2.Org_Id)          		, -- 4核算机构编号
		NVL(T2.AGENT_ORG_ID,${NULL_STR})          		, -- 5代理机构编号
		NVL(T3.Stl_Cur_Cd,${NULL_STR})                	, -- 6币种代码
		NVL(T2.Bond_Id,${NULL_STR})               		, -- 7债券编号
		NVL(T3.BOND_NM,${NULL_STR})               		, -- 8债券名称
		NVL(T3.BOND_CATE_CD,${NULL_STR})          		, -- 9债券类型代码
		${NULL_STR}              						, -- 10债券类型名称
		NVL(T1.TX_DT,T2.STL_DT)                 		, -- 11交易日期
		NVL(T3.BOND_ST_INT_DT,\'00010101\')        		, -- 12债券起息日期
		NVL(T3.BOND_MATR_DT,\'99991231\')          		, -- 13债券到期日期
		NVL(T1.STL_DT,T2.STL_DT)                		, -- 14结算日期
		NVL(T1.SECURIT_FACE_TOTL_AMT,T2.Book_Val) 		, -- 15券面总额
		NVL(T2.PAR_INT_RATE,0)          				, -- 16票面利率
		NVL(T3.INT_PAY_FREQ_CD,${NULL_STR})       		, -- 17付息频率代码
		NVL(T1.NET_PRC,T2.Net_Prc)               		, -- 18净价
		NVL(T1.FULL_PRC,T2.Init_Full_Prc)              	, -- 19全价
		NVL(T1.TX_AMT,T2.STL_BAL)                		, -- 20交易金额
		NVL(T2.REMN_MAK_POS_QTY,0)      				, -- 21剩余持仓量
		NVL(T1.PRFT_RATE,0)             				, -- 22收益率
		NVL(T1.TX_CATE_CD,${NULL_STR})            		, -- 23交易类型代码
		NVL(T1.STL_MOD_CD,${NULL_STR})            		, -- 24结算方式代码
		NVL(T1.PRFT_RATE_STL_MOD_CD,${NULL_STR})  		, -- 25收益率结算方式代码
		NVL(T1.INVST_MAIN_CATE_CD,${NULL_STR})    		, -- 26投资主体类型代码
		NVL(T3.REPAY_MOD_CD,${NULL_STR})          		, -- 27还本方式代码
		NVL(T3.INT_BASE_CD,${NULL_STR})           		, -- 28计息基准代码
		NVL(T4.TX_CHNL_CD,${NULL_STR})            		, -- 29交易渠道代码
		NVL(T4.CUST_MGR_ID,${NULL_STR})           		, -- 30客户经理编号
		NVL(T4.LINE_CD,${NULL_STR})               		, -- 31条线代码
		NVL(T2.INVST_CATE_CD,${NULL_STR})         		, -- 32投资类型代码
		NVL(T1.VALID_IND,${NULL_STR})             		, -- 33有效标志
		NVL(T1.MBANK_CAP_ACCT_NUM,${NULL_STR})    		, -- 34本方资金账号
		NVL(T1.MBANK_PAY_BANK_NUM,${NULL_STR})    		, -- 35本方付款行行号
		NVL(T1.MBANK_TRUST_ORG_ID,${NULL_STR})    		, -- 36本方托管机构编号
		NVL(T1.CNTPTY_CAP_ACCT_NUM,${NULL_STR})   		, -- 37对方资金账号
		NVL(T1.CNTPTY_PAY_BANK_NUM,${NULL_STR})   		, -- 38对方付款行行号
		NVL(T1.CNTPTY_TRUST_ORG_ID,${NULL_STR})   		, -- 39对方托管机构编号
		NVL(rate.MDL_PRC,0)               				, -- 40汇率
		NVL(t5.subj_id,${NULL_STR})              		, -- 41本金科目编号
		NVL(T2.Cost_Amt,0)              				, -- 42账面余额
		NVL(T2.Cost_Amt,0)*NVL(rate.Mdl_Prc,0)          , -- 43 折人民币账面余额
		0              									, -- 44月累积余额, -- 44按当前余额累计计算
		0              									, -- 45季累积余额, -- 45按当前余额累计计算
		0              									, -- 46年累积余额, -- 46按当前余额累计计算
		0              									, -- 47人民币月累积余额, -- 47按照当前余额累计（折人民币）
		0              									, -- 48人民币季累积余额, -- 48按照当前余额累计（折人民币）
		0              									, -- 49人民币年累积余额, -- 49按照当前余额累计（折人民币）
		0              									, -- 50月日均余额, -- 50按当前余额累计计算
		0              									, -- 51季日均余额, -- 51按当前余额累计计算
		0              									, -- 52年日均余额, -- 52按当前余额累计计算
		0              									, -- 53人民币月日均余额
		0              									, -- 54人民币季日均余额
		0              									, -- 55人民币年日均余额
		${NULL_STR}              						, -- 56应计利息科目编号
		0    											, -- 57应计利息余额 
		NVL(T6.subj_id, ${NULL_STR})	  				, -- 58应收利息科目编号 20220701 ADD
		NVL(t2.Recvbl_Int_Bal, 0)		  				, -- 59应收利息余额 	20220701 ADD
		COALESCE(T7.subj_id,T7_1.subj_id,${NULL_STR})   , -- 58公允价值变动科目	20220701 MODIF
		NVL(T2.Fvtpl_Bal,0)             				, -- 59公允价值变动余额	20220701 MODIF
		COALESCE(T8.subj_id,T8_1.subj_id,${NULL_STR})   , -- 60利息调整科目编号 20220701 MODIF
		NVL(T2.Int_Adj_Bal,0)          	 				, -- 61利息调整余额		20220701 MODIF
		NVL(T2.COST_AMT,0)              				, -- 62成本金额
		NVL(T2.STL_BAL,0)                				  -- 63结算金余额
	FROM PDM.T03_CAP_BOND_INVST T1 -- 资金债券投资

	RIGHT JOIN PDM.T05_BOND_INVST_EVT T2 -- 债券投资事件
		ON T1.AGMT_ID	= T2.Bond_Agmt_Id
		AND T1.STATT_DT = ${TX_DATE}
		
	INNER JOIN PDM.T02_CAP_BOND_H T3 -- 资金债券（产品）
		ON ${TX_DATE} BETWEEN T3.START_DT AND T3.END_DT
		AND \'FDS\'||T2.BOND_ID = T3.PROD_ID
		AND T3.DATA_VALID_IND = \'E\'
		-- AND ${TX_DATE} BETWEEN T3.BOND_ST_INT_DT AND T3.BOND_MATR_DT
		
	LEFT JOIN (SELECT distinct AGMT_ID,TX_CHNL_CD,CUST_MGR_ID,LINE_CD 
		FROM PDM.T03_CAP_ACCT_INFO_H  -- 资金账务信息
		WHERE ${TX_DATE} BETWEEN START_DT AND END_DT) t4
		ON T2.Bond_Agmt_Id=T4.AGMT_ID
		
	LEFT JOIN PDM.T03_CAP_SUBJ_COMP_H T5 -- 资金科目对照 本金科目
		ON ${TX_DATE} BETWEEN T5.START_DT AND T5.END_DT
		AND T2.Bond_Agmt_Id 	= T5.AGMT_ID
		AND T5.AMT_CATE_CD 		= \'ZJJE0001\'
		and T5.Subj_Id!=\'88888888\'
		AND T2.Invst_Cate_Cd 	= T5.MEASR_MOD_CD 
		
	LEFT JOIN PDM.T03_CAP_SUBJ_COMP_H T6 -- 资金科目对照 应收利息科目
		ON ${TX_DATE} BETWEEN T6.START_DT AND T6.END_DT
		AND T2.Bond_Agmt_Id 	= T6.AGMT_ID
		AND T6.AMT_CATE_CD 		= \'ZJJE0002\' -- 应收利息
	    AND T2.Invst_Cate_Cd 	= T6.MEASR_MOD_CD   
	
	LEFT JOIN PDM.T03_CAP_SUBJ_COMP_H T7 -- 资金科目对照 公允价值变动科目
		ON ${TX_DATE} BETWEEN T7.START_DT AND T7.END_DT
		AND T2.Bond_Agmt_Id		= T7.AGMT_ID
		AND T7.AMT_CATE_CD		= \'ZJJE0016\' -- 公允价值变动-借
	    AND T2.Invst_Cate_Cd 	= T7.MEASR_MOD_CD  

	LEFT JOIN PDM.T03_CAP_SUBJ_COMP_H T7_1 -- 资金科目对照 公允价值变动科目
		ON ${TX_DATE} BETWEEN T7_1.START_DT AND T7_1.END_DT
		AND T2.Bond_Agmt_Id 	= T7_1.AGMT_ID
		AND T7_1.AMT_CATE_CD 	= \'ZJJE0017\' -- 公允价值变动-贷
	    AND T2.Invst_Cate_Cd 	= T7_1.MEASR_MOD_CD  
	
	LEFT JOIN PDM.T03_CAP_SUBJ_COMP_H T8 -- 资金科目对照 利息调整科目
		ON ${TX_DATE} BETWEEN T8.START_DT AND T8.END_DT
		AND T2.Bond_Agmt_Id		= T8.AGMT_ID
		AND T8.AMT_CATE_CD		= \'ZJJE0022\' -- 利息调整-借
	    AND T2.Invst_Cate_Cd 	= T8.MEASR_MOD_CD  

	LEFT JOIN PDM.T03_CAP_SUBJ_COMP_H T8_1 -- 资金科目对照 利息调整科目
		ON ${TX_DATE} BETWEEN T8_1.START_DT AND T8_1.END_DT
		AND T2.Bond_Agmt_Id 	= T8_1.AGMT_ID
		AND T8_1.AMT_CATE_CD 	= \'ZJJE0023\' -- 利息调整-贷
	    AND T2.Invst_Cate_Cd 	= T8_1.MEASR_MOD_CD   	
		
	LEFT JOIN PDM.t88_exchg_rate rate 
		ON T3.Stl_Cur_Cd = rate.Init_Cur 
		AND rate.STATT_DT = ${TX_DATE}
	WHERE T2.STATT_DT = ${TX_DATE}
		AND T2.COST_AMT>0
	';
	CALL ETL.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	
	-- 第二组：资管债券
	-- 已作废 保本理财业务已停止 
	/*SET @SQL_STR = '
	insert into VT_t88_cap_bond_invst_biz_info(	
	Statt_Dt	,-- 	1	统计日期
	Agmt_ID	,-- 	2	协议编号
	Org_ID	,-- 	3	机构编号
	Accti_Org_ID	,-- 	4	核算机构编号
	Agent_Org_ID	,-- 	5	代理机构编号
	Cur_Cd	,-- 	6	币种代码
	Bond_ID	,-- 	7	债券编号
	Bond_Nm	,-- 	8	债券名称
	Bond_Cate_Cd	,-- 	9	债券类型代码
	Bond_Cate_Nm	,-- 	10	债券类型名称
	TX_Dt	,-- 	11	交易日期
	St_Int_Dt	,-- 	12	起息日期
	Matr_Dt	,-- 	13	到期日期
	Stl_Dt	,-- 	14	结算日期
	Prin_Amt	,-- 	15	券面金额
	Par_Int_Rate	,-- 	16	票面利率
	Int_Pay_Freq	,-- 	17	付息频率
	Net_Prc	,-- 	18	净价
	Full_Prc	,-- 	19	全价
	TX_Amt	,-- 	20	交易金额
	Remn_Mak_Pos_Qty	,-- 	21	剩余持仓量
	Matr_Prft_Rate	,-- 	22	到期收益率
	TX_Cate_Cd	,-- 	23	交易类型代码
	Stl_Mod_Cd	,-- 	24	结算方式代码
	Prft_Rate_Stl_Mod_Cd	,-- 	25	收益率结算方式代码
	Invst_Main_Cate_Cd	,-- 	26	投资主体类型代码
	Repay_Mod_Cd	,-- 	27	还本方式代码
	Int_Base_Cd	,-- 	28	计息基础代码
	TX_Chnl_Cd	,-- 	29	交易渠道代码
	Cust_Mgr	,-- 	30	客户经理
	line_Cd	,-- 	31	条线代码
	Invst_Cate_Cd	,-- 	32	投资类型代码
	Valid_Ind	,-- 	33	有效标志
	Mbank_Cap_Acct_Num	,-- 	34	本方资金账号
	Mbank_Pay_Bank_Num	,-- 	35	本方付款行行号
	Mbank_Trust_Org_ID	,-- 	36	本方托管机构编号
	CntPty_Cap_Acct_Num	,-- 	37	对方资金账号
	CntPty_Pay_Bank_Num	,-- 	38	对方付款行行号
	CntPty_Trust_Org_ID	,-- 	39	对方托管机构编号
	Exchg_Rate	,-- 	40	汇率
	Prin_Subj_ID	,-- 	41	本金科目编号
	Book_Bal	,-- 	42	账面余额
	To_Rmb_Bal	,-- 	43	折人民币账面余额
	Mth_Total_Bal	,-- 	44	月累积余额
	Quar_Total_Bal	,-- 	45	季累积余额
	Yr_Total_Bal	,-- 	46	年累积余额
	Rmb_Mth_Total_Bal	,-- 	47	人民币月累积余额
	Rmb_Quar_Total_Bal	,-- 	48	人民币季累积余额
	Rmb_Yr_Total_Bal	,-- 	49	人民币年累积余额
	Mth_DAvg	,-- 	50	月日均余额
	Quar_DAvg	,-- 	51	季日均余额
	Yr_DAvg	,-- 	52	年日均余额
	Rmb_Mth_DAvg	,-- 	53	人民币月日均余额
	Rmb_Quar_DAvg	,-- 	54	人民币季日均余额
	Rmb_Yr_DAvg	,-- 	55	人民币年日均余额
	Accrd_Int_Subj_ID	,-- 	56	应计利息科目编号
	Accrd_Int_Bal	,-- 	57	应计利息余额
	FVTPL_Subj	,-- 	58	公允价值变动科目
	FVTPL_Amt	,-- 	59	公允价值变动金额
	Int_Adj_Subj_ID	,-- 	60	利息调整科目编号
	Int_Adj_Amt	,-- 	61	利息调整金额
	Cost_Amt	,-- 	62	成本金额
	Stl_Bal	-- 	63	结算金余额
)
	select
		${TX_DATE}	,-- 	1	统计日期
		\'ZGS\'||t.Src_Sys_Invst_Seq_Num	,-- 	2	标的代码 标的类型:2-债券
		${NULL_STR}	,-- 	3	机构编号
		${NULL_STR}	,-- 	4	核算机构编号
		${NULL_STR}	,-- 	5	代理机构编号
		t.Cur_Cd	,-- 	6	币种代码
		t.Subj_Matr_Cd	,-- 	7	标的代码
		t.Subj_Matr_Nm	,-- 	8	标的名称
		t.Subj_Matr_Cate_Cd	,-- 	9	标的类型代码
		${NULL_STR}	,-- 	10	债券类型名称
		t.Tx_Dt	,-- 	11	交易日期
		t.St_Int_Dt	,-- 	12	起息日期 数据有效标识 :E
		t.Matr_Dt	,-- 	13	到期日期
		\'00010101\'	,-- 	14	结算日期
		0	,-- 	15	券面金额
		t.Bond_Par_Int_Rate	,-- 	16	债券票面利率
		0	,-- 	17	付息频率
		t.Net_Prc	,-- 	18	净价
		0	,-- 	19	全价
		t.Biz_Amt	,-- 	20	买卖金额
		0	,-- 	21	剩余持仓量
		0	,-- 	22	到期收益率
		t.Tx_Cate_Cd	,-- 	23	交易类型代码
		t.Int_Stl_Mod_Cd	,-- 	24	结息方式代码
		${NULL_STR}	,-- 	25	收益率结算方式代码
		${NULL_STR}	,-- 	26	投资主体类型代码
		${NULL_STR}	,-- 	27	还本方式代码	
		t.Int_Base_Cd	,-- 	28	计息基准代码
		${NULL_STR}	,-- 	29	交易渠道代码
		${NULL_STR}	,-- 	30	客户经理
		${NULL_STR}	,-- 	31	条线代码
		t.Invst_Cate_Cd	,-- 	32	投资类型代码
		t.Flow_Stat_Cd	,-- 	33	流水状态代码
		${NULL_STR}	,-- 	34	本方资金账号
		${NULL_STR}	,-- 	35	本方付款行行号
		${NULL_STR}	,-- 	36	本方托管机构编号
		t.Tx_Cntpty_Id	,-- 	37	交易对手编号
		${NULL_STR}	,-- 	38	对方付款行行号
		${NULL_STR}	,-- 	39	对方托管机构编号
		t1.Mdl_Prc	,-- 	40	汇率
		${NULL_STR}	,-- 	41	本金科目编号
		t.Cost_Amt	,-- 	42	成本金额
		T.Cost_Amt*T1.Mdl_Prc	,-- 	43	折人民币账面余额
		0	,-- 	44	月累积余额
		0	,-- 	45	季累积余额
		0	,-- 	46	年累积余额
		0	,-- 	47	人民币月累积余额
		0	,-- 	48	人民币季累积余额
		0	,-- 	49	人民币年累积余额
		0	,-- 	50	月日均余额
		0	,-- 	51	季日均余额
		0	,-- 	52	年日均余额
		0	,-- 	53	人民币月日均余额
		0	,-- 	54	人民币季日均余额
		0	,-- 	55	人民币年日均余额
		${NULL_STR}	,-- 	56	应计利息科目编号
		t.Accm_Accrd_Int_Amt	,-- 	57	累计应计利息金额
		${NULL_STR}	,-- 	58	公允价值变动科目
		0	,-- 	59	公允价值变动金额
		${NULL_STR}	,-- 	60	利息调整科目编号
		0	,-- 	61	利息调整金额	
		0	,-- 	62	成本金额
		0	-- 	63	结算金余额
	from pdm.t05_chrem_prod_invst_evt t 
	left join pdm.t88_exchg_rate t1
		on t.Cur_Cd = t1.Init_Cur 
		and t1.Statt_Dt = ${TX_DATE}
	where t.tx_dt = ${TX_DATE}
	';
	-- CALL ETL.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	-- IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	-- SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	*/
	
	-- 第三组：OPICS系统-外币债券
	/* SET @SQL_STR = '
	insert into VT_t88_cap_bond_invst_biz_info(
	Statt_Dt              , -- 1统计日期 
	Agmt_ID               , -- 2协议编号
	Org_ID                , -- 3机构编号
	Accti_Org_ID          , -- 4核算机构编号
	Agent_Org_ID          , -- 5代理机构编号
	Cur_Cd                , -- 6币种代码
	Bond_ID               , -- 7债券编号
	Bond_Nm               , -- 8债券名称
	Bond_Cate_Cd          , -- 9债券类型代码
	Bond_Cate_Nm          , -- 10债券类型名称
	TX_Dt                 , -- 11交易日期
	St_Int_Dt             , -- 12起息日期
	Matr_Dt               , -- 13到期日期
	Stl_Dt                , -- 14结算日期
	Prin_Amt              , -- 15券面金额
	Par_Int_Rate          , -- 16票面利率
	Int_Pay_Freq          , -- 17付息频率
	Net_Prc               , -- 18净价
	Full_Prc              , -- 19全价
	TX_Amt                , -- 20交易金额
	Remn_Mak_Pos_Qty      , -- 21剩余持仓量
	Matr_Prft_Rate        , -- 22到期收益率
	TX_Cate_Cd            , -- 23交易类型代码
	Stl_Mod_Cd            , -- 24结算方式代码
	Prft_Rate_Stl_Mod_Cd  , -- 25收益率结算方式代码
	Invst_Main_Cate_Cd    , -- 26投资主体类型代码
	Repay_Mod_Cd          , -- 27还本方式代码
	Int_Base_Cd           , -- 28计息基础代码
	TX_Chnl_Cd            , -- 29交易渠道代码
	Cust_Mgr              , -- 30客户经理
	line_Cd               , -- 31条线代码
	Invst_Cate_Cd         , -- 32投资类型代码
	Valid_Ind             , -- 33有效标志
	Mbank_Cap_Acct_Num    , -- 34本方资金账号
	Mbank_Pay_Bank_Num    , -- 35本方付款行行号
	Mbank_Trust_Org_ID    , -- 36本方托管机构编号
	CntPty_Cap_Acct_Num   , -- 37对方资金账号
	CntPty_Pay_Bank_Num   , -- 38对方付款行行号
	CntPty_Trust_Org_ID   , -- 39对方托管机构编号
	Exchg_Rate            , -- 40汇率
	Prin_Subj_ID          , -- 41本金科目编号
	Book_Bal              , -- 42账面余额
	To_Rmb_Bal            , -- 43折人民币账面余额
	Mth_Total_Bal         , -- 44月累积余额
	Quar_Total_Bal        , -- 45季累积余额
	Yr_Total_Bal          , -- 46年累积余额
	Rmb_Mth_Total_Bal     , -- 47人民币月累积余额
	Rmb_Quar_Total_Bal    , -- 48人民币季累积余额
	Rmb_Yr_Total_Bal      , -- 49人民币年累积余额
	Mth_DAvg              , -- 50月日均余额
	Quar_DAvg             , -- 51季日均余额
	Yr_DAvg               , -- 52年日均余额
	Rmb_Mth_DAvg          , -- 53人民币月日均余额
	Rmb_Quar_DAvg         , -- 54人民币季日均余额
	Rmb_Yr_DAvg           , -- 55人民币年日均余额
	Accrd_Int_Subj_ID     , -- 56应计利息科目编号
	Accrd_Int_Bal         , -- 57应计利息余额
	Recvbl_Int_Subj_Id	  , -- 58应收利息科目编号 20220701 ADD
	Recvbl_Int_Bal		  , -- 59应收利息余额 	  20220701 ADD
	FVTPL_Subj            , -- 60公允价值变动科目
	FVTPL_Amt             , -- 61公允价值变动金额
	Int_Adj_Subj_ID       , -- 62利息调整科目编号
	Int_Adj_Amt           , -- 63利息调整金额
	Cost_Amt              , -- 64成本金额
	Stl_Bal                 -- 65结算金余额
)
	SELECT 
		${TX_DATE}                    			, -- 1统计日期
		T1.Agmt_Id                  			, -- 2协议编号
		${NULL_STR}								, -- 3机构编号
		${NULL_STR}								, -- 4核算机构编号
		${NULL_STR}								, -- 5代理机构编号
		T1.Cur_Cd                   			, -- 6币种代码
		T1.Bond_Id                  			, -- 7债券编号
		NVL(T2.Bond_Nm,${NULL_STR})             , -- 8债券名称
		NVL(T2.Bond_Cate_Cd,${NULL_STR})        , -- 9债券类型代码
		${NULL_STR}								, -- 10债券类型名称
		T1.Tx_Dt                    			, -- 11交易日期
		NVL(T2.St_Int_Dt,\'00010101\')          , -- 12起息日期
		NVL(T2.Matr_Dt,\'99991231\')            , -- 13到期日期
		T1.Stl_Dt                   			, -- 14结算日期
		T1.Securit_Face_Totl_Amt    			, -- 15券面总额
		NVL(T2.Par_Int_Rate,0)      			, -- 16票面利率
		0										, -- 17付息频率
		0										, -- 18净价
		T1.Full_Prc                 			, -- 19全价
		T1.Cost_Amt                 			, -- 20成本金额
		0										, -- 21剩余持仓量
		T1.Prft_Rate                			, -- 22收益率
		${NULL_STR}								, -- 23交易类型代码
		T1.Cur_Stl_Mod_Cd           			, -- 24货币结算方式代码
		${NULL_STR}								, -- 25收益率结算方式代码
		${NULL_STR}								, -- 26投资主体类型代码
		${NULL_STR}								, -- 27还本方式代码
		NVL(T2.Int_Base_Cd,${NULL_STR})         , -- 28计息基准代码
		${NULL_STR}								, -- 29交易渠道代码
		${NULL_STR}								, -- 30客户经理
		${NULL_STR}								, -- 31条线代码
		T1.Invst_Cate               			, -- 32投资类型
		${NULL_STR}								, -- 33有效标志
		T1.Stl_Acct_Num             			, -- 34结算账号
		${NULL_STR}								, -- 35本方付款行行号
		${NULL_STR}								, -- 36本方托管机构编号
		T1.Tx_Cntpty_Id             			, -- 37交易对手编号
		${NULL_STR}								, -- 38对方付款行行号
		${NULL_STR}								, -- 39对方托管机构编号
		NVL(T4.Mdl_Prc,0)                  		, -- 40中间价
		NVL(T3.CORE_SUBJ_ID,\'15030101\')       , -- 41科目编号
		DECODE(T1.TX_DRCT_CD,\'P\',-T1.PRIN_AMT,\'S\',T1.PRIN_AMT,0)                 , -- 42成本金额
		DECODE(T1.TX_DRCT_CD,\'P\',-T1.PRIN_AMT,\'S\',T1.PRIN_AMT,0)*T4.MDL_PRC		 , -- 43折人民币账面余额
		0										,-- 44月累积余额
		0										,-- 45季累积余额
		0										,-- 46年累积余额
		0										,-- 47人民币月累积余额 -- 47按照当前余额累计（折人民币）
		0										,-- 48人民币季累积余额 -- 48按照当前余额累计（折人民币）
		0										,-- 49人民币年累积余额 -- 49按照当前余额累计（折人民币）
		0										,-- 50月日均余额
		0										,-- 51季日均余额
		0										,-- 52年日均余额
		0										,-- 53人民币月日均余额 
		0										,-- 54人民币季日均余额 
		0										,-- 55人民币年日均余额 
		NVL(T3.Int_Subj_Id,${NULL_STR})         ,-- 56利息科目编号
		0										,-- 57应计利息余额
		${NULL_STR}								,-- 58公允价值变动科目
		0										,-- 59公允价值变动金额
		${NULL_STR}								,-- 60利息调整科目编号
		0										,-- 61利息调整金额
		T1.Cost_Amt                 			,-- 62成本金额
		T1.Stl_Amt                   			 -- 63结算金额
	FROM PDM.t03_fx_bond_invst	T1 -- 外汇债券投资
	
	INNER JOIN PDM.t02_fx_bond_h	T2 -- 外汇债券（产品）
		ON \'OPI\'||T1.Bond_Id=T2.Prod_Id
		and ${TX_DATE} between t2.Issu_Dt and t2.Matr_Dt -- 筛掉过期债券
		and ${TX_DATE} between t2.Start_Dt and t2.End_Dt
		
	LEFT JOIN (select *,row_number() over(partition by h.Fx_Agmt_Id, h.Src_Sys_Proc_Num order by Src_Sys_Proc_Ordr_Num) as SEQ
			from PDM.t05_fx_acct_evt h
			where h.tx_dt = ${TX_DATE})	T3 -- 外汇账务事件
		ON T1.Agmt_Id=T3.Fx_Agmt_Id -- dealno
		and abs(T1.Prin_Amt)=abs(T3.Amt)
		and seq=1
		and t3.Core_Subj_Id like \'1503%\'
		
	LEFT JOIN PDM.t88_exchg_rate	T4 -- 汇率牌价表
		ON T1.Cur_Cd = T4.Init_Cur
		and t4.Statt_Dt = ${TX_DATE}
		
	where t1.STATT_DT = ${TX_DATE}
        and t1.Invst_Cate=\'A\'
		and t1.Recall_Dt = \'00010101\' -- 撤销日期为空
	';
	*/
	
	
	SET @SQL_STR = '
	insert into VT_t88_cap_bond_invst_biz_info(
	Statt_Dt              , -- 1统计日期 
	Agmt_ID               , -- 2协议编号
	Org_ID                , -- 3机构编号
	Accti_Org_ID          , -- 4核算机构编号
	Agent_Org_ID          , -- 5代理机构编号
	Cur_Cd                , -- 6币种代码
	Bond_ID               , -- 7债券编号
	Bond_Nm               , -- 8债券名称
	Bond_Cate_Cd          , -- 9债券类型代码
	Bond_Cate_Nm          , -- 10债券类型名称
	TX_Dt                 , -- 11交易日期
	St_Int_Dt             , -- 12起息日期
	Matr_Dt               , -- 13到期日期
	Stl_Dt                , -- 14结算日期
	Prin_Amt              , -- 15券面金额
	Par_Int_Rate          , -- 16票面利率
	Int_Pay_Freq          , -- 17付息频率
	Net_Prc               , -- 18净价
	Full_Prc              , -- 19全价
	TX_Amt                , -- 20交易金额
	Remn_Mak_Pos_Qty      , -- 21剩余持仓量
	Matr_Prft_Rate        , -- 22到期收益率
	TX_Cate_Cd            , -- 23交易类型代码
	Stl_Mod_Cd            , -- 24结算方式代码
	Prft_Rate_Stl_Mod_Cd  , -- 25收益率结算方式代码
	Invst_Main_Cate_Cd    , -- 26投资主体类型代码
	Repay_Mod_Cd          , -- 27还本方式代码
	Int_Base_Cd           , -- 28计息基础代码
	TX_Chnl_Cd            , -- 29交易渠道代码
	Cust_Mgr              , -- 30客户经理
	line_Cd               , -- 31条线代码
	Invst_Cate_Cd         , -- 32投资类型代码
	Valid_Ind             , -- 33有效标志
	Mbank_Cap_Acct_Num    , -- 34本方资金账号
	Mbank_Pay_Bank_Num    , -- 35本方付款行行号
	Mbank_Trust_Org_ID    , -- 36本方托管机构编号
	CntPty_Cap_Acct_Num   , -- 37对方资金账号
	CntPty_Pay_Bank_Num   , -- 38对方付款行行号
	CntPty_Trust_Org_ID   , -- 39对方托管机构编号
	Exchg_Rate            , -- 40汇率
	Prin_Subj_ID          , -- 41本金科目编号
	Book_Bal              , -- 42账面余额
	To_Rmb_Bal            , -- 43折人民币账面余额
	Mth_Total_Bal         , -- 44月累积余额
	Quar_Total_Bal        , -- 45季累积余额
	Yr_Total_Bal          , -- 46年累积余额
	Rmb_Mth_Total_Bal     , -- 47人民币月累积余额
	Rmb_Quar_Total_Bal    , -- 48人民币季累积余额
	Rmb_Yr_Total_Bal      , -- 49人民币年累积余额
	Mth_DAvg              , -- 50月日均余额
	Quar_DAvg             , -- 51季日均余额
	Yr_DAvg               , -- 52年日均余额
	Rmb_Mth_DAvg          , -- 53人民币月日均余额
	Rmb_Quar_DAvg         , -- 54人民币季日均余额
	Rmb_Yr_DAvg           , -- 55人民币年日均余额
	Accrd_Int_Subj_ID     , -- 56应计利息科目编号
	Accrd_Int_Bal         , -- 57应计利息余额
	Recvbl_Int_Subj_Id	  , -- 58应收利息科目编号 20220701 ADD
	Recvbl_Int_Bal		  , -- 59应收利息余额 	  20220701 ADD
	FVTPL_Subj            , -- 60公允价值变动科目
	FVTPL_Amt             , -- 61公允价值变动金额
	Int_Adj_Subj_ID       , -- 62利息调整科目编号
	Int_Adj_Amt           , -- 63利息调整金额
	Cost_Amt              , -- 64成本金额
	Stl_Bal                 -- 65结算金余额
)
	SELECT 
		${TX_DATE}                    		, -- 1统计日期
		T5.Bond_Id || T5.Invst_Comb_Cd || T5.Cost_Ctr_Cd|| T5.Invst_Cate_Cd , -- 2协议编号
		NVL(T2_SUBJT.Acct_Num, \'6001\')		, -- 3机构编号
		${NULL_STR}							, -- 4核算机构编号
		${NULL_STR}							, -- 5代理机构编号
		T5.Cur_Cd                   		, -- 6币种代码
		T5.Bond_Id                  		, -- 7债券编号
		NVL(T2.Bond_Nm,\'\')             	, -- 8债券名称
		NVL(T2.Bond_Cate_Cd,\'\')        	, -- 9债券类型代码
		${NULL_STR}							, -- 10债券类型名称
		T1.Tx_Dt                    		, -- 11交易日期
		NVL(T2.St_Int_Dt,\'00010101\')        , -- 12起息日期
		NVL(T2.Matr_Dt,\'99991231\')          , -- 13到期日期
		\'0001-01-01\',-- T1.Stl_Dt           , -- 14结算日期
		0,-- T1.Securit_Face_Totl_Amt    		, -- 15券面总额
		NVL(T2.Par_Int_Rate,0)      		, -- 16票面利率
		0									, -- 17付息频率
		0									, -- 18净价
		0,-- T1.Full_Prc                 		, -- 19全价
		0,-- T1.Cost_Amt                 		, -- 20成本金额
		t5.Qty								, -- 21剩余持仓量
		T5.Actl_Int_Rate                	, -- 22收益率
		${NULL_STR}							, -- 23交易类型代码
		0,-- T1.Cur_Stl_Mod_Cd           		, -- 24货币结算方式代码
		${NULL_STR}							, -- 25收益率结算方式代码
		${NULL_STR}							, -- 26投资主体类型代码
		${NULL_STR}							, -- 27还本方式代码
		NVL(T2.Int_Base_Cd,${NULL_STR})     , -- 28计息基准代码
		${NULL_STR}							, -- 29交易渠道代码
		${NULL_STR}							, -- 30客户经理
		${NULL_STR}							, -- 31条线代码
		T5.Invst_Cate_Cd               		, -- 32投资类型 H-以摊余成本计量(AC)  A-以公允价值计量且变动计入其他综合收益(FVTOCI)  T-以公允价值计量且其变动计入当期损益(FVTPL)
		${NULL_STR}							, -- 33有效标志
		0,-- T1.Stl_Acct_Num             		, -- 34结算账号
		${NULL_STR}							, -- 35本方付款行行号
		${NULL_STR}							, -- 36本方托管机构编号
		0,-- T1.Tx_Cntpty_Id             		, -- 37交易对手编号
		${NULL_STR}							, -- 38对方付款行行号
		${NULL_STR}							, -- 39对方托管机构编号
		NVL(T4.Mdl_Prc,0)                  	, -- 40中间价
		NVL(T2_SUBJT.Int_Subj_Id,\'\')		, -- 41科目编号
		-- DECODE(T1.TX_DRCT_CD,\'P\',-T1.PRIN_AMT,\'S\',T1.PRIN_AMT,0)                 , -- 42成本金额
		-NVL(T5.Par_Amt,0)  				, -- 42账面余额
		-- DECODE(T1.TX_DRCT_CD,\'P\',-T1.PRIN_AMT,\'S\',T1.PRIN_AMT,0)*T4.MDL_PRC		, -- 43折人民币账面余额
		NVL(T5.Par_Amt,0) * T4.MDL_PRC		, -- 43折人民币账面余额
		0										,-- 44月累积余额
		0										,-- 45季累积余额
		0										,-- 46年累积余额
		0										,-- 47人民币月累积余额 -- 47按照当前余额累计（折人民币）
		0										,-- 48人民币季累积余额 -- 48按照当前余额累计（折人民币）
		0										,-- 49人民币年累积余额 -- 49按照当前余额累计（折人民币）
		0										,-- 50月日均余额
		0										,-- 51季日均余额
		0										,-- 52年日均余额
		0										,-- 53人民币月日均余额 
		0										,-- 54人民币季日均余额 
		0										,-- 55人民币年日均余额 
		${NULL_STR}      						,-- 56应计利息科目编号
		0										,-- 57应计利息余额
		NVL(T2_ACCRUAL.Int_Subj_Id,\'\')		,-- 58应收利息科目编号
		-NVL(T5.Paybl_Unpay_Int_Amt,0)		  	,-- 59应收利息余额
		NVL(T2_FAIR.Int_Subj_Id,\'\')			,-- 60公允价值变动科目
		NVL(T5.Today_Mkt_Val_Amt,0)				,-- 61公允价值变动金额
		NVL(T2_INT.Int_Subj_Id,\'\')			,-- 62利息调整科目编号
		NVL(T5.Un_Amort_Amt,0)					,-- 63利息调整金额
		0,-- T1.Cost_Amt                 		,-- 64成本金额
		0-- T1.Stl_Amt                   		 -- 65结算金额
	FROM (SELECT 
			T.Bond_Id,
			T.Invst_Comb,
			T.Invst_Cate,
			T.Cost_Ctr_Cd,
			-- T.TRAD, -- 交易员
			T.Tx_Dt,
			T.RANK_NUM
	FROM (
		 SELECT t.Agmt_Id,
				T.Bond_Id,
				T.Invst_Comb,
				T.Invst_Cate,
				T.Cost_Ctr_Cd,
				-- T.TRAD,
				T.Tx_Dt,	
				rank() over(partition by t.Bond_Id,t.Invst_Comb,t.Invst_Cate,t.Cost_Ctr_Cd order by t.Tx_Dt,t.Stl_Dt,t.Tx_Tm) AS RANK_NUM
		 FROM PDM.t03_fx_bond_invst T
		WHERE T.Statt_Dt=${TX_DATE}
		  AND T.Chk_Ind =1
		  AND T.Recall_Dt =\'00010101\'
	) T 
    WHERE T.RANK_NUM = 1
GROUP BY T.Bond_Id, T.Invst_Comb, T.Invst_Cate, T.Cost_Ctr_Cd, T.Tx_Dt, T.RANK_NUM -- ,T.TRAD,
)	T1 -- 外汇债券投资 OPI_SPSH
	
	INNER JOIN PDM.t02_fx_bond_h	T2 -- 外汇债券（产品） OPI_SECM
		ON \'OPI\'||T1.Bond_Id=T2.Prod_Id
		-- and 20220531 between t2.Issu_Dt and t2.Matr_Dt -- 筛掉过期债券
		AND t2.Start_Dt <=${TX_DATE} AND t2.End_Dt >= ${TX_DATE}
	
	INNER JOIN PDM.t03_fx_bond_day_end_pos T5 -- 外汇债券日终头寸 OPI_TPOS
		ON T1.Bond_Id 		= T5.Bond_Id
		AND T1.Invst_Comb 	= T5.Invst_Comb_Cd
		AND T1.Invst_Cate 	= T5.Invst_Cate_Cd
		AND T1.Cost_Ctr_Cd 	= T5.Cost_Ctr_Cd
		AND T5.Statt_Dt		= ${TX_DATE}
		
	LEFT JOIN (SELECT distinct SUBSTR(Memo,1,INSTR(Memo,\'|\',1,4)-1) as Memo, Int_Subj_Id, Brch_Num, Acct_Num  FROM PDM.t05_fx_acct_evt 
					where STATT_DT = ${TX_DATE}
					  and (Int_Subj_Id IN (\'11010101\', \'15010101\', \'15030101\') OR SUBSTR(Int_Subj_Id, 1, 6) = \'250201\')
			  ) T2_SUBJT   -- 本金科目
		ON T5.Bond_Id || \' \' || \'|\' || T5.Invst_Comb_Cd || \'|\' || T5.Cost_Ctr_Cd || \'|\' || T5.Invst_Cate_Cd = T2_SUBJT.Memo
		AND DECODE(T5.Mbank_Ind,\'1\',\'01\',\'0\',\'00\',T5.Mbank_Ind) = T2_SUBJT.Brch_Num
   
    LEFT JOIN (SELECT distinct SUBSTR(Memo,1,INSTR(Memo,\'|\',1,4)-1) as Memo,Int_Subj_Id  FROM PDM.t05_fx_acct_evt 
					where STATT_DT = ${TX_DATE}
					and (Int_Subj_Id IN (\'11010103\',\'15010103\',\'15030103\',\'11320501\',\'11320601\',\'11320701\',\'22310500\') OR
                        SUBSTR(Int_Subj_Id, 1, 6) = \'250202\')
			  ) T2_ACCRUAL -- 应收利息科目
		ON T5.Bond_Id || \' \' || \'|\' || T5.Invst_Comb_Cd || \'|\' || T5.Cost_Ctr_Cd || \'|\' || T5.Invst_Cate_Cd = T2_ACCRUAL.Memo
		
	
	LEFT JOIN (SELECT distinct SUBSTR(Memo,1,INSTR(Memo,\'|\',1,4)-1) as Memo,Int_Subj_Id  FROM PDM.t05_fx_acct_evt
					where STATT_DT = ${TX_DATE}
					and Int_Subj_Id IN (\'11010102\', \'15010104\', \'15030104\')
			  ) T2_FAIR -- 公允价值变动科目
		ON T5.Bond_Id || \' \' || \'|\' || T5.Invst_Comb_Cd || \'|\' || T5.Cost_Ctr_Cd || \'|\' || T5.Invst_Cate_Cd = T2_FAIR.Memo
	
	LEFT JOIN (SELECT distinct SUBSTR(Memo,1,INSTR(Memo,\'|\',1,4)-1) as Memo,Int_Subj_Id  FROM PDM.t05_fx_acct_evt
					where STATT_DT = ${TX_DATE}
					and (Int_Subj_Id IN (\'15010102\', \'15030102\') OR  SUBSTR(Int_Subj_Id, 1, 6) = \'250203\')
			  ) T2_INT -- 利息调整科目
		ON T5.Bond_Id || \' \' || \'|\' || T5.Invst_Comb_Cd || \'|\' || T5.Cost_Ctr_Cd || \'|\' || T5.Invst_Cate_Cd = T2_INT.Memo
		
	LEFT JOIN PDM.t88_exchg_rate	T4 -- 汇率牌价表
		ON T5.Cur_Cd = T4.Init_Cur
		and t4.Statt_Dt = ${TX_DATE}
	';
	
	
	CALL ETL.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	
	/*计算累计VT_AT_*/
	SET @SQL_STR = 'DROP TEMPORARY TABLE IF EXISTS VT_AT_'||ETL_T_TAB_ENG_NAME;
	CALL ETL.PR_EXEC_SQL(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	SET @SQL_STR = 'CREATE TEMPORARY TABLE VT_AT_'||ETL_T_TAB_ENG_NAME||' LIKE PDM.'||ETL_T_TAB_ENG_NAME;
	CALL ETL.PR_EXEC_SQL(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	-- 插入到AT表
	SET @SQL_STR = '
	INSERT INTO VT_AT_t88_cap_bond_invst_biz_info
	SELECT
		NVL(T.Statt_Dt,T1.Statt_Dt)					,-- 1 统计日期
		NVL(T.Agmt_ID,T1.Agmt_ID)					,-- 2 协议编号
		NVL(T.Org_ID,T1.Org_ID)						,-- 3 机构编号
		NVL(T.Accti_Org_ID,T1.Accti_Org_ID)			,-- 4 核算机构编号
		NVL(T.Agent_Org_ID,T1.Agent_Org_ID)			,-- 5 代理机构编号
		NVL(T.Cur_Cd,T1.Cur_Cd)						,-- 6 币种代码
		NVL(T.Bond_ID,T1.Bond_ID)					,-- 7 债券编号
		NVL(T.Bond_Nm,T1.Bond_Nm)					,-- 8 债券名称
		NVL(T.Bond_Cate_Cd,T1.Bond_Cate_Cd)			,-- 9 债券类型代码
		NVL(T.Bond_Cate_Nm,T1.Bond_Cate_Nm)			,-- 10 债券类型名称
		NVL(T.TX_Dt,T1.TX_Dt)						,-- 11 交易日期
		NVL(T.St_Int_Dt,T1.St_Int_Dt)				,-- 12 起息日期
		NVL(T.Matr_Dt,T1.Matr_Dt)					,-- 13 到期日期
		NVL(T.Stl_Dt,T1.Stl_Dt)						,-- 14 结算日期
		NVL(T.Prin_Amt,T1.Prin_Amt)					,-- 15 券面金额
		NVL(T.Par_Int_Rate,T1.Par_Int_Rate)			,-- 16 票面利率
		NVL(T.Int_Pay_Freq,T1.Int_Pay_Freq)			,-- 17 付息频率
		NVL(T.Net_Prc,T1.Net_Prc)					,-- 18 净价
		NVL(T.Full_Prc,T1.Full_Prc)					,-- 19 全价
		NVL(T.TX_Amt,T1.TX_Amt)						,-- 20 交易金额
		NVL(T.Remn_Mak_Pos_Qty,T1.Remn_Mak_Pos_Qty)	,-- 21 剩余持仓量
		NVL(T.Matr_Prft_Rate,T1.Matr_Prft_Rate)		,-- 22 到期收益率
		NVL(T.TX_Cate_Cd,T1.TX_Cate_Cd)				,-- 23 交易类型代码
		NVL(T.Stl_Mod_Cd,T1.Stl_Mod_Cd)				,-- 24 结算方式代码
		NVL(T.Prft_Rate_Stl_Mod_Cd,T1.Prft_Rate_Stl_Mod_Cd)	,-- 25 收益率结算方式代码
		NVL(T.Invst_Main_Cate_Cd,T1.Invst_Main_Cate_Cd)		,-- 26 投资主体类型代码
		NVL(T.Repay_Mod_Cd,T1.Repay_Mod_Cd)					,-- 27 还本方式代码
		NVL(T.Int_Base_Cd,T1.Int_Base_Cd)					,-- 28 计息基础代码
		NVL(T.TX_Chnl_Cd,T1.TX_Chnl_Cd)						,-- 29 交易渠道代码
		NVL(T.Cust_Mgr,T1.Cust_Mgr)							,-- 30 客户经理
		NVL(T.line_Cd,T1.line_Cd)							,-- 31 条线代码
		NVL(T.Invst_Cate_Cd,T1.Invst_Cate_Cd)				,-- 32 投资类型代码
		NVL(T.Valid_Ind,T1.Valid_Ind)						,-- 33 有效标志
		NVL(T.Mbank_Cap_Acct_Num,T1.Mbank_Cap_Acct_Num)		,-- 34 本方资金账号
		NVL(T.Mbank_Pay_Bank_Num,T1.Mbank_Pay_Bank_Num)		,-- 35 本方付款行行号
		NVL(T.Mbank_Trust_Org_ID,T1.Mbank_Trust_Org_ID)		,-- 36 本方托管机构编号
		NVL(T.CntPty_Cap_Acct_Num,T1.CntPty_Cap_Acct_Num)	,-- 37 对方资金账号
		NVL(T.CntPty_Pay_Bank_Num,T1.CntPty_Pay_Bank_Num)	,-- 38 对方付款行行号
		NVL(T.CntPty_Trust_Org_ID,T1.CntPty_Trust_Org_ID)	,-- 39 对方托管机构编号
		NVL(T.Exchg_Rate,T1.Exchg_Rate)						,-- 40 汇率
		NVL(T.Prin_Subj_ID,T1.Prin_Subj_ID)					,-- 41 本金科目编号
		NVL(T.Book_Bal,T1.Book_Bal)							,-- 42 账面余额
		NVL(T.To_Rmb_Bal,T1.To_Rmb_Bal)						,-- 43 折人民币账面余额
		NVL(T.Accrd_Int_Subj_ID,T1.Accrd_Int_Subj_ID)		,-- 44 应计利息科目编号
		NVL(T.Accrd_Int_Bal,T1.Accrd_Int_Bal)				,-- 45 应计利息余额
		NVL(T.Recvbl_Int_Subj_Id,T1.Recvbl_Int_Subj_Id)		,-- 46 应收利息科目编号
		NVL(T.Recvbl_Int_Bal,T1.Recvbl_Int_Bal)				,-- 47 应收利息余额
		NVL(T.FVTPL_Subj,T1.FVTPL_Subj)						,-- 48 公允价值变动科目
		NVL(T.FVTPL_Amt,T1.FVTPL_Amt)						,-- 49 公允价值变动金额
		NVL(T.Int_Adj_Subj_ID,T1.Int_Adj_Subj_ID)			,-- 50 利息调整科目编号
		NVL(T.Int_Adj_Amt,T1.Int_Adj_Amt)					,-- 51 利息调整金额
		NVL(T.Cost_Amt,T1.Cost_Amt)							,-- 52 成本金额
		NVL(T.Stl_Bal,T1.Stl_Bal)							,-- 53 结算金余额
		CASE WHEN ${TX_DATE}=${THIS_MONTH_BEGIN} THEN T.Book_Bal
			ELSE NVL(T1.MTH_TOTAL_BAL,0)+T.Book_Bal END AS MTH_TOTAL_BAL			,-- 54 月累积余额
		CASE WHEN ${TX_DATE}=${THIS_QUART_BEGIN} THEN T.Book_Bal
			ELSE NVL(T1.QUAR_TOTAL_BAL,0)+T.Book_Bal END AS QUAR_TOTAL_BAL			,-- 55 季累积余额
		CASE WHEN ${TX_DATE}=${THIS_YEAR_BEGIN} THEN T.Book_Bal
			ELSE NVL(T1.YR_TOTAL_BAL,0)+T.Book_Bal END AS YR_TOTAL_BAL				,-- 56 年累积余额
		CASE WHEN ${TX_DATE}=${THIS_MONTH_BEGIN} THEN T.TO_RMB_BAL
			ELSE NVL(T1.RMB_MTH_TOTAL_BAL,0)+T.TO_RMB_BAL END AS RMB_MTH_TOTAL_BAL	,-- 57 人民币月累积余额
		CASE WHEN ${TX_DATE}=${THIS_QUART_BEGIN} THEN T.TO_RMB_BAL
			ELSE NVL(T1.RMB_QUAR_TOTAL_BAL,0)+T.TO_RMB_BAL END AS RMB_QUAR_TOTAL_BAL,-- 58 人民币季累积余额
		CASE WHEN ${TX_DATE}=${THIS_YEAR_BEGIN} THEN T.TO_RMB_BAL
			ELSE NVL(T1.RMB_YR_TOTAL_BAL,0)+T.TO_RMB_BAL END AS RMB_YR_TOTAL_BAL	,-- 59 人民币年累积余额
		CASE WHEN ${TX_DATE}=${THIS_MONTH_BEGIN} THEN T.Book_Bal
			ELSE (NVL(T1.MTH_TOTAL_BAL,0)+T.Book_Bal)/(${TX_DATE}-${THIS_MONTH_BEGIN}+1) END AS MTH_DAVG	,-- 60 月日均余额
		CASE WHEN ${TX_DATE}=${THIS_QUART_BEGIN} THEN T.Book_Bal
			ELSE (NVL(T1.QUAR_TOTAL_BAL,0)+T.Book_Bal)/(${TX_DATE}-${THIS_QUART_BEGIN}+1) END AS QUAR_DAVG	,-- 61 季日均余额
		CASE WHEN ${TX_DATE}=${THIS_YEAR_BEGIN} THEN T.Book_Bal
			ELSE (NVL(T1.YR_TOTAL_BAL,0)+T.Book_Bal)/(${TX_DATE}-${THIS_YEAR_BEGIN}+1) END AS YR_DAVG		,-- 62 年日均余额
		CASE WHEN ${TX_DATE}=${THIS_MONTH_BEGIN} THEN T.TO_RMB_BAL
			ELSE (NVL(T1.RMB_MTH_TOTAL_BAL,0)+T.TO_RMB_BAL)/(${TX_DATE}-${THIS_MONTH_BEGIN}+1) END AS RMB_MTH_TOTAL_BAL		,-- 63 人民币月日均余额
		CASE WHEN ${TX_DATE}=${THIS_QUART_BEGIN} THEN T.TO_RMB_BAL
			ELSE (NVL(T1.RMB_QUAR_TOTAL_BAL,0)+T.TO_RMB_BAL)/(${TX_DATE}-${THIS_QUART_BEGIN}+1) END AS RMB_QUAR_TOTAL_BAL	,-- 64 人民币季日均余额
		CASE WHEN ${TX_DATE}=${THIS_YEAR_BEGIN} THEN T.TO_RMB_BAL
			ELSE (NVL(T1.RMB_YR_TOTAL_BAL,0)+T.TO_RMB_BAL)/(${TX_DATE}-${THIS_YEAR_BEGIN}+1) END AS RMB_YR_TOTAL_BAL		 -- 65 人民币年日均余额
	FROM VT_t88_cap_bond_invst_biz_info T
	FULL JOIN VT_PRE_t88_cap_bond_invst_biz_info T1
		ON T.AGMT_ID=T1.AGMT_ID
	WHERE T.STATT_DT=${TX_DATE}
	';
	CALL ETL.PR_EXEC_SQL(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	/*检查插入的临时表数据是否有主键错误*/
	
	-- 获取主键字段
	/*SELECT PHYSICAL_PRI_KEY INTO @PK_COLUMN FROM etl.DATAMAPPING_TASK WHERE T_TAB_ENG_NAME=ETL_T_TAB_ENG_NAME;
	-- 0 正常
	SET @SQL_STR = 'SELECT COUNT(1) INTO @PK_COUNT FROM(SELECT 1 FROM VT_'||ETL_T_TAB_ENG_NAME||' GROUP BY '||@PK_COLUMN||' HAVING COUNT(1)>1) T';
	CALL ETL.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	*/
	
	/*
	IF @PK_COUNT <> 0
	THEN
		SET OUT_RES_MSG = '9999';
		UPDATE ETL.ETL_JOB_STATUS_EDW SET STEP_STATUS='Failed',STEP_ERR_LOG='主键重复', LAST_END_TIME=CURRENT_TIMESTAMP WHERE SQL_UNIT=ETL_T_TAB_ENG_NAME AND TX_DATE=TX_DATE AND STEP_NO=ETL_STEP_NO-1;
		LEAVE LABLE;
	END IF;
	*/
	
	/*通过主键检查的数据插入正式表中*/
	SET @SQL_STR='INSERT INTO PDM.'||ETL_T_TAB_ENG_NAME||' SELECT * FROM VT_AT_'||ETL_T_TAB_ENG_NAME||' WHERE STATT_DT=${TX_DATE}'; 
	CALL ETL.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

	SET OUT_RES_MSG='SUCCESSFUL';
	
END |