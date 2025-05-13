DELIMITER |

CREATE DEFINER="cq_sjpt"@"%" PROCEDURE "pr_f5_T88_IBANK_CAP_BIZ_INFO_ljz"(
	OUT	OUT_RES_MSG		VARCHAR(200),
	IN	IN_TX_DATE		VARCHAR(8)
	)
LABLE:BEGIN
/**********************************
 * LJZ 202107228 新建
 * 同业资金业务信息
 *********************************/
	
	DECLARE ETL_T_TAB_ENG_NAME 		VARCHAR(100)	DEFAULT 't88_ibank_cap_biz_info_ljz';
	DECLARE ETL_STEP_NO				INTEGER			DEFAULT 1;
	DECLARE ETL_TX_DATE 			VARCHAR(8)		DEFAULT IN_TX_DATE;
	SET OUT_RES_MSG = 'FAILED';
	
	
	/*支持数据重跑*/
	SET @SQL_STR = 'DELETE FROM PDM.'||ETL_T_TAB_ENG_NAME||' WHERE STATT_DT >= ${TX_DATE}';
	CALL ETL.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	/*定义临时表*/
	SET @SQL_STR = 'DROP TEMPORARY TABLE IF EXISTS ETL.VT_'||ETL_T_TAB_ENG_NAME;
	CALL ETL.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	/*创建临时表*/
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

	-- 前五组：0306 同业存放 0307 存放同业 0305 同业拆借 0308 同业借款 0304 同业拆借（外汇拆借） ${NULL_STR}
	SET @SQL_STR = '
	INSERT INTO VT_t88_ibank_cap_biz_info_ljz(
		Statt_Dt               ,-- 1统计日期
		Agmt_ID                ,-- 2协议编号
		Cur_Cd                 ,-- 3币种代码
		Org_ID                 ,-- 4机构编号
		Agmt_Cate_Cd           ,-- 5同业业务产品分类代码
		Exec_Int_Rate          ,-- 6执行利率
		TX_Dt                  ,-- 7交易日期
		St_Int_Dt              ,-- 8起息日期
		Matr_Dt                ,-- 9到期日期
		Term_Corp_Cd           ,-- 10期限单位代码
		Term                   ,-- 11期限
		TX_Prin                ,-- 12交易本金
		TX_Mod_Cd              ,-- 13交易方式代码
		Ibank_Cate_Cd          ,-- 14同业类型代码
		Int_Base_Cd            ,-- 15计息基础代码
		Int_Stl_Mod_Cd         ,-- 16结息方式代码
		Int_Stl_Freq_Cd        ,-- 17结息频率代码
		Sys_inout_Ind          ,-- 18系统内外标志
		Valid_Ind              ,-- 19有效标志
		CntPty_Cust_ID         ,-- 20对手客户编号
		CntPty_Org_ID          ,-- 21付款行机构编号
		Pay_Acct_Cate_Cd       ,-- 22付款账户类型代码
		Pay_Bank_Num           ,-- 23付款行行号
		Pay_Open_Acct_Bank     ,-- 24付款开户行
		Pay_Acct_Num           ,-- 25付款账号
		Pay_Acct_Nm            ,-- 26付款账户名称
		Recv_Acct_Cate_Cd      ,-- 27收款账户类型代码
		Recv_Bank_Num          ,-- 28收款行行号
		Recv_Open_Acct_Bank    ,-- 29收款开户行
		Recv_Acct_Num          ,-- 30收款账号
		Recv_Acct_Nm           ,-- 31收款账户名称
		Exchg_Rate             ,-- 32汇率
		Prin_Subj_ID           ,-- 33本金科目编号
		Curr_Bal               ,-- 34当前余额
		To_Rmb_Bal             ,-- 35折人民币余额
		Mth_Total_Bal          ,-- 36月累积余额
		Quar_Total_Bal         ,-- 37季累积余额
		Yr_Total_Bal           ,-- 38年累积余额
		Rmb_Mth_Total_Bal      ,-- 39人民币月累积余额
		Rmb_Quar_Total_Bal     ,-- 40人民币季累积余额
		Rmb_Yr_Total_Bal       ,-- 41人民币年累积余额
		Mth_DAvg               ,-- 42月日均余额
		Quar_DAvg              ,-- 43季日均余额
		Yr_DAvg                ,-- 44年日均余额
		Rmb_Mth_DAvg           ,-- 45人民币月日均余额
		Rmb_Quar_DAvg          ,-- 46人民币季日均余额
		Rmb_Yr_DAvg            ,-- 47人民币年日均余额
		Recvbl_Int_Subj_ID     ,-- 48应收利息科目编号
		Recvbl_Int_Bal         ,-- 49应收利息余额
		Main_Src_Task			-- 50主要源系统任务
	)
	SELECT 
		${TX_DATE}                   , -- 1统计日期
		T1.Agmt_Id                , -- 2协议编号
		T1.Cur_Cd                 , -- 3币种代码
		T1.Org_Id                 , -- 4机构编号
		T1.Agmt_Cate_Cd     , -- 5同业业务产品分类
		T1.Int_Rate               , -- 6利率
		T1.Tx_Dt                  , -- 7交易日期
		T1.St_Int_Dt              , -- 8起息日期
		T1.Matr_Dt                , -- 9到期日期
		T1.Term_Corp_Cd           , -- 10期限单位代码
		T1.Term_Len_Cd            , -- 11期限长度代码
		CASE
			WHEN T1.Agmt_Cate_Cd IN (\'0306\',\'0307\') THEN T1.Stor_Amt
			WHEN T1.Agmt_Cate_Cd IN (\'0305\',\'0308\') THEN T1.Stor_Amt
			WHEN T1.Agmt_Cate_Cd=\'0304\' THEN T1.Remn_Amt
			ELSE 0
		END  					  , -- 12存放金额
		T1.Tx_Mod_Cd              , -- 13交易方式代码
		T1.Prod_Cls_Cd            , -- 14产品分类代码
		T1.Int_Base_Cd            , -- 15计息基准代码
		T1.Int_Mod_Cd             , -- 16计息方式代码
		T1.Int_Stl_Freq_Cd        , -- 17结息频率代码
		T1.Sys_Inout_Ind          , -- 18系统内外标志
		T1.Valid_Ind              , -- 19有效标志
		T1.Cntpty_Cust_Id         , -- 20对手客户编号
		T1.Pay_Org_Id             , -- 21付款行机构编号
		T1.Pay_Acct_Cate_Cd       , -- 22付款账户类型代码
		T1.Pay_Bank_Num           , -- 23付款行行号
		T1.Pay_Open_Acct_Bank     , -- 24付款开户行
		T1.Pay_Acct_Num           , -- 25付款账号
		T1.Pay_Acct_Nm            , -- 26付款账户名称
		T1.Recv_Acct_Cate_Cd      , -- 27收款账户类型代码
		T1.Recv_Bank_Num          , -- 28收款行行号
		T1.Recv_Open_Acct_Bank    , -- 29收款开户行
		T1.Recv_Acct_Num          , -- 30收款账号
		T1.Recv_Acct_Nm           , -- 31收款账户名称
		T4.Mdl_Prc                , -- 32中间价
		CASE
		  WHEN T1.Agmt_Cate_Cd=\'0306\' THEN \'2012\' -- 暂时无法确定三级科目
		  WHEN T1.Agmt_Cate_Cd IN (\'0307\',\'0305\',\'0308\') THEN NVL(T2.SUBJ_ID,\'Z\')
		  WHEN T1.Agmt_Cate_Cd=\'0304\' THEN \'13020101\'
		  ELSE \'Z\'
		END, -- 33本金科目编号
		CASE
			WHEN T1.Agmt_Cate_Cd=\'0306\' THEN NVL(T1.Remn_Amt,0) -- 同业存放             
			WHEN T1.Agmt_Cate_Cd=\'0307\' THEN NVL(T3.Bal,0) -- 存放同业
			WHEN T1.Agmt_Cate_Cd=\'0305\' THEN NVL(T3.Bal,0) -- 同业拆借
			WHEN T1.Agmt_Cate_Cd=\'0308\' THEN NVL(T3.Bal,0) -- 同业借款
			WHEN T1.Agmt_Cate_Cd=\'0304\' THEN NVL(T1.Remn_Amt,0) -- 外汇借款
			ELSE 0
		END, -- 34余额
		(CASE
			WHEN T1.Agmt_Cate_Cd=\'0306\' THEN NVL(T1.Remn_Amt,0) -- 同业存放             
			WHEN T1.Agmt_Cate_Cd=\'0307\' THEN NVL(T3.Bal,0) -- 存放同业
			WHEN T1.Agmt_Cate_Cd=\'0305\' THEN NVL(T3.Bal,0) -- 同业拆借
			WHEN T1.Agmt_Cate_Cd=\'0308\' THEN NVL(T3.Bal,0) -- 同业借款
			WHEN T1.Agmt_Cate_Cd=\'0304\' THEN NVL(T1.Remn_Amt,0) -- 外汇借款
			ELSE 0
		END)*NVL(T4.Mdl_Prc,0), -- 35折人余额
		0         ,-- 36月累积余额
		0         ,-- 37季累积余额
		0         ,-- 38年累积余额
		0         ,-- 39人民币月累积余额
		0     	   ,-- 40人民币季累积余额
		0         ,-- 41人民币年累积余额
		0         ,-- 42月日均余额
		0         ,-- 43季日均余额
		0         ,-- 44年日均余额
		0         ,-- 45人民币月日均余额
		0         ,-- 46人民币季日均余额
		0         ,-- 47人民币年日均余额
		${NULL_STR},-- 48应收利息科目编号
		0 		   ,-- 49余额
		''T03_IBANK_CAP_BIZ'' -- 主要源系统任务
	FROM PDM.t03_ibank_cap_biz	T1 -- 同业资金业务
	LEFT JOIN PDM.t03_cap_subj_comp_h	T2 -- 资金科目对照
		ON T1.Agmt_Id=T2.Agmt_Id
		and t2.Amt_Cate_Cd=\'ZJJE0001\'
		and t2.Start_Dt <= ${TX_DATE}
		and t2.End_Dt >= ${TX_DATE}
	LEFT JOIN PDM.t03_cap_realtm_bal	T3 -- 资金实时余额
		ON T1.Agmt_Id=T3.Agmt_Id
		and t3.Amt_Cate=\'ZJJE0001\'
		and t3.Statt_Dt = ${TX_DATE}
		AND Data_Valid_Ind=\'E\' AND T3.Bal<>0
	LEFT JOIN PDM.t88_exchg_rate	T4 -- 汇率牌价表
		on T1.Cur_Cd=T4.Init_Cur
		and t4.Statt_Dt = ${TX_DATE}
	where t1.Statt_Dt = ${TX_DATE}
		and ( (T1.Agmt_Cate_Cd in (\'0306\') and ${TX_DATE} between t1.St_Int_Dt and t1.Matr_Dt and t1.Valid_Ind=\'E\') -- 0306同业存放
		 or (T1.Agmt_Cate_Cd in (\'0307\',\'0308\') and t1.Remn_Amt<>0) -- 0307存放同业(定期) 0308同业借款(定期)
	     or (T1.Agmt_Cate_Cd in (\'0305\') and t1.Remn_Amt<>0 and t1.Valid_Ind=\'E\' and ${TX_DATE} between t1.St_Int_Dt and t1.Matr_Dt) -- 0305同业拆借(定期)
	     or (T1.Agmt_Cate_Cd in (\'0304\') and (t1.St_Int_Dt<=${TX_DATE} and t1.Matr_Dt>${TX_DATE}) and valid_ind=\'1\') -- 0304同业拆借(外币)
	     )
	';
	CALL ETL.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	-- 0307 存放同业
	SET @SQL_STR = '
	INSERT INTO VT_t88_ibank_cap_biz_info_ljz(
		Statt_Dt               ,-- 1统计日期
		Agmt_ID                ,-- 2协议编号
		Cur_Cd                 ,-- 3币种代码
		Org_ID                 ,-- 4机构编号
		Agmt_Cate_Cd           ,-- 5同业业务产品分类代码
		Exec_Int_Rate          ,-- 6执行利率
		TX_Dt                  ,-- 7交易日期
		St_Int_Dt              ,-- 8起息日期
		Matr_Dt                ,-- 9到期日期
		Term_Corp_Cd           ,-- 10期限单位代码
		Term                   ,-- 11期限
		TX_Prin                ,-- 12交易本金
		TX_Mod_Cd              ,-- 13交易方式代码
		Ibank_Cate_Cd          ,-- 14同业类型代码
		Int_Base_Cd            ,-- 15计息基础代码
		Int_Stl_Mod_Cd         ,-- 16结息方式代码
		Int_Stl_Freq_Cd        ,-- 17结息频率代码
		Sys_inout_Ind          ,-- 18系统内外标志
		Valid_Ind              ,-- 19有效标志
		CntPty_Cust_ID         ,-- 20对手客户编号
		CntPty_Org_ID          ,-- 21付款行机构编号
		Pay_Acct_Cate_Cd       ,-- 22付款账户类型代码
		Pay_Bank_Num           ,-- 23付款行行号
		Pay_Open_Acct_Bank     ,-- 24付款开户行
		Pay_Acct_Num           ,-- 25付款账号
		Pay_Acct_Nm            ,-- 26付款账户名称
		Recv_Acct_Cate_Cd      ,-- 27收款账户类型代码
		Recv_Bank_Num          ,-- 28收款行行号
		Recv_Open_Acct_Bank    ,-- 29收款开户行
		Recv_Acct_Num          ,-- 30收款账号
		Recv_Acct_Nm           ,-- 31收款账户名称
		Exchg_Rate             ,-- 32汇率
		Prin_Subj_ID           ,-- 33本金科目编号
		Curr_Bal               ,-- 34当前余额
		To_Rmb_Bal             ,-- 35折人民币余额
		Mth_Total_Bal          ,-- 36月累积余额
		Quar_Total_Bal         ,-- 37季累积余额
		Yr_Total_Bal           ,-- 38年累积余额
		Rmb_Mth_Total_Bal      ,-- 39人民币月累积余额
		Rmb_Quar_Total_Bal     ,-- 40人民币季累积余额
		Rmb_Yr_Total_Bal       ,-- 41人民币年累积余额
		Mth_DAvg               ,-- 42月日均余额
		Quar_DAvg              ,-- 43季日均余额
		Yr_DAvg                ,-- 44年日均余额
		Rmb_Mth_DAvg           ,-- 45人民币月日均余额
		Rmb_Quar_DAvg          ,-- 46人民币季日均余额
		Rmb_Yr_DAvg            ,-- 47人民币年日均余额
		Recvbl_Int_Subj_ID     ,-- 48应收利息科目编号
		Recvbl_Int_Bal         ,-- 49应收利息余额
		Main_Src_Task			-- 50主要源系统任务
	)
	SELECT 
		${TX_DATE}                , -- 1统计日期
		T1.Agmt_Id                , -- 2协议编号
		T1.Cur_Cd                 , -- 3币种代码
		T1.Org_ID                 , -- 4机构编号
		''0307''     			  , -- 5同业业务产品分类
		T1.Yr_Int_Rate            , -- 6利率
		''0001-01-01''            , -- 7交易日期
		T1.St_Int_Dt              , -- 8起息日期
		''0001-01-01''            , -- 9到期日期
		${NULL_STR}           	  , -- 10期限单位代码
		${NULL_STR}            	  , -- 11期限长度代码
		0  					  	  , -- 12交易本金
		${NULL_STR}               , -- 13交易方式代码
		${NULL_STR}            	  , -- 14产品分类代码
		T1.Int_Base_Cd            , -- 15计息基准代码
		${NULL_STR}               , -- 16计息方式代码
		T1.Int_Stl_Freq_Cd        , -- 17结息频率代码
		T1.Sys_Ext_Acct_Ind       , -- 18系统内外标志
		''E''              		  , -- 19有效标志 E:有效
		${NULL_STR}         	  , -- 20对手客户编号
		${NULL_STR}               , -- 21付款行机构编号
		${NULL_STR}       		  , -- 22付款账户类型代码
		${NULL_STR}               , -- 23付款行行号
		${NULL_STR}     		  , -- 24付款开户行
		${NULL_STR}               , -- 25付款账号
		${NULL_STR}               , -- 26付款账户名称
		${NULL_STR}      		  , -- 27收款账户类型代码
		${NULL_STR}         	  , -- 28收款行行号
		${NULL_STR}    			  , -- 29收款开户行
		${NULL_STR}          	  , -- 30收款账号
		${NULL_STR}               , -- 31收款账户名称
		T4.Mdl_Prc                , -- 32中间价
		T2.Subj_Id				  , -- 33本金科目编号
		T3.Bal					  , -- 34余额
		T3.Bal*NVL(T4.Mdl_Prc,0)  , -- 35折人余额
		0         ,-- 36月累积余额
		0         ,-- 37季累积余额
		0         ,-- 38年累积余额
		0         ,-- 39人民币月累积余额
		0     	  ,-- 40人民币季累积余额
		0         ,-- 41人民币年累积余额
		0         ,-- 42月日均余额
		0         ,-- 43季日均余额
		0         ,-- 44年日均余额
		0         ,-- 45人民币月日均余额
		0         ,-- 46人民币季日均余额
		0         ,-- 47人民币年日均余额
		${NULL_STR}, -- 48应收利息科目编号
		0          , -- 49应收利息余额
		''t03_ext_acct'' -- 50主要源系统任务
	FROM PDM.t03_ext_acct	T1 -- 外部账户
	LEFT JOIN PDM.t03_cap_subj_comp_h	T2 -- 资金科目对照
		ON T1.Agmt_Id=T2.Agmt_Id
		and t2.Amt_Cate_Cd = \'ZJJE0001\'
		and t2.Start_Dt <= ${TX_DATE}
		and t2.End_Dt >= ${TX_DATE}
	LEFT JOIN PDM.t03_cap_realtm_bal	T3 -- 资金实时余额
		   ON T1.Agmt_Id = T3.Agmt_Id
		  AND T3.Amt_Cate = \'ZJJE0001\'
		  AND T3.Data_Valid_Ind = \'E\'
		  AND T3.Statt_Dt = ${TX_DATE}
	LEFT JOIN PDM.t88_exchg_rate	T4 -- 汇率牌价表
		on T1.Cur_Cd = T4.Init_Cur
		and t4.Statt_Dt = ${TX_DATE}
	where t1.Statt_Dt = ${TX_DATE}
	  AND T3.Bal <> 0
	';
	CALL ETL.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	
	
	-- 第六组：存放同业 030305 人行借款
	SET @SQL_STR = '
	INSERT INTO VT_t88_ibank_cap_biz_info_ljz(
		Statt_Dt               ,-- 1统计日期
		Agmt_ID                ,-- 2协议编号
		Cur_Cd                 ,-- 3币种代码
		Org_ID                 ,-- 4机构编号
		Agmt_Cate_Cd  ,-- 5同业业务产品分类代码
		Exec_Int_Rate          ,-- 6执行利率
		TX_Dt                  ,-- 7交易日期
		St_Int_Dt              ,-- 8起息日期
		Matr_Dt                ,-- 9到期日期
		Term_Corp_Cd           ,-- 10期限单位代码
		Term                   ,-- 11期限
		TX_Prin                ,-- 12交易本金
		TX_Mod_Cd              ,-- 13交易方式代码
		Ibank_Cate_Cd          ,-- 14同业类型代码
		Int_Base_Cd            ,-- 15计息基础代码
		Int_Stl_Mod_Cd         ,-- 16结息方式代码
		Int_Stl_Freq_Cd        ,-- 17结息频率代码
		Sys_inout_Ind          ,-- 18系统内外标志
		Valid_Ind              ,-- 19有效标志
		CntPty_Cust_ID         ,-- 20对手客户编号
		CntPty_Org_ID          ,-- 21付款行机构编号
		Pay_Acct_Cate_Cd       ,-- 22付款账户类型代码
		Pay_Bank_Num           ,-- 23付款行行号
		Pay_Open_Acct_Bank     ,-- 24付款开户行
		Pay_Acct_Num           ,-- 25付款账号
		Pay_Acct_Nm            ,-- 26付款账户名称
		Recv_Acct_Cate_Cd      ,-- 27收款账户类型代码
		Recv_Bank_Num          ,-- 28收款行行号
		Recv_Open_Acct_Bank    ,-- 29收款开户行
		Recv_Acct_Num          ,-- 30收款账号
		Recv_Acct_Nm           ,-- 31收款账户名称
		Exchg_Rate             ,-- 32汇率
		Prin_Subj_ID           ,-- 33本金科目编号
		Curr_Bal               ,-- 34当前余额
		To_Rmb_Bal             ,-- 35折人民币余额
		Mth_Total_Bal          ,-- 36月累积余额
		Quar_Total_Bal         ,-- 37季累积余额
		Yr_Total_Bal           ,-- 38年累积余额
		Rmb_Mth_Total_Bal      ,-- 39人民币月累积余额
		Rmb_Quar_Total_Bal     ,-- 40人民币季累积余额
		Rmb_Yr_Total_Bal       ,-- 41人民币年累积余额
		Mth_DAvg               ,-- 42月日均余额
		Quar_DAvg              ,-- 43季日均余额
		Yr_DAvg                ,-- 44年日均余额
		Rmb_Mth_DAvg           ,-- 45人民币月日均余额
		Rmb_Quar_DAvg          ,-- 46人民币季日均余额
		Rmb_Yr_DAvg            ,-- 47人民币年日均余额
		Recvbl_Int_Subj_ID     ,-- 48应收利息科目编号
		Recvbl_Int_Bal         ,-- 49应收利息余额
		Main_Src_Task			-- 50主要源系统任务
	)
	SELECT 
		${TX_DATE}                   , -- 1统计日期
		T1.Agmt_Id                , -- 2协议编号
		T1.Cur_Cd                 , -- 3币种代码
		T1.Appl_Org_Id            , -- 4申请机构编号
		\'0309\'                  , -- 5同业业务产品分类
		T1.Brw_Money_Int_Rate     , -- 6借款利率
		T1.Appl_Dt                , -- 7 申请日期
		T1.St_Int_Dt              , -- 8 起息日期
		T1.Matr_Dt                , -- 9 到期日期
		T1.Term_Corp_Cd           , -- 10期限单位代码
		T1.Term_Len_Cd            , -- 11期限长度代码
		T1.Brw_Money_Amt          , -- 12借款金额
		${NULL_STR}                        , -- 13交易方式代码
		${NULL_STR}                        , -- 14产品分类代码
		T1.Int_Base_Cd            , -- 15计息基准代码
		T1.Int_Mod_Cd             , -- 16计息方式代码
		T1.Int_Stl_Freq_Cd        , -- 17结息频率代码
		${NULL_STR}                        , -- 18系统内外标志
		T1.Valid_Ind              , -- 19有效标志
		\'中国人民银行\'                        , -- 20对手客户编号
		\'中国人民银行\'                        , -- 21付款行机构编号
		\'中国人民银行\'                        , -- 22付款账户类型代码
		\'中国人民银行\'                        , -- 23付款行行号
		\'中国人民银行\'                        , -- 24付款开户行
		${NULL_STR}                        , -- 25付款账号
		\'中国人民银行\'                        , -- 26付款账户名称
		T1.Recv_Acct_Cate_Cd      , -- 27收款账户类型代码
		T1.Recv_Acct_Num_Open_Acct_Bank_Num          , -- 28收款行行号
		T1.Recv_Acct_Num_Open_Acct_Bank    , -- 29收款开户行
		T1.Recv_Acct_Num          , -- 30收款账号
		T1.RECV_ACCT_NM           , -- 31收款账户名称
		T4.MDL_PRC                , -- 32中间价
		NVL(T2.SUBJ_ID,\'20040000\'), -- 33本金科目编号
		NVL(T3.BAL,0), -- 34余额
		NVL(T3.BAL*T4.MDL_PRC,0),-- 35折人民币余额
		0,-- 36月累积余额
		0,-- 37季累积余额
		0,-- 38年累积余额
		0,-- 39人民币月累积余额
		0,-- 40人民币季累积余额
		0,-- 41人民币年累积余额
		0,-- 42月日均余额
		0,-- 43季日均余额
		0,-- 44年日均余额
		0,-- 45人民币月日均余额
		0,-- 46人民币季日均余额
		0,-- 47人民币年日均余额
		NVL(T22.SUBJ_ID,\'22310800\')   , -- 48应收利息科目编号
		NVL(T33.BAL,0) ,-- 49余额
		''T03_PBC_BRW_MONEY'' -- 50主要源系统任务
	FROM PDM.T03_PBC_BRW_MONEY	T1 -- 人行借款
	LEFT JOIN PDM.T03_CAP_SUBJ_COMP_H	T2 -- 资金科目对照
		ON T1.AGMT_ID=T2.AGMT_ID
		AND T2.AMT_CATE_CD=\'ZJJE0001\' -- ZJJE0001本金ZJJE0010利息
		AND T2.START_DT <= ${TX_DATE}
		AND T2.END_DT >= ${TX_DATE}
	LEFT JOIN PDM.T03_CAP_REALTM_BAL	T3 -- 资金实时余额
		ON T1.AGMT_ID=T3.AGMT_ID
		AND T3.STATT_DT = ${TX_DATE}
		AND T3.AMT_CATE=\'ZJJE0001\'
		AND T3.DATA_VALID_IND=\'E\' 
	LEFT JOIN PDM.T03_CAP_SUBJ_COMP_H	T22 -- 资金科目对照
		ON T1.AGMT_ID=T22.AGMT_ID
		AND T22.AMT_CATE_CD=\'ZJJE0010\' -- ZJJE0001本金ZJJE0010利息
		AND T22.START_DT <= ${TX_DATE}
		AND T22.END_DT >= ${TX_DATE}
	LEFT JOIN PDM.T03_CAP_REALTM_BAL	T33 -- 资金实时余额
		ON T1.AGMT_ID=T33.AGMT_ID
		AND T33.STATT_DT = ${TX_DATE}
		AND T33.AMT_CATE=\'ZJJE0010\'
		AND T33.DATA_VALID_IND=\'E\' 
	LEFT JOIN PDM.T88_EXCHG_RATE	T4 -- 汇率牌价表
		ON T1.CUR_CD=T4.INIT_CUR
		AND T4.STATT_DT = ${TX_DATE}
	WHERE T1.STATT_DT = ${TX_DATE}
		-- AND T1.MATR_DT>=${TX_DATE} -- 未到期 20221016 去掉到期日条件
		AND T1.ST_INT_DT<=${TX_DATE}
		AND T1.VALID_IND=\'1\' -- 有效
		AND T1.HAD_MATR_PROC_IND<>\'1\' -- 未做到期处理
		AND T1.BRW_MONEY_AMT<>0 -- 金额不为0
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
	INSERT INTO VT_AT_t88_ibank_cap_biz_info_ljz
	SELECT 
		${TX_DATE},-- 1 统计日期
		NVL(T.Agmt_ID,T1.Agmt_ID),-- 2 协议编号
		NVL(T.Cur_Cd,T1.Cur_Cd),-- 3 币种代码
		NVL(T.Org_ID,T1.Org_ID),-- 4 机构编号
		NVL(T.Agmt_Cate_Cd,T1.Agmt_Cate_Cd),-- 5 同业业务产品分类代码
		NVL(T.Exec_Int_Rate,T1.Exec_Int_Rate),-- 6 执行利率
		NVL(T.TX_Dt,T1.TX_Dt),-- 7 交易日期
		NVL(T.St_Int_Dt,T1.St_Int_Dt),-- 8 起息日期
		NVL(T.Matr_Dt,T1.Matr_Dt),-- 9 到期日期
		NVL(T.Term_Corp_Cd,T1.Term_Corp_Cd),-- 10 期限单位代码
		NVL(T.Term,T1.Term),-- 11 期限
		NVL(T.TX_Prin,0),-- 12 交易本金
		NVL(T.TX_Mod_Cd,T1.TX_Mod_Cd),-- 13 交易方式代码
		NVL(T.Ibank_Cate_Cd,T1.Ibank_Cate_Cd),-- 14 同业类型代码
		NVL(T.Int_Base_Cd,T1.Int_Base_Cd),-- 15 计息基础代码
		NVL(T.Int_Stl_Mod_Cd,T1.Int_Stl_Mod_Cd),-- 16 结息方式代码
		NVL(T.Int_Stl_Freq_Cd,T1.Int_Stl_Freq_Cd),-- 17 结息频率代码
		NVL(T.Sys_inout_Ind,T1.Sys_inout_Ind),-- 18 系统内外标志
		NVL(T.Valid_Ind,T1.Valid_Ind),-- 19 有效标志
		NVL(T.CntPty_Cust_ID,T1.CntPty_Cust_ID),-- 20 对手客户编号
		NVL(T.CntPty_Org_ID,T1.CntPty_Org_ID),-- 21 对方机构编号
		NVL(T.Pay_Acct_Cate_Cd,T1.Pay_Acct_Cate_Cd),-- 22 付款账户类型代码
		NVL(T.Pay_Bank_Num,T1.Pay_Bank_Num),-- 23 付款行行号
		NVL(T.Pay_Open_Acct_Bank,T1.Pay_Open_Acct_Bank),-- 24 付款开户行
		NVL(T.Pay_Acct_Num,T1.Pay_Acct_Num),-- 25 付款账号
		NVL(T.Pay_Acct_Nm,T1.Pay_Acct_Nm),-- 26 付款账户名称
		NVL(T.Recv_Acct_Cate_Cd,T1.Recv_Acct_Cate_Cd),-- 27 收款账户类型代码
		NVL(T.Recv_Bank_Num,T1.Recv_Bank_Num),-- 28 收款行行号
		NVL(T.Recv_Open_Acct_Bank,T1.Recv_Open_Acct_Bank),-- 29 收款开户行
		NVL(T.Recv_Acct_Num,T1.Recv_Acct_Num),-- 30 收款账号
		NVL(T.Recv_Acct_Nm,T1.Recv_Acct_Nm),-- 31 收款账户名称
		NVL(T.Exchg_Rate,T1.Exchg_Rate),-- 32 汇率
		NVL(T.Prin_Subj_ID,T1.Prin_Subj_ID),-- 33 本金科目编号
		NVL(T.Curr_Bal,0),-- 34 当前余额
		NVL(T.To_Rmb_Bal,0),-- 35 折人民币余额
		CASE WHEN ${TX_DATE}=${THIS_MONTH_BEGIN} THEN NVL(T.Curr_Bal,0)
			ELSE NVL(T1.MTH_TOTAL_BAL,0)+NVL(T.Curr_Bal,0) END AS MTH_TOTAL_BAL,-- 52 月累积余额
		CASE WHEN ${TX_DATE}=${THIS_QUART_BEGIN} THEN NVL(T.Curr_Bal,0)
			ELSE NVL(T1.QUAR_TOTAL_BAL,0)+NVL(T.Curr_Bal,0) END AS QUAR_TOTAL_BAL,-- 53 季累积余额
		CASE WHEN ${TX_DATE}=${THIS_YEAR_BEGIN} THEN NVL(T.Curr_Bal,0)
			ELSE NVL(T1.YR_TOTAL_BAL,0)+NVL(T.Curr_Bal,0) END AS YR_TOTAL_BAL,-- 54 季累积余额
		CASE WHEN ${TX_DATE}=${THIS_MONTH_BEGIN} THEN NVL(T.To_Rmb_Bal,0)
			ELSE NVL(T1.RMB_MTH_TOTAL_BAL,0)+NVL(T.To_Rmb_Bal,0) END AS RMB_MTH_TOTAL_BAL,-- 55 人民币月累积余额
		CASE WHEN ${TX_DATE}=${THIS_QUART_BEGIN} THEN NVL(T.To_Rmb_Bal,0)
			ELSE NVL(T1.RMB_QUAR_TOTAL_BAL,0)+NVL(T.To_Rmb_Bal,0) END AS RMB_QUAR_TOTAL_BAL,-- 56 人民币季累积余额
		CASE WHEN ${TX_DATE}=${THIS_YEAR_BEGIN} THEN NVL(T.To_Rmb_Bal,0)
			ELSE NVL(T1.RMB_YR_TOTAL_BAL,0)+NVL(T.To_Rmb_Bal,0) END AS RMB_YR_TOTAL_BAL,-- 57 人民币年累积余额
		CASE WHEN ${TX_DATE}=${THIS_MONTH_BEGIN} THEN NVL(T.Curr_Bal,0)
			ELSE (NVL(T1.MTH_TOTAL_BAL,0)+NVL(T.Curr_Bal,0))/(${TX_DATE}-${THIS_MONTH_BEGIN}+1) END AS MTH_DAVG,-- 58 月日均余额
		CASE WHEN ${TX_DATE}=${THIS_QUART_BEGIN} THEN NVL(T.Curr_Bal,0)
			ELSE (NVL(T1.QUAR_TOTAL_BAL,0)+NVL(T.Curr_Bal,0))/(${TX_DATE}-${THIS_QUART_BEGIN}+1) END AS QUAR_DAVG,-- 59 季日均余额
		CASE WHEN ${TX_DATE}=${THIS_YEAR_BEGIN} THEN NVL(T.Curr_Bal,0)
			ELSE (NVL(T1.YR_TOTAL_BAL,0)+NVL(T.Curr_Bal,0))/(${TX_DATE}-${THIS_YEAR_BEGIN}+1) END AS YR_DAVG,-- 60 年日均余额
		CASE WHEN ${TX_DATE}=${THIS_MONTH_BEGIN} THEN NVL(T.To_Rmb_Bal,0)
			ELSE (NVL(T1.RMB_MTH_TOTAL_BAL,0)+NVL(T.To_Rmb_Bal,0))/(${TX_DATE}-${THIS_MONTH_BEGIN}+1) END AS RMB_MTH_TOTAL_BAL,-- 61 人民币月日均余额
		CASE WHEN ${TX_DATE}=${THIS_QUART_BEGIN} THEN NVL(T.To_Rmb_Bal,0)
			ELSE (NVL(T1.RMB_QUAR_TOTAL_BAL,0)+NVL(T.To_Rmb_Bal,0))/(${TX_DATE}-${THIS_QUART_BEGIN}+1) END AS RMB_QUAR_TOTAL_BAL,-- 62 人民币季日均余额
		CASE WHEN ${TX_DATE}=${THIS_YEAR_BEGIN} THEN NVL(T.To_Rmb_Bal,0)
			ELSE (NVL(T1.RMB_YR_TOTAL_BAL,0)+NVL(T.To_Rmb_Bal,0))/(${TX_DATE}-${THIS_YEAR_BEGIN}+1) END AS RMB_YR_TOTAL_BAL,-- 63 人民币年日均余额
		NVL(T.Recvbl_Int_Subj_ID,T1.Recvbl_Int_Subj_ID),-- 48 应收利息科目编号
		NVL(T.Recvbl_Int_Bal,T1.Recvbl_Int_Bal),-- 49 应收利息余额
		NVL(T.Main_Src_Task,T1.Main_Src_Task) -- 主要源系统任务
	FROM VT_t88_ibank_cap_biz_info_ljz T
	FULL JOIN VT_PRE_t88_ibank_cap_biz_info_ljz T1
		ON T.AGMT_ID=T1.AGMT_ID
	';
	CALL ETL.PR_EXEC_SQL(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	/*检查插入的临时表数据是否有主键错误*/
	
	-- 获取主键字段
	SELECT PHYSICAL_PRI_KEY INTO @PK_COLUMN FROM DATAMAPPING_TASK WHERE T_TAB_ENG_NAME||'_ljz'=ETL_T_TAB_ENG_NAME;
	-- 0 正常
	SET @SQL_STR = 'SELECT COUNT(1) INTO @PK_COUNT FROM(SELECT 1 FROM VT_AT_'||ETL_T_TAB_ENG_NAME||' GROUP BY '||@PK_COLUMN||' HAVING COUNT(1)>1) T';
	CALL ETL.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	IF @PK_COUNT <> 0
	THEN
		SET OUT_RES_MSG = '9999';
		UPDATE ETL.ETL_JOB_STATUS_EDW SET STEP_STATUS='Failed',STEP_ERR_LOG='主键重复', LAST_END_TIME=CURRENT_TIMESTAMP WHERE SQL_UNIT=ETL_T_TAB_ENG_NAME AND TX_DATE=TX_DATE AND STEP_NO=ETL_STEP_NO-1;
		LEAVE LABLE;
	END IF;
	
	/*通过主键检查的数据插入正式表中*/
	SET @SQL_STR='INSERT INTO PDM.'||ETL_T_TAB_ENG_NAME||' SELECT * FROM VT_AT_'||ETL_T_TAB_ENG_NAME||' WHERE STATT_DT=${TX_DATE}'; 
	CALL ETL.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

	SET OUT_RES_MSG='SUCCESSFUL';
	
END |