DELIMITER |

CREATE DEFINER="cq_sjpt"@"%" PROCEDURE "pr_f5_t88_loan_dubil"(
	OUT	OUT_RES_MSG		VARCHAR(200),
	IN	IN_TX_DATE		VARCHAR(8)
)
lable:BEGIN
/**********************************
 * whd 20210520 新建
 * whd 20210705 增加日志记录
 * whd 20210712 增加日均、累计数
 * whd 20210818 拆分为 普通贷款+联合贷+消费金融贷款 和微粒贷  两部分
 * whd 20211104 拆分为 普通贷款+联合贷、消费金融贷款、微粒贷  三部分
 * whd 20211119 微粒贷跑批日期改为T+2
 * whd 20220112 調整了普通貸款VT表的where条件，保留了贷款余额为0，但是其他合同号，到期日等信息会变化的数据
 * whd 20220119 调整了微粒贷     VT表的where条件，保留了贷款余额为0，但是其他合同号，到期日等信息会变化的数据
 * whd 20220328 微粒贷跑批日期改为T+1 微粒贷的日均计算和普通贷款合并一起
 * whd 20220703 增加原系统借据号
 * whd 20220902 原VT表今天数据不存在时取昨天AT表数据时，昨天数据不是最新的；
 *	            总体逻辑修改为：1先取贷款科目余额，2计算余额累计和均值，3以累计余额AT表作主表，以借据号关联取每个借据的最新信息
 * 贷款借据
 *******************************/
	
	DECLARE ETL_T_TAB_ENG_NAME 		VARCHAR(30)		DEFAULT 't88_loan_dubil';
	DECLARE ETL_STEP_NO				INTEGER			DEFAULT 1;
	DECLARE ETL_TX_DATE 			VARCHAR(8)		DEFAULT IN_TX_DATE;
	DECLARE PK_COUNT				BIGINT			DEFAULT 0;

	
	/*定义临时表*/
-- ETL_STEP_NO = 1 
	SET @SQL_STR = '
DROP   TABLE IF EXISTS pdm.VT_BAL_t88_loan_dubil';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
-- ETL_STEP_NO = 2 
	SET @SQL_STR = '
CREATE  TABLE pdm.VT_BAL_t88_loan_dubil LIKE pdm.t88_loan_dubil';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;


	/*用于累计、日均临时表*/
-- ETL_STEP_NO = 3 
	SET @SQL_STR = '
DROP  TABLE IF EXISTS pdm.AT_BAL_t88_loan_dubil';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
-- ETL_STEP_NO = 4 
	SET @SQL_STR = '
CREATE   TABLE pdm.AT_BAL_t88_loan_dubil LIKE pdm.t88_loan_dubil';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;	
	

-- ETL_STEP_NO = 5
	SET @SQL_STR = '
DROP  TABLE IF EXISTS pdm.AT_t88_loan_dubil';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
-- ETL_STEP_NO = 6 
	SET @SQL_STR = '
CREATE   TABLE pdm.AT_t88_loan_dubil LIKE pdm.t88_loan_dubil';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;	
		

/*今日数据插入到临时表*/
/*第一部分 传统贷款+联合贷款-取科目余额*/
-- ETL_STEP_NO = 7	
	 SET @SQL_STR = '
INSERT INTO pdm.VT_BAL_t88_loan_dubil(
		Statt_Dt				,-- 	1	统计日期
		Agmt_ID					,-- 	2	协议编号
		Dtl_Subj_ID				,-- 	12	明细科目编号
		Dubil_Bal				,-- 	71	借据余额
		To_Rmb_Dubil_Bal		,-- 	72	折人名币借据余额		
		Data_Src_Cd				 -- 	85	数据来源代码
 )
select ${TX_DATE} as Statt_Dt					,-- 	1	统计日期
		nvl(T.Agmt_Id,'''') as Agmt_ID			,-- 	2	协议编号
		nvl(t13.Subj_Id,'''') as Dtl_Subj_ID	,-- 	12	科目编号
		nvl(t13.Bal,0) as Dubil_Bal				,-- 	71	借据余额
		nvl(t13.Bal*t7.Mdl_Prc,0)				,-- 	72	折人名币借据余额
		''t03_loan_dubil_h'' as Data_Src_Cd      -- 	85	数据来源表名
FROM pdm.t03_loan_dubil_h t -- 贷款借据

LEFT JOIN pdm.t88_exchg_rate t7 -- 汇率牌价表	
	ON t.Cur_Cd = t7.Init_Cur  
	AND t7.statt_dt = ${TX_DATE}

LEFT JOIN (
	SELECT   a.Agmt_Id, 
			 CASE WHEN a.Data_Src_Cd=\'RMPS\' THEN a.Agmt_Id 
				  ELSE b.Rel_Agmt_Id 
			 END AS Rel_Agmt_Id, -- 借据号
			 a.Subj_Id, -- 科目号
			 c.Bal -- 余额
	    FROM pdm.t03_agmt_subj_rel_h a 
   LEFT JOIN pdm.t03_agmt_rel_h b
	    ON a.Agmt_Id = b.Agmt_Id
		AND a.Agmt_Cate_Cd = b.Agmt_Cate_Cd
		AND b.Agmt_Cate_Cd in (\'1101\',\'0102\')
		AND b.Agmt_Rel_Cate_Cd = \'0064\' 
		AND b.Start_Dt <= ${TX_DATE}
		AND b.End_Dt >= ${TX_DATE}
   LEFT JOIN pdm.t03_agmt_bal_h c
		ON a.Agmt_ID = c.Agmt_ID
		AND a.Agmt_Cate_Cd = c.Agmt_Cate_Cd
		AND c.Agmt_Bal_Typ_Cd in(\'06\',\'01\')
		AND c.Start_Dt <= ${TX_DATE}
		AND c.End_Dt >= ${TX_DATE}
	WHERE a.Agmt_Cate_Cd in (\'1101\',\'0102\') -- 贷款账户
		AND a.Subj_Typ_Cd = \'01\' -- 本金科目
		AND a.Subj_Id <> \'\'
		AND a.Subj_Id not like \'7%\'
		AND a.Start_Dt <= ${TX_DATE} 
		AND a.End_Dt >= ${TX_DATE}
	    AND a.Agmt_Id <> \'NCS31180072\' 
) t13
 	ON t.Agmt_Id = t13.Rel_Agmt_Id
 	AND t13.Subj_Id <> ''''
	AND t13.Bal <> 0

WHERE t.Start_Dt <= ${TX_DATE} 
  AND t.End_dt >= ${TX_DATE}
  AND (Finali_Dt =''0001-01-01'' OR Finali_Dt >= ${THIS_YEAR_BEGIN} OR t.Prod_Id=''NCM11105020'') -- 剔除无效的借据  (NCM11105020代理业务资产-公积金中心委贷本金的终结日期特殊，该业务不能按终结日期剔除无效借据)
  AND t.Prod_Id <> ''NCM11103030'' -- 微粒贷单独取
  AND t.Agmt_Cate_Cd <> ''0102'' -- 消费金融单独取
'
;

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
/*第二部分 消费金融-取科目余额*/
-- ETL_STEP_NO = 8	
	SET @SQL_STR = 	
	'INSERT INTO pdm.VT_BAL_t88_loan_dubil(
		Statt_Dt				,-- 	1	统计日期
		Agmt_ID					,-- 	2	协议编号
		Dtl_Subj_ID				,-- 	12	明细科目编号
		Dubil_Bal				,-- 	71	借据余额
		To_Rmb_Dubil_Bal		,-- 	72	折人名币借据余额
		Data_Src_Cd				 -- 	85	数据来源代码
 )
select ${TX_DATE} as Statt_Dt					,-- 	1	统计日期
		nvl(T.Agmt_Id,'''') as Agmt_ID			,-- 	2	协议编号
		nvl(t13.Subj_Id,'''') as Dtl_Subj_ID	,-- 	12	科目编号
		nvl(t13.Bal,0) as Dubil_Bal				,-- 	71	借据余额
		nvl(t13.Bal*t7.Mdl_Prc,0)				,-- 	72	折人名币借据余额
		''RMPS'' as Data_Src_Cd                  -- 	85	数据来源表名
FROM pdm.t03_loan_dubil_h t -- 贷款借据 RMPS_CQ_LOAN 消费金融部分20210222-20210710部分无数据

LEFT JOIN pdm.t88_exchg_rate t7 -- 汇率牌价表	
	ON t.Cur_Cd = t7.Init_Cur  
	AND t7.statt_dt = ${TX_DATE}

LEFT JOIN (
	SELECT   a.Agmt_Id, 
			 CASE WHEN a.Data_Src_Cd=\'RMPS\' THEN a.Agmt_Id 
				  ELSE b.Rel_Agmt_Id 
			 END AS Rel_Agmt_Id, -- 借据号
			 a.Subj_Id, -- 科目号
			 c.Bal -- 余额
	    FROM pdm.t03_agmt_subj_rel_h a 
   LEFT JOIN pdm.t03_agmt_rel_h b
	    ON a.Agmt_Id = b.Agmt_Id
		AND a.Agmt_Cate_Cd = b.Agmt_Cate_Cd
		AND b.Agmt_Cate_Cd in (\'1101\',\'0102\')
		AND b.Agmt_Rel_Cate_Cd = \'0064\' 
		AND b.Start_Dt <= ${TX_DATE}
		AND b.End_Dt >= ${TX_DATE}
   LEFT JOIN pdm.t03_agmt_bal_h c
		ON a.Agmt_ID = c.Agmt_ID
		AND a.Agmt_Cate_Cd = c.Agmt_Cate_Cd
		AND c.Agmt_Bal_Typ_Cd in(\'06\',\'01\')
		AND c.Start_Dt <= ${TX_DATE}
		AND c.End_Dt >= ${TX_DATE}
	WHERE a.Agmt_Cate_Cd in (\'1101\',\'0102\') -- 贷款账户
		AND a.Subj_Typ_Cd = \'01\' -- 本金科目
		AND a.Subj_Id <> \'\'
		AND a.Subj_Id not like \'7%\'
		AND a.Start_Dt <= ${TX_DATE} 
		AND a.End_Dt >= ${TX_DATE}
	    AND a.Agmt_Id <> \'NCS31180072\' 
) t13
 	ON t.Agmt_Id = t13.Rel_Agmt_Id
 	AND t13.Subj_Id <> ''''
	AND t13.Bal <> 0

WHERE t.Start_Dt <= ${TX_DATE} 
  AND t.End_dt >= ${TX_DATE}
  AND t.Agmt_Cate_Cd = ''0102'' -- 消费金融
'
;

    CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

	/*第三部分 微粒贷-取科目余额*/
-- ETL_STEP_NO = 9
	 SET @SQL_STR = '
INSERT INTO pdm.VT_BAL_t88_loan_dubil(
		Statt_Dt					,-- 	1	统计日期
		Agmt_ID						,-- 	2	协议编号
		Dtl_Subj_ID					,-- 	12	明细科目编号
		Dubil_Bal					,-- 	71	借据余额
		To_Rmb_Dubil_Bal			,-- 	72	折人名币借据余额
		Data_Src_Cd					 -- 	85	数据来源代码
 )
select ${TX_DATE} as Statt_Dt													,-- 	1	统计日期
		nvl(T.Agmt_Id,'''') as Agmt_ID											,-- 	2	协议编号
		CASE WHEN t.Ovrd_Days > 0 THEN ''13100001''
			 WHEN t.Core_Prod_Id = ''1220111'' THEN ''13030106''
			 WHEN t.Core_Prod_Id = ''1220211'' THEN ''13030206''
			ELSE ''''
        END as Dtl_Subj_ID														,-- 	12	科目编号
		nvl(t13.Bal,0) as Dubil_Bal												,-- 	71	借据余额
		nvl(t13.Bal*t7.Mdl_Prc,0)												,-- 	72	折人名币借据余额
		''WLD'' as Data_Src_Cd 													 -- 	85	数据来源表名
  FROM pdm.t03_loan_dubil_h t -- 贷款借据 

LEFT JOIN pdm.t03_agmt_bal_NCM_h t13 -- 微粒贷部分未入主表
 	ON t.Agmt_Id = t13.Agmt_ID
   AND t.Agmt_Cate_Cd = t13.Agmt_Cate_Cd
   AND t13.Agmt_Cate_Cd = ''0101'' -- 贷款借据
   AND t13.Agmt_Bal_Typ_Cd =''01'' -- 借据余额
   -- AND t13.bal <> 0 
   AND t13.Start_Dt <= ${TX_DATE} - 1  
   AND t13.End_Dt >= ${TX_DATE} - 1 -- 微粒贷余额取前一日期，ods表sdate和inputdate相同

LEFT JOIN pdm.t03_loan_dubil_oth_info_h t6 -- 贷款借据其他信息
	ON T.Agmt_Id = t6.Agmt_Id 
	AND t.Agmt_Cate_Cd = t6.Agmt_Cate_Cd
    AND t6.Start_Dt <= ${TX_DATE} 
	AND t6.end_dt >= ${TX_DATE}
	
LEFT JOIN pdm.t88_exchg_rate t7 -- 汇率牌价表	
	ON decode(T.Cur_Cd,'''',''CNY'',T.Cur_Cd) = t7.Init_Cur  
	AND t7.statt_dt = ${TX_DATE}
	
WHERE t.Start_dt <= ${TX_DATE} 
	AND t.End_dt >= ${TX_DATE}
	AND (Finali_Dt = CAST(''00010101'' AS DATE) OR FINALI_DT >= CAST(''20210101'' AS DATE) OR Dubil_Stat_Cd =''T'') -- 剔除无效的借据
 	AND t.Prod_Id=''NCM11103030'' -- 微粒贷
	AND (t6.Be_Abs_Ind <> ''99'' OR t6.Be_Abs_Ind ='''')
	-- AND t13.bal <> 0
'
;


	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

	/* 累计和日均处理*/
-- ETL_STEP_NO = 10
	SET @SQL_STR = '
insert into pdm.AT_BAL_t88_loan_dubil(
		Statt_Dt				,-- 	1	统计日期
		Agmt_ID					,-- 	2	协议编号
		Dtl_Subj_ID				,-- 	12	明细科目编号
		Dubil_Bal				,-- 	71	借据余额
		To_Rmb_Dubil_Bal		,-- 	72	折人名币借据余额
		Mth_Total_Bal			,-- 	73	月累积余额
		Quar_Total_Bal			,-- 	74	季累积余额
		Yr_Total_Bal			,-- 	75	年累积余额
		Rmb_Mth_Total_Bal		,-- 	76	人民币月累积余额
		Rmb_Quar_Total_Bal		,-- 	77	人民币季累积余额
		Rmb_Yr_Total_Bal		,-- 	78	人民币年累积余额
		Mth_DAvg				,-- 	79	月日均余额
		Quar_DAvg				,-- 	80	季日均余额
		Yr_DAvg					,-- 	81	年日均余额
		Rmb_Mth_DAvg			,-- 	82	人民币月日均余额
		Rmb_Quar_DAvg			,-- 	83	人民币季日均余额
		Rmb_Yr_DAvg				,-- 	84	人民币年日均余额
		Data_Src_Cd				 -- 	85	数据来源代码
 )
select  ${TX_DATE}										,-- 	1	统计日期
		COALESCE(t.Agmt_ID, b.Agmt_ID)					,-- 	2	协议编号
		COALESCE(t.Dtl_Subj_ID, b.Dtl_Subj_ID)			,-- 	12	明细科目编号
		COALESCE(t.Dubil_Bal, 0)											,-- 	71	借据余额
		COALESCE(t.To_Rmb_Dubil_Bal, 0)										,-- 	72	折人名币借据余额
		CASE WHEN 
			${TX_DATE} = ${THIS_MONTH_BEGIN} THEN COALESCE(t.Dubil_Bal,0)
		 ELSE 
			COALESCE(t.Dubil_Bal,0) + COALESCE(b.Mth_Total_Bal,0) 
		END AS Mth_Total_Bal												,-- 	73	月累积余额

		CASE WHEN 
			${TX_DATE} = ${THIS_QUART_BEGIN} THEN COALESCE(t.Dubil_Bal, 0) 
		 ELSE COALESCE(t.Dubil_Bal, 0) + COALESCE(b.Quar_Total_Bal, 0) 
		END AS Quar_Total_Bal												,-- 	74	季累积余额

		CASE WHEN 
			${TX_DATE} = ${THIS_YEAR_BEGIN} THEN COALESCE(t.Dubil_Bal, 0) 
		 ELSE 
			COALESCE(t.Dubil_Bal, 0) + COALESCE(b.Yr_Total_Bal, 0) 
		END AS Yr_Total_Bal  												,-- 	75	年累积余额

		CASE WHEN 
			${TX_DATE} = ${THIS_MONTH_BEGIN} THEN COALESCE(t.To_Rmb_Dubil_Bal,0)
		 ELSE 
			COALESCE(t.To_Rmb_Dubil_Bal,0) + COALESCE(b.Rmb_Mth_Total_Bal,0) 
		END AS Rmb_Mth_Total_Bal											,-- 	76	人民币月累积余额

		CASE WHEN 
			${TX_DATE} = ${THIS_QUART_BEGIN} THEN COALESCE(t.To_Rmb_Dubil_Bal, 0) 
		 ELSE 
			COALESCE(t.To_Rmb_Dubil_Bal, 0) + COALESCE(b.Rmb_Quar_Total_Bal, 0) 
		END AS Rmb_Quar_Total_Bal											,-- 	77	人民币季累积余额

		CASE WHEN 
			${TX_DATE} = ${THIS_YEAR_BEGIN} THEN COALESCE(t.To_Rmb_Dubil_Bal, 0) 
		 ELSE 
			COALESCE(t.To_Rmb_Dubil_Bal, 0) + COALESCE(b.Rmb_Yr_Total_Bal, 0) 
		END AS Rmb_Yr_Total_Bal												,-- 	78	人民币年累积余额

		CASE WHEN 
			${TX_DATE} = ${THIS_MONTH_BEGIN} THEN COALESCE(t.Dubil_Bal,0)
		 ELSE 
			(COALESCE(t.Dubil_Bal,0) + COALESCE(b.Mth_Total_Bal,0)) / (${TX_DATE}-${THIS_MONTH_BEGIN} + 1)
		END AS Mth_DAvg														,-- 	79	月日均余额

		CASE WHEN 
			${TX_DATE} = ${THIS_QUART_BEGIN} THEN COALESCE(t.Dubil_Bal,0)
		 ELSE 
			(COALESCE(t.Dubil_Bal,0) + COALESCE(b.Quar_Total_Bal,0)) / (${TX_DATE}-${THIS_QUART_BEGIN} + 1)
		END AS Quar_DAvg													,-- 	80	季日均余额

		CASE WHEN 
			${TX_DATE} = ${THIS_YEAR_BEGIN} THEN COALESCE(t.Dubil_Bal,0)
		 ELSE 
			(COALESCE(t.Dubil_Bal,0) + COALESCE(b.Yr_Total_Bal,0)) / (${TX_DATE}-${THIS_YEAR_BEGIN} + 1)
		END AS Yr_DAvg														,-- 	81	年日均余额

		CASE WHEN 
			${TX_DATE} = ${THIS_MONTH_BEGIN} THEN COALESCE(t.To_Rmb_Dubil_Bal,0)
		 ELSE (COALESCE(t.To_Rmb_Dubil_Bal,0) + COALESCE(b.Rmb_Mth_Total_Bal,0)) / (${TX_DATE}-${THIS_MONTH_BEGIN} + 1)
		END AS Rmb_Mth_DAvg													,-- 	82	人民币月日均余额

		CASE WHEN 
			${TX_DATE} = ${THIS_QUART_BEGIN} THEN COALESCE(t.To_Rmb_Dubil_Bal,0)
		 ELSE 
			(COALESCE(t.To_Rmb_Dubil_Bal,0) + COALESCE(b.Rmb_Quar_Total_Bal,0)) / (${TX_DATE}-${THIS_QUART_BEGIN} + 1)
		END AS Rmb_Quar_DAvg												,-- 	83	人民币季日均余额

		CASE WHEN 
			${TX_DATE} = ${THIS_YEAR_BEGIN} THEN COALESCE(t.To_Rmb_Dubil_Bal,0)
		 ELSE 
			(COALESCE(t.To_Rmb_Dubil_Bal,0) + COALESCE(b.Rmb_Yr_Total_Bal,0)) / (${TX_DATE}-${THIS_YEAR_BEGIN} + 1)
		END AS Rmb_Yr_DAvg													,-- 	84	人民币年日均余额
		COALESCE(t.Data_Src_Cd, b.Data_Src_Cd)								 -- 	85	数据来源代码
FROM pdm.VT_BAL_t88_loan_dubil t 

 FULL JOIN (
	SELECT * FROM PDM.t88_loan_dubil 
		WHERE STATT_DT = ${LAST_TX_DATE} 
		AND (
			  CASE WHEN ${TX_DATE} = ${THIS_YEAR_BEGIN}
				THEN 0
				ELSE 1
			  END
			) = 1       
          ) b -- 昨日数据

	ON t.Agmt_ID = b.Agmt_ID  -- 以主键作on条件
';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
-- 最终结果临时表	
/*第一部分 传统贷款+联合贷款*/
-- ETL_STEP_NO = 11	
	 SET @SQL_STR = '
INSERT INTO pdm.AT_t88_loan_dubil(
		Statt_Dt				,-- 	1	统计日期
		Agmt_ID					,-- 	2	协议编号
		Dubil_Id					,-- 86 借据编号
		Cust_ID					,-- 	3	客户编号
		Cust_Nm					,-- 	4	客户名称
		Loan_Contr_Num			,-- 	5	贷款合同号
		Prod_ID					,-- 	6	产品编号
		Cur_Cd					,-- 	7	币种代码
		Bank_Org_Cd				,-- 	8	银行机构代码
		Fin_Lics				,-- 	9	金融许可证号
		Mgmt_Org_ID				,-- 	10	管理机构编号
		Acct_Org_ID				,-- 	11	账务机构编号
		Dtl_Subj_ID				,-- 	12	明细科目编号
		Dtl_Subj_Nm				,-- 	13	明细科目名称
		Acct_Org_Nm				,-- 	14	账户机构名称
		Mgmt_Org_Nm				,-- 	15	管理机构名称
		Int_Rate_Cate_Cd		,-- 	16	利率类型代码
		Int_Rate_Flt_Mod_Cd		,-- 	17	利率浮动方式代码
		Int_Base_Cd				,-- 	18	计息基础代码
		Base_Int_Rate			,-- 	19	基准利率
		Exec_Int_Rate			,-- 	20	执行利率
		Int_Rate_Flt			,-- 	21	利率浮动值
		Pric_Cate_Cd			,-- 	22	定价类型代码
		Dubil_Amt				,-- 	23	借据金额
		Loan_Term				,-- 	24	贷款期限
		Expd_Ind				,-- 	25	展期标志
		Expd_Cnt				,-- 	26	展期次数
		Expd_Happ_Dt			,-- 	27	展期发生日期
		Expd_Matr_Dt			,-- 	28	展期到期日期
		Expd_Amt				,-- 	29	展期金额
		Expd_Int_Rate			,-- 	30	展期利率
		Loan_Actl_Distr_Dt		,-- 	31	贷款实际发放日期
		Loan_Init_Matr_Dt		,-- 	32	贷款原始到期日期
		Loan_Actl_Matr_Dt		,-- 	33	贷款实际到期日期
		In_Bal_Debt_Int_Bal		,-- 	34	表内欠息余额
		Off_Bal_Debt_Int_Bal	,-- 	35	表外欠息余额
		End_Dt					,-- 	36	终结日期
		Loan_Ent_Acct_Acct_Num	,-- 	37	贷款入账账号
		Repay_Acct_Num			,-- 	38	还款账号
		Loan_Stat_Cd			,-- 	39	贷款状态代码
		Loan_Fiv_Cls_Cd			,-- 	40	贷款五级分类代码
		Loan_Ten_Sec_Cls_Cd		,-- 	41	贷款十二级分类代码
		Repay_Mod_Cd			,-- 	42	还款方式代码
		Int_Prd_Cd				,-- 	43	计息周期代码
		Main_Guar_Mod_Cd		,-- 	44	主担保方式代码
		Crdt_Mem_Nm				,-- 	45	信贷员姓名
		Crdt_Mem_Emp_Num		,-- 	46	信贷员员工号
		Shd_Pay_Unreturn_Pnsh_Int	,-- 	47	应还未还罚息
		Shd_Pay_Unreturn_Cmpd_Int	,-- 	48	应还未还复利
		Shd_Pay_Unreturn_Ovrd_Int	,-- 	49	应还未还逾期利息
		Shd_Pay_Unreturn_Norm_Int	,-- 	50	应还未还正常利息
		Prin_Ovrd_Days				,-- 	51	本金逾期天数
		Prin_Ovrd_Bal				,-- 	52	本金逾期余额
		Int_Ovrd_Days				,-- 	53	利息逾期天数
		Int_Ovrd_Bal				,-- 	54	利息逾期余额
		End_Cate_Cd					,-- 	55	终结类型代码
		Wrtoff_Dt					,-- 	56	核销日期
		Wrtoff_Amt					,-- 	57	核销金额
		Wrtoff_Int					,-- 	58	核销利息
		Claim_Settle_Totl_Amt		,-- 	59	理赔总金额
		Claim_Settle_Prin			,-- 	60	理赔本金
		Accm_Owe_Money_Prd_Cnt		,-- 	61	累计欠款期数
		Cont_Owe_Money_Prd_Cnt		,-- 	62	连续欠款期数
		Fst_Term_Sht_Term_Is_Merg	,-- 	63	首期不足期是否合并
		End_Term_Is_Merg			,-- 	64	末期是否合并
		ABS_Pkg_Ind					,-- 	65	出表状态（资产证券化打包标志）
		Int_Rate_Cd					,-- 	66	利率代码 
		Int_Rate_Term				,-- 	67	利率期限
		Int_Rate_Term_Prd_Cd		,-- 	68	利率期限周期代码
		Loan_Sbsd_Cd				,-- 	69	贷款贴息代码
		Exchg_Rate					,-- 	70	汇率
		Dubil_Bal					,-- 	71	借据余额
		To_Rmb_Dubil_Bal			,-- 	72	折人名币借据余额
		Mth_Total_Bal				,-- 	73	月累积余额
		Quar_Total_Bal				,-- 	74	季累积余额
		Yr_Total_Bal				,-- 	75	年累积余额
		Rmb_Mth_Total_Bal			,-- 	76	人民币月累积余额
		Rmb_Quar_Total_Bal			,-- 	77	人民币季累积余额
		Rmb_Yr_Total_Bal			,-- 	78	人民币年累积余额
		Mth_DAvg					,-- 	79	月日均余额
		Quar_DAvg					,-- 	80	季日均余额
		Yr_DAvg						,-- 	81	年日均余额
		Rmb_Mth_DAvg				,-- 	82	人民币月日均余额
		Rmb_Quar_DAvg				,-- 	83	人民币季日均余额
		Rmb_Yr_DAvg					,-- 	84	人民币年日均余额
		Data_Src_Cd					 -- 	85	数据来源代码
 )
select ${TX_DATE} as Statt_Dt					,-- 	1	统计日期
		AT.Agmt_Id as Agmt_ID			,-- 	2	协议编号
		case when t.Data_Src_Cd =\'NCM\'  then substring(t.Agmt_Id,4,100)
			   when t.Data_Src_Cd =\'RMPS\' then substring(t.Agmt_Id,5,100) 
               else t.Agmt_Id end				,-- 	86 借据编号
		nvl(t.Cust_Id,'''') as Cust_ID			,-- 	3	客户编号
		nvl(t1.Cust_Nm,''@'')	as Cust_Nm		,-- 	4	客户名称
		nvl(t.Contr_Agmt_Id,'''')	as Loan_Contr_Num	,-- 	5	贷款合同号
		nvl(T.Prod_Id,'''')	as Prod_ID					,-- 	6	产品编号
		nvl(T.Cur_Cd,'''')	as Cur_Cd					,-- 	7	币种代码
		nvl(t2.Fin_Org_Cd,'''') as Bank_Org_Cd			,-- 	8	金融机构编码
		nvl(t2.Fin_Lics,'''')	as Fin_Lics				,-- 	9	金融许可证号
		nvl(T.Pln_Mgmt_Org_Id,'''') as Mgmt_Org_ID		,-- 	10	贷后管理机构编号
		nvl(t.Ent_Acct_Org_Id,'''') as Acct_Org_ID		,-- 	11	上账机构编号	
		nvl(AT.Dtl_Subj_Id,'''') as Dtl_Subj_ID			,-- 	12	科目编号
		nvl(t4.Subj_Nm,'''') as Dtl_Subj_Nm				,-- 	13	科目名称
		nvl(t3.Org_Nm,'''') as Acct_Org_Nm				,-- 	14	机构名称 账户机构名称
		nvl(t2.Org_Nm,'''') as Mgmt_Org_Nm				,-- 	15	机构名称 管理机构名称
		nvl(t.Int_Rate_Adj_Mod_Cd,'''') as Int_Rate_Cate_Cd		,-- 	16	利率调整方式代码 1=固定/其他=浮动
		nvl(t.Int_Rate_Flt_Mod_Cd,'''') as Int_Rate_Flt_Mod_Cd	,-- 	17	利率浮动方式代码  浮动比例/浮动点差
		nvl(t9.Int_Mod_Cd,'''') as Int_Base_Cd					,-- 	18	计息基础代码 t03_loan_dubil增加business_contract.INTERESTACCRUALTYPE计息方式字段，基础层没取 ACT/360这种
		nvl(T.Base_Int_Rate,0) as Base_Int_Rate					,-- 	19	基准利率
		nvl(t.Exec_Yr_Int_Rate,0) as Exec_Int_Rate				,-- 	20	执行年利率
		nvl(t.Int_Rate_Flt,0) as Int_Rate_Flt					,-- 	21	利率浮动值
		nvl(t.Base_Int_Rate_Cate_Cd,'''') as Pric_Cate_Cd		,-- 	22	定价类型代码 基准利率类型代码 LPR利率/央行基准利率  贷款合同表.BASERATETYPE, 100, LPR, YHJZ （这样分的？）
		nvl(T.Dubil_Amt,0) as Dubil_Amt							,-- 	23	借据金额
		nvl(T.Term_Mth,0) as Loan_Term							,-- 	24	期限月
		case when T.Expd_Cnt >0 then 1 else 0 end as Expd_Ind 	,-- 	25	展期标志   1:是 0：否
		nvl(T.Expd_Cnt,0) as Expd_Cnt							,-- 	26	展期次数
		nvl(T.Expd_Happ_Dt,''0001-01-01'') as Expd_Happ_Dt		,-- 	27	展期发生日期 
		nvl(T.Expd_Matr_Dt,''0001-01-01'') as Expd_Matr_Dt		,-- 	28	展期到期日期
		nvl(T.Expd_Amt,0) as Expd_Amt							,-- 	29	展期金额
		nvl(T.Expd_Int_Rate,0) as Expd_Int_Rate					,-- 	30	展期利率
		nvl(T.Distr_Dt,''0001-01-01'') as Loan_Actl_Distr_Dt	,-- 	31	放款日期
		''0001-01-01'' as Loan_Init_Matr_Dt						,-- 	32	贷款原始到期日期
		nvl(T.Actl_Matr_Dt,''0001-01-01'') as Loan_Actl_Matr_Dt	,-- 	33	实际到期日期
		nvl(T14.bal,0) as In_Bal_Debt_Int_Bal					,-- 	34	表内欠息余额
		nvl(T15.bal,0) as Off_Bal_Debt_Int_Bal					,-- 	35	表外欠息余额
		nvl(T.End_Dt,''0001-01-01'') as End_Dt					,-- 	36	终结日期
		nvl(T.Core_Loan_Acct_Num,'''') as Loan_Ent_Acct_Acct_Num,-- 	37	核心贷款账号
		nvl(T.Repay_Acct_Num,'''') as Repay_Acct_Num			,-- 	38	还款帐号
		nvl(T.Dubil_Stat_Cd,'''') as Loan_Stat_Cd				,-- 	39	借据状态代码
		nvl(T.Fiv_Cls_Cd,'''') as Loan_Fiv_Cls_Cd				,-- 	40	五级分类代码
		nvl(t.Ten_Sec_Cls_Cd,'''') as Loan_Ten_Sec_Cls_Cd		,-- 	41	十二级分类代码
		nvl(T.Repay_Mod_Cd,'''') as Repay_Mod_Cd				,-- 	42	还款方式代码
		'''' as Int_Prd_Cd										,-- 	43	计息周期代码 按年/月 计息
		nvl(T.Guar_Mod_Cd,'''') as Main_Guar_Mod_Cd				,-- 	44	担保方式代码
		nvl(t5.Tellr_Nm,'''')	 as Crdt_Mem_Nm					,-- 	45	柜员名称
		nvl(T.Pln_Mgmt_Pers_Mem_Id,'''') as Crdt_Mem_Emp_Num 	,-- 	46	贷后管理人员编号
		nvl(T.Shd_Pay_Unreturn_Pnsh_Int,0) as Shd_Pay_Unreturn_Pnsh_Int	,-- 	47	应还未还罚息
		nvl(T.Shd_Pay_Unreturn_Cmpd_Int,0) as Shd_Pay_Unreturn_Cmpd_Int	,-- 	48	应还未还复利
		nvl(T.Shd_Pay_Unreturn_Ovrd_Int,0) as Shd_Pay_Unreturn_Ovrd_Int	,-- 	49	应还未还逾期利息
		nvl(T.Shd_Pay_Unreturn_Norm_Int,0) as Shd_Pay_Unreturn_Norm_Int	,-- 	50	应还未还正常利息
		nvl(T.Ovrd_Days,0) as Prin_Ovrd_Days							,-- 	51	逾期天数
		CASE WHEN AT.Dtl_Subj_Id = ''13100000'' THEN AT.Dubil_Bal ELSE 0 END	,-- 	52	逾期余额
		nvl(t.Debt_Int_Days,0) as Int_Ovrd_Days							,-- 	53	欠息天数
		nvl(t14.bal,0) + nvl(t15.bal,0) as Int_Ovrd_Bal					,-- 	54	利息逾期余额 表内欠息余额+表外欠息余额
		nvl(t6.End_Cate_Cd,'''') as End_Cate_Cd 						,-- 	55 	 终结类型代码	终结类型代码 060=核销  
		nvl(t.End_Dt,''0001-01-01'') as Wrtoff_Dt 						,-- 	56	终结日期  终结类型=核销时，终结日期=核销日期
		nvl(T.Wrtoff_Amt,0) as Wrtoff_Amt								,-- 	57	核销金额
		nvl(T.Wrtoff_Int,0) as Wrtoff_Int								,-- 	58	核销利息
		nvl(T.Claim_Settle_Totl_Amt,0) as Claim_Settle_Totl_Amt			,-- 	59	我行理赔总金额
		nvl(T.Claim_Settle_Prin,0) as Claim_Settle_Prin					,-- 	60	我行理赔本金 
		nvl(t6.Accm_Owe_Money_Prd_Cnt,0) as Accm_Owe_Money_Prd_Cnt		,-- 	61	累计欠款期数
		nvl(t6.Cont_Owe_Money_Prd_Cnt,0) as Cont_Owe_Money_Prd_Cnt		,-- 	62	连续欠款期数
		nvl(t8.Fst_Term_Sht_Is_Merg,'''') as Fst_Term_Sht_Term_Is_Merg 	,--  	63	首期不足期是否合并  0-不合并,1-合并
		nvl(t8.End_Term_Merg,'''') as End_Term_Is_Merg 					,-- 	64	末期是否合并  Y-合并,N-不合并
		nvl(t8.Acct_Cate_Cd,'''') as ABS_Pkg_Ind  						,-- 	65	出表状态（资产证券化打包标志） 账户类型代码 acct_type,‘L‘资产转让成功；“E”委托贷款
		nvl(t10.Ref_Int_Rate_Cate_Cd,'''') as Int_Rate_Cd				,-- 	66 	利率类型代码
		'''' as Int_Rate_Term											,-- 	67 	利率期限
		'''' as Int_Rate_Term_Prd_Cd									,-- 	68 	利率期限周期代码
		nvl(t11.Loan_Sbsd_Cd,'''')										,-- 	69	贷款贴息代码
		nvl(t7.Mdl_Prc,1) as Exchg_Rate									,-- 	70	中间价
		nvl(AT.Dubil_Bal,0) as Dubil_Bal								,-- 	71	借据余额
		nvl(AT.To_Rmb_Dubil_Bal,0)	as Dubil_Bal						,-- 	72	折人名币借据余额
		AT.Mth_Total_Bal												,-- 	73	月累积余额
		AT.Quar_Total_Bal												,-- 	74	季累积余额
		AT.Yr_Total_Bal													,-- 	75	年累积余额
		AT.Rmb_Mth_Total_Bal											,-- 	76	人民币月累积余额
		AT.Rmb_Quar_Total_Bal											,-- 	77	人民币季累积余额
		AT.Rmb_Yr_Total_Bal												,-- 	78	人民币年累积余额
		AT.Mth_DAvg														,-- 	79	月日均余额
		AT.Quar_DAvg													,-- 	80	季日均余额
		AT.Yr_DAvg														,-- 	81	年日均余额
		AT.Rmb_Mth_DAvg													,-- 	82	人民币月日均余额
		AT.Rmb_Quar_DAvg												,-- 	83	人民币季日均余额
		AT.Rmb_Yr_DAvg													,-- 	84	人民币年日均余额
		''t03_loan_dubil_h'' as Data_Src_Cd  							 -- 	85	数据来源表名
FROM PDM.AT_BAL_t88_loan_dubil AT

LEFT JOIN pdm.t03_loan_dubil_h t -- 贷款借据
	 ON AT.Agmt_ID = T.Agmt_ID
	AND T.Start_Dt <= ${TX_DATE}
	AND T.End_Dt >= ${TX_DATE}

LEFT JOIN PDM.t03_agmt_bal_h T14
	 ON T.Agmt_Id = T14.Agmt_Id
	AND T.Agmt_Cate_Cd = T14.Agmt_Cate_Cd
	AND T14.Start_Dt <= ${TX_DATE}
	AND T14.End_Dt >= ${TX_DATE}
	AND T14.Agmt_Bal_Typ_Cd = ''04'' -- 表内欠息余额 监管系统取的busienss_duebill,这里取的business_history

LEFT JOIN PDM.t03_agmt_bal_h T15
	ON T.Agmt_Id = T15.Agmt_Id
	AND T.Agmt_Cate_Cd = T15.Agmt_Cate_Cd
	AND T15.Start_Dt <= ${TX_DATE}
	AND T15.End_Dt >= ${TX_DATE}
	AND T15.Agmt_Bal_Typ_Cd = ''05'' -- 表外欠息余额 监管系统取的busienss_duebill,这里取的business_history


LEFT JOIN pdm.t01_cust_h t1 -- 客户历史
	ON t.Cust_Id = t1.Cust_Id 
	AND t1.Start_Dt <= ${TX_DATE}  
	AND t1.End_Dt >= ${TX_DATE}

LEFT JOIN pdm.t04_org t2 -- 机构
	ON t.Pln_Mgmt_Org_Id = t2.Org_Id 
	AND t2.Data_Src_Cd =''NCS'' 

LEFT JOIN pdm.t04_org t3 
	ON t.Ent_Acct_Org_Id = t3.Org_Id 
	AND t3.Data_Src_Cd =''NCM'' 

LEFT JOIN pdm.t10_gl_bas_info_h t4 -- 会计科目基本信息历史 ods只有20210714号以后有数据
	ON AT.Dtl_Subj_Id = t4.Subj_Id 
	AND t4.Start_Dt <= ${TX_DATE}  
	AND t4.End_Dt >= ${TX_DATE}

LEFT JOIN pdm.t04_core_tellr t5 -- 核心柜员
	ON T.Pln_Mgmt_Pers_Mem_Id = t5.Tellr_Id 
	AND t5.Statt_Dt = ${TX_DATE}

LEFT JOIN pdm.t03_loan_dubil_oth_info_h t6 -- 贷款借据其他信息
	ON T.Agmt_Id = t6.Agmt_Id
	AND T.Agmt_Cate_Cd = t6.Agmt_Cate_Cd 
	AND t6.Start_Dt <= ${TX_DATE} 
	AND t6.End_dt >= ${TX_DATE}

LEFT JOIN pdm.t88_exchg_rate t7 -- 汇率牌价表	
	ON t.Cur_Cd = t7.Init_Cur  
	AND t7.statt_dt = ${TX_DATE}

LEFT JOIN pdm.t03_agmt_rel_h t12
	ON t.Agmt_Id = t12.Agmt_Id
	AND t.Agmt_Cate_Cd = t12.Agmt_Cate_Cd
	AND t12.agmt_rel_cate_cd = ''0002'' -- 借据与出账关系ncm
	AND t12.Start_Dt <= ${TX_DATE} 
	AND t12.End_Dt >= ${TX_DATE}
	
LEFT JOIN pdm.t03_loan_distr_h t11 
	ON t12.Rel_Agmt_Id = t11.Agmt_Id 
	AND t11.Start_Dt <= ${TX_DATE} 
	AND t11.End_Dt >= ${TX_DATE}

LEFT JOIN pdm.t03_loan_contr_h t9
	ON t.Contr_Agmt_Id = t9.Src_Sys_Contr_Id
	AND T.DATA_SRC_CD=T9.DATA_SRC_CD
	AND t9.Start_Dt <= ${TX_DATE} 
	AND t9.End_Dt >= ${TX_DATE}

LEFT JOIN 
(select Dubil_Agmt_Id,Agmt_Id,Agmt_Cate_Cd,Fst_Term_Sht_Is_Merg,End_Term_Merg,Acct_Cate_Cd,
	row_number() over(partition by Dubil_Agmt_Id order by Dubil_Agmt_Id) rm
	from pdm.t03_loan_acct_h 
	where Start_Dt <= ${TX_DATE} 
	and  End_Dt >= ${TX_DATE}
) T8 -- 贷款账户历史  NCS_MB_AGREEMENT_LOAN.INTERNAL_KEY=34156854有两笔记录 关联会重复
	ON T.Agmt_Id = T8.Dubil_Agmt_Id
	-- and T.Agmt_Cate_Cd = T8.Agmt_Cate_Cd
	AND T8.rm=1

LEFT JOIN pdm.t03_agmt_rate_h t10
	ON t8.Agmt_Id = t10.Agmt_Id 	
	-- and t8.Agmt_Cate_Cd = t10.Agmt_Cate_Cd
	AND t10.Agmt_Rate_Typ_Cd = ''01''
	AND t10.start_dt <= ${TX_DATE}
	AND t10.end_dt >= ${TX_DATE}

WHERE AT.Data_Src_Cd = ''t03_loan_dubil_h'' 
'
;

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
/*第二部分 消费金融*/
-- ETL_STEP_NO = 12	
	SET @SQL_STR = 	
	'INSERT INTO pdm.AT_t88_loan_dubil(
		Statt_Dt				,-- 	1	统计日期
		Agmt_ID					,-- 	2	协议编号
		Dubil_Id					,-- 	86 借据编号
		Cust_ID					,-- 	3	客户编号
		Cust_Nm					,-- 	4	客户名称
		Loan_Contr_Num			,-- 	5	贷款合同号
		Prod_ID					,-- 	6	产品编号
		Cur_Cd					,-- 	7	币种代码
		Bank_Org_Cd				,-- 	8	银行机构代码
		Fin_Lics				,-- 	9	金融许可证号
		Mgmt_Org_ID				,-- 	10	管理机构编号
		Acct_Org_ID				,-- 	11	账务机构编号
		Dtl_Subj_ID				,-- 	12	明细科目编号
		Dtl_Subj_Nm				,-- 	13	明细科目名称
		Acct_Org_Nm				,-- 	14	账户机构名称
		Mgmt_Org_Nm				,-- 	15	管理机构名称
		Int_Rate_Cate_Cd		,-- 	16	利率类型代码
		Int_Rate_Flt_Mod_Cd		,-- 	17	利率浮动方式代码
		Int_Base_Cd				,-- 	18	计息基础代码
		Base_Int_Rate			,-- 	19	基准利率
		Exec_Int_Rate			,-- 	20	执行利率
		Int_Rate_Flt			,-- 	21	利率浮动值
		Pric_Cate_Cd			,-- 	22	定价类型代码
		Dubil_Amt				,-- 	23	借据金额
		Loan_Term				,-- 	24	贷款期限
		Expd_Ind				,-- 	25	展期标志
		Expd_Cnt				,-- 	26	展期次数
		Expd_Happ_Dt			,-- 	27	展期发生日期
		Expd_Matr_Dt			,-- 	28	展期到期日期
		Expd_Amt				,-- 	29	展期金额
		Expd_Int_Rate			,-- 	30	展期利率
		Loan_Actl_Distr_Dt		,-- 	31	贷款实际发放日期
		Loan_Init_Matr_Dt		,-- 	32	贷款原始到期日期
		Loan_Actl_Matr_Dt		,-- 	33	贷款实际到期日期
		In_Bal_Debt_Int_Bal		,-- 	34	表内欠息余额
		Off_Bal_Debt_Int_Bal	,-- 	35	表外欠息余额
		End_Dt					,-- 	36	终结日期
		Loan_Ent_Acct_Acct_Num	,-- 	37	贷款入账账号
		Repay_Acct_Num			,-- 	38	还款账号
		Loan_Stat_Cd			,-- 	39	贷款状态代码
		Loan_Fiv_Cls_Cd			,-- 	40	贷款五级分类代码
		Loan_Ten_Sec_Cls_Cd		,-- 	41	贷款十二级分类代码
		Repay_Mod_Cd			,-- 	42	还款方式代码
		Int_Prd_Cd				,-- 	43	计息周期代码
		Main_Guar_Mod_Cd		,-- 	44	主担保方式代码
		Crdt_Mem_Nm				,-- 	45	信贷员姓名
		Crdt_Mem_Emp_Num		,-- 	46	信贷员员工号
		Shd_Pay_Unreturn_Pnsh_Int	,-- 	47	应还未还罚息
		Shd_Pay_Unreturn_Cmpd_Int	,-- 	48	应还未还复利
		Shd_Pay_Unreturn_Ovrd_Int	,-- 	49	应还未还逾期利息
		Shd_Pay_Unreturn_Norm_Int	,-- 	50	应还未还正常利息
		Prin_Ovrd_Days				,-- 	51	本金逾期天数
		Prin_Ovrd_Bal				,-- 	52	本金逾期余额
		Int_Ovrd_Days				,-- 	53	利息逾期天数
		Int_Ovrd_Bal				,-- 	54	利息逾期余额
		End_Cate_Cd					,-- 	55	终结类型代码
		Wrtoff_Dt					,-- 	56	核销日期
		Wrtoff_Amt					,-- 	57	核销金额
		Wrtoff_Int					,-- 	58	核销利息
		Claim_Settle_Totl_Amt		,-- 	59	理赔总金额
		Claim_Settle_Prin			,-- 	60	理赔本金
		Accm_Owe_Money_Prd_Cnt		,-- 	61	累计欠款期数
		Cont_Owe_Money_Prd_Cnt		,-- 	62	连续欠款期数
		Fst_Term_Sht_Term_Is_Merg	,-- 	63	首期不足期是否合并
		End_Term_Is_Merg			,-- 	64	末期是否合并
		ABS_Pkg_Ind					,-- 	65	出表状态（资产证券化打包标志）
		Int_Rate_Cd					,-- 	66	利率代码 
		Int_Rate_Term				,-- 	67	利率期限
		Int_Rate_Term_Prd_Cd		,-- 	68	利率期限周期代码
		Loan_Sbsd_Cd				,-- 	69	贷款贴息代码
		Exchg_Rate					,-- 	70	汇率
		Dubil_Bal					,-- 	71	借据余额
		To_Rmb_Dubil_Bal			,-- 	72	折人名币借据余额
		Mth_Total_Bal				,-- 	73	月累积余额
		Quar_Total_Bal				,-- 	74	季累积余额
		Yr_Total_Bal				,-- 	75	年累积余额
		Rmb_Mth_Total_Bal			,-- 	76	人民币月累积余额
		Rmb_Quar_Total_Bal			,-- 	77	人民币季累积余额
		Rmb_Yr_Total_Bal			,-- 	78	人民币年累积余额
		Mth_DAvg					,-- 	79	月日均余额
		Quar_DAvg					,-- 	80	季日均余额
		Yr_DAvg						,-- 	81	年日均余额
		Rmb_Mth_DAvg				,-- 	82	人民币月日均余额
		Rmb_Quar_DAvg				,-- 	83	人民币季日均余额
		Rmb_Yr_DAvg					,-- 	84	人民币年日均余额
		Data_Src_Cd					 -- 	85	数据来源代码
 )
select ${TX_DATE} as Statt_Dt					,-- 	1	统计日期
		AT.Agmt_Id as Agmt_ID			,-- 	2	协议编号
		case when t.Data_Src_Cd =\'NCM\'  then substring(t.Agmt_Id,4,100)
			   when t.Data_Src_Cd =\'RMPS\' then substring(t.Agmt_Id,5,100) 
               else t.Agmt_Id end				,-- 	86 借据编号
		nvl(t.Cust_Id,'''') as Cust_ID			,-- 	3	客户编号
		nvl(t1.Cust_Nm,''@'')	as Cust_Nm		,-- 	4	客户名称
		nvl(t.Contr_Agmt_Id,'''')	as Loan_Contr_Num	,-- 	5	贷款合同号
		nvl(T.Prod_Id,'''')	as Prod_ID					,-- 	6	产品编号
		nvl(T.Cur_Cd,'''')	as Cur_Cd					,-- 	7	币种代码
		nvl(t2.Fin_Org_Cd,'''') as Bank_Org_Cd			,-- 	8	金融机构编码
		nvl(t2.Fin_Lics,'''')	as Fin_Lics				,-- 	9	金融许可证号
		nvl(T.Pln_Mgmt_Org_Id,'''') as Mgmt_Org_ID		,-- 	10	贷后管理机构编号
		nvl(t.Ent_Acct_Org_Id,'''') as Acct_Org_ID		,-- 	11	上账机构编号	
		nvl(AT.Dtl_Subj_Id,'''') as Dtl_Subj_ID				,-- 	12	科目编号
		nvl(t4.Subj_Nm,'''') as Dtl_Subj_Nm				,-- 	13	科目名称
		nvl(t3.Org_Nm,'''') as Acct_Org_Nm				,-- 	14	机构名称 账户机构名称
		nvl(t2.Org_Nm,'''') as Mgmt_Org_Nm				,-- 	15	机构名称 管理机构名称
		nvl(t.Int_Rate_Adj_Mod_Cd,'''') as Int_Rate_Cate_Cd		,-- 	16	利率调整方式代码 1=固定/其他=浮动
		nvl(t.Int_Rate_Flt_Mod_Cd,'''') as Int_Rate_Flt_Mod_Cd	,-- 	17	利率浮动方式代码  浮动比例/浮动点差
		nvl(t9.Int_Mod_Cd,'''') as Int_Base_Cd					,-- 	18	计息基础代码 t03_loan_dubil增加business_contract.INTERESTACCRUALTYPE计息方式字段，基础层没取 ACT/360这种
		nvl(T.Base_Int_Rate,0) as Base_Int_Rate					,-- 	19	基准利率
		nvl(t.Exec_Yr_Int_Rate,0) as Exec_Int_Rate				,-- 	20	执行年利率
		nvl(t.Int_Rate_Flt,0) as Int_Rate_Flt					,-- 	21	利率浮动值
		nvl(t.Base_Int_Rate_Cate_Cd,'''') as Pric_Cate_Cd		,-- 	22	定价类型代码 基准利率类型代码 LPR利率/央行基准利率  贷款合同表.BASERATETYPE, 100, LPR, YHJZ （这样分的？）
		nvl(T.Dubil_Amt,0) as Dubil_Amt							,-- 	23	借据金额
		nvl(T.Term_Mth,0) as Loan_Term							,-- 	24	期限月
		case when T.Expd_Cnt >0 then 1 else 0 end as Expd_Ind 	,-- 	25	展期标志   1:是 0：否
		nvl(T.Expd_Cnt,0) as Expd_Cnt							,-- 	26	展期次数
		nvl(T.Expd_Happ_Dt,''0001-01-01'') as Expd_Happ_Dt				,-- 	27	展期发生日期 
		nvl(T.Expd_Matr_Dt,''0001-01-01'') as Expd_Matr_Dt				,-- 	28	展期到期日期
		nvl(T.Expd_Amt,0) as Expd_Amt							,-- 	29	展期金额
		nvl(T.Expd_Int_Rate,0) as Expd_Int_Rate					,-- 	30	展期利率
		nvl(T.Distr_Dt,''0001-01-01'') as Loan_Actl_Distr_Dt				,-- 	31	放款日期
		''0001-01-01'' as Loan_Init_Matr_Dt						,-- 	32	贷款原始到期日期
		nvl(T.Actl_Matr_Dt,''0001-01-01'') as Loan_Actl_Matr_Dt			,-- 	33	实际到期日期
		nvl(T14.bal,0) as In_Bal_Debt_Int_Bal					,-- 	34	表内欠息余额
		nvl(T15.bal,0) as Off_Bal_Debt_Int_Bal					,-- 	35	表外欠息余额
		nvl(T.End_Dt,''0001-01-01'') as End_Dt							,-- 	36	终结日期
		nvl(T.Core_Loan_Acct_Num,'''') as Loan_Ent_Acct_Acct_Num,-- 	37	核心贷款账号
		nvl(T.Repay_Acct_Num,'''') as Repay_Acct_Num			,-- 	38	还款帐号
		nvl(T.Dubil_Stat_Cd,'''') as Loan_Stat_Cd				,-- 	39	借据状态代码
		nvl(T.Fiv_Cls_Cd,'''') as Loan_Fiv_Cls_Cd				,-- 	40	五级分类代码
		nvl(t.Ten_Sec_Cls_Cd,'''') as Loan_Ten_Sec_Cls_Cd		,-- 	41	十二级分类代码
		nvl(T.Repay_Mod_Cd,'''') as Repay_Mod_Cd				,-- 	42	还款方式代码
		'''' as Int_Prd_Cd										,-- 	43	计息周期代码 按年/月 计息
		nvl(T.Guar_Mod_Cd,'''') as Main_Guar_Mod_Cd				,-- 	44	担保方式代码
		nvl(t5.Tellr_Nm,'''')	 as Crdt_Mem_Nm					,-- 	45	柜员名称
		nvl(T.Pln_Mgmt_Pers_Mem_Id,'''') as Crdt_Mem_Emp_Num 	,-- 	46	贷后管理人员编号
		nvl(T.Shd_Pay_Unreturn_Pnsh_Int,0) as Shd_Pay_Unreturn_Pnsh_Int	,-- 	47	应还未还罚息
		nvl(T.Shd_Pay_Unreturn_Cmpd_Int,0) as Shd_Pay_Unreturn_Cmpd_Int	,-- 	48	应还未还复利
		nvl(T.Shd_Pay_Unreturn_Ovrd_Int,0) as Shd_Pay_Unreturn_Ovrd_Int	,-- 	49	应还未还逾期利息
		nvl(T.Shd_Pay_Unreturn_Norm_Int,0) as Shd_Pay_Unreturn_Norm_Int	,-- 	50	应还未还正常利息
		nvl(T.Ovrd_Days,0) as Prin_Ovrd_Days							,-- 	51	逾期天数
		-- nvl(T16.bal,0) as Prin_Ovrd_Bal								,-- 	52	逾期余额
		CASE WHEN AT.Dtl_Subj_Id = ''13100000'' THEN AT.Dubil_Bal ELSE 0 END	,-- 	52	逾期余额
		nvl(t.Debt_Int_Days,0) as Int_Ovrd_Days							,-- 	53	欠息天数
		nvl(t14.bal,0) + nvl(t15.bal,0) as Int_Ovrd_Bal					,-- 	54	利息逾期余额 表内欠息余额+表外欠息余额
		'''', -- nvl(t6.End_Cate_Cd,'''') as End_Cate_Cd 				,-- 	55 	 终结类型代码	终结类型代码 060=核销  
		nvl(t.End_Dt,''0001-01-01'') as Wrtoff_Dt 								,-- 	56	终结日期  终结类型=核销时，终结日期=核销日期
		nvl(T.Wrtoff_Amt,0) as Wrtoff_Amt								,-- 	57	核销金额
		nvl(T.Wrtoff_Int,0) as Wrtoff_Int								,-- 	58	核销利息
		nvl(T.Claim_Settle_Totl_Amt,0) as Claim_Settle_Totl_Amt			,-- 	59	我行理赔总金额
		nvl(T.Claim_Settle_Prin,0) as Claim_Settle_Prin					,-- 	60	我行理赔本金 
		nvl(t6.Accm_Ovrd_Cnt,0) as Accm_Owe_Money_Prd_Cnt				,-- 	61	累计欠款期数
		0,-- nvl(t6.Cont_Owe_Money_Prd_Cnt,0) as Cont_Owe_Money_Prd_Cnt	,-- 	62	连续欠款期数
		nvl(t8.Fst_Term_Sht_Is_Merg,'''') as Fst_Term_Sht_Term_Is_Merg 	,--  	63	首期不足期是否合并  0-不合并,1-合并
		nvl(t8.End_Term_Merg,'''') as End_Term_Is_Merg 					,-- 	64	末期是否合并  Y-合并,N-不合并
		nvl(t6.Out_Form_Stat_Cd,'''') as ABS_Pkg_Ind  					,-- 	65	出表状态（资产证券化打包标志） TAB_OUT_STATUS IN(C04,C05)=1,否则为0；
		nvl(t10.Ref_Int_Rate_Cate_Cd,'''') as Int_Rate_Cd				,-- 	66 	利率类型代码
		'''' as Int_Rate_Term											,-- 	67 	利率期限
		'''' as Int_Rate_Term_Prd_Cd									,-- 	68 	利率期限周期代码
		nvl(t11.Loan_Sbsd_Cd,'''')										,-- 	69	贷款贴息代码
		nvl(t7.Mdl_Prc,1) as Exchg_Rate									,-- 	70	中间价
		nvl(AT.Dubil_Bal,0) as Dubil_Bal								,-- 	71	借据余额
		nvl(AT.To_Rmb_Dubil_Bal,0)	as Dubil_Bal						,-- 	72	折人名币借据余额
		AT.Mth_Total_Bal												,-- 	73	月累积余额
		AT.Quar_Total_Bal												,-- 	74	季累积余额
		AT.Yr_Total_Bal													,-- 	75	年累积余额
		AT.Rmb_Mth_Total_Bal											,-- 	76	人民币月累积余额
		AT.Rmb_Quar_Total_Bal											,-- 	77	人民币季累积余额
		AT.Rmb_Yr_Total_Bal												,-- 	78	人民币年累积余额
		AT.Mth_DAvg														,-- 	79	月日均余额
		AT.Quar_DAvg													,-- 	80	季日均余额
		AT.Yr_DAvg														,-- 	81	年日均余额
		AT.Rmb_Mth_DAvg													,-- 	82	人民币月日均余额
		AT.Rmb_Quar_DAvg												,-- 	83	人民币季日均余额
		AT.Rmb_Yr_DAvg													,-- 	84	人民币年日均余额
		''RMPS'' as Data_Src_Cd  									     -- 	85	数据来源表名
FROM pdm.AT_BAL_t88_loan_dubil AT

LEFT JOIN pdm.t03_loan_dubil_h t -- 贷款借据 RMPS_CQ_LOAN 消费金融部分20210222-20210710部分无数据
	 ON AT.Agmt_ID = T.Agmt_ID
	AND T.Start_Dt <= ${TX_DATE}
	AND T.End_Dt >= ${TX_DATE}

LEFT JOIN PDM.t03_agmt_bal_h T14
	 ON T.Agmt_Id = T14.Agmt_Id
	AND T.Agmt_Cate_Cd = T14.Agmt_Cate_Cd
	AND T14.Start_Dt <= ${TX_DATE}
	AND T14.End_Dt >= ${TX_DATE}
	AND T14.Agmt_Bal_Typ_Cd = ''04'' -- 表内欠息余额

LEFT JOIN PDM.t03_agmt_bal_h T15
	ON T.Agmt_Id = T15.Agmt_Id
	AND T.Agmt_Cate_Cd = T15.Agmt_Cate_Cd
	AND T15.Start_Dt <= ${TX_DATE}
	AND T15.End_Dt >= ${TX_DATE}
	AND T15.Agmt_Bal_Typ_Cd = ''05'' -- 表外欠息余额


LEFT JOIN pdm.t01_cust_h t1 -- 客户历史
	ON t.Cust_Id = t1.Cust_Id 
	AND t1.Start_Dt <= ${TX_DATE}  
	AND t1.End_Dt >= ${TX_DATE}

LEFT JOIN pdm.t04_org t2 -- 机构
	ON t.Pln_Mgmt_Org_Id = t2.Org_Id 
	AND t2.Data_Src_Cd =''NCS'' 

LEFT JOIN pdm.t04_org t3 
	ON t.Ent_Acct_Org_Id = t3.Org_Id 
	AND t3.Data_Src_Cd =''NCM'' 

LEFT JOIN pdm.t10_gl_bas_info_h t4 -- 会计科目基本信息历史 ods只有20210714号以后有数据
	ON AT.Dtl_Subj_Id = t4.Subj_Id 
	AND t4.Start_Dt <= ${TX_DATE}  
	AND t4.End_Dt >= ${TX_DATE}

LEFT JOIN pdm.t04_core_tellr t5 -- 核心柜员
	ON T.Pln_Mgmt_Pers_Mem_Id = t5.Tellr_Id 
	AND t5.Statt_Dt = ${TX_DATE}

LEFT JOIN pdm.t03_consm_fin_dubil_oth_info t6 -- 消费金融贷款借据其他信息
	ON T.Agmt_Id = t6.Agmt_Id
	-- AND T.Agmt_Cate_Cd = t6.Agmt_Cate_Cd 
	AND t6.Statt_Dt = ${TX_DATE} 

LEFT JOIN pdm.t88_exchg_rate t7 -- 汇率牌价表	
	ON t.Cur_Cd = t7.Init_Cur  
	AND t7.statt_dt = ${TX_DATE}

LEFT JOIN pdm.t03_agmt_rel_h t12
	ON t.Agmt_Id = t12.Agmt_Id
	AND t.Agmt_Cate_Cd = t12.Agmt_Cate_Cd
	AND t12.agmt_rel_cate_cd = ''0002'' -- 借据与出账关系ncm
	AND t12.Start_Dt <= ${TX_DATE} 
	AND t12.End_Dt >= ${TX_DATE}	
LEFT JOIN pdm.t03_loan_distr_h t11 
	ON t12.Rel_Agmt_Id = t11.Agmt_Id 
	AND t11.Start_Dt <= ${TX_DATE} 
	AND t11.End_Dt >= ${TX_DATE}

LEFT JOIN pdm.t03_loan_contr_h t9
	ON t.Contr_Agmt_Id = t9.Src_Sys_Contr_Id
	AND t9.Start_Dt <= ${TX_DATE} 
	AND t9.End_Dt >= ${TX_DATE}

LEFT JOIN 
(select Dubil_Agmt_Id,Agmt_Id,Agmt_Cate_Cd,Fst_Term_Sht_Is_Merg,End_Term_Merg,Acct_Cate_Cd,
	row_number() over(partition by Dubil_Agmt_Id order by Dubil_Agmt_Id) rm
	from pdm.t03_loan_acct_h 
	where Start_Dt <= ${TX_DATE} 
	and  End_Dt >= ${TX_DATE}
) T8 -- 贷款账户历史  NCS_MB_AGREEMENT_LOAN.INTERNAL_KEY=34156854有两笔记录 关联会重复
	ON T.Agmt_Id = T8.Dubil_Agmt_Id
	-- and T.Agmt_Cate_Cd = T8.Agmt_Cate_Cd
	AND T8.rm=1

LEFT JOIN pdm.t03_agmt_rate_h t10
	ON t8.Agmt_Id = t10.Agmt_Id 	
	-- and t8.Agmt_Cate_Cd = t10.Agmt_Cate_Cd
	AND t10.Agmt_Rate_Typ_Cd = ''01''
	AND t10.start_dt <= ${TX_DATE}
	AND t10.end_dt >= ${TX_DATE}

WHERE AT.Data_Src_Cd = ''RMPS''  -- 消费金融
'
;

    CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

	/*第三部分 微粒贷 */
-- ETL_STEP_NO = 13
	 SET @SQL_STR = '

INSERT INTO pdm.AT_t88_loan_dubil(
		Statt_Dt					,-- 	1	统计日期
		Agmt_ID						,-- 	2	协议编号
		Dubil_Id						,-- 86 借据编号
		Cust_ID						,-- 	3	客户编号
		Cust_Nm						,-- 	4	客户名称
		Loan_Contr_Num				,-- 	5	贷款合同号
		Prod_ID						,-- 	6	产品编号
		Cur_Cd						,-- 	7	币种代码
		Bank_Org_Cd					,-- 	8	银行机构代码
		Fin_Lics					,-- 	9	金融许可证号
		Mgmt_Org_ID					,-- 	10	管理机构编号
		Acct_Org_ID					,-- 	11	账务机构编号
		Dtl_Subj_ID					,-- 	12	明细科目编号
		Dtl_Subj_Nm					,-- 	13	明细科目名称
		Acct_Org_Nm					,-- 	14	账户机构名称
		Mgmt_Org_Nm					,-- 	15	管理机构名称
		Int_Rate_Cate_Cd			,-- 	16	利率类型代码
		Int_Rate_Flt_Mod_Cd			,-- 	17	利率浮动方式代码
		Int_Base_Cd					,-- 	18	计息基础代码
		Base_Int_Rate				,-- 	19	基准利率
		Exec_Int_Rate				,-- 	20	执行利率
		Int_Rate_Flt				,-- 	21	利率浮动值
		Pric_Cate_Cd				,-- 	22	定价类型代码
		Dubil_Amt					,-- 	23	借据金额
		Loan_Term					,-- 	24	贷款期限
		Expd_Ind					,-- 	25	展期标志
		Expd_Cnt					,-- 	26	展期次数
		Expd_Happ_Dt				,-- 	27	展期发生日期
		Expd_Matr_Dt				,-- 	28	展期到期日期
		Expd_Amt					,-- 	29	展期金额
		Expd_Int_Rate				,-- 	30	展期利率
		Loan_Actl_Distr_Dt			,-- 	31	贷款实际发放日期
		Loan_Init_Matr_Dt			,-- 	32	贷款原始到期日期
		Loan_Actl_Matr_Dt			,-- 	33	贷款实际到期日期
		In_Bal_Debt_Int_Bal			,-- 	34	表内欠息余额
		Off_Bal_Debt_Int_Bal		,-- 	35	表外欠息余额
		End_Dt						,-- 	36	终结日期
		Loan_Ent_Acct_Acct_Num		,-- 	37	贷款入账账号
		Repay_Acct_Num				,-- 	38	还款账号
		Loan_Stat_Cd				,-- 	39	贷款状态代码
		Loan_Fiv_Cls_Cd				,-- 	40	贷款五级分类代码
		Loan_Ten_Sec_Cls_Cd			,-- 	41	贷款十二级分类代码
		Repay_Mod_Cd				,-- 	42	还款方式代码
		Int_Prd_Cd					,-- 	43	计息周期代码
		Main_Guar_Mod_Cd			,-- 	44	主担保方式代码
		Crdt_Mem_Nm					,-- 	45	信贷员姓名
		Crdt_Mem_Emp_Num			,-- 	46	信贷员员工号
		Shd_Pay_Unreturn_Pnsh_Int	,-- 	47	应还未还罚息
		Shd_Pay_Unreturn_Cmpd_Int	,-- 	48	应还未还复利
		Shd_Pay_Unreturn_Ovrd_Int	,-- 	49	应还未还逾期利息
		Shd_Pay_Unreturn_Norm_Int	,-- 	50	应还未还正常利息
		Prin_Ovrd_Days				,-- 	51	本金逾期天数
		Prin_Ovrd_Bal				,-- 	52	本金逾期余额
		Int_Ovrd_Days				,-- 	53	利息逾期天数
		Int_Ovrd_Bal				,-- 	54	利息逾期余额
		End_Cate_Cd					,-- 	55	终结类型代码
		Wrtoff_Dt					,-- 	56	核销日期
		Wrtoff_Amt					,-- 	57	核销金额
		Wrtoff_Int					,-- 	58	核销利息
		Claim_Settle_Totl_Amt		,-- 	59	理赔总金额
		Claim_Settle_Prin			,-- 	60	理赔本金
		Accm_Owe_Money_Prd_Cnt		,-- 	61	累计欠款期数
		Cont_Owe_Money_Prd_Cnt		,-- 	62	连续欠款期数
		Fst_Term_Sht_Term_Is_Merg	,-- 	63	首期不足期是否合并
		End_Term_Is_Merg			,-- 	64	末期是否合并
		ABS_Pkg_Ind					,-- 	65	出表状态（资产证券化打包标志）
		Int_Rate_Cd					,-- 	66	利率代码 
		Int_Rate_Term				,-- 	67	利率期限
		Int_Rate_Term_Prd_Cd		,-- 	68	利率期限周期代码
		Loan_Sbsd_Cd				,-- 	69	贷款贴息代码
		Exchg_Rate					,-- 	70	汇率
		Dubil_Bal					,-- 	71	借据余额
		To_Rmb_Dubil_Bal			,-- 	72	折人名币借据余额
		Mth_Total_Bal				,-- 	73	月累积余额
		Quar_Total_Bal				,-- 	74	季累积余额
		Yr_Total_Bal				,-- 	75	年累积余额
		Rmb_Mth_Total_Bal			,-- 	76	人民币月累积余额
		Rmb_Quar_Total_Bal			,-- 	77	人民币季累积余额
		Rmb_Yr_Total_Bal			,-- 	78	人民币年累积余额
		Mth_DAvg					,-- 	79	月日均余额
		Quar_DAvg					,-- 	80	季日均余额
		Yr_DAvg						,-- 	81	年日均余额
		Rmb_Mth_DAvg				,-- 	82	人民币月日均余额
		Rmb_Quar_DAvg				,-- 	83	人民币季日均余额
		Rmb_Yr_DAvg					,-- 	84	人民币年日均余额
		Data_Src_Cd					 -- 	85	数据来源代码
 )
select ${TX_DATE} as Statt_Dt													,-- 	1	统计日期
		AT.Agmt_Id as Agmt_ID												,-- 	2	协议编号
		case when t.Data_Src_Cd =\'NCM\'  then substring(t.Agmt_Id,4,100)
			   when t.Data_Src_Cd =\'RMPS\' then substring(t.Agmt_Id,5,100) 
               else t.Agmt_Id end				,-- 	86 借据编号
		nvl(t.Cust_Id,'''') as Cust_ID												,-- 	3	客户编号
		nvl(t1.Cust_Nm,''@'')	as Cust_Nm											,-- 	4	客户名称
		nvl(t.Contr_Agmt_Id,'''')	as Loan_Contr_Num								,-- 	5	贷款合同号
		nvl(T.Prod_Id,'''')	as Prod_ID												,-- 	6	产品编号
		decode(T.Cur_Cd,'''',''CNY'',T.Cur_Cd)	as Cur_Cd							,-- 	7	币种代码
		nvl(t2.Fin_Org_Cd,'''') as Bank_Org_Cd										,-- 	8	金融机构编码
		nvl(t2.Fin_Lics,'''')	as Fin_Lics											,-- 	9	金融许可证号
		nvl(T.Pln_Mgmt_Org_Id,'''') as Mgmt_Org_ID									,-- 	10	贷后管理机构编号
		nvl(t.Ent_Acct_Org_Id,'''') as Acct_Org_ID									,-- 	11	上账机构编号	
		AT.Dtl_Subj_Id as Dtl_Subj_ID													,-- 	12	科目编号
		CASE WHEN AT.Dtl_Subj_Id = ''13100001'' THEN ''逾期贷款-微粒贷逾期贷款''
			 WHEN AT.Dtl_Subj_Id = ''13030106'' THEN ''贷款-个人短期微粒贷贷款本金''
			 WHEN AT.Dtl_Subj_Id = ''13030206'' THEN ''贷款-个人中长期微粒贷贷款本金''
			ELSE ''''
        END as Dtl_Subj_Nm															,-- 	13	科目名称
		nvl(t3.Org_Nm,'''') as Acct_Org_Nm											,-- 	14	机构名称 账户机构名称
		nvl(t2.Org_Nm,'''') as Mgmt_Org_Nm											,-- 	15	机构名称 管理机构名称
		nvl(t.Int_Rate_Adj_Mod_Cd,'''') as Int_Rate_Cate_Cd							,-- 	16	利率调整方式代码 1=固定/其他=浮动
		nvl(t.Int_Rate_Flt_Mod_Cd,'''') as Int_Rate_Flt_Mod_Cd						,-- 	17	利率浮动方式代码  浮动比例/浮动点差
		nvl(t9.Int_Mod_Cd,'''') as Int_Base_Cd										,-- 	18	计息基础代码 t03_loan_dubil增加business_contract.INTERESTACCRUALTYPE计息方式字段，基础层没取 ACT/360这种
		nvl(T.Base_Int_Rate,0) as Base_Int_Rate										,-- 	19	基准利率
		nvl(t.Exec_Yr_Int_Rate,0) as Exec_Int_Rate									,-- 	20	执行年利率
		nvl(t.Int_Rate_Flt,0) as Int_Rate_Flt										,-- 	21	利率浮动值
		nvl(t.Base_Int_Rate_Cate_Cd,'''') as Pric_Cate_Cd							,-- 	22	定价类型代码 基准利率类型代码 LPR利率/央行基准利率  贷款合同表.BASERATETYPE, 100, LPR, YHJZ （这样分的？）
		nvl(T.Dubil_Amt,0) as Dubil_Amt												,-- 	23	借据金额
		nvl(T.Term_Mth,0) as Loan_Term												,-- 	24	期限月
		CASE WHEN T.Expd_Cnt >0 THEN 1 ELSE 0 END as Expd_Ind 						,-- 	25	展期标志   1:是 0：否
		nvl(T.Expd_Cnt,0) as Expd_Cnt												,-- 	26	展期次数
		nvl(T.Expd_Happ_Dt,''0001-01-01'') as Expd_Happ_Dt									,-- 	27	展期发生日期 
		nvl(T.Expd_Matr_Dt,''0001-01-01'') as Expd_Matr_Dt									,-- 	28	展期到期日期
		nvl(T.Expd_Amt,0) as Expd_Amt												,-- 	29	展期金额
		nvl(T.Expd_Int_Rate,0) as Expd_Int_Rate										,-- 	30	展期利率
		nvl(T.Distr_Dt,''0001-01-01'') as Loan_Actl_Distr_Dt									,-- 	31	放款日期
		''0001-01-01'' as Loan_Init_Matr_Dt											,-- 	32	贷款原始到期日期
		nvl(T.Actl_Matr_Dt,''0001-01-01'') as Loan_Actl_Matr_Dt								,-- 	33	实际到期日期
		nvl(T14.bal,0) as In_Bal_Debt_Int_Bal										,-- 	34	表内欠息余额
		nvl(T15.bal,0) as Off_Bal_Debt_Int_Bal										,-- 	35	表外欠息余额
		nvl(T.End_Dt,''0001-01-01'') as End_Dt												,-- 	36	终结日期
		nvl(T.Core_Loan_Acct_Num,'''') as Loan_Ent_Acct_Acct_Num					,-- 	37	核心贷款账号
		nvl(T.Repay_Acct_Num,'''') as Repay_Acct_Num								,-- 	38	还款帐号
		nvl(T.Dubil_Stat_Cd,'''') as Loan_Stat_Cd									,-- 	39	借据状态代码
		nvl(T.Fiv_Cls_Cd,'''') as Loan_Fiv_Cls_Cd									,-- 	40	五级分类代码
		nvl(t.Ten_Sec_Cls_Cd,'''') as Loan_Ten_Sec_Cls_Cd							,-- 	41	十二级分类代码
		nvl(T.Repay_Mod_Cd,'''') as Repay_Mod_Cd									,-- 	42	还款方式代码
		'''' as Int_Prd_Cd															,-- 	43	计息周期代码 按年/月 计息
		nvl(T.Guar_Mod_Cd,'''') as Main_Guar_Mod_Cd									,-- 	44	担保方式代码
		nvl(t5.Tellr_Nm,'''')	 as Crdt_Mem_Nm										,-- 	45	柜员名称
		nvl(T.Pln_Mgmt_Pers_Mem_Id,'''') as Crdt_Mem_Emp_Num 						,-- 	46	贷后管理人员编号
		nvl(T.Shd_Pay_Unreturn_Pnsh_Int,0) as Shd_Pay_Unreturn_Pnsh_Int				,-- 	47	应还未还罚息
		nvl(T.Shd_Pay_Unreturn_Cmpd_Int,0) as Shd_Pay_Unreturn_Cmpd_Int				,-- 	48	应还未还复利
		nvl(T.Shd_Pay_Unreturn_Ovrd_Int,0) as Shd_Pay_Unreturn_Ovrd_Int				,-- 	49	应还未还逾期利息
		nvl(T.Shd_Pay_Unreturn_Norm_Int,0) as Shd_Pay_Unreturn_Norm_Int				,-- 	50	应还未还正常利息
		nvl(T.Ovrd_Days,0) as Prin_Ovrd_Days										,-- 	51	逾期天数
		CASE WHEN AT.Dtl_Subj_Id = ''13100001'' THEN AT.Dubil_Bal
			 ELSE 0
		END as Prin_Ovrd_Bal														,-- 	52	逾期余额
		nvl(t.Debt_Int_Days,0) as Int_Ovrd_Days										,-- 	53	欠息天数
		nvl(t14.bal,0) + nvl(t15.bal,0) as Int_Ovrd_Bal								,-- 	54	利息逾期余额 表内欠息余额+表外欠息余额
		nvl(t6.End_Cate_Cd,'''') as End_Cate_Cd 									,-- 	55  终结类型代码	终结类型代码 060=核销  
		nvl(t.End_Dt,''0001-01-01'') as Wrtoff_Dt 											,-- 	56	终结日期  终结类型=核销时，终结日期=核销日期
		nvl(T.Wrtoff_Amt,0) as Wrtoff_Amt											,-- 	57	核销金额
		nvl(T.Wrtoff_Int,0) as Wrtoff_Int											,-- 	58	核销利息
		0 as Claim_Settle_Totl_Amt													,-- 	59	我行理赔总金额
		0 as Claim_Settle_Prin														,-- 	60	我行理赔本金 
		nvl(t6.Accm_Owe_Money_Prd_Cnt,0) as Accm_Owe_Money_Prd_Cnt					,-- 	61	累计欠款期数
		nvl(t6.Cont_Owe_Money_Prd_Cnt,0) as Cont_Owe_Money_Prd_Cnt					,-- 	62	连续欠款期数
		'''' as Fst_Term_Sht_Term_Is_Merg 											,--  	63	首期不足期是否合并  0-不合并,1-合并
		'''' as End_Term_Is_Merg 													,-- 	64	末期是否合并  Y-合并,N-不合并
		-- nvl(t8.Acct_Cate_Cd,'''') as ABS_Pkg_Ind  	
		'''' as ABS_Pkg_Ind															,-- 	65	出表状态（资产证券化打包标志） -- NCM_ABS_PROJECT_INFO
		nvl(t10.Ref_Int_Rate_Cate_Cd,'''') as Int_Rate_Cd							,-- 	66 	利率类型代码
		'''' as Int_Rate_Term														,-- 	67 	利率期限
		'''' as Int_Rate_Term_Prd_Cd												,-- 	68 	利率期限周期代码
		'''' as Loan_Sbsd_Cd														,-- 	69	贷款贴息代码
		nvl(t7.Mdl_Prc,1) as Exchg_Rate												,-- 	70	中间价
		nvl(AT.Dubil_Bal,0) as Dubil_Bal								,-- 	71	借据余额
		nvl(AT.To_Rmb_Dubil_Bal,0)	as Dubil_Bal						,-- 	72	折人名币借据余额
		AT.Mth_Total_Bal												,-- 	73	月累积余额
		AT.Quar_Total_Bal												,-- 	74	季累积余额
		AT.Yr_Total_Bal													,-- 	75	年累积余额
		AT.Rmb_Mth_Total_Bal											,-- 	76	人民币月累积余额
		AT.Rmb_Quar_Total_Bal											,-- 	77	人民币季累积余额
		AT.Rmb_Yr_Total_Bal												,-- 	78	人民币年累积余额
		AT.Mth_DAvg														,-- 	79	月日均余额
		AT.Quar_DAvg													,-- 	80	季日均余额
		AT.Yr_DAvg														,-- 	81	年日均余额
		AT.Rmb_Mth_DAvg													,-- 	82	人民币月日均余额
		AT.Rmb_Quar_DAvg												,-- 	83	人民币季日均余额
		AT.Rmb_Yr_DAvg													,-- 	84	人民币年日均余额
		''WLD'' as Data_Src_Cd -- 85	数据来源表名
  FROM  pdm.AT_BAL_t88_loan_dubil AT

LEFT JOIN pdm.t03_loan_dubil_h t 
	 ON AT.Agmt_ID = T.Agmt_ID
	AND T.Start_Dt <= ${TX_DATE}
	AND T.End_Dt >= ${TX_DATE}

LEFT JOIN pdm.t01_cust_h t1 -- 客户历史
	ON t.Cust_Id = t1.Cust_Id 
	AND t1.Start_Dt <= ${TX_DATE}  
	AND t1.End_Dt >= ${TX_DATE} 

LEFT JOIN pdm.t04_org t2 -- 机构
	ON t.Pln_Mgmt_Org_Id = t2.Org_Id 
	AND t2.Data_Src_Cd =''NCS'' 

LEFT JOIN pdm.t04_org t3 
	ON t.Ent_Acct_Org_Id = t3.Org_Id 
	AND t3.Data_Src_Cd =''NCM'' 

LEFT JOIN pdm.t04_core_tellr t5 -- 核心柜员
	ON T.Pln_Mgmt_Pers_Mem_Id = t5.Tellr_Id 
	AND t5.Statt_Dt = ${TX_DATE}

LEFT JOIN pdm.t88_exchg_rate t7 -- 汇率牌价表	
	ON decode(T.Cur_Cd,'''',''CNY'',T.Cur_Cd) = t7.Init_Cur  
	AND t7.statt_dt = ${TX_DATE}
  
LEFT JOIN pdm.t03_agmt_rate_h t10
	ON t.Agmt_Id = t10.Agmt_Id 	
	AND t.Agmt_Cate_Cd = t10.Agmt_Cate_Cd
	AND t10.Agmt_Rate_Typ_Cd = ''01''
	AND t10.start_dt <= ${TX_DATE}
	AND t10.end_dt >= ${TX_DATE}

LEFT JOIN pdm.t03_agmt_bal_NCM_h t14 
	ON t.Agmt_Id = t14.Agmt_Id
	AND t.Agmt_Cate_Cd = t14.Agmt_Cate_Cd
 	AND t14.Agmt_Cate_Cd=''0101'' -- 贷款借据
	AND t14.Agmt_Bal_Typ_Cd =''04'' -- 余额类型代码: 04表内欠息余额 
	AND t14.Start_Dt <= ${TX_DATE} 
	AND t14.End_Dt >= ${TX_DATE}

LEFT JOIN pdm.t03_agmt_bal_NCM_h t15 
	ON t.Agmt_Id = t15.Agmt_Id
	AND t.Agmt_Cate_Cd = t15.Agmt_Cate_Cd
 	AND t15.Agmt_Cate_Cd=''0101'' -- 贷款借据
	AND t15.Agmt_Bal_Typ_Cd =''05'' -- 余额类型代码：05表外欠息余额
	AND t15.Start_Dt <= ${TX_DATE} 
	AND t15.End_Dt >= ${TX_DATE}


LEFT JOIN pdm.t03_loan_dubil_oth_info_h t6 -- 贷款借据其他信息
	ON T.Agmt_Id = t6.Agmt_Id 
	AND t.Agmt_Cate_Cd = t6.Agmt_Cate_Cd
	-- AND (t6.Be_Abs_Ind <> ''99'' OR t6.Be_Abs_Ind ='''')
    AND t6.Start_Dt <= ${TX_DATE} 
	AND t6.end_dt >= ${TX_DATE}

LEFT JOIN pdm.t03_loan_contr_h t9
	ON t.Contr_Agmt_Id = t9.Src_Sys_Contr_Id
	AND t9.Start_Dt <= ${TX_DATE} 
	AND t9.end_dt >= ${TX_DATE}
	
WHERE AT.Data_Src_Cd = ''WLD''
'
;


	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

/*检查插入的临时表数据是否有主键重复*/
-- ETL_STEP_NO = 14	
	DELETE FROM ETL.ETL_JOB_STATUS_EDW 	WHERE tx_date = ETL_TX_DATE   AND step_no = ETL_STEP_NO	AND sql_unit = ETL_T_TAB_ENG_NAME;
	INSERT INTO ETL.ETL_JOB_STATUS_EDW  VALUES ('',SESSION_USER(),ETL_T_TAB_ENG_NAME,ETL_TX_DATE,ETL_STEP_NO,'主键是否重复验证',0,'Running','',CURRENT_TIMESTAMP,'');
  
	SELECT COUNT(*) INTO PK_COUNT 
	FROM 
	(
		SELECT Agmt_ID FROM pdm.AT_t88_loan_dubil
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
-- ETL_STEP_NO = 15	

	SET @SQL_STR = '
DELETE FROM pdm.t88_loan_dubil WHERE Statt_Dt >= ${TX_DATE} ' ;

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
		 
	
	/*通过主键检查的数据插入正式表中*/
-- ETL_STEP_NO = 16
	
	SET @SQL_STR = '
INSERT INTO pdm.t88_loan_dubil SELECT * FROM pdm.AT_t88_loan_dubil where Statt_Dt = ${TX_DATE} ';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;


	
/*	-- 删除今日临时表
	-- ETL_STEP_NO = 16
	SET @SQL_STR = '
DROP  TABLE pdm.VT_t88_loan_dubil';
	
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

	-- 删除金额汇总临时表
	-- ETL_STEP_NO = 17
	SET @SQL_STR = '
DROP  TABLE pdm.AT_t88_loan_dubil';
	
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
*/
	
SET OUT_RES_MSG = 'SUCCESSFUL';
	
END |