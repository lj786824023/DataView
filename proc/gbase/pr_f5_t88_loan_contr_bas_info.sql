DELIMITER |

CREATE DEFINER="cq_sjpt"@"%" PROCEDURE "pr_f5_t88_loan_contr_bas_info"(
	OUT	OUT_RES_MSG		VARCHAR(200),
	IN	IN_TX_DATE		VARCHAR(8)
)
lable:BEGIN
/**********************************
 * whd 20210519 新建
 * 贷款合同基本信息
 * 暂时不用,可从基础层贷款合同表取
 *******************************/
	
	DECLARE ETL_T_TAB_ENG_NAME 		VARCHAR(50)		DEFAULT 't88_loan_contr_bas_info';
	DECLARE ETL_STEP_NO				INTEGER			DEFAULT 1;
	DECLARE ETL_TX_DATE 			VARCHAR(8)		DEFAULT IN_TX_DATE;
	DECLARE PK_COUNT				BIGINT			DEFAULT 0;
	

	/*定义临时表*/
-- ETL_STEP_NO = 1 
	SET @SQL_STR = '
DROP TEMPORARY TABLE IF EXISTS VT_t88_loan_contr_bas_info';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
-- ETL_STEP_NO = 2 
	SET @SQL_STR = '
CREATE TEMPORARY TABLE VT_t88_loan_contr_bas_info LIKE pdm.t88_loan_contr_bas_info';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;	

	/*数据首先插入临时表VT_*/
-- ETL_STEP_NO = 3
	SET @SQL_STR = '
	insert into VT_t88_loan_contr_bas_info(
		Statt_Dt	,-- 1	统计日期
		Contr_ID	,-- 2	合同编号
		Opr_Org_ID	,-- 3	经办机构编号
		Cust_ID	,-- 4	客户编号
		Cust_Mgr_ID	,-- 5	客户经理编号
		Prod_ID	,-- 6	产品编号
		Cur_Cd	,-- 7	币种代码
		Start_Dt	,-- 8	起始日期
		Matr_Dt	,-- 9	到期日期
		Fst_Distr_Dt	,-- 10	首次放款日期
		Contr_Amt	,-- 11	合同金额
		Contr_Out_Acct_Amt	,-- 12	合同出账金额
		Contr_Un_Out_Acct_Amt	,-- 13	合同未出账金额
		Loan_Term_Prd_Typ_Cd	,-- 14	贷款期限周期种类代码
		Loan_Term	,-- 15	贷款期限
		Contr_Int_Rate	,-- 16	合同利率
		Int_Rate_Attr_Cd	,-- 17	利率属性代码
		Int_Rate_Adj_Mod_Cd	,-- 18	利率调整方式代码
		Loan_Sbsd_Ind	,-- 19	贴息标志
		Farm_Loan_Ind	,-- 20	农户贷款标志
		Entr_Loan_Ind	,-- 21	委托贷款标志
		Contr_Stat_Cd	,-- 22	合同状态代码
		Repay_Mod_Cd	,-- 23	还款方式代码
		Guar_Mod_Cd		,-- 24	担保方式代码
		Loan_Drct_Cd	,-- 25	贷款投向代码
		Loan_Usg		,-- 26	贷款用途代码
		Loan_Fiv_Cls_Cd	,-- 27	贷款五级分类代码
		Loan_Ten_Sec_Cls_Cd	,-- 28	贷款十二级分类代码
		Expd_Cnt		,-- 29	展期次数
		Data_Src_Cd		-- 30	数据来源表名
 )
	select ${TX_DATE}	,-- 1	统计日期
		T.Src_Sys_Contr_Id	,-- 2	源系统合同编号
		T.Opr_Org_Id	,-- 3	经办机构编号
		T.Cust_Id	,-- 4	客户编号
		T.Oprr_Id	,-- 5	经办人编号
		T.Prod_Id	,-- 6	产品编号
		T.Cur_Cd	,-- 7	币种代码
		T.Start_Day	,-- 8	起始日
		T.Matr_Dt	,-- 9	到期日期
		T.Start_Day	,-- 10	起始日 首次放款日期
		T.Contr_Amt	,-- 11	合同金额
		T.Distr_Amt	,-- 12	放款金额
		T.Contr_Amt-T.Distr_Amt	,-- 13	合同金额-放款金额 合同未出账金额
		\'M\'	,-- 14	贷款期限周期种类代码 按年Y/月M 默认：月
		T.Matr_Dt-T.Start_Day	,-- 15	到期日-起始日 贷款期限
		T.Exec_Yr_Int_Rate	,-- 16	执行年利率
		case when t2.Int_Rate_Adj_Mod_Cd = \'1\' then \'1\' else \'0\' end 	,-- 17	利率属性代码  1:固定/0:浮动利率 消费金融都为固定利率
		nvl(t2.Int_Rate_Adj_Mod_Cd,${NULL_STR})	,-- 18	利率调整方式代码
		nvl(t3.Loan_Sbsd_Cd	,${NULL_STR}),-- 19	贴息标志
		nvl(T1.Impt_Ind,${NULL_STR})	,-- 20	农户贷款标志 是否农户
		case when t.Prod_Id in(\'1050\',\'3020\',\'111050\',\'105010\',\'11105010\') then \'1\' else \'0\' end	,-- 21	委托贷款标志 1:是 0:否
		T.Contr_Stat_Cd	,-- 22	合同状态代码
		T.Repay_Mod_Cd	,-- 23	还款方式代码
		T.Guar_Mod_Cd	,-- 24	担保方式代码
		T.Inds_Drct_Cd	,-- 25	行业投向代码
		nvl(t2.Loan_Usg,${NULL_STR})	,-- 26	贷款用途代码
		t.Fiv_Cls_Cd	,-- 27	五级分类代码
		t.Ten_Sec_Cls_Cd	,-- 28	十二级分类代码
		nvl(t2.Expd_Cnt,0)	,-- 29	展期次数
		\'t03_loan_contr\'	-- 30	数据来源表名
   from pdm.t03_loan_contr t -- 贷款合同
left join pdm.t01_cust_impt_ind_h t1 -- 客户重要标志历史
	on t.Cust_ID = t1.Cust_ID and t1.Impt_Ind_Cate_Cd = \'02002\' -- 农户标志
  and t1.Start_Dt <= ${TX_DATE}  and t1.End_Dt > ${TX_DATE}  
-- left join pdm.t03_loan_dubil t2 
	-- on t.Src_Sys_Contr_Id = t2.Contr_Agmt_Id and t2.Statt_Dt = ${TX_DATE}
left join pdm.t03_loan_distr t3 
	on t2.Agmt_Id = t3.Agmt_Id and t3.Statt_Dt = ${TX_DATE}
 where t.Statt_Dt = ${TX_DATE}
 ';
  
 	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;	
	
  /*检查插入的临时表数据是否有主键错误*/
-- ETL_STEP_NO = 4	
  DELETE FROM ETL.ETL_JOB_STATUS_EDW 	WHERE tx_date = ETL_TX_DATE   AND step_no = ETL_STEP_NO	AND sql_unit = ETL_T_TAB_ENG_NAME;
  INSERT INTO ETL.ETL_JOB_STATUS_EDW VALUES ('',SESSION_USER(),ETL_T_TAB_ENG_NAME,ETL_TX_DATE,ETL_STEP_NO,'主键是否重复验证',0,'Running','',CURRENT_TIMESTAMP,'');
  
	SELECT COUNT(*) INTO PK_COUNT
	FROM 
	(
		SELECT Contr_ID FROM VT_t88_loan_contr_bas_info
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
-- ETL_STEP_NO = 5	
SET @SQL_STR = '	
	DELETE FROM pdm.t88_loan_contr_bas_info WHERE Statt_Dt >= ${TX_DATE}'; 
	
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	/*通过主键检查的数据插入正式表中*/
-- ETL_STEP_NO = 6	
	SET @SQL_STR = '
INSERT INTO pdm.t88_loan_contr_bas_info	SELECT * FROM VT_t88_loan_contr_bas_info where Statt_Dt = ${TX_DATE}';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
-- ETL_STEP_NO = 7	
	SET @SQL_STR = '	
DROP TEMPORARY TABLE VT_t88_loan_contr_bas_info';
	
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;

END |