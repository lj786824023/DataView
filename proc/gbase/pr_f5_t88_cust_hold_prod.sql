DELIMITER |

CREATE DEFINER="cqbank_sj"@"%" PROCEDURE "pr_f5_t88_cust_hold_prod"(
	OUT	OUT_RES_MSG		VARCHAR(200),
	IN	IN_TX_DATE		VARCHAR(8)
)
lable:begin
	/* 
	 * 客户持有产品
	 * whd 20230113 new
	 * 
	 */
	
	DECLARE ETL_T_TAB_ENG_NAME 		VARCHAR(50)		default 't88_cust_hold_prod';
	DECLARE ETL_STEP_NO				INTEGER			DEFAULT 1;
	DECLARE ETL_TX_DATE 			VARCHAR(8)		DEFAULT IN_TX_DATE;
	DECLARE	ETL_USER_ID				VARCHAR(30)		DEFAULT SESSION_USER();
	DECLARE PK_ERR_CNT				BIGINT			DEFAULT 0;
	DECLARE RET_CODE				INTEGER			DEFAULT 0;
	
	
	-- ETL_STEP_NO = 1
	SET @SQL_STR = 'DROP TEMPORARY TABLE IF EXISTS VT_t88_cust_hold_prod';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;	
	
	
	-- ETL_STEP_NO = 2
	SET @SQL_STR = 'CREATE TEMPORARY TABLE VT_t88_cust_hold_prod LIKE ${AUTO_PDM}.t88_cust_hold_prod';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;	
	
	-- ETL_STEP_NO = 3
	-- 存款
	SET @SQL_STR = 'INSERT INTO VT_t88_cust_hold_prod(
	 SELECT ${TX_DATE} ,
			cust_id,
			cur_cd,
			CASE WHEN dept_tm_curr_cate_cd = ''S'' THEN curr_bal ELSE 0 END, -- 活期
			CASE WHEN dept_tm_curr_cate_cd = ''T'' THEN curr_bal ELSE 0 END, -- 定期
			0,
			0,
			0,
			0,
			0			
		FROM ${AUTO_PDM}.t88_dpst_acct_bas_info 
	 WHERE statt_dt = ${TX_DATE} 
	   AND curr_bal <> 0
)';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;	
	
	-- ETL_STEP_NO = 4
	-- 贷款
	SET @SQL_STR = 'INSERT INTO VT_t88_cust_hold_prod(
	 SELECT ${TX_DATE} ,
			cust_id,
			cur_cd,
			0,
			0,
			dubil_bal,
			0,
			0,
			0,
			0			
		FROM ${AUTO_PDM}.t88_loan_dubil 
	 WHERE statt_dt = ${TX_DATE} 
	   AND dubil_bal <> 0
)';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;	
	
	-- ETL_STEP_NO = 5
	-- 基金+理财+信托
	SET @SQL_STR = 'INSERT INTO VT_t88_cust_hold_prod(
	 SELECT ${TX_DATE} ,
			cust_id,
			cur_cd,
			0,
			0,
			0,
			case when prod_ctgy_cd = ''0'' then Curr_Bal else 0 end , -- 基金余额
			case when prod_ctgy_cd = ''1'' then Curr_Bal + in_trans_purch_amt else 0 end , -- 理财余额
			case when prod_ctgy_cd = ''8'' then Curr_Bal else 0 end  , -- 信托余额
			0			
		FROM ${AUTO_PDM}.t88_chrem_fund_acct_info 
	 WHERE statt_dt = ${TX_DATE} 
	   AND Curr_Bal <> 0
)';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;	
	
	-- ETL_STEP_NO = 6
	-- 保险
	SET @SQL_STR = 'INSERT INTO VT_t88_cust_hold_prod(
	 SELECT ${TX_DATE} ,
			cust_id,
			cur_cd,
			0,
			0,
			0,
			0,
			0,
			0,
			Curr_Bal			
		FROM ${AUTO_PDM}.t88_insu_acct_info 
	 WHERE statt_dt = ${TX_DATE} 
	   AND Curr_Bal <> 0
)';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;	
	
	-- ETL_STEP_NO = 7
	SET @SQL_STR = 'INSERT INTO ${AUTO_PDM}.t88_cust_hold_prod(
	 SELECT ${TX_DATE} ,
			cust_id,
			cur_cd,
			SUM(Curr_Dpst_Bal),
			SUM(Tm_Dpst_Bal),
			SUM(Loan_Bal),
			SUM(Fund_Bal),
			SUM(Chrem_Bal),
			SUM(Index_Bal),
			SUM(Insu_Bal)
		FROM VT_t88_cust_hold_prod 
	GROUP BY cust_id, cur_cd
)';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;	
	
	SET OUT_RES_MSG = '0';
	
END |