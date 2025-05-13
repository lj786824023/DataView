DELIMITER |

CREATE DEFINER="cq_sjpt"@"%" PROCEDURE "pr_f5_t88_ccard_acct_info"(
	OUT	OUT_RES_MSG		VARCHAR(200),
	IN	IN_TX_DATE		VARCHAR(8)
	)
lable:BEGIN
/**********************************
 * whd 20210519 新建
 * 信用卡账户信息
 * 20230210 增加t03_ccard_acct_dtl关联
 *******************************/
	
	DECLARE ETL_T_TAB_ENG_NAME 		VARCHAR(50)		DEFAULT 't88_ccard_acct_info';
	DECLARE ETL_STEP_NO				INTEGER			DEFAULT 1;
	DECLARE ETL_TX_DATE 			VARCHAR(8)		DEFAULT IN_TX_DATE;
	DECLARE PK_COUNT				BIGINT			DEFAULT 0;
	

	/*定义临时表*/
-- ETL_STEP_NO = 1 
	SET @SQL_STR = '
DROP TEMPORARY TABLE IF EXISTS pdm.VT_t88_ccard_acct_info';
	
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
-- ETL_STEP_NO = 2 
	SET @SQL_STR = '
CREATE TEMPORARY  TABLE pdm.VT_t88_ccard_acct_info LIKE pdm.t88_ccard_acct_info';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;	
	
	/*数据首先插入临时表VT_*/
-- ETL_STEP_NO = 3
		SET @SQL_STR = 	'
INSERT INTO PDM.VT_t88_ccard_acct_info(
		Statt_Dt						,-- 1	统计日期
		Agmt_ID							,-- 2	协议编号
		Card_Num						,-- 3	卡号
		Cust_ID							,-- 4	客户编号
		Cur_Cd							,-- 5	币种代码
		Org_ID							,-- 6	机构编号
		Prod_ID							,-- 7	产品编号
		OpenAcct_Dt						,-- 8	开户日期
		Acct_Day						,-- 9	账单日
		Matr_Repay_Day					,-- 10	到期还款日
		Pri_Auto_deduct_Repay_Acct_Num	,-- 11	第一自扣还款账号
		Int_Base_Cd						,-- 12	计息基础代码
		Cmpd_Int_Calc_Mod_Cd			,-- 13	复利计算方式代码
		Acct_Stat_Cd					,-- 14	账户状态代码
		Crdt_Lmt						,-- 15	信用额度
		Had_Use_Lmt						,-- 16	已使用额度
		Amort_Pay_Lmt					,-- 17	分期付款额度
		Cash_Amt						,-- 18	取现金额
		Cash_Int						,-- 19	取现利息
		Tran_Od_Dt						,-- 20	转透支日期
		Od_Int_Rate						,-- 21	透支利率
		Prin_Subj_Overdraft				,-- 22	本金科目—透支
		Od_Amt							,-- 23	透支金额
		Prin_Subj_Dpst					,-- 24	本金科目—存款
		Overflow_Pay_Amt				,-- 25	溢缴款金额
		In_Bal_Int_Amt					,-- 26	表内利息金额
		Off_Bal_Int_Amt					,-- 27	表外利息金额
		Ovrd_Stat_Cd					,-- 28	逾期状态代码
		Ovrd_Dt							,-- 29	逾期日期
		Ovrd_Amt						,-- 30	逾期金额
		Ovrd_Prd_Cnt					,-- 31	逾期期数
		Remn_Ovrd_Prin					,-- 32	剩余逾期本金
		Remn_Ovrd_Int					,-- 33	剩余逾期利息
		Fiv_Cls_Cd						,-- 34  五级分类代码
		SttAg							 -- 35	账龄
	)
	SELECT ${TX_DATE}						,-- 1	统计日期
		t.Agmt_Id							,-- 2	协议编号
		NVL(t1.Pri_Card_Num,${NULL_STR})	,-- 3	卡号
		t.Cust_ID							,-- 4	客户编号
		t.Agmt_Cur_Cd						,-- 5	协议币种代码
		t.Org_Id							,-- 6	机构编号
		t.Card_Max_Prod_ID					,-- 7   产品编号	
		t.Acct_Open_Dt						,-- 8	账户开户日期
		t.Acct_Day							,-- 9	账单日
		t.Matr_Repay_Proc_Day				,-- 10	到期还款处理日
		NVL(t2.Pri_Auto_Deduct_Repay_Acct_Num,${NULL_STR})	,-- 11	第一自扣还款账号
		t.Int_Cd							,-- 12	利息代码
		\'DAY\'								,-- 13	按天
		t.Acct_Stat_Cd						,-- 14	账户状态代码
		-- t.Syn_Crdt_Lmt					,-- 15	综合授信额度
		t4.totl_crdt_lmt					,-- 15	综合授信额度
		CASE
            WHEN t.Curr_Cash_Amt + t.Proph_Cash_Amt >= 0 THEN t.Curr_Consm_Amt + t.Bill_Consm_Bal
            ELSE 0
         END								,-- 16	已使用额度 
		t.Proph_Amort_Amt					,-- 17	前期分期金额
		/*CASE
            WHEN T.Curr_Cash_Amt + T.Proph_Cash_Amt >= 0 THEN
                 T.Curr_Cash_Amt + T.Proph_Cash_Amt
            ELSE 0
         END								,-- 18  取现金额	帐单日记息余额+日记息余额（未出账单组成）*/
		t4.cash_bal							,-- 18  取现余额
		t.Cash_Int							,-- 19	取现利息
		/*CASE WHEN t.Ovrd_Stat_Cd <> \'0\' THEN NVL(T3.Tran_Od_Dt,${NULL_DATE}) 
			 ELSE ${NULL_DATE}
	    END 								,-- 20    转透支日期
		*/
		${NULL_DATE}						,-- 20    转透支日期
		t.Od_Int_Rate 						,-- 21    透支利率  
		CASE
            WHEN t.Acct_Cate_Cd = \'20\' THEN \'13070401\'
            WHEN t.Acct_Cate_Cd = \'10\' THEN \'13070402\'
            WHEN t.Acct_Cate_Cd = \'30\' THEN \'13070402\'
			ELSE ${NULL_STR}
        END									,-- 22	本金科目—透支
		-- t.Proph_Cash_Amt + t.Curr_Cash_Amt  + t.Curr_Consm_Amt + t.Proph_Amort_Amt + t.Curr_Amort_Amt + t.Amort_Defandpre  + t.Bill_Consm_Bal,-- 	23  透支金额
		t4.consm_bal + t4.cash_bal + t4.amort_bal + t4.amort_un_appo_bal + t4.larg_amt_amort_un_appo_bal ,-- 	23  透支金额
		CASE
            WHEN t.Acct_Cate_Cd = \'10\' THEN \'20110504\'
            WHEN t.Acct_Cate_Cd = \'20\' THEN \'20110113\'
            WHEN t.Acct_Cate_Cd = \'30\' THEN \'20110504\'
			ELSE ${NULL_STR}
        END									,-- 24	本金科目—存款
		/*CASE
            WHEN T.Curr_Cash_Amt + T.Proph_Cash_Amt >= 0 THEN T.Curr_Cash_Amt + T.Proph_Cash_Amt
            ELSE 0
        END									,-- 25	溢缴款金额	帐单日记息余额+日记息余额（未出账单组成）*/
		t4.ird								,-- 25	溢缴款金额
		t.Proph_Int+t.Curr_Int 				,-- 26  表内利息金额	 (帐单利息余额)+利息余额(未出账单组成)	
		t.Off_Bal_Int						,-- 27	表外利息金额
		t.Ovrd_Stat_Cd						,-- 28	逾期状态代码
		-- NVL(t3.Ovrd_Dt,${NULL_DATE})		,-- 29	逾期日期
		${NULL_DATE},-- 29	逾期日期 
		-- NVL(CASE WHEN t.Ovrd_Stat_Cd <> \'0\' THEN T3.Remn_Ovrd_Prin + T3.Remn_Ovrd_Fee_Bal  END,0)	,-- 30	逾期金额   剩余逾期本金+剩余逾期费用余额
		t4.remn_ovrd_prin + t4.remn_ovrd_int_bal + t4.remn_ovrd_fee_bal ,-- 30	逾期金额
		t3.curr_ovrd_prd_cnt 				,-- 31	逾期期数  
		t4.remn_ovrd_prin 					,-- 32	剩余逾期本金 -- NVL(CASE WHEN t.Ovrd_Stat_Cd <> \'0\' THEN t3.Remn_Ovrd_Prin END,0) 	,-- 32	剩余逾期本金
		t4.remn_ovrd_int_bal				,-- 33	剩余逾期利息 -- NVL(CASE WHEN t.Ovrd_Stat_Cd <> \'0\' THEN t3.Remn_Ovrd_Int END,0) 		,-- 33	剩余逾期利息
		CASE
             WHEN NVL(t3.curr_ovrd_prd_cnt,0) = 0 THEN
              \'1\'
             WHEN t3.curr_ovrd_prd_cnt >= 1 AND t3.curr_ovrd_prd_cnt <= 3 THEN
              \'2\'
             WHEN t3.curr_ovrd_prd_cnt = 4 THEN
              \'3\'
             WHEN t3.curr_ovrd_prd_cnt >= 5 AND t3.curr_ovrd_prd_cnt <= 6 THEN
              \'4\'
             WHEN t3.curr_ovrd_prd_cnt > 6 THEN
              \'5\'
        END									,-- 34 五级分类代码
		${TX_DATE} - t.Acct_Open_Dt			 -- 35	账龄	当前日期-账户开户日期	
	FROM pdm.t03_ccard_acct_h t -- 贷记卡账户历史

LEFT JOIN pdm.t03_ccard_acct_dtl t4
	ON t.agmt_id = t4.agmt_id
	AND t4.statt_dt = ${TX_DATE}

LEFT JOIN (SELECT Agmt_Id, Pri_Card_Num,
                   ROW_NUMBER() OVER(PARTITION BY Agmt_Id ORDER BY Issu_Card_Ordr_Num DESC) rm
            FROM pdm.t03_ccard_h -- 贷记卡历史
          WHERE Card_Hldr_Ordr_Num = 1
           AND Start_Dt <= ${TX_DATE} AND End_Dt >= ${TX_DATE}
			) t1
 	ON t.Agmt_Id = t1.Agmt_Id 
	AND t1.rm = 1

LEFT JOIN pdm.t03_ccard_depay_dtl_h t2 -- 贷记卡自扣还款明细历史
 	ON t.Agmt_Id = t2.Agmt_Id 
	AND t2.Start_Dt <= ${TX_DATE}  AND t2.End_Dt >= ${TX_DATE}

/*
LEFT JOIN (select Agmt_Id,
		       min(Ovrd_Dt) 			AS Ovrd_Dt,
		       max(Tran_Od_Dt) 			AS Tran_Od_Dt,
		       sum(Remn_Ovrd_Prin) 		AS Remn_Ovrd_Prin,
		       sum(Remn_Ovrd_Int) 		AS Remn_Ovrd_Int,
			   sum(Remn_Ovrd_Fee_Bal) 	AS Remn_Ovrd_Fee_Bal
		FROM pdm.t03_ccard_acct_ovrd_h
	  WHERE Start_Dt <= ${TX_DATE} AND End_Dt >= ${TX_DATE}
		AND Ovrd_Ind <> 0 -- 排除已正常的记录
		  -- and agmt_cur_cd = \'CNY\'
	GROUP BY Agmt_Id
) T3 -- 贷记卡帐户逾期历史 
 	ON T.Agmt_Id = T3.Agmt_Id 
*/
LEFT JOIN (SELECT cust_id,MAX(curr_ovrd_prd_cnt) AS curr_ovrd_prd_cnt FROM pdm.t03_ccard_acct_dtl 
            WHERE acct_stat <>\'W\'  -- 20220701 卡部修订五级分类计算逻辑 
			  AND statt_dt = ${TX_DATE}
		   GROUP BY cust_id
           ) t3 
		   ON t.cust_id = t3.cust_id
  WHERE t.Start_Dt <= ${TX_DATE}  AND t.End_Dt >= ${TX_DATE}
  ';
  
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
  /*检查插入的临时表数据是否有主键错误*/
  	-- ETL_STEP_NO = 4
	/*
	DELETE FROM ETL.ETL_JOB_STATUS_EDW 	WHERE tx_date = ETL_TX_DATE   AND step_no = ETL_STEP_NO	AND sql_unit = ETL_T_TAB_ENG_NAME;
	INSERT INTO ETL.ETL_JOB_STATUS_EDW VALUES ('',SESSION_USER(),ETL_T_TAB_ENG_NAME,ETL_TX_DATE,ETL_STEP_NO,'主键是否重复验证',0,'Running','',CURRENT_TIMESTAMP,'');
  
  
	SELECT COUNT(*) INTO PK_COUNT
	FROM 
	(
		SELECT Agmt_ID FROM pdm.VT_t88_ccard_acct_info
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
	
	*/
 	
	/* 支持数据重跑*/
  	-- ETL_STEP_NO = 5
	SET @SQL_STR = '
DELETE FROM pdm.t88_ccard_acct_info WHERE Statt_Dt >= ${TX_DATE}';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	/*通过主键检查的数据插入正式表中*/
	  -- ETL_STEP_NO = 6
	  SET @SQL_STR = '
INSERT INTO pdm.t88_ccard_acct_info SELECT * FROM pdm.VT_t88_ccard_acct_info where Statt_Dt = ${TX_DATE}';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	/*删除临时表*/
	-- ETL_STEP_NO = 7
	SET @SQL_STR = '
DROP TEMPORARY TABLE pdm.VT_t88_ccard_acct_info';
	
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	SET OUT_RES_MSG = 'SUCCESSFUL';
	
END |