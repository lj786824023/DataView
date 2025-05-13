DELIMITER |

CREATE DEFINER="cq_sjpt"@"%" PROCEDURE "pr_f5_t88_ccard_acct_amort_info_form"(
	OUT	OUT_RES_MSG		VARCHAR(200),
	IN	IN_TX_DATE		VARCHAR(8)
	)
LABLE:BEGIN
/**********************************
 * LJZ 20210726 新建
 * 信用卡账户分期信息
 *********************************/
	
	DECLARE ETL_T_TAB_ENG_NAME 		VARCHAR(100)	DEFAULT 't88_ccard_acct_amort_info_form';
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
	INSERT INTO PDM.VT_t88_ccard_acct_amort_info_form(
		Statt_Dt					,-- 1统计日期
		Agmt_ID						,-- 2协议编号
		Amort_Pay_Ordr_Num			,-- 3分期付款序号
		Card_Num					,-- 4卡号
		Cust_ID						,-- 5客户编号
		Cur_Cd						,-- 6币种代码
		St_Int_Dt					,-- 7起息日期
		Crdt_Card_Acct_Typ_Cd		,-- 8信用卡账户种类代码
		Prin_Subj					,-- 9本金科目
		Int_Subj					,-- 10利息科目
		Amort_Pay_Stat_Cd			,-- 11分期付款状态代码
		Amort_Pay_Typ_Cd			,-- 12分期付款种类代码
		Amort_Amt					,-- 13分期金额
		Totl_Amort_Mths				,-- 14总分期月数
		Mthly_Amort_Amort_Pay_Amt	,-- 15每月摊消分期付款金额
		Appo_Intrv_Prd_Cnt			,-- 16分摊间隔期数
		Had_Amort_Amort_Prd_Cnt		,-- 17已分期摊消期数
		Amort_Pay_Int_Rate			,-- 18分期付款利率
		Amort_Int					,-- 19分期利息
		Amort_Pay_Comm_Fee			,-- 20分期付款手续费
		Last_Amort_Amort_Dt			,-- 21上次分期摊消日期
		pre_Mth_Amort_Amort_Int_Amt	,-- 22上月分期摊消利息金额
		pre_Mth_Amort_Amort_Prin_Amt,-- 23上月分期摊消本金金额
		Remn_Unreturn_Prin			,-- 24剩余未还本金
		Remn_Unreturn_Fee			,-- 25剩余未还费用
		Remn_Unreturn_Int			,-- 26剩余未还利息
		Data_Src_Cd,				 -- 27数据来源表名
		Main_Src_Task
 )
	SELECT ${TX_DATE}						,-- 1统计日期
		t.Agmt_Id							,-- 2协议编号
		t.Src_Sys_Amort_Pay_Ordr_Num		,-- 3源系统分期付款序号
		t.Card_Num							,-- 4卡号
		NVL(t1.Cust_Id,${NULL_STR})			,-- 5客户编号
		t.Cur_Cd							,-- 6币种代码
		NVL(t1.Acct_Day,0)					,-- 7账单日
		NVL(t1.Acct_Cate_Cd,${NULL_STR})	,-- 8账户类型代码
		CASE
            WHEN t1.Acct_Cate_Cd = \'10\' THEN \'13070404\'
            WHEN t1.Acct_Cate_Cd = \'20\' THEN \'13070403\'
            WHEN t1.Acct_Cate_Cd = \'30\' THEN \'13070404\'
            WHEN t1.Acct_Cate_Cd = \'40\' THEN \'13070404\'
            WHEN t1.Acct_Cate_Cd  IN ( \'50\',\'60\') THEN \'13070404\'
			ELSE \'0000\'
        END									,-- 9本金科目
		${NULL_STR}							,-- 10利息科目
		t.Amort_Pay_Stat_Cd					,-- 11分期付款状态代码
		t.Amort_Pay_Cate_Cd					,-- 12分期付款类型代码
		t.Prin_Totl_Amt						,-- 13本金总金额
		t.Totl_Amort_Mths					,-- 14总分期月数
		t.Mthly_Amort_Amort_Pay_Amt			,-- 15每月摊销分期付款金额
		t.Appo_Intrv_Prd_Cnt				,-- 16分摊间隔期数
		t.Had_Amort_Amort_Prd_Cnt			,-- 17已分期摊消期数
		t.Amort_Pay_Int_Rate				,-- 18分期付款利率
		t.Totl_Int_Amt						,-- 19总利息金额
		t.Totl_Fee							,-- 20总费用
		t.Last_Amort_Amort_Dt				,-- 21上次分期摊消日期
		t.Pre_Mth_Amort_Amort_Int_Amt		,-- 22上月分期摊销利息金额
		t.Pre_Mth_Amort_Amort_Prin_Amt		,-- 23上月分期摊销本金金额
		t.Remn_Unreturn_Prin_Amt			,-- 24剩余未还本金金额
		t.Remn_Unreturn_Fee					,-- 25剩余未还费用
		t.Remn_Unreturn_Int_Amt				,-- 26剩余未还利息金额
		\'t05_ccard_amort_evt\'				,-- 27数据来源表名
		${NULL_STR}
	FROM pdm.t05_ccard_amort_evt t -- 贷记卡分期事件
 LEFT JOIN pdm.t03_ccard_acct_h t1 -- 贷记卡账户历史
		ON t.Agmt_Id = t1.Agmt_Id 
	   AND t1.Start_Dt <= ${TX_DATE}  
	   AND t1.End_Dt > ${TX_DATE}
	WHERE t.Tx_Dt = ${TX_DATE}
	';
	/*where 
	rem_ppl <> 0 or  ( rem_ppl = 0 and  case when TRIM(t8.cancl_code) NOT IN ('关闭', '核销') then    case
         when to_number(T2.BAL_INTFLAG || to_char(T2.bal_int)) +
              to_number(T2.STMBALINTFLAG || to_char(T2.stm_balint)) >= 0 then
          (T2.bal_mp + T2.stm_balmp + T2.mp_rem_ppl + nvl(T7.mp_rem_ppl, 0)) / 100
         when to_number(T2.BAL_INTFLAG || to_char(T2.bal_int)) +
              to_number(T2.STMBALINTFLAG || to_char(T2.stm_balint)) < 0 then
          (T2.mp_rem_ppl + nvl(T7.mp_rem_ppl, 0)) / 100
       end else 0 end  <>0) AND t1.STATUS not in ('E','F','X')-----获取账户分期余额不为0，但是分期表已摊还完成的分期业务
      */ 
	CALL ETL.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	/*检查插入的临时表数据是否有主键错误*/
	
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
	
	/*通过主键检查的数据插入正式表中*/
	SET @SQL_STR='INSERT INTO PDM.'||ETL_T_TAB_ENG_NAME||' SELECT * FROM PDM.VT_'||ETL_T_TAB_ENG_NAME||' WHERE STATT_DT=${TX_DATE}'; 
	CALL ETL.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

	SET OUT_RES_MSG='SUCCESSFUL';
	
	
END |