DELIMITER |

CREATE DEFINER="cq_sjpt"@"%" PROCEDURE "pr_f5_t88_exchg_rate"(
	OUT	OUT_RES_MSG		VARCHAR(200),
	IN	IN_TX_DATE		VARCHAR(8)
)
lable:begin
	/********************
	 * 汇总层汇率信息：取每天最后时间的汇率
	 * 20210722 whd 创建 
	 *********************/
	
	DECLARE ETL_T_TAB_ENG_NAME 		VARCHAR(50)		default 't88_exchg_rate';
	DECLARE ETL_STEP_NO				INTEGER			DEFAULT 1;
	DECLARE ETL_TX_DATE 			VARCHAR(8)		DEFAULT IN_TX_DATE;
	DECLARE PK_COUNT				BIGINT			DEFAULT 0;
 
	/*定义临时表 */
-- ETL_STEP_NO = 1 
SET @SQL_STR = '
DROP TEMPORARY TABLE IF EXISTS pdm.VT_t88_exchg_rate';

CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
IF @RTC <> 0 THEN LEAVE LABLE;END IF;
SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;


-- ETL_STEP_NO = 2 
SET @SQL_STR = '
CREATE TEMPORARY  TABLE pdm.VT_t88_exchg_rate LIKE pdm.t88_exchg_rate';

CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
IF @RTC <> 0 THEN LEAVE LABLE;END IF;
SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

-- ETL_STEP_NO = 3 
SET @SQL_STR = '
INSERT INTO pdm.VT_t88_exchg_rate(
	Statt_Dt			,-- 1统计日期
	Efft_Dt				,-- 2汇率日期
	Org_Id				,-- 3机构编号
	Exchg_Rate_Cate_Cd	,-- 4汇率类型代码
	Init_Cur			,-- 5源币种代码
	Init_Cur_Nm			,-- 6源币种名称
	Trg_Cur				,-- 7目标币种代码
	Trg_Cur_Nm			,-- 8目标币种名称
	Quot_Cate_Cd		,-- 9牌价类型
	Mdl_Prc				,-- 10中间价
	Exchg_Buy_Prc		,-- 11汇买价
	Exchg_Sell_Prc		,-- 12汇卖价
	Pbc_Ref_Exchg_Rate	,-- 13央行参考汇率
	Banknote_Buy_Prc	,-- 14钞买价
	Banknote_Sell_Prc	 -- 15钞卖价
)
SELECT ${TX_DATE}			,-- 1统计日期
	t.Efft_Dt				,-- 2汇率日期
	t.Org_Id				,-- 3机构编号
	t.Exchg_Rate_Cate_Cd	,-- 4汇率类型代码
	t.Init_Cur				,-- 5源币种代码
	t1.Des_Curr_Desc		,-- 6源币种名称
	t.Trg_Cur				,-- 7目标币种代码
	t2.Des_Curr_Desc		,-- 8目标币种名称
	t.Quot_Cate_Cd			,-- 9牌价类型
	t.Mdl_Prc				,-- 10中间价
	t.Exchg_Buy_Prc			,-- 11汇买价
	t.Exchg_Sell_Prc		,-- 12汇卖价
	t.Pbc_Ref_Exchg_Rate	,-- 13央行参考汇率
	t.Banknote_Buy_Prc		,-- 14钞买价
	t.Banknote_Sell_Prc		 -- 15钞卖价
FROM (SELECT e1.* from (
		 	SELECT e.Efft_Dt,e.Org_Id,e.Exchg_Rate_Cate_Cd,e.Init_Cur,e.Trg_Cur,e.Quot_Cate_Cd,e.Mdl_Prc,e.Exchg_Buy_Prc,
		 	       e.Exchg_Sell_Prc,e.Pbc_Ref_Exchg_Rate,e.Banknote_Buy_Prc,e.Banknote_Sell_Prc,
		 	ROW_NUMBER() OVER(PARTITION BY e.Init_Cur ORDER BY e.Efft_Tm DESC) rm 
		 	 FROM pdm.t02_exchg_rate_quot_form e 
		 	WHERE Statt_Dt = ${TX_DATE}  
		 	  AND Efft_Dt = ${TX_DATE}
			) e1
		WHERE e1.rm = 1
	 ) t 

LEFT JOIN pdm.t99_curr_cd t1 
	 ON t.Init_Cur = t1.Des_Curr_Cd
	 AND t1.Data_Src_Cd = ''NCS''
	 AND t1.Start_Dt <= ${TX_DATE}
	 AND t1.End_Dt >= ${TX_DATE}

LEFT JOIN pdm.t99_curr_cd t2 
	 ON t.Trg_Cur = t2.Des_Curr_Cd 
	AND t2.Data_Src_Cd = ''NCS''
	AND t2.Start_Dt <= ${TX_DATE}
	AND t2.End_Dt >= ${TX_DATE}
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
		 SELECT Statt_Dt,Efft_Dt,Init_Cur,Trg_Cur FROM pdm.VT_t88_exchg_rate
		  GROUP BY 1 ,2,3,4
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
delete from pdm.t88_exchg_rate where Statt_Dt >= ${TX_DATE}';

CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
IF @RTC <> 0 THEN LEAVE LABLE;END IF;
SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

/*通过主键检查的数据插入正式表中*/
-- ETL_STEP_NO = 6
SET @SQL_STR = '
insert into pdm.t88_exchg_rate select * from pdm.VT_t88_exchg_rate where Statt_Dt = ${TX_DATE}';

CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
IF @RTC <> 0 THEN LEAVE LABLE;END IF;
SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

/*删除临时表*/
-- ETL_STEP_NO = 7	
SET @SQL_STR = 'DROP TEMPORARY TABLE pdm.VT_t88_exchg_rate'; 

CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
IF @RTC <> 0 THEN LEAVE LABLE;END IF;
SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

SET OUT_RES_MSG='SUCCESSFUL';


END |