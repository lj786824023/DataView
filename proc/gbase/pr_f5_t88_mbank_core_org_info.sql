DELIMITER |

CREATE DEFINER="gbase"@"%" PROCEDURE "pr_f5_t88_mbank_core_org_info"(
	OUT	OUT_RES_MSG		VARCHAR(200),
	IN	IN_TX_DATE		VARCHAR(8)
	)
LABLE:BEGIN
/**********************************
 * LJZ 20210831 新建
 * 债券回购业务信息
 *********************************/
	
	DECLARE ETL_T_TAB_ENG_NAME 		VARCHAR(100)	DEFAULT 't88_mbank_core_org_info';
	DECLARE ETL_STEP_NO				INTEGER			DEFAULT 1;
	DECLARE ETL_TX_DATE 			VARCHAR(8)		DEFAULT IN_TX_DATE;
	SET OUT_RES_MSG = 'FAILED';
	
	
	/*支持数据重跑*/
	SET @SQL_STR = 'DELETE FROM PDM.'||ETL_T_TAB_ENG_NAME||' WHERE STATT_DT >= ${TX_DATE}';
	CALL ETL.PR_EXEC_SQL(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	/*定义临时表*/
	SET @SQL_STR = 'DROP TABLE IF EXISTS PDM.VT_'||ETL_T_TAB_ENG_NAME;
	CALL ETL.PR_EXEC_SQL(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	-- select @SQL_STR;
	-- select * from VT_t88_mbank_core_org_info;
	/*创建临时表*/
	SET @SQL_STR = 'CREATE TABLE PDM.VT_'||ETL_T_TAB_ENG_NAME||' LIKE PDM.'||ETL_T_TAB_ENG_NAME;
	CALL ETL.PR_EXEC_SQL(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	/*数据首先插入临时表VT_*/
	-- 第一组：插入一级机构 ${NULL_STR}
	SET @SQL_STR = '
	INSERT INTO PDM.VT_t88_mbank_core_org_info(
		Statt_Dt,-- 1.统计日期
		Core_Org_Id,-- 2.核心机构编号
		Org_Cate_Cd,-- 3.机构类型代码
		Org_Nm,-- 4.机构名称
		Org_Hrcy_Cd,-- 5.机构层级代码
		Org_Stat_Cd,-- 6.机构状态代码
		Lvl1_Org_Id,-- 7.一级机构编号
		Lvl1_Org_Nm,-- 8.一级机构名称
		Lvl2_Org_Id,-- 9.二级机构编号
		Lvl2_Org_Nm,-- 10.二级机构名称
		Lvl3_Org_Id,-- 11.三级机构编号
		Lvl3_Org_Nm,-- 12.三级机构名称
		Lvl4_Org_Id,-- 13.四级机构编号
		Lvl4_Org_Nm,-- 14.四级机构名称
		Up_Org_Id,-- 15.上级机构编号
		Org_Regn_Cd,-- 16.机构区域代码
		Data_Src_Cd,-- 17.数据来源代码
		Main_Src_Task-- 18.主要源系统任务
	)
	select 
		${TX_DATE},-- 1.统计日期
		t.org_id,-- 2.核心机构编号
		t.Org_Cate_Cd,-- 3.机构类型代码
		t.Org_Nm,-- 4.机构名称
		1,-- 5.机构层级代码
		t.Org_Stat_Cd,-- 6.机构状态代码
		t.org_id,-- 7.一级机构编号
		t.Org_Nm,-- 8.一级机构名称
		${NULL_STR},-- 9.二级机构编号
		${NULL_STR},-- 10.二级机构名称
		${NULL_STR},-- 11.三级机构编号
		${NULL_STR},-- 12.三级机构名称
		${NULL_STR},-- 13.四级机构编号
		${NULL_STR},-- 14.四级机构名称
		${NULL_STR},-- 15.上级机构编号
		${NULL_STR},-- 16.机构区域代码
		\'NCS\',-- 17.数据来源代码
		\'t04_core_org_h\'-- 18.主要源系统任务
	FROM pdm.t04_core_org_h T
	where ${TX_DATE} between Start_Dt and End_Dt
	and t.Org_Id=\'9998\' -- 总行
	';
	CALL ETL.PR_EXEC_SQL(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	
	-- 第二组：插入二级机构 ${NULL_STR}
	SET @SQL_STR = '
	INSERT INTO PDM.VT_t88_mbank_core_org_info(
		Statt_Dt,-- 1.统计日期
		Core_Org_Id,-- 2.核心机构编号
		Org_Cate_Cd,-- 3.机构类型代码
		Org_Nm,-- 4.机构名称
		Org_Hrcy_Cd,-- 5.机构层级代码
		Org_Stat_Cd,-- 6.机构状态代码
		Lvl1_Org_Id,-- 7.一级机构编号
		Lvl1_Org_Nm,-- 8.一级机构名称
		Lvl2_Org_Id,-- 9.二级机构编号
		Lvl2_Org_Nm,-- 10.二级机构名称
		Lvl3_Org_Id,-- 11.三级机构编号
		Lvl3_Org_Nm,-- 12.三级机构名称
		Lvl4_Org_Id,-- 13.四级机构编号
		Lvl4_Org_Nm,-- 14.四级机构名称
		Up_Org_Id,-- 15.上级机构编号
		Org_Regn_Cd,-- 16.机构区域代码
		Data_Src_Cd,-- 17.数据来源代码
		Main_Src_Task-- 18.主要源系统任务
	)
	SELECT 
		${TX_DATE},-- 1.统计日期
		LV2.ORG_ID,-- 2.核心机构编号
		LV2.ORG_CATE_CD,-- 3.机构类型代码
		LV2.ORG_NM,-- 4.机构名称
		2,-- 5.机构层级代码
		LV2.ORG_STAT_CD,-- 6.机构状态代码
		T.Core_Org_Id,-- 7.一级机构编号
		T.Org_Nm,-- 8.一级机构名称
		LV2.ORG_ID,-- 9.二级机构编号
		LV2.ORG_NM,-- 10.二级机构名称
		${NULL_STR},-- 11.三级机构编号
		${NULL_STR},-- 12.三级机构名称
		${NULL_STR},-- 13.四级机构编号
		${NULL_STR},-- 14.四级机构名称
		LV2.UP_ORG_ID,-- 15.上级机构编号
		${NULL_STR},-- 16.机构区域代码
		\'NCS\',-- 17.数据来源代码
		\'T04_CORE_ORG_H\'-- 18.主要源系统任务
	FROM PDM.VT_T88_MBANK_CORE_ORG_INFO T
	INNER JOIN PDM.T04_CORE_ORG_H LV2
		ON T.CORE_ORG_ID=LV2.UP_ORG_ID
		AND ${TX_DATE} BETWEEN LV2.START_DT AND LV2.END_DT
	WHERE T.ORG_HRCY_CD=1
	';
	CALL ETL.PR_EXEC_SQL(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	-- 第三组：插入三级机构 ${NULL_STR}
	SET @SQL_STR = '
	INSERT INTO PDM.VT_t88_mbank_core_org_info(
		Statt_Dt,-- 1.统计日期
		Core_Org_Id,-- 2.核心机构编号
		Org_Cate_Cd,-- 3.机构类型代码
		Org_Nm,-- 4.机构名称
		Org_Hrcy_Cd,-- 5.机构层级代码
		Org_Stat_Cd,-- 6.机构状态代码
		Lvl1_Org_Id,-- 7.一级机构编号
		Lvl1_Org_Nm,-- 8.一级机构名称
		Lvl2_Org_Id,-- 9.二级机构编号
		Lvl2_Org_Nm,-- 10.二级机构名称
		Lvl3_Org_Id,-- 11.三级机构编号
		Lvl3_Org_Nm,-- 12.三级机构名称
		Lvl4_Org_Id,-- 13.四级机构编号
		Lvl4_Org_Nm,-- 14.四级机构名称
		Up_Org_Id,-- 15.上级机构编号
		Org_Regn_Cd,-- 16.机构区域代码
		Data_Src_Cd,-- 17.数据来源代码
		Main_Src_Task-- 18.主要源系统任务
	)
	SELECT 
		${TX_DATE},-- 1.统计日期
		LV3.ORG_ID,-- 2.核心机构编号
		LV3.ORG_CATE_CD,-- 3.机构类型代码
		LV3.ORG_NM,-- 4.机构名称
		3,-- 5.机构层级代码
		LV3.ORG_STAT_CD,-- 6.机构状态代码
		T.Core_Org_Id,-- 7.一级机构编号
		T.Org_Nm,-- 8.一级机构名称
		LV2.CORE_ORG_ID,-- 9.二级机构编号
		LV2.ORG_NM,-- 10.二级机构名称
		LV3.ORG_ID,-- 11.三级机构编号
		LV3.ORG_NM,-- 12.三级机构名称
		${NULL_STR},-- 13.四级机构编号
		${NULL_STR},-- 14.四级机构名称
		LV3.UP_ORG_ID,-- 15.上级机构编号
		${NULL_STR},-- 16.机构区域代码
		\'NCS\',-- 17.数据来源代码
		\'T04_CORE_ORG_H\'-- 18.主要源系统任务
	FROM PDM.VT_T88_MBANK_CORE_ORG_INFO T
	LEFT JOIN PDM.VT_T88_MBANK_CORE_ORG_INFO LV2
		ON T.CORE_ORG_ID=LV2.Up_Org_Id
		AND lv2.ORG_HRCY_CD=2
	INNER JOIN PDM.T04_CORE_ORG_H LV3
		ON LV2.CORE_ORG_ID=LV3.UP_ORG_ID
		AND ${TX_DATE} BETWEEN LV3.START_DT AND LV3.END_DT
	WHERE T.ORG_HRCY_CD=1
	';
	CALL ETL.PR_EXEC_SQL(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	-- 第四组：插入四级机构 ${NULL_STR}
	SET @SQL_STR = '
	INSERT INTO PDM.VT_t88_mbank_core_org_info(
		Statt_Dt,-- 1.统计日期
		Core_Org_Id,-- 2.核心机构编号
		Org_Cate_Cd,-- 3.机构类型代码
		Org_Nm,-- 4.机构名称
		Org_Hrcy_Cd,-- 5.机构层级代码
		Org_Stat_Cd,-- 6.机构状态代码
		Lvl1_Org_Id,-- 7.一级机构编号
		Lvl1_Org_Nm,-- 8.一级机构名称
		Lvl2_Org_Id,-- 9.二级机构编号
		Lvl2_Org_Nm,-- 10.二级机构名称
		Lvl3_Org_Id,-- 11.三级机构编号
		Lvl3_Org_Nm,-- 12.三级机构名称
		Lvl4_Org_Id,-- 13.四级机构编号
		Lvl4_Org_Nm,-- 14.四级机构名称
		Up_Org_Id,-- 15.上级机构编号
		Org_Regn_Cd,-- 16.机构区域代码
		Data_Src_Cd,-- 17.数据来源代码
		Main_Src_Task-- 18.主要源系统任务
	)
	SELECT 
		${TX_DATE},-- 1.统计日期
		LV4.ORG_ID,-- 2.核心机构编号
		LV4.ORG_CATE_CD,-- 3.机构类型代码
		LV4.ORG_NM,-- 4.机构名称
		4,-- 5.机构层级代码
		LV4.ORG_STAT_CD,-- 6.机构状态代码
		T.Core_Org_Id,-- 7.一级机构编号
		T.Org_Nm,-- 8.一级机构名称
		LV2.CORE_ORG_ID,-- 9.二级机构编号
		LV2.ORG_NM,-- 10.二级机构名称
		LV3.CORE_ORG_ID,-- 11.三级机构编号
		LV3.ORG_NM,-- 12.三级机构名称
		LV4.ORG_ID,-- 13.四级机构编号
		LV4.ORG_NM,-- 14.四级机构名称
		LV4.UP_ORG_ID,-- 15.上级机构编号
		${NULL_STR},-- 16.机构区域代码
		\'NCS\',-- 17.数据来源代码
		\'T04_CORE_ORG_H\'-- 18.主要源系统任务
	FROM PDM.VT_T88_MBANK_CORE_ORG_INFO T
	INNER JOIN PDM.VT_T88_MBANK_CORE_ORG_INFO LV2
		ON T.CORE_ORG_ID=LV2.Up_Org_Id
		AND lv2.ORG_HRCY_CD=2
	INNER JOIN PDM.VT_T88_MBANK_CORE_ORG_INFO LV3
		ON LV2.CORE_ORG_ID=LV3.Up_Org_Id
		AND lv3.ORG_HRCY_CD=3
	INNER JOIN PDM.T04_CORE_ORG_H LV4
		ON LV3.CORE_ORG_ID=LV4.UP_ORG_ID
		AND ${TX_DATE} BETWEEN LV4.START_DT AND LV4.END_DT
	WHERE T.ORG_HRCY_CD=1
	';
	CALL ETL.PR_EXEC_SQL(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	
	/*检查插入的临时表数据是否有主键错误*/
	
	-- 获取主键字段
	SELECT PHYSICAL_PRI_KEY INTO @PK_COLUMN FROM DATAMAPPING_TASK WHERE T_TAB_ENG_NAME=ETL_T_TAB_ENG_NAME;
	-- 0正常
	SET @SQL_STR = 'SELECT COUNT(1) INTO @PK_COUNT FROM(SELECT 1 FROM PDM.VT_'||ETL_T_TAB_ENG_NAME||' GROUP BY '||@PK_COLUMN||' HAVING COUNT(1)>1) T';
	CALL ETL.PR_EXEC_SQL(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	 
	IF @PK_COUNT <> 0
	THEN
		UPDATE ETL.ETL_JOB_STATUS_EDW SET STEP_STATUS='Failed',STEP_ERR_LOG='主键重复', LAST_END_TIME=CURRENT_TIMESTAMP WHERE SQL_UNIT=ETL_T_TAB_ENG_NAME AND TX_DATE=TX_DATE AND STEP_NO=ETL_STEP_NO-1;
		LEAVE LABLE;
	END IF;
	
	/*通过主键检查的数据插入正式表中*/
	SET @SQL_STR='INSERT INTO PDM.'||ETL_T_TAB_ENG_NAME||' SELECT * FROM PDM.VT_'||ETL_T_TAB_ENG_NAME||' WHERE STATT_DT=${TX_DATE}'; 
	CALL ETL.PR_EXEC_SQL(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

	SET OUT_RES_MSG='SUCCESSFUL';
	

end |