DELIMITER |

CREATE DEFINER="cq_sjpt"@"%" PROCEDURE "pr_chk_tab_cnt"(
	out	P_O_RESULT varchar(100),	
	in P_DATA_DATE varchar(10)
	)
begin
   
	/*
	 * 作者：ljz
	 * 创建日期：20220327
	 * 修改记录：
	 *   20220327：初版
	 * 
	 */

	DECLARE TAB_NAME VARCHAR(100);
	DECLARE EXEC_SQL longtext;
	DECLARE DONE INT DEFAULT(0);
	DECLARE COND CONDITION FOR 1;
	DECLARE CUR REF CURSOR;
	-- 异常处理
	DECLARE CONTINUE HANDLER FOR NOT FOUND
	BEGIN
		SET done=1;
	END;
	
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
		-- 写入日志表rwa.sys_job_status_edw
		GET DIAGNOSTICS condition 1 @ERR_CODE = returned_sqlstate, @ERR_MSG = message_text;
		CALL rwa.pr_sys_job_status_edw(@V_SQL,'Failed');
		SET P_O_RESULT = '12';
	END;
	
	-- 初始化变量
	SET @V_DATA_DATE = P_DATA_DATE; -- 数据日期
	SET @V_TABLE_NAME = 'etl_pdm_check'; -- 目标表名
	SET @V_ROW_COUNT = 0; -- 影响行数(DML步骤需更新)
	CALL rwa.pr_sys_job_status_edw(@V_SQL,'init'); -- 初始化日志表

	DELETE FROM etl.etl_pdm_check WHERE statt_dt = @V_DATA_DATE;
	
	-- 统计接口表数据量
	OPEN CUR FOR
	SELECT T_TAB_ENG_NAME,CHECK_SQL FROM etl.etl_pdm_check_sql;
	
	LOOP_CUR:
	LOOP
	  FETCH CUR INTO TAB_NAME,EXEC_SQL;
	  CALL rwa.pr_sys_job_status_edw('当前统计模型名：'||TAB_NAME,'Running');
	  IF DONE THEN LEAVE LOOP_CUR;END IF;
	  SET @V_SQL = EXEC_SQL;
      PREPARE STMT FROM @V_SQL;
	  EXECUTE STMT;
	  CALL rwa.pr_sys_job_status_edw('当前统计模型名：'||TAB_NAME,'Done');
	END LOOP LOOP_CUR;
	CLOSE CUR;
	
	CALL rwa.pr_sys_job_status_edw('统计结束','Running');
	CALL rwa.pr_sys_job_status_edw('统计结束','Done');
	
    SET P_O_RESULT = '0';
	  
END |