DELIMITER |

CREATE DEFINER="cq_sjpt"@"%" PROCEDURE "pr_call_pro"(
	IN	TABLE_NAME        VARCHAR(100),
	IN	BEGIN_DATE        VARCHAR(100),
	IN	END_DATE          VARCHAR(100)
)
LABLE:begin
   
	/*
	 * 作者：ljz
	 * 创建日期：20220512
	 * 修改记录：
	 *   20220512：初版
	 *   参数1：模型表名
	 *   参数2：开始日期
	 *   参数3：结束日期（省略为只执行开始日期那一天）
	 *  涉及表：datamapping_task、etl_job_status_edw
	 */

	-- 定义变量
	DECLARE V_TASK_DB VARCHAR(100) DEFAULT 'etl'; -- 算法、映射库
	DECLARE V_DATA_DB VARCHAR(100) DEFAULT 'pdm'; -- 目标表数据库
	DECLARE V_BEGIN_DATE DATE DEFAULT CAST(BEGIN_DATE AS DATE);
	DECLARE V_END_DATE DATE DEFAULT CAST(DECODE(END_DATE,'',BEGIN_DATE,END_DATE) AS DATE);
	DECLARE V_DATA_DATE DATE DEFAULT CAST(BEGIN_DATE AS DATE);
    DECLARE O_RESULT TEXT DEFAULT '';

	-- 校验日期
	IF V_BEGIN_DATE > V_END_DATE THEN
	  SELECT '开始日期大于结束日期！'; -- 错误输出
	  LEAVE LABLE;
	END IF;

	-- 获取算法
	SET @V_SQL = 'select lower(etl_algorithm) into @V_ETL_ALGORITHM from '||V_TASK_DB||'.datamapping_task where lower(t_tab_eng_name)=lower(?)';
	SET @V_SQL_P1 = TABLE_NAME;
	PREPARE STMT FROM @V_SQL;
	EXECUTE STMT USING @V_SQL_P1;
	IF @V_ETL_ALGORITHM IS NULL THEN
	    SELECT '在datamapping_task中未找到'||TABLE_NAME||'对应的算法！'; -- 错误输出
	    LEAVE LABLE;
	END IF;
	
	-- 开始循环执行
	WHILE V_DATA_DATE <= V_END_DATE DO
	  SET @V_SQL = 'call '||V_TASK_DB||'.pr_'||@V_ETL_ALGORITHM||'(@RTA,?,?,?,?)';
	  SET @V_PR_P1 = REPLACE(V_DATA_DATE,'-','');
	  SET @V_PR_P2 = V_DATA_DB;
	  SET @V_PR_P3 = TABLE_NAME;
	  SET @V_PR_P4 = '';
	  PREPARE V_STMT FROM @V_SQL;
	  EXECUTE V_STMT USING @V_PR_P1,@V_PR_P2,@V_PR_P3,@V_PR_P4;
	  SET O_RESULT = REGEXP_REPLACE(@V_SQL,'\\?,\\?,\\?,\\?','\''||@V_PR_P1||'\',\''||@V_PR_P2||'\',\''||@V_PR_P3||'\',\'\'');
	  
	  -- 报错返回
	  IF @RTA <> 0 THEN
		SET @V_SQL = 'select step_err_log into @V_ERR from '||V_TASK_DB||'.etl_job_status_edw where lower(sql_unit)=lower(?) and tx_DATE=? and step_status=? and user_id=?';
	    SET @V_PR_P1 = TABLE_NAME;
	    SET @V_PR_P2 = REPLACE(V_DATA_DATE,'-','');
	    SET @V_PR_P3 = 'Failed';
	    SET @V_PR_P4 = SESSION_USER();
	    PREPARE V_STMT FROM @V_SQL;
	    EXECUTE V_STMT USING @V_PR_P1,@V_PR_P2,@V_PR_P3,@V_PR_P4;
		SET @V_ERR = NVL(@V_ERR,'');
		
	    SELECT '执行"'||O_RESULT||'"时报错！\n错误日志 ：\n'||@V_ERR; -- 错误输出
	    LEAVE LABLE;
	  END IF;
	  SET V_DATA_DATE = V_DATA_DATE + 1;
    END WHILE;
    
    SELECT '模型：'||V_DATA_DB||'.'||TABLE_NAME||'，算法：'||V_TASK_DB||'.pr_'||@V_ETL_ALGORITHM||'，日期：'||BEGIN_DATE||'~'||DECODE(END_DATE,'',BEGIN_DATE,END_DATE)||'执行成功！'; -- 正常输出

END |