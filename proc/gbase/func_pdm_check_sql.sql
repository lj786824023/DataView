DELIMITER |

CREATE DEFINER="cq_sjpt"@"%" FUNCTION "func_pdm_check_sql"(
	P_TAB_NAME VARCHAR(100)
	) RETURNS longtext CHARSET gbk
begin
	
	/*
	 * 作者：ljz
	 * 创建日期：20230401
	 * 修改记录：
	 *   20230401：初版:生成校验sql
	 * 
	 */
	
	DECLARE V_CHECK_SQL LONGTEXT DEFAULT NULL;
	
	-- SELECT etl_algorithm,physical_pri_key INTO @V_ETL_ALGORITHM,@PHYSICAL_PRI_KEY
    -- FROM etl.datamapping_task WHERE t_tab_eng_name='t01_ext_mercht_info_h';
	SELECT count(1) INTO @V_CHECK_SQL FROM etl.datamapping_task;
	-- IF @V_ETL_ALGORITHM IS NULL THEN
	--  RETURN NULL;
	-- END IF;
	
    -- SET V_CHECK_SQL='select \''||P_TAB_NAME||'\' as tablename,count(1) as allcnt from pdm.'||P_TAB_NAME;
	
	RETURN V_CHECK_SQL;
	
END |