DELIMITER |

CREATE DEFINER="cq_sjpt"@"%" PROCEDURE "pr_sys_shrink_table"(
	out	P_O_RESULT varchar(10),
	IN P_SCHEMA_NAME varchar(20)
	)
begin
   
	/*
	 * 作者：ljz
	 * 创建日期：20230817
	 * 修改记录：
	 *   20230817：初版 清理shrinkable_size前100的表大小
	 * 
	 */
	
    DECLARE O_TABLE_VC VARCHAR(60);
    DECLARE O_TABLE_SCHEMA VARCHAR(100);
    DECLARE O_TABLE_NAME VARCHAR(100);
    DECLARE O_MAX_ROWID VARCHAR(100);
    DECLARE O_DELETE_ROWS VARCHAR(100);
    DECLARE O_TABLE_ROWS VARCHAR(100);
    DECLARE O_STORAGE_SIZE VARCHAR(100);
    DECLARE O_DELETABLE_SIZE VARCHAR(100);
    DECLARE O_SHRINKABLE_SIZE VARCHAR(100);
    DECLARE O_DELETE_RATIO VARCHAR(100);
    
    DECLARE V_TABLE_NAME VARCHAR(100);
	DECLARE DONE INT DEFAULT(0);
	DECLARE cur REF CURSOR;
	DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET done = 1;

	SET P_SCHEMA_NAME = decode(nvl(P_SCHEMA_NAME,''),'','ods',P_SCHEMA_NAME);
	OPEN cur FOR
	SELECT TABLE_SCHEMA,TABLE_NAME FROM etl.sys_performance_schema_tables t
    WHERE table_schema=P_SCHEMA_NAME
    AND statt_dt=(SELECT max(statt_dt) FROM etl.sys_performance_schema_tables WHERE table_schema=P_SCHEMA_NAME)
    ORDER BY shrinkable_size DESC LIMIT 100;
    
	REPEAT
	FETCH cur INTO O_TABLE_SCHEMA,O_TABLE_NAME;
	IF NOT DONE THEN
        -- alter table ods.aabc shrink space full;
        SET @V_SQL = 'alter table '||O_TABLE_SCHEMA||'.'||O_TABLE_NAME||' shrink space full;';
        SELECT @V_SQL;
		-- prepare V_STMT from @V_SQL;
		-- execute V_STMT;
		END IF;
	UNTIL DONE END REPEAT;
	CLOSE cur;
	SET P_O_RESULT = '0';
END |