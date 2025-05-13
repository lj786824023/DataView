DELIMITER |

CREATE DEFINER="cq_sjpt"@"%" PROCEDURE "pr_sys_pdm_move"(
	OUT	P_O_RESULT		VARCHAR(200),
	IN	P_I_DATE		VARCHAR(8)
	)
begin
   
	/*
	 * 作者：ljz
	 * 创建日期：20230816
	 * 修改记录：
	 *   20230816：初版 清理ods2年前且不为月底的数据
	 * 
	 */

    DECLARE V_TABLE_NAME VARCHAR(100);
    DECLARE V_MOVE_DATE DATE DEFAULT date('20230501');
	DECLARE DONE INT DEFAULT(0);
	DECLARE cur REF CURSOR;
	DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET done = 1;
	
	OPEN cur FOR
	SELECT t_tab_eng_name FROM etl.datamapping_task WHERE etl_algorithm='F3' AND t_tab_eng_name='t01_cust_h' ORDER BY 1;
	
	REPEAT
	FETCH cur INTO V_TABLE_NAME;
	IF NOT DONE THEN
        -- 表不存在建表
        SET @V_SQL = 'create table if not exists pdm.'||V_TABLE_NAME||'_f3_h like pdm.'||V_TABLE_NAME;
        SELECT @V_SQL;
        -- 字段不存在加字段
        -- 迁移
        SET @V_SQL = 'insert into pdm.'||V_TABLE_NAME||'_f3_h select * from pdm.'||V_TABLE_NAME||' where end_dt<'||V_MOVE_DATE;
        SELECT @V_SQL;
        -- 核对
        SET @V_SQL = 'SELECT count(1) FROM (SELECT * FROM (SELECT * FROM pdm.'||V_TABLE_NAME||' WHERE end_dt<'||V_MOVE_DATE||') a minus SELECT * FROM (SELECT * FROM pdm.'||V_TABLE_NAME||'_f3_h WHERE end_dt<'||V_MOVE_DATE||') b) t';
        SELECT @V_SQL;
        -- 删除
	    SET @V_SQL = 'delete from pdm.'||V_TABLE_NAME||' where end_dt<'||V_MOVE_DATE;
	    SELECT @V_SQL;
	    -- prepare V_STMT from @V_SQL;
		-- execute V_STMT;
		END IF;
	UNTIL DONE END REPEAT;
	CLOSE cur;
	SET P_O_RESULT = '0';
END |