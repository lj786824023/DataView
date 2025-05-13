DELIMITER |

CREATE DEFINER="cq_sjpt"@"%" PROCEDURE "pr_sys_table_clear"(
	out	P_O_RESULT varchar(10)
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
	DECLARE DONE INT DEFAULT(0);
	DECLARE cur REF CURSOR;
	DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET done = 1;
	
	OPEN cur FOR
	SELECT tbName FROM gbase.table_distribution d
	INNER JOIN information_schema.tables t
	ON d.dbName=t.table_schema
	AND d.tbName=t.table_name
	AND t.engine='EXPRESS'
	WHERE d.dbName='ods' AND tbName IN ('jcf_acc_check_bill_merchant','jcf_cle_cleaning_bill');
	
	REPEAT
	FETCH cur INTO V_TABLE_NAME;
	IF NOT DONE THEN
	    -- DELETE FROM ods.ncs_mb_acct WHERE sdate < to_char(current_date-730,'yyyymmdd') AND sdate<>to_char(last_day(sdate),'yyyymmdd')
	    SET @V_SQL = 'delete from ods.'||V_TABLE_NAME||' where sdate<to_char(current_date-730,\'yyyymmdd\') and sdate<>to_char(last_day(sdate),\'yyyymmdd\')';
		SELECT @V_SQL;
	    -- prepare V_STMT from @V_SQL;
		-- execute V_STMT;
		END IF;
	UNTIL DONE END REPEAT;
	CLOSE cur;
	SET P_O_RESULT = '0';
END |