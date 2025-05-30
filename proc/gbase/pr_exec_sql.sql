DELIMITER |

CREATE DEFINER="cq_sjpt"@"%" PROCEDURE "pr_exec_sql"(
	OUT	OUT_CODE		INTEGER,	
	IN	IN_USER_NAME	VARCHAR(60),
	IN	IN_SQL_UNIT		VARCHAR(100),
	IN	IN_STEP_NO		INTEGER,
	IN	IN_STEP_SQL		TEXT,
	IN	IN_TX_DATE		VARCHAR(8)
)
lable:BEGIN
	DECLARE	ETL_USER_NAME			VARCHAR(200)	DEFAULT IN_USER_NAME;
	DECLARE ETL_USER_ID				VARCHAR(100)	DEFAULT '';
	DECLARE	ETL_SQL_UNIT			VARCHAR(100)	DEFAULT IN_SQL_UNIT;
	DECLARE	ETL_STEP_NO				INTEGER			DEFAULT	IN_STEP_NO;
	DECLARE	ETL_STEP_SQL			TEXT			DEFAULT IN_STEP_SQL;
	DECLARE ETL_TX_DATE				VARCHAR(8)		DEFAULT IN_TX_DATE;
	DECLARE ETL_LAST_TIMESTAMP		VARCHAR(19)		DEFAULT '--';
	DECLARE ETL_LAST_ACTIVE_COUNT	BIGINT			DEFAULT 0;
	DECLARE ETL_RUNNING_STEP_COUNT	BIGINT			DEFAULT 0;
	DECLARE ERR_CODE				VARCHAR(2000);
	DECLARE ERR_MSG					VARCHAR(2000);
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
		GET DIAGNOSTICS condition 1 ERR_CODE = returned_sqlstate, ERR_MSG = message_text;
		SELECT COUNT(*) INTO ETL_RUNNING_STEP_COUNT 
		FROM etl.etl_job_status_edw 
		WHERE user_id 	= ETL_USER_ID 
		AND tx_date 	= ETL_TX_DATE 
		AND step_no     = ETL_STEP_NO
		AND sql_unit 	= ETL_SQL_UNIT
		AND step_status	= 'Running';
		
		IF ETL_RUNNING_STEP_COUNT = 0 THEN
			INSERT INTO etl.etl_job_status_edw VALUES ('',ETL_USER_ID,ETL_SQL_UNIT,ETL_TX_DATE,ETL_STEP_NO,ETL_STEP_SQL,-1,'Failed',ERR_CODE||':'||ERR_MSG,ETL_LAST_TIMESTAMP,CURRENT_TIMESTAMP);
		ELSE 
			UPDATE etl.etl_job_status_edw 
			SET step_status = 'Failed'
			,step_err_log 	= ERR_CODE||':'||ERR_MSG
			,last_end_time 	= current_timestamp
			WHERE user_id 	= ETL_USER_ID 
			AND tx_date 	= ETL_TX_DATE 
			AND step_no 	= ETL_STEP_NO
			AND sql_unit 	= ETL_SQL_UNIT
			AND step_status = 'Running';
		END IF;
		SET OUT_CODE = 12;
	END;
	SET OUT_CODE = 0;
	SELECT SESSION_USER() INTO ETL_USER_ID;
	SELECT CURRENT_TIMESTAMP INTO ETL_LAST_TIMESTAMP;
	DELETE FROM etl.etl_job_status_edw 
	WHERE user_id 	= ETL_USER_ID 
	AND	tx_date 	= ETL_TX_DATE
    AND step_no    >= ETL_STEP_NO
	AND sql_unit 	= ETL_SQL_UNIT;
	SELECT etl.etl_func_rep_sql(ETL_STEP_SQL,ETL_TX_DATE) INTO ETL_STEP_SQL; 
	SET	@EXEC_SQL = ETL_STEP_SQL;	
	PREPARE	EXEC_SQL_STMT	FROM @EXEC_SQL;
	SELECT CURRENT_TIMESTAMP INTO ETL_LAST_TIMESTAMP;
	INSERT INTO etl.etl_job_status_edw VALUES ('',ETL_USER_ID,ETL_SQL_UNIT,ETL_TX_DATE,ETL_STEP_NO,ETL_STEP_SQL,0,'Running','',ETL_LAST_TIMESTAMP,'');
	EXECUTE EXEC_SQL_STMT;
	SELECT ROW_COUNT() INTO ETL_LAST_ACTIVE_COUNT;
	UPDATE	etl.etl_job_status_edw 
	SET last_end_time 	= CURRENT_TIMESTAMP
	,step_status 		= 'Done'
	,step_active_count 	= ETL_LAST_ACTIVE_COUNT
	WHERE user_id 	= ETL_USER_ID 
	AND	tx_date 	= ETL_TX_DATE
	AND step_no 	= ETL_STEP_NO
	AND sql_unit 	= ETL_SQL_UNIT
	AND step_status = 'Running';
	DEALLOCATE PREPARE EXEC_SQL_STMT;
END |