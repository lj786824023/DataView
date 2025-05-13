DELIMITER |

CREATE DEFINER="gbase"@"%" FUNCTION "etl_func_rep_schema"(
in_env varchar(10) ,
in_str varchar(8000) ,
in_dt  varchar(8)
) RETURNS varchar(8000) CHARSET gbk
BEGIN
	declare etl_OUT_RES_MSG varchar(8000);
	declare etl_in_str varchar(8000) default in_str;
	declare etl_in_env varchar(10) default in_env;
	declare etl_env_dev varchar(8000) default in_str;
	declare etl_env_pro varchar(8000) default in_str;
	-- declare etl_tx_date varchar(8) default in_dt;
	declare IN_TX_DATE varchar(8) default in_dt;
	

/* DEFINE DATE TYPE VARIABLES */
	SELECT 'CAST('''||IN_TX_DATE||''' AS DATE)' 						INTO @TX_DATE;
	SELECT 'CAST('''||IN_TX_DATE||''' AS DATE) - 1'						INTO @LAST_TX_DATE;
	SELECT 'CAST('''||TO_CHAR(DATE_SUB(DATE_SUB(IN_TX_DATE,INTERVAL (MONTH(IN_TX_DATE)-1)%3 MONTH) ,INTERVAL DAY(IN_TX_DATE)-1 DAY),'YYYY-MM-DD')||''' AS DATE)' INTO @THIS_QUART_BEGIN;
	SELECT 'CAST('''||SUBSTR(IN_TX_DATE,1,6)||'01'||''' AS DATE)' 		INTO @THIS_MONTH_BEGIN;
	SELECT 'CAST('''||SUBSTR(IN_TX_DATE,1,6)||'01'||''' AS DATE) - 1'	INTO @LAST_MONTH_END;
	SELECT 'CAST('''||SUBSTR(IN_TX_DATE,1,4)||'0101'||''' AS DATE)'		INTO @THIS_YEAR_BEGIN;
	SELECT 'CAST('''||SUBSTR(IN_TX_DATE,1,4)||'0101'||''' AS DATE) - 1'	INTO @LAST_YEAR_END;
	SELECT 'CAST(''0001-01-01'' AS DATE)'								INTO @NULL_DATE;
	SELECT 'CAST(''0001-01-02'' AS DATE)'								INTO @ILL_DATE;
	SELECT 'CAST(''9999-12-31'' AS DATE)'								INTO @MAX_DATE;
	SELECT 'CAST(''1900-01-01'' AS DATE)'								INTO @INIT_DATE;
	
	SELECT ''''||IN_TX_DATE||''''										INTO @TX_DATE_8;
	SELECT 'TO_CHAR('||@LAST_TX_DATE||',''YYYYMMDD'')'					INTO @LAST_TX_DATE_8;
	SELECT 'TO_CHAR('||@THIS_QUART_BEGIN||',''YYYYMMDD'')'				INTO @THIS_QUART_BEGIN_8;
	SELECT 'TO_CHAR('||@THIS_MONTH_BEGIN||',''YYYYMMDD'')'				INTO @THIS_MONTH_BEGIN_8;
	SELECT 'TO_CHAR('||@LAST_MONTH_END||',''YYYYMMDD'')'				INTO @LAST_MONTH_END_8;
	SELECT 'TO_CHAR('||@THIS_YEAR_BEGIN||',''YYYYMMDD'')'				INTO @THIS_YEAR_BEGIN_8;
	SELECT 'TO_CHAR('||@LAST_YEAR_END||',''YYYYMMDD'')'					INTO @LAST_YEAR_END_8;
	SELECT '00010101'													INTO @NULL_DATE_8;
	SELECT '00010102'													INTO @ILL_DATE_8;
	SELECT '99991231'													INTO @MAX_DATE_8;
	SELECT '19000101'													INTO @INIT_DATE_8;
	
	/* DEFINE SCEHMA VARIABLES */
	SELECT 'ODS'														INTO @ODS_SCHEMA;
	SELECT 'PDM'														INTO @PDM_SCHEMA;


	
	/* REPLACE DATE TYPE VARIABLES */
	SELECT REPLACE(etl_env_dev,'${TX_DATE}',@TX_DATE) INTO etl_env_dev;
	SELECT REPLACE(etl_env_dev,'${LAST_TX_DATE}',@LAST_TX_DATE) INTO etl_env_dev;
	SELECT REPLACE(etl_env_dev,'${THIS_QUART_BEGIN}',@THIS_QUART_BEGIN) INTO etl_env_dev;
	SELECT REPLACE(etl_env_dev,'${THIS_MONTH_BEGIN}',@THIS_MONTH_BEGIN) INTO etl_env_dev;
	SELECT REPLACE(etl_env_dev,'${LAST_MONTH_END}',@LAST_MONTH_END) INTO etl_env_dev;
	SELECT REPLACE(etl_env_dev,'${THIS_YEAR_BEGIN}',@THIS_YEAR_BEGIN) INTO etl_env_dev;
	SELECT REPLACE(etl_env_dev,'${LAST_YEAR_END}',@LAST_YEAR_END) INTO etl_env_dev;
	SELECT REPLACE(etl_env_dev,'${NULL_DATE}',@NULL_DATE) INTO etl_env_dev;
	SELECT REPLACE(etl_env_dev,'${ILL_DATE}',@ILL_DATE) INTO etl_env_dev;
	SELECT REPLACE(etl_env_dev,'${MAX_DATE}',@MAX_DATE) INTO etl_env_dev;
	SELECT REPLACE(etl_env_dev,'${INIT_DATE}',@INIT_DATE) INTO etl_env_dev;
	
	SELECT REPLACE(etl_env_dev,'${TX_DATE_8}',@TX_DATE_8) INTO etl_env_dev;
	SELECT REPLACE(etl_env_dev,'${LAST_TX_DATE_8}',@LAST_TX_DATE_8) INTO etl_env_dev;
	SELECT REPLACE(etl_env_dev,'${THIS_QUART_BEGIN_8}',@THIS_QUART_BEGIN_8) INTO etl_env_dev;
	SELECT REPLACE(etl_env_dev,'${THIS_MONTH_BEGIN_8}',@THIS_MONTH_BEGIN_8) INTO etl_env_dev;
	SELECT REPLACE(etl_env_dev,'${LAST_MONTH_END_8}',@LAST_MONTH_END_8) INTO etl_env_dev;
	SELECT REPLACE(etl_env_dev,'${THIS_YEAR_BEGIN_8}',@THIS_YEAR_BEGIN_8) INTO etl_env_dev;
	SELECT REPLACE(etl_env_dev,'${LAST_YEAR_END_8}',@LAST_YEAR_END_8) INTO etl_env_dev;
	SELECT REPLACE(etl_env_dev,'${NULL_DATE_8}',@NULL_DATE_8) INTO etl_env_dev;
	SELECT REPLACE(etl_env_dev,'${ILL_DATE_8}',@ILL_DATE_8) INTO etl_env_dev;
	SELECT REPLACE(etl_env_dev,'${MAX_DATE_8}',@MAX_DATE_8) INTO etl_env_dev;
	SELECT REPLACE(etl_env_dev,'${INIT_DATE_8}',@INIT_DATE_8) INTO etl_env_dev;
	SELECT REPLACE(etl_env_dev,'${NULL_STR}','\'\'') INTO etl_env_dev;
	
	/* REPLACE SCEHMA VARIABLES */
	SELECT REPLACE(etl_env_dev,'${AUTO_ODS}',@ODS_SCHEMA) INTO etl_env_dev;
	SELECT REPLACE(etl_env_dev,'${AUTO_PDM}',@PDM_SCHEMA) INTO etl_env_dev;
	
	/* REPLACE DATE TYPE VARIABLES */
	SELECT REPLACE(etl_env_pro,'${TX_DATE}',@TX_DATE) INTO etl_env_pro;
	SELECT REPLACE(etl_env_pro,'${LAST_TX_DATE}',@LAST_TX_DATE) INTO etl_env_pro;
	SELECT REPLACE(etl_env_pro,'${THIS_QUART_BEGIN}',@THIS_QUART_BEGIN) INTO etl_env_pro;
	SELECT REPLACE(etl_env_pro,'${THIS_MONTH_BEGIN}',@THIS_MONTH_BEGIN) INTO etl_env_pro;
	SELECT REPLACE(etl_env_pro,'${LAST_MONTH_END}',@LAST_MONTH_END) INTO etl_env_pro;
	SELECT REPLACE(etl_env_pro,'${THIS_YEAR_BEGIN}',@THIS_YEAR_BEGIN) INTO etl_env_pro;
	SELECT REPLACE(etl_env_pro,'${LAST_YEAR_END}',@LAST_YEAR_END) INTO etl_env_pro;
	SELECT REPLACE(etl_env_pro,'${NULL_DATE}',@NULL_DATE) INTO etl_env_pro;
	SELECT REPLACE(etl_env_pro,'${ILL_DATE}',@ILL_DATE) INTO etl_env_pro;
	SELECT REPLACE(etl_env_pro,'${MAX_DATE}',@MAX_DATE) INTO etl_env_pro;
	SELECT REPLACE(etl_env_pro,'${INIT_DATE}',@INIT_DATE) INTO etl_env_pro;
	
	SELECT REPLACE(etl_env_pro,'${TX_DATE_8}',@TX_DATE_8) INTO etl_env_pro;
	SELECT REPLACE(etl_env_pro,'${LAST_TX_DATE_8}',@LAST_TX_DATE_8) INTO etl_env_pro;
	SELECT REPLACE(etl_env_pro,'${THIS_QUART_BEGIN_8}',@THIS_QUART_BEGIN_8) INTO etl_env_pro;
	SELECT REPLACE(etl_env_pro,'${THIS_MONTH_BEGIN_8}',@THIS_MONTH_BEGIN_8) INTO etl_env_pro;
	SELECT REPLACE(etl_env_pro,'${LAST_MONTH_END_8}',@LAST_MONTH_END_8) INTO etl_env_pro;
	SELECT REPLACE(etl_env_pro,'${THIS_YEAR_BEGIN_8}',@THIS_YEAR_BEGIN_8) INTO etl_env_pro;
	SELECT REPLACE(etl_env_pro,'${LAST_YEAR_END_8}',@LAST_YEAR_END_8) INTO etl_env_pro;
	SELECT REPLACE(etl_env_pro,'${NULL_DATE_8}',@NULL_DATE_8) INTO etl_env_pro;
	SELECT REPLACE(etl_env_pro,'${ILL_DATE_8}',@ILL_DATE_8) INTO etl_env_pro;
	SELECT REPLACE(etl_env_pro,'${MAX_DATE_8}',@MAX_DATE_8) INTO etl_env_pro;
	SELECT REPLACE(etl_env_pro,'${INIT_DATE_8}',@INIT_DATE_8) INTO etl_env_pro;
	SELECT REPLACE(etl_env_pro,'${NULL_STR}','\'\'') INTO etl_env_pro;
	
	/* REPLACE SCEHMA VARIABLES */
	SELECT REPLACE(etl_env_pro,'${AUTO_ODS}',@ODS_SCHEMA) INTO etl_env_pro;
	SELECT REPLACE(etl_env_pro,'${AUTO_PDM}',@PDM_SCHEMA) INTO etl_env_pro;
	
	select 
		case when upper(etl_in_env) = 'DEV' then etl_env_dev
		when upper(etl_in_env) = 'PRO' then etl_env_pro
		else etl_env_pro end 
	into etl_OUT_RES_MSG;
	
	return etl_OUT_RES_MSG;
	
END |