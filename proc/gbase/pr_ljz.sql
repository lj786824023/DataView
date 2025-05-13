DELIMITER |

CREATE DEFINER="cq_sjpt"@"%" PROCEDURE "pr_ljz"()
begin
	
	-- select * from datamapping_task where t_tab_eng_name in ('t02_prod_feat_h','t03_agmt_dt_h')
	-- set @V_SQL='select * from datamapping_task where t_tab_eng_name in (?,?)';
	-- set @V1='t02_prod_feat_h';
	-- set @V2='t03_agmt_dt_h';
	-- prepare stmt from @V_SQL;
	-- execute stmt using @V1,@V2;
	
	-- select 666;
	-- show variables like '%group_concat%';
	declare V_SCHEMA_NAME varchar(100) default 't07_camp_tool';
	-- select lower(etl_algorithm) into @V_ETL_ALGORITHM from etl.datamapping_task where lower(t_tab_eng_name)=lower(TABLE_NAME);
	SET @V_SQL = 'select lower(etl_algorithm) into @V_ETL_ALGORITHM from etl.datamapping_task where lower(t_tab_eng_name)=lower(?)';
	SET @V_P1 = 'etl';
	SET @V_P2 = 't07_camp_tool';
	SELECT @V_SQL,V_SCHEMA_NAME,@V_SCHEMA_NAME;
	PREPARE STMT FROM @V_SQL;
	EXECUTE STMT USING @V_P2;
	SELECT @V_ETL_ALGORITHM;
end |