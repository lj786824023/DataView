DELIMITER |

CREATE DEFINER="gbase"@"%" PROCEDURE "pr_code_earch"(
        OUT	OUT_RES_MSG		VARCHAR(200),
      	IN	IN_TX_DATE		VARCHAR(8)
      	)
lable:BEGIN
   /**********************************
 * YJ 2022222 新建
 * 基础数据标准码值检测
 *********************************/  
      	DECLARE ETL_TX_DATE				VARCHAR(8)		DEFAULT IN_TX_DATE;
        DECLARE	ETL_S_SQL			    LONGTEXT		;
        DECLARE ETL_T_TAB_ENG_NAME 		VARCHAR(50)		DEFAULT 't99_code_earch';
	    DECLARE ETL_STEP_NO				INTEGER			DEFAULT 1;
	    DECLARE PK_COUNT				BIGINT			DEFAULT 0;
	    DECLARE COL_COUNT               INTEGER     DEFAULT 0;
	    DECLARE COL_NUM                 INTEGER     DEFAULT 0;
	   -- 第一步删除临时表 
	   SET @SQL_STR = '
	     DROP TEMPORARY TABLE IF EXISTS pdm.VT_pre_CODE_EARCH';
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0  THEN SET OUT_RES_MSG = '12';LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	SET @SQL_STR = 'DROP TEMPORARY TABLE IF EXISTS pdm.VT_CODE_EARCH';
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN SET OUT_RES_MSG = '12';LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	SET @SQL_STR = 'DELETE FROM pdm.T99_CODE_EARCH WHERE Statt_Dt >= ${TX_DATE}';
      CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
      IF @RTC <> 0 THEN SET OUT_RES_MSG = '12';LEAVE LABLE;END IF;
      SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	SET @SQL_STR = ' DROP TEMPORARY TABLE IF EXISTS pdm.WT_CODE_EARCH';
      CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
      IF @RTC <> 0 THEN SET OUT_RES_MSG = '12';LEAVE LABLE;END IF;
        SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	SET @SQL_STR = 'CREATE TABLE pdm.WT_CODE_EARCH 
                   select t_tab_eng_name,
                           t_col_eng_name,
                           s_system,
                           s_tab_eng_name,
                           s_col_eng_name,
                           code_value,
                           statt_Dt,
                           remark from 
                       (SELECT t_tab_eng_name,
                               t_col_eng_name,
                               s_system,
                               s_tab_eng_name,
                               s_col_eng_name,
                               code_value,
                               statt_Dt,
                               remark,
                               row_number()over (partition by t_col_eng_name,s_system,s_tab_eng_name,s_col_eng_name,code_value order by statt_Dt desc)as rm 
                         FROM pdm.T99_CODE_EARCH where  remark is not null and remark <>'''')t where rm =1';
      CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
      IF @RTC <> 0 THEN SET OUT_RES_MSG = '12';LEAVE LABLE;END IF;
        SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	 -- 第二步创建临时表 
      	SET @SQL_STR = '
	       CREATE TEMPORARY TABLE pdm.VT_pre_CODE_EARCH(
		    ETL_STEP_SQL	LONGTEXT	
	)';
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN SET OUT_RES_MSG = '12'; LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
      	   	   /**********************************
            * 1,在t99_code_mapping中获取目标表和目标字段，
            * 2,获取datamapping_task的算法得到日期字段
            * 3,拼接成执行SQL，把sql放入临时表
            *********************************/   
      SET @SQL_STR = '
                 INSERT INTO pdm.VT_pre_CODE_EARCH
                  select distinct
                         concat(''INSERT INTO pdm.T99_CODE_EARCH select  ''  
                         ,'''''''', trg_Form_En_Nm,'''''',
                         '','''''''',trg_Fld_En_Nm,'''''',
                         '','''''''',Src_Sys_Sht_Nm,'''''',
                         '','''''''',src_Form_En_Nm,'''''',
                         '','''''''',src_Fld_En_Nm,'''''',
                         '',''TO_CHAR('',Trg_Fld_En_Nm,'')''
                         '',''''${Tx_Dt}''''''
                         '',''''''''''
          ''\nfrom pdm.'',Trg_Form_En_Nm,''
           where  '',Trg_Fld_En_Nm,'' like ''''@%'''''',
            '' \nAND '',Trg_Fld_En_Nm,'' NOT IN (''''@'''',''''@null'''')'',
          case when etl_algorithm = ''F1'' then '' 
          AND Data_Dt = ${Tx_Dt}''
          when etl_algorithm = ''F2'' then '' 
          AND Tx_Dt = ${Tx_Dt}''
          when etl_algorithm = ''F3'' then '' 
          AND Start_Dt <= ${Tx_Dt} 
          and End_dt >=  ${Tx_Dt}''
           when etl_algorithm = ''F5'' then '' 
          AND Statt_Dt =  ${Tx_Dt}''
           end
                   , '' \ngroup by '' , Trg_Fld_En_Nm,'';''
                   
                  )
                  from 
                   (select 
                                   upper(Src_Form_En_Nm) as Src_Form_En_Nm,
                                   upper(Src_Fld_En_Nm) as Src_Fld_En_Nm,
                                   upper(Trg_Form_En_Nm) as Trg_Form_En_Nm,
                                   upper(Trg_Fld_En_Nm) as Trg_Fld_En_Nm,
                                   upper(t2.etl_algorithm) as etl_algorithm,
                                   upper(t1.Src_Sys_Sht_Nm) as Src_Sys_Sht_Nm
                                    from pdm.t99_code_mapping  t1
                            left join etl.datamapping_task t2
                            on t1.Trg_Form_En_Nm = t2.t_tab_eng_name
                                  where t1.Src_Fld_En_Nm <> ''''
                                  and t1.Src_Form_En_Nm <>''''
                                  and t1.Trg_Form_En_Nm  <>''''
                                  and t1.Trg_Fld_En_Nm <>''''
                                  and t1.Src_Sys_Sht_Nm <>''''
                            group by upper(Src_Form_En_Nm),
                                  upper(Src_Fld_En_Nm),
                                  upper(Trg_Form_En_Nm),
                                  upper(Trg_Fld_En_Nm),
                                  upper(t2.etl_algorithm),
                                  upper(t1.Src_Sys_Sht_Nm)
                  )t';
      CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN SET OUT_RES_MSG = '12';LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

	 SET @SQL_STR = '                              
          INSERT INTO pdm.VT_pre_CODE_EARCH
                  select distinct  concat('' insert into pdm.t99_code_earch \n  select
         t1.t_tab_eng_name,
         t1.t_col_eng_name,
         t1.Src_Sys_Sht_Nm,
         t1.s_tab_eng_name,
         t1.s_col_eng_name,
         t1.code_value,
         t1.statt_dt,
         t1.remark
    from(
    select  ''
                        ,'''''''',"t_tab_eng_name",'''''''',''  AS t_tab_eng_name,\n     ''
                     ,'''''''',"t_col_eng_name",'''''''',''  AS t_col_eng_name,\n     ''
                     ,'''''''',"Src_Sys_Sht_Nm",'''''''',''  AS Src_Sys_Sht_Nm,\n     ''
                     ,'''''''',"s_tab_eng_name",'''''''',''  AS s_tab_eng_name,\n     ''
                    ,'''''''',"s_col_eng_name",'''''''',''  AS s_col_eng_name,\n      ''
                     ,"t_col_eng_name",'' as code_value,\n     '',
                  '''','''''''',"map_id",'''''''','' as map_id, \n      '',
                         ''${Tx_Dt}'', '' as statt_dt,\n       '',
                  '''''''''''',''as remark'',
                  ''\n   from pdm.'' 
                ,"t_tab_eng_name", case when UPPER(etl_algorithm) = ''F1'' 
                       then '' WHERE Data_Dt = ${Tx_Dt}'' 
                       when UPPER(etl_algorithm) = ''F2'' 
                       then '' \nWHERE TX_DT = ${Tx_Dt}''
                       when UPPER(etl_algorithm) = ''F3'' 
                       then 
                         '' \n            where Start_Dt <= ${Tx_Dt} 
            and End_dt >=  ${Tx_Dt}'' 
                       when UPPER(etl_algorithm) = ''F5'' 
                       then ''\n         WHERE STATT_DT = ${Tx_Dt}''
                          end ,'' 
            group by '',"t_col_eng_name",'')t1
            left join (select '',''
                       std_cd_cls_cd,
                       trg_sys_std_cd'',''
                     from pdm.t99_pub_std_cd)t2 
                       on t1.map_id =t2.std_cd_cls_cd''
                       ''
                      and t1.code_value = t2.trg_sys_std_cd 
                      WHERE t2.trg_sys_std_cd is null;''
           
                        )from                            
                              (select    
                                   upper(t1.s_tab_eng_name) as s_tab_eng_name,   
                                   upper(t1.s_col_eng_name) as s_col_eng_name,
                                   upper(t1.t_tab_eng_name) as t_tab_eng_name,
                                   upper(t1.t_col_eng_name) as t_col_eng_name,
                                   upper(t2.etl_algorithm) as etl_algorithm,
                                   upper(t1.s_system) as Src_Sys_Sht_Nm,
                                   upper(t1.map_id) as map_id
                                    from pdm.t99_parament_val  t1
                            left join etl.datamapping_task t2
                            on t1.t_tab_eng_name = t2.t_tab_eng_name
                                  where t1.s_col_eng_name <> ''''
                                  and t1.s_tab_eng_name <>''''
                                  and t1.t_tab_eng_name  <>''''
                                  and t1.t_col_eng_name <>''''
                                  and t1.s_system <>''''
                                  and t1.map_id <>''''
                            group by upper(s_tab_eng_name),
                                  upper(t1.s_col_eng_name),
                                  upper(t1.t_tab_eng_name),
                                  upper(t1.t_col_eng_name),
                                  upper(t2.etl_algorithm),
                                  upper(t1.s_system),
                                   upper(t1.map_id)
                                  )t
';	
     CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN SET OUT_RES_MSG = '12';LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	-- 第四步 给拼接的sql赋一个日期，执行拼接的sql
	SET @SQL_STR = 'CREATE TABLE pdm.VT_CODE_EARCH  SELECT  REPLACE(ETL_STEP_SQL,''${Tx_Dt}'',replace(${TX_DATE},''-'','''')) AS ETL_STEP_SQL  from pdm.VT_pre_CODE_EARCH WHERE ETL_STEP_SQL IS NOT NULL';
     CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN SET OUT_RES_MSG = '12'; LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	 -- 循环执行拼接的SQL
	select count(*) into COL_COUNT from pdm.VT_CODE_EARCH;
	 while COL_COUNT >COL_NUM DO
	 select ETL_STEP_SQL into @ETL_S_SQL 
	 from (
	 select ETL_STEP_SQL, row_number()over (order by ETL_STEP_SQL) AS COL    
	     from  pdm.VT_CODE_EARCH 
	     where ETL_STEP_SQL is not null
	 ) T where T.COL = COL_NUM+1;
	     PREPARE	ETL_S_SQL	FROM @ETL_S_SQL;
	     --  EXECUTE ETL_S_SQL;
	     SET COL_NUM = COL_NUM+1;
     CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@ETL_S_SQL,ETL_TX_DATE);
	IF @RTC <> 0 THEN SET OUT_RES_MSG = '12'; LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
    	end while;
    	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
    	SET @SQL_STR = '
	          update pdm.t99_code_earch t 
              set t.remark = (
              select  t1.remark  from 
              pdm.wt_code_earch t1
              where t.t_tab_eng_name = t1.t_tab_eng_name
              and t.t_col_eng_name = t1.t_col_eng_name 
              and t.s_system = t1.s_system
              and t.s_tab_eng_name = t1.s_tab_eng_name
              and t.s_col_eng_name = t1.s_col_eng_name
              and t.code_value = t1.code_value
               ) 
               where t.statt_Dt = ${TX_DATE}';
	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN SET OUT_RES_MSG = '12'; LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
    SET OUT_RES_MSG = '0';	
	 END |