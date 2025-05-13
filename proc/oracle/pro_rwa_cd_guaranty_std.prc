CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_CD_GUARANTY_STD(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_CD_GUARANTY_STD
    实现功能:RWA系统-合规数据集-权重法合格缓释物认定(保证)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-04-21
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.RWA_EI_GUARANTEE|保证表
    源  表2 :RWA_DEV.RWA_EI_CLINET|参与主体表
    源  表3 :RWA_DEV.RWA_CD_GUARANTY_QUALIFIED|保证合格映射表
    源  表3 :RWA_DEV.RWA_CD_GUARANTY_TYPE|保证担保方式表
    源  表3 :RWA_DEV.RWA_CD_GUARANTY_STD|合格保证权重法映射表
    目标表  :RWA_DEV.RWA_EI_GUARANTEE|保证表
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_CD_GUARANTY_STD';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;
  --定义varchar2(4000)数组
  type varchartab is table of varchar2(4000) index by binary_integer;
  v_sqlTab varchartab;
  --定义数组最大长度
  v_tab_len INTEGER := 1;
  --定义游标,权重法合格保证认定规则(老押品目录)
  cursor cc_qlifd is
  	select guaranty_type,client_sub_type,country_level_no,qualified_flag from RWA_DEV.RWA_CD_GUARANTY_QUALIFIED;

  v_cc_qlifd cc_qlifd%rowtype;
  --定义游标,权重法合格保证类型规则(老押品目录)
  --cursor cc_type is
  --	select guaranty_id,std_code from RWA_DEV.RWA_CD_GUARANTY_TYPE;

  --v_cc_type cc_type%rowtype;
  --定义游标,权重法合格保证人细分规则(老押品目录)
  cursor cc_sdvs is
  	select guaranty_type,client_sub_type,country_level,guaranty_std_detail from RWA_DEV.RWA_CD_GUARANTY_STD;

  v_cc_sdvs cc_sdvs%rowtype;
  --定义更新sql前缀
  v_sql_pre varchar2(4000);
  --定义更新where条件
  v_clause varchar2(4000);

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --1.更新保证表的合格认定状态为空
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_GUARANTEE SET QUALFLAGSTD = '''' WHERE DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;

    --2.获取待执行的更新sql
    --2.1 获取合格认定sql
    for v_cc_qlifd in cc_qlifd loop
    	if v_cc_qlifd.qualified_flag <> '01' then
    		v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_GUARANTEE SET QUALFLAGSTD = ''0'', GUARANTEETYPESTD = '''', GUARANTORSDVSSTD = '''' WHERE GUARANTEEWAY = ''' || v_cc_qlifd.guaranty_type || ''' AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    	else
    		v_sql_pre := 'UPDATE RWA_DEV.RWA_EI_GUARANTEE T1 SET T1.QUALFLAGSTD = ''1'', T1.GUARANTEETYPESTD = ''020101'', T1.GUARANTORSDVSSTD = '''' WHERE T1.GUARANTEEWAY = ''' || v_cc_qlifd.guaranty_type || '''';
    		v_clause := '';
    		if v_cc_qlifd.client_sub_type is not null then
    			v_clause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.CLIENTSUBTYPE = ''' || v_cc_qlifd.client_sub_type || ''' AND T2.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
	    		if v_cc_qlifd.country_level_no = '01' then		--AA-级及以上
	    			v_clause := v_clause || ' AND T2.RCERATING <= ''0104'')';
	    		elsif v_cc_qlifd.country_level_no = '02' then	--AA-级以下，A-级及以上
	    			v_clause := v_clause || ' AND T2.RCERATING > ''0104'' AND T2.RCERATING <= ''0107'')';
	    		elsif v_cc_qlifd.country_level_no = '03' then	--A-级以下，BBB-级及以上
	    			v_clause := v_clause || ' AND T2.RCERATING > ''0107'' AND T2.RCERATING <= ''0110'')';
	    		elsif v_cc_qlifd.country_level_no = '04' then	--BBB-级以下，B-级及以上
	    			v_clause := v_clause || ' AND T2.RCERATING > ''0110'' AND T2.RCERATING <= ''0116'')';
	    		elsif v_cc_qlifd.country_level_no = '05' then	--B-级以下
	    			v_clause := v_clause || ' AND T2.RCERATING > ''0116'')';
	    		elsif v_cc_qlifd.country_level_no = '07' then	--A-级及以上
	    			v_clause := v_clause || ' AND T2.RCERATING <= ''0107'')';
	    		elsif v_cc_qlifd.country_level_no = '08' then	--BBB-级及以上
	    			v_clause := v_clause || ' AND T2.RCERATING <= ''0110'')';
	    		else																					--未评级或无评级条件
	    			v_clause := v_clause || ')';
	    		end if;
    		else
    			v_clause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    			if v_cc_qlifd.country_level_no = '01' then		--AA-级及以上
	    			v_clause := v_clause || ' AND T2.RCERATING <= ''0104'')';
	    		elsif v_cc_qlifd.country_level_no = '02' then	--AA-级以下，A-级及以上
	    			v_clause := v_clause || ' AND T2.RCERATING > ''0104'' AND T2.RCERATING <= ''0107'')';
	    		elsif v_cc_qlifd.country_level_no = '03' then	--A-级以下，BBB-级及以上
	    			v_clause := v_clause || ' AND T2.RCERATING > ''0107'' AND T2.RCERATING <= ''0110'')';
	    		elsif v_cc_qlifd.country_level_no = '04' then	--BBB-级以下，B-级及以上
	    			v_clause := v_clause || ' AND T2.RCERATING > ''0110'' AND T2.RCERATING <= ''0116'')';
	    		elsif v_cc_qlifd.country_level_no = '05' then	--B-级以下
	    			v_clause := v_clause || ' AND T2.RCERATING > ''0116'')';
	    		elsif v_cc_qlifd.country_level_no = '07' then	--A-级及以上
	    			v_clause := v_clause || ' AND T2.RCERATING <= ''0107'')';
	    		elsif v_cc_qlifd.country_level_no = '08' then	--BBB-级及以上
	    			v_clause := v_clause || ' AND T2.RCERATING <= ''0110'')';
	    		else																					--未评级或无评级条件
	    			v_clause := '';
	    		end if;
    		end if;
    		v_sqlTab(v_tab_len) := v_sql_pre || v_clause || ' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    	end if;
    	v_tab_len := v_tab_len + 1;
    end loop;
    --添加剩余数据认定未不合格处理sql
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_GUARANTEE SET QUALFLAGSTD = ''0'', GUARANTEETYPESTD = '''', GUARANTORSDVSSTD = '''' WHERE QUALFLAGSTD IS NULL AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;

    --2.2 获取已合格保证类型
    /**因保证类型固定，故无需从配置表中获取
    for v_cc_type in cc_type loop
    	v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_GUARANTEE SET GUARANTEETYPESTD = ''' || v_cc_type.std_code || ''' WHERE GUARANTEEWAY = ''' || v_cc_type.guaranty_id || ''' AND QUALFLAGSTD = ''1'' AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    	v_tab_len := v_tab_len + 1;
    end loop;
    */

    --2.3 获取已合格保证人细分
    for v_cc_sdvs in cc_sdvs loop
    	v_sql_pre := 'UPDATE RWA_DEV.RWA_EI_GUARANTEE T1 SET T1.GUARANTORSDVSSTD = ''' || v_cc_sdvs.guaranty_std_detail || ''' WHERE T1.GUARANTEEWAY = ''' || v_cc_sdvs.guaranty_type || ''' AND T1.QUALFLAGSTD = ''1''';
    	v_clause := '';
  		if v_cc_sdvs.client_sub_type is not null then
  			v_clause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.CLIENTSUBTYPE = ''' || v_cc_sdvs.client_sub_type || ''' AND T2.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    		if v_cc_sdvs.country_level = '01' then		--AA-级及以上
    			v_clause := v_clause || ' AND T2.RCERATING <= ''0104'')';
    		elsif v_cc_sdvs.country_level = '02' then	--AA-级以下，A-级及以上
    			v_clause := v_clause || ' AND T2.RCERATING > ''0104'' AND T2.RCERATING <= ''0107'')';
    		elsif v_cc_sdvs.country_level = '03' then	--A-级以下，BBB-级及以上
    			v_clause := v_clause || ' AND T2.RCERATING > ''0107'' AND T2.RCERATING <= ''0110'')';
    		elsif v_cc_sdvs.country_level = '04' then	--BBB-级以下，B-级及以上
    			v_clause := v_clause || ' AND T2.RCERATING > ''0110'' AND T2.RCERATING <= ''0116'')';
    		elsif v_cc_sdvs.country_level = '05' then	--B-级以下
    			v_clause := v_clause || ' AND T2.RCERATING > ''0116'')';
    		elsif v_cc_sdvs.country_level = '07' then	--A-级及以上
    			v_clause := v_clause || ' AND T2.RCERATING <= ''0107'')';
    		elsif v_cc_sdvs.country_level = '08' then	--BBB-级及以上
    			v_clause := v_clause || ' AND T2.RCERATING <= ''0110'')';
    		else																			--未评级或无评级条件
    			v_clause := v_clause || ')';
    		end if;
  		else
  			v_clause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
  			if v_cc_sdvs.country_level = '01' then		--AA-级及以上
    			v_clause := v_clause || ' AND T2.RCERATING <= ''0104'')';
    		elsif v_cc_sdvs.country_level = '02' then	--AA-级以下，A-级及以上
    			v_clause := v_clause || ' AND T2.RCERATING > ''010104'' AND T2.RCERATING <= ''0107'')';
    		elsif v_cc_sdvs.country_level = '03' then	--A-级以下，BBB-级及以上
    			v_clause := v_clause || ' AND T2.RCERATING > ''0107'' AND T2.RCERATING <= ''0110'')';
    		elsif v_cc_sdvs.country_level = '04' then	--BBB-级以下，B-级及以上
    			v_clause := v_clause || ' AND T2.RCERATING > ''0110'' AND T2.RCERATING <= ''0116'')';
    		elsif v_cc_sdvs.country_level = '05' then	--B-级以下
    			v_clause := v_clause || ' AND T2.RCERATING > ''0116'')';
    		elsif v_cc_sdvs.country_level = '07' then	--A-级及以上
    			v_clause := v_clause || ' AND T2.RCERATING <= ''0107'')';
    		elsif v_cc_sdvs.country_level = '08' then	--BBB-级及以上
    			v_clause := v_clause || ' AND T2.RCERATING <= ''0110'')';
    		else																			--未评级或无评级条件
    			v_clause := '';
    		end if;
  		end if;
    	v_sqlTab(v_tab_len) := v_sql_pre || v_clause || ' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    	v_tab_len := v_tab_len + 1;
    end loop;

    --3.执行更新sql
    for i in 1..v_sqlTab.count loop
    	if v_sqlTab(i) IS NOT NULL THEN
	    	EXECUTE IMMEDIATE v_sqlTab(i);

	    	COMMIT;
    	end if;
    end loop;

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_GUARANTEE WHERE QUALFLAGSTD = '1' AND DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_GUARANTEE表认定权重法下合格的保证数据记录为: ' || v_count || ' 条');



    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '权重法合格保证('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_CD_GUARANTY_STD;
/

