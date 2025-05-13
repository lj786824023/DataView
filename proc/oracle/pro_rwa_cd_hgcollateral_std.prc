CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_CD_HGCOLLATERAL_STD(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_CD_HGCOLLATERAL_STD
    实现功能:RWA系统-合规数据集-权重法回购类合格缓释物认定(抵质押品)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-04-21
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.RWA_EI_COLLATERAL|抵质押品表
    源  表2 :RWA_DEV.RWA_EI_SFTDETAIL|证券融资交易信息表
    源  表3 :RWA_DEV.RWA_CD_HGCOLLATERAL_QUALIFIED|回购类抵质押品合格映射表
    源  表4 :RWA_DEV.RWA_CD_HGCOLLATERAL_TYPE|回购类抵质押品类型代码表
    源  表5 :RWA_DEV.RWA_CD_HGCOLLATERAL_STD|合格回购类抵质押品权重法映射表
    源  表6 :RWA_DEV.RWA_EI_CLIENT|参与主体汇总表
    源  表7 :RWA_DEV.RWA_EI_EXPOSURE|风险暴露汇总表
    目标表1 :RWA_DEV.RWA_EI_COLLATERAL|抵质押品表
    目标表2 :RWA_DEV.RWA_EI_SFTDETAIL|证券融资交易信息表
    变更记录(修改人|修改时间|修改内容):
    */
  Authid Current_User
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_CD_HGCOLLATERAL_STD';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;
  v_count1 INTEGER;
  --定义varchar2(4000)数组
  type varchartab is table of varchar2(4000) index by binary_integer;
  v_sqlTab varchartab;
  --定义数组最大长度
  v_tab_len INTEGER := 1;
  --定义游标,权重法回购类合格缓释物认定规则(老押品目录)
  cursor cc_qlifd is
  	select client_type,collateral_type,issue_intent_type,country_level_no,qualified_flag from RWA_DEV.RWA_CD_HGCOLLATERAL_QUALIFIED;

  v_cc_qlifd cc_qlifd%rowtype;
  --定义游标,权重法回购类合格缓释物类型规则(老押品目录)
  cursor cc_type is
  	select client_type,collateral_type,std_code from RWA_DEV.RWA_CD_HGCOLLATERAL_TYPE;

  v_cc_type cc_type%rowtype;
  --定义游标,权重法回购类合格缓释物细分规则(老押品目录)
  cursor cc_sdvs is
  	select client_type,collateral_type,country_level,collateral_std_detail from RWA_DEV.RWA_CD_HGCOLLATERAL_STD;

  v_cc_sdvs cc_sdvs%rowtype;

  --定义抵质押品更新sql
	v_col_sql varchar2(4000);
	--定义证券融资交易更新sql
	v_sft_sql varchar2(4000);
	--定义抵质押品更新sql前缀
	v_col_sql_pre varchar2(4000);
	--定义证券融资交易更新sql前缀
	v_sft_sql_pre varchar2(4000);
	--定义抵质押品发行人类型条件
	v_colClientClause varchar2(4000);
	--定义证券融资交易发行人类型条件
	v_sftClientClause varchar2(4000);
	--定义抵质押品缓释物类型条件
	v_col_colTypeClause varchar2(4000);
	--定义证券融资交易缓释物类型条件
	v_sft_colTypeClause varchar2(4000);
	--定义抵质押品债券发行目的条件
	v_col_intentClause varchar2(4000);
	--定义证券融资交易债券发行目的条件
	v_sft_intentClause varchar2(4000);
	--定义抵质押品发行人国家评级条件
	v_col_ratingClause varchar2(4000);
	--定义证券融资交易发行人国家评级条件
	v_sft_ratingClause varchar2(4000);

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
		--1.更新回购类抵质押品表、证券融资交易信息表的合格认定状态为空
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL SET QUALFLAGSTD = '''' WHERE SSYSID = ''HG'' AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;
    --仅处理买入返售回购类的证券端数据
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_SFTDETAIL SET QUALFLAGSTD = '''' WHERE SSYSID = ''HG'' AND SFTDETAILID LIKE ''%MRFSZQ'' AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;

    --2.获取待执行的更新sql
    --2.1 获取合格认定sql
    for v_cc_qlifd in cc_qlifd loop
    	--发行人参与主体小类条件
    	v_colClientClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T2.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'') AND T2.CLIENTID = T1.ISSUERID AND T2.CLIENTSUBTYPE = ''' || v_cc_qlifd.client_type || '''';
    	v_sftClientClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T2.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'') AND T2.CLIENTID = T1.SECUISSUERID AND T2.CLIENTSUBTYPE = ''' || v_cc_qlifd.client_type || '''';
    	if v_cc_qlifd.qualified_flag <> '01' then --不合格
    		--缓释物类型条件
    		if v_cc_qlifd.collateral_type = '01' then			--债券
    			v_col_colTypeClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_EXPOSURE T3 WHERE T1.SGUARCONTRACTID = T3.EXPOSUREID AND (T3.ACCSUBJECT1 LIKE ''111103%'' OR T3.ACCSUBJECT1 LIKE ''211103%'') AND T3.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD''))';
    			v_sft_colTypeClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_EXPOSURE T3 WHERE T1.EXPOSUREID = T3.EXPOSUREID AND (T3.ACCSUBJECT1 LIKE ''111103%'' OR T3.ACCSUBJECT1 LIKE ''211103%'') AND T3.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD''))';
    		elsif v_cc_qlifd.collateral_type = '02' then	--票据
    			v_col_colTypeClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_EXPOSURE T3 WHERE T1.SGUARCONTRACTID = T3.EXPOSUREID AND (T3.ACCSUBJECT1 LIKE ''111102%'' OR T3.ACCSUBJECT1 LIKE ''211102%'') AND T3.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD''))';
    			v_sft_colTypeClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_EXPOSURE T3 WHERE T1.EXPOSUREID = T3.EXPOSUREID AND (T3.ACCSUBJECT1 LIKE ''111102%'' OR T3.ACCSUBJECT1 LIKE ''211102%'') AND T3.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD''))';
    		else																					--其他无此条件
    			v_col_colTypeClause := '';
    			v_sft_colTypeClause := '';
    		end if;
    		--更新回购类抵质押品不合格
    		v_col_sql_pre := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL T1 SET T1.QUALFLAGSTD = ''0'', T1.COLLATERALTYPESTD = '''', T1.COLLATERALSDVSSTD = '''' WHERE T1.SSYSID = ''HG'' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')' || v_colClientClause || ')' || v_col_colTypeClause;
    		--证券融资交易仅买入返售回购证券端需要认定，资金端和卖出回购证券端默认为合格的现金类资产
    		v_sft_sql_pre := 'UPDATE RWA_DEV.RWA_EI_SFTDETAIL T1 SET T1.QUALFLAGSTD = ''0'', T1.COLLATERALSDVSSTD = '''' WHERE T1.SSYSID = ''HG'' AND T1.SFTDETAILID LIKE ''%MRFSZQ'' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')' || v_sftClientClause || ')' || v_sft_colTypeClause;

    		v_col_sql := v_col_sql_pre;
    		v_sft_sql := v_sft_sql_pre;
    	else																			--合格
    		v_col_sql_pre := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL T1 SET T1.QUALFLAGSTD = ''1'' WHERE T1.SSYSID = ''HG'' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    		v_sft_sql_pre := 'UPDATE RWA_DEV.RWA_EI_SFTDETAIL T1 SET T1.QUALFLAGSTD = ''1'' WHERE T1.SSYSID = ''HG'' AND T1.SFTDETAILID LIKE ''%MRFSZQ'' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    		--注册地国家评级条件
    		if v_cc_qlifd.country_level_no = '01' then		--AA-级及以上
    			v_col_ratingClause := v_colClientClause || ' AND T2.RCERATING <= ''0104'')';
    			v_sft_ratingClause := v_sftClientClause || ' AND T2.RCERATING <= ''0104'')';
    		elsif v_cc_qlifd.country_level_no = '02' then	--AA-级以下，A-级及以上
    			v_col_ratingClause := v_colClientClause || ' AND T2.RCERATING > ''0104'' AND T2.RCERATING <= ''0107'')';
    			v_sft_ratingClause := v_sftClientClause || ' AND T2.RCERATING > ''0104'' AND T2.RCERATING <= ''0107'')';
    		elsif v_cc_qlifd.country_level_no = '03' then	--A-级以下，BBB-级及以上
    			v_col_ratingClause := v_colClientClause || ' AND T2.RCERATING > ''0107'' AND T2.RCERATING <= ''0110'')';
    			v_sft_ratingClause := v_sftClientClause || ' AND T2.RCERATING > ''0107'' AND T2.RCERATING <= ''0110'')';
    		elsif v_cc_qlifd.country_level_no = '04' then	--BBB-级以下，B-级及以上
    			v_col_ratingClause := v_colClientClause || ' AND T2.RCERATING > ''0110'' AND T2.RCERATING <= ''0116'')';
    			v_sft_ratingClause := v_sftClientClause || ' AND T2.RCERATING > ''0110'' AND T2.RCERATING <= ''0116'')';
    		elsif v_cc_qlifd.country_level_no = '05' then	--B-级以下
    			v_col_ratingClause := v_colClientClause || ' AND T2.RCERATING > ''0116'')';
    			v_sft_ratingClause := v_sftClientClause || ' AND T2.RCERATING > ''0116'')';
    		elsif v_cc_qlifd.country_level_no = '07' then	--A-级及以上
    			v_col_ratingClause := v_colClientClause || ' AND T2.RCERATING <= ''0107'')';
    			v_sft_ratingClause := v_sftClientClause || ' AND T2.RCERATING <= ''0107'')';
    		elsif v_cc_qlifd.country_level_no = '08' then	--BBB-级及以上
    			v_col_ratingClause := v_colClientClause || ' AND T2.RCERATING <= ''0110'')';
    			v_sft_ratingClause := v_sftClientClause || ' AND T2.RCERATING <= ''0110'')';
    		else																					--未评级或无评级条件
    			v_col_ratingClause := v_colClientClause || ')';
    			v_sft_ratingClause := v_sftClientClause || ')';
    		end if;
    		--缓释物类型条件
    		if v_cc_qlifd.collateral_type = '01' then			--债券
    			v_col_colTypeClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_EXPOSURE T3 WHERE T1.SGUARCONTRACTID = T3.EXPOSUREID AND (T3.ACCSUBJECT1 LIKE ''111103%'' OR T3.ACCSUBJECT1 LIKE ''211103%'') AND T3.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD''))';
    			v_sft_colTypeClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_EXPOSURE T3 WHERE T1.EXPOSUREID = T3.EXPOSUREID AND (T3.ACCSUBJECT1 LIKE ''111103%'' OR T3.ACCSUBJECT1 LIKE ''211103%'') AND T3.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD''))';
    		elsif v_cc_qlifd.collateral_type = '02' then	--票据
    			v_col_colTypeClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_EXPOSURE T3 WHERE T1.SGUARCONTRACTID = T3.EXPOSUREID AND (T3.ACCSUBJECT1 LIKE ''111102%'' OR T3.ACCSUBJECT1 LIKE ''211102%'') AND T3.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD''))';
    			v_sft_colTypeClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_EXPOSURE T3 WHERE T1.EXPOSUREID = T3.EXPOSUREID AND (T3.ACCSUBJECT1 LIKE ''111102%'' OR T3.ACCSUBJECT1 LIKE ''211102%'') AND T3.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD''))';
    		else																					--其他无此条件
    			v_col_colTypeClause := '';
    			v_sft_colTypeClause := '';
    		end if;
    		--债券发行目的条件
    		if v_cc_qlifd.issue_intent_type = '01' then			--收购国有银行不良贷款
    			v_col_intentClause := ' AND T1.SPECPURPBONDFLAG = ''1''';
    			v_sft_intentClause := ' AND T1.BONDISSUEINTENT = ''01''';
    		elsif v_cc_qlifd.issue_intent_type = '02' then	--其他
    			v_col_intentClause := ' AND T1.SPECPURPBONDFLAG = ''0''';
    			v_sft_intentClause := ' AND T1.BONDISSUEINTENT = ''02''';
    		else																						--无此条件
    			v_col_intentClause := '';
    			v_sft_intentClause := '';
    		end if;

    		v_col_sql := v_col_sql_pre || v_col_ratingClause || v_col_colTypeClause || v_col_intentClause;
    		v_sft_sql := v_sft_sql_pre || v_sft_ratingClause || v_sft_colTypeClause || v_sft_intentClause;
    	end if;
    	v_sqlTab(v_tab_len) := v_col_sql;
    	v_tab_len := v_tab_len + 1;
    	v_sqlTab(v_tab_len) := v_sft_sql;
    	v_tab_len := v_tab_len + 1;
    end loop;
    --添加剩余数据认定未不合格处理sql
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL SET QUALFLAGSTD = ''0'', COLLATERALTYPESTD = '''', COLLATERALSDVSSTD = '''' WHERE SSYSID = ''HG'' AND QUALFLAGSTD IS NULL AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;
    --仅证券端需要认定
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_SFTDETAIL SET QUALFLAGSTD = ''0'', COLLATERALSDVSSTD = '''' WHERE SSYSID = ''HG'' AND SFTDETAILID LIKE ''%MRFSZQ'' AND QUALFLAGSTD IS NULL AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;

    --2.2 获取已合格抵质押品类型
    for v_cc_type in cc_type loop
    	v_col_sql_pre := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL T1 SET T1.COLLATERALTYPESTD = ''' || v_cc_type.std_code || ''' WHERE T1.SSYSID = ''HG'' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'') AND T1.QUALFLAGSTD = ''1''';
    	v_colClientClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T2.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'') AND T2.CLIENTID = T1.ISSUERID AND T2.CLIENTSUBTYPE = ''' || v_cc_type.client_type || ''')';
    	--缓释物类型条件
  		if v_cc_type.collateral_type = '01' then			--债券
  			v_col_colTypeClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_EXPOSURE T3 WHERE T1.SGUARCONTRACTID = T3.EXPOSUREID AND (T3.ACCSUBJECT1 LIKE ''111103%'' OR T3.ACCSUBJECT1 LIKE ''211103%'') AND T3.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD''))';
  		elsif v_cc_type.collateral_type = '02' then		--票据
  			v_col_colTypeClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_EXPOSURE T3 WHERE T1.SGUARCONTRACTID = T3.EXPOSUREID AND (T3.ACCSUBJECT1 LIKE ''111102%'' OR T3.ACCSUBJECT1 LIKE ''211102%'') AND T3.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD''))';
  		else																					--其他无此条件
  			v_col_colTypeClause := '';
  		end if;
  		v_col_sql := v_col_sql_pre || v_colClientClause || v_col_colTypeClause;
    	v_sqlTab(v_tab_len) := v_col_sql;
    	v_tab_len := v_tab_len + 1;
    end loop;

    --2.3 获取已合格抵质押品细分
    for v_cc_sdvs in cc_sdvs loop
    	--发行人参与主体小类条件
    	v_colClientClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T2.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'') AND T2.CLIENTID = T1.ISSUERID AND T2.CLIENTSUBTYPE = ''' || v_cc_sdvs.client_type || '''';
    	v_sftClientClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T2.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'') AND T2.CLIENTID = T1.SECUISSUERID AND T2.CLIENTSUBTYPE = ''' || v_cc_sdvs.client_type || '''';
    	--更新前置语句
    	v_col_sql_pre := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL T1 SET T1.COLLATERALSDVSSTD = ''' || v_cc_sdvs.collateral_std_detail || ''' WHERE T1.QUALFLAGSTD = ''1'' AND T1.SSYSID = ''HG'' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    	--证券融资交易不合格也更新细分
    	v_sft_sql_pre := 'UPDATE RWA_DEV.RWA_EI_SFTDETAIL T1 SET T1.COLLATERALSDVSSTD = ''' || v_cc_sdvs.collateral_std_detail || ''' WHERE T1.SSYSID = ''HG'' AND T1.SFTDETAILID LIKE ''%MRFSZQ'' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
			--注册地国家评级条件
  		if v_cc_sdvs.country_level = '01' then				--AA-级及以上
  			v_col_ratingClause := v_colClientClause || ' AND T2.RCERATING <= ''0104'')';
  			v_sft_ratingClause := v_sftClientClause || ' AND T2.RCERATING <= ''0104'')';
  		elsif v_cc_sdvs.country_level = '02' then			--AA-级以下，A-级及以上
  			v_col_ratingClause := v_colClientClause || ' AND T2.RCERATING > ''0104'' AND T2.RCERATING <= ''0107'')';
  			v_sft_ratingClause := v_sftClientClause || ' AND T2.RCERATING > ''0104'' AND T2.RCERATING <= ''0107'')';
  		elsif v_cc_sdvs.country_level = '03' then			--A-级以下，BBB-级及以上
  			v_col_ratingClause := v_colClientClause || ' AND T2.RCERATING > ''0107'' AND T2.RCERATING <= ''0110'')';
  			v_sft_ratingClause := v_sftClientClause || ' AND T2.RCERATING > ''0107'' AND T2.RCERATING <= ''0110'')';
  		elsif v_cc_sdvs.country_level = '04' then			--BBB-级以下，B-级及以上
  			v_col_ratingClause := v_colClientClause || ' AND T2.RCERATING > ''0110'' AND T2.RCERATING <= ''0116'')';
  			v_sft_ratingClause := v_sftClientClause || ' AND T2.RCERATING > ''0110'' AND T2.RCERATING <= ''0116'')';
  		elsif v_cc_sdvs.country_level = '05' then			--B-级以下
  			v_col_ratingClause := v_colClientClause || ' AND T2.RCERATING > ''0116'')';
  			v_sft_ratingClause := v_sftClientClause || ' AND T2.RCERATING > ''0116'')';
  		elsif v_cc_sdvs.country_level = '07' then			--A-级及以上
  			v_col_ratingClause := v_colClientClause || ' AND T2.RCERATING <= ''0107'')';
  			v_sft_ratingClause := v_sftClientClause || ' AND T2.RCERATING <= ''0107'')';
  		elsif v_cc_sdvs.country_level = '08' then			--BBB-级及以上
  			v_col_ratingClause := v_colClientClause || ' AND T2.RCERATING <= ''0110'')';
  			v_sft_ratingClause := v_sftClientClause || ' AND T2.RCERATING <= ''0110'')';
  		else																					--无评级条件
  			v_col_ratingClause := v_colClientClause || ')';
  			v_sft_ratingClause := v_sftClientClause || ')';
  		end if;
  		--缓释物类型条件
  		if v_cc_sdvs.collateral_type = '01' then			--债券
  			v_col_colTypeClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_EXPOSURE T3 WHERE T1.SGUARCONTRACTID = T3.EXPOSUREID AND (T3.ACCSUBJECT1 LIKE ''111103%'' OR T3.ACCSUBJECT1 LIKE ''211103%'') AND T3.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD''))';
  			v_sft_colTypeClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_EXPOSURE T3 WHERE T1.EXPOSUREID = T3.EXPOSUREID AND (T3.ACCSUBJECT1 LIKE ''111103%'' OR T3.ACCSUBJECT1 LIKE ''211103%'') AND T3.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD''))';
  		elsif v_cc_sdvs.collateral_type = '02' then		--票据
  			v_col_colTypeClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_EXPOSURE T3 WHERE T1.SGUARCONTRACTID = T3.EXPOSUREID AND (T3.ACCSUBJECT1 LIKE ''111102%'' OR T3.ACCSUBJECT1 LIKE ''211102%'') AND T3.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD''))';
  			v_sft_colTypeClause := ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_EXPOSURE T3 WHERE T1.EXPOSUREID = T3.EXPOSUREID AND (T3.ACCSUBJECT1 LIKE ''111102%'' OR T3.ACCSUBJECT1 LIKE ''211102%'') AND T3.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD''))';
  		else																					--其他无此条件
  			v_col_colTypeClause := '';
  			v_sft_colTypeClause := '';
  		end if;
    	v_col_sql := v_col_sql_pre || v_col_ratingClause || v_col_colTypeClause;
    	v_sft_sql := v_sft_sql_pre || v_sft_ratingClause || v_sft_colTypeClause;
    	v_sqlTab(v_tab_len) := v_col_sql;
    	v_tab_len := v_tab_len + 1;
    	v_sqlTab(v_tab_len) := v_sft_sql;
    	v_tab_len := v_tab_len + 1;
    end loop;

    --3.执行更新sql
    for i in 1..v_sqlTab.count loop
    	if v_sqlTab(i) IS NOT NULL THEN
    		--Dbms_output.Put_line(v_sqlTab(i));
	    	EXECUTE IMMEDIATE v_sqlTab(i);

	    	COMMIT;
    	end if;
    end loop;

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_COLLATERAL WHERE QUALFLAGSTD = '1' AND SSYSID = 'HG' AND DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_COLLATERAL表认定权重法下合格的回购类抵质押品数据记录为: ' || v_count || ' 条');

    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_EI_SFTDETAIL WHERE QUALFLAGSTD = '1' AND SSYSID = 'HG' AND SFTDETAILID LIKE '%MRFSZQ' AND DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_SFTDETAIL表认定权重法下合格的回购类抵质押品数据记录为: ' || v_count || ' 条');


    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || (v_count + v_count1);
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '权重法回购类合格抵质押品('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_CD_HGCOLLATERAL_STD;
/

