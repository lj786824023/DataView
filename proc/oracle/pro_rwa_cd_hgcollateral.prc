CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_CD_HGCOLLATERAL(
                                                                p_data_dt_str    IN    VARCHAR2,        --数据日期 yyyyMMdd
                                                                p_po_rtncode    OUT    VARCHAR2,        --返回编号 1 成功,0 失败
                                                                p_po_rtnmsg     OUT    VARCHAR2         --返回描述
                                                                )
  /*
    存储过程名称:RWA_DEV.PRO_RWA_CD_HGCOLLATERAL
    实现功能:RWA系统-合规数据集-权重法/内评法回购类合格缓释物认定(抵质押品)
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
    源  表7 :RWA_DEV.CMS_BOND_INFO|债券信息表
    目标表1 :RWA_DEV.RWA_EI_COLLATERAL|抵质押品表
    目标表2 :RWA_DEV.RWA_EI_SFTDETAIL|证券融资交易信息表
    变更记录(修改人|修改时间|修改内容):

               TODO 内评法.缓释物评级条件2 中的评级等级为原表，评级等级需转换
    */
  Authid Current_User
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_CD_HGCOLLATERAL';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;
  --定义varchar2(4000)数组
  type varchartab is table of varchar2(4000) index by binary_integer;
  v_sqlTab varchartab;
  --定义数组最大长度
  v_tab_len INTEGER := 1;

  --v_beginTime BINARY_INTEGER;
  --v_endTime BINARY_INTEGER;
  --v_costTime varchar2(100);

  --定义游标,权重法/内评法回购类合格缓释物认定规则(老押品目录)
  cursor cc_qlifd is
      select
             --权重法/内评法.发行人参与主体小类条件1
             case when client_type is not null
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T2.DATADATE = T1.DATADATE AND T2.CLIENTID = T1.ISSUERID AND T2.CLIENTSUBTYPE = ''' || client_type || ''')'
                  else ' ' end as client_type_col
             --权重法/内评法.发行人参与主体小类条件2
            ,case when client_type is not null
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T2.DATADATE = T1.DATADATE AND T2.CLIENTID = T1.SECUISSUERID AND T2.CLIENTSUBTYPE = ''' || client_type || ''')'
                  else ' ' end as client_type_sft
             --权重法/内评法.缓释物类型条件1
            ,case when collateral_type is not null and collateral_type = '01' --债券
                  then ' AND T1.SOURCECOLTYPE = ''001003'''
                  when collateral_type is not null and collateral_type = '02' --票据
                  then ' AND T1.SOURCECOLTYPE = ''001004'''
                  else ' ' end as collateral_type_col
             --权重法/内评法.缓释物类型条件2
            ,case when collateral_type is not null and collateral_type = '01' --债券 证券融资交易全是债券
                  then ' AND 1 = 1'
                  when collateral_type is not null and collateral_type = '02' --票据 证券融资交易没有票据
                  then ' AND 1 = 2'
                  else ' ' end as collateral_type_sft
             --权重法/内评法.债券发行目的条件1
            ,case when issue_intent_type is not null and issue_intent_type = '01' --收购国有银行不良贷款
                  then ' AND T1.SPECPURPBONDFLAG = ''1'''
                  when issue_intent_type is not null and issue_intent_type = '02' --其他
                  then ' AND T1.SPECPURPBONDFLAG = ''0'''
                  else ' ' end as issue_intent_type_col
             --权重法/内评法.债券发行目的条件2
            ,case when issue_intent_type is not null and issue_intent_type = '01' --收购国有银行不良贷款
                  then ' AND T1.BONDISSUEINTENT = ''01'''
                  when issue_intent_type is not null and issue_intent_type = '02' --其他
                  then ' AND T1.BONDISSUEINTENT = ''02'''
                  else ' ' end as issue_intent_type_sft
            --权重法.注册地国家评级条件
            ,country_level_no
            --内评法.缓释物评级条件1
            ,case when rating_result is not null and rating_result like '01%'
                  then ' AND T1.FCISSUERATING <= '''||rating_result||''' AND T1.FCISSUERATING like ''01%'' '
                  when rating_result is not null and rating_result like '02%'
                  then ' AND T1.FCISSUERATING <= '''||rating_result||''' AND T1.FCISSUERATING like ''02%'' '
                  else ' ' end as rating_result_col
            --内评法.缓释物评级条件2
            ,case when rating_result is not null and rating_result like '01%'
                  then ' AND T1.SECUISSUERATING <= '''||rating_result||''' AND T1.SECUISSUERATING like ''01%'' '
                  when rating_result is not null and rating_result like '02%'
                  then ' AND T1.SECUISSUERATING <= '''||rating_result||''' AND T1.SECUISSUERATING like ''02%'' '
                  else ' ' end as rating_result_sft
            ,qualified_flag
       from RWA_DEV.RWA_CD_HGCOLLATERAL_QUALIFIED order by serial_no asc;

  v_cc_qlifd cc_qlifd%rowtype;
  --定义游标,权重法/内评法回购类合格缓释物类型规则(老押品目录)
  cursor cc_type is
      select
             --发行人参与主体小类条件1
             case when client_type is not null
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T2.DATADATE = T1.DATADATE AND T2.CLIENTID = T1.ISSUERID AND T2.CLIENTSUBTYPE = ''' || client_type || ''')'
                  else ' ' end as client_type_col
             --发行人参与主体小类条件2
            ,case when client_type is not null
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T2.DATADATE = T1.DATADATE AND T2.CLIENTID = T1.SECUISSUERID AND T2.CLIENTSUBTYPE = ''' || client_type || ''')'
                  else ' ' end as client_type_sft
             --缓释物类型条件1
            ,case when collateral_type is not null and collateral_type = '01' --债券
                  then ' AND T1.SOURCECOLTYPE = ''001003'''
                  when collateral_type is not null and collateral_type = '02' --票据
                  then ' AND T1.SOURCECOLTYPE = ''001004'''
                  else ' ' end as collateral_type_col
             --缓释物类型条件2
            ,case when collateral_type is not null and collateral_type = '01' --债券 证券融资交易全是债券
                  then ' AND 1 = 1'
                  when collateral_type is not null and collateral_type = '02' --票据 证券融资交易没有票据
                  then ' AND 1 = 2'
                  else ' ' end as collateral_type_sft
            ,std_code
            ,irb_code
            ,irb_financial_code
        from RWA_DEV.RWA_CD_HGCOLLATERAL_TYPE;

  v_cc_type cc_type%rowtype;
  --定义游标,权重法/内评法回购类合格缓释物细分规则(老押品目录)
  cursor cc_sdvs is
      select
             --权重法/内评法.发行人参与主体小类条件1
             case when client_type is not null
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T2.DATADATE = T1.DATADATE AND T2.CLIENTID = T1.ISSUERID AND T2.CLIENTSUBTYPE = ''' || client_type || ''')'
                  else ' ' end as client_type_col
             --权重法/内评法.发行人参与主体小类条件2
            ,case when client_type is not null
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T2.DATADATE = T1.DATADATE AND T2.CLIENTID = T1.SECUISSUERID AND T2.CLIENTSUBTYPE = ''' || client_type || ''')'
                  else ' ' end as client_type_sft
             --权重法/内评法.缓释物类型条件1
            ,case when collateral_type is not null and collateral_type = '01' --债券
                  then ' AND T1.SOURCECOLTYPE = ''001003'''
                  when collateral_type is not null and collateral_type = '02' --票据
                  then ' AND T1.SOURCECOLTYPE = ''001004'''
                  else ' ' end as collateral_type_col
             --权重法/内评法.缓释物类型条件2
            ,case when collateral_type is not null and collateral_type = '01' --债券 证券融资交易全是债券
                  then ' AND 1 = 1'
                  when collateral_type is not null and collateral_type = '02' --票据 证券融资交易没有票据
                  then ' AND 1 = 2'
                  else ' ' end as collateral_type_sft
            --权重法.注册地国家评级条件
            ,country_level
            ,collateral_std_detail
        from RWA_DEV.RWA_CD_HGCOLLATERAL_STD;

  v_cc_sdvs cc_sdvs%rowtype;

  --定义抵质押品更新sql
  v_col_sql varchar2(4000);
  --定义证券融资交易更新sql
  v_sft_sql varchar2(4000);

  --定义抵质押品更新条件
  v_col_clause varchar2(4000);
  --定义证券融资交易更新条件
  v_sft_clause varchar2(4000);

  --定义主体评级更新条件
  v_ratingClause varchar2(4000);

  --定义权重法合格标识
  v_qualflagstd varchar2(1);
  --定义内评法合格标识
  v_qualflagfirb varchar2(1);
	--定义执行错误的sql
  v_err_sql VARCHAR2(4000) := '';

  BEGIN

    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
        --1.更新回购类抵质押品表、证券融资交易信息表的合格认定状态为空
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL SET QUALFLAGSTD = '''',COLLATERALTYPESTD = '''',COLLATERALSDVSSTD = '''',QUALFLAGFIRB='''',COLLATERALTYPEIRB = '''',FCTYPE = '''' WHERE SSYSID = ''HG'' AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;
    --仅处理买入返售回购类的证券端数据
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_SFTDETAIL SET QUALFLAGSTD = '''',COLLATERALSDVSSTD = '''',QUALFLAGFIRB='''',FCTYPE = '''' WHERE SSYSID = ''HG'' AND SFTDETAILID LIKE ''%MRFSZQ'' AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;

    --2.获取待执行的更新sql
    --2.1 获取合格认定sql
    for v_cc_qlifd in cc_qlifd loop

        --注册地国家评级条件
        if v_cc_qlifd.country_level_no = '01' then        --AA-级及以上
            v_ratingClause := ' AND T1.RCERATING <= ''0104'' ';
        elsif v_cc_qlifd.country_level_no = '02' then    --AA-级以下，A-级及以上
            v_ratingClause := ' AND T1.RCERATING > ''0104'' AND T1.RCERATING <= ''0107'' ';
        elsif v_cc_qlifd.country_level_no = '03' then    --A-级以下，BBB-级及以上
            v_ratingClause := ' AND T1.RCERATING > ''0107'' AND T2.RCERATING <= ''0110'' ';
        elsif v_cc_qlifd.country_level_no = '04' then    --BBB-级以下，B-级及以上
            v_ratingClause := ' AND T1.RCERATING > ''0110'' AND T2.RCERATING <= ''0116'' ';
        elsif v_cc_qlifd.country_level_no = '05' then    --B-级以下
            v_ratingClause := ' AND T1.RCERATING > ''0116'' ';
        elsif v_cc_qlifd.country_level_no = '06' then    --未评级
            v_ratingClause := ' AND NVL(T1.RCERATING,''0124'') IN (''0124'',''0207'') ';
        elsif v_cc_qlifd.country_level_no = '07' then    --A-级及以上
            v_ratingClause := ' AND T1.RCERATING <= ''0107'' ';
        elsif v_cc_qlifd.country_level_no = '08' then    --BBB-级及以上
            v_ratingClause := ' AND T1.RCERATING <= ''0110'' ';
        elsif v_cc_qlifd.country_level_no = '09' then    --B-级及以上，A-级以下
            v_ratingClause := ' AND T1.RCERATING <=''0116'' and T1.RCERATING > ''0107'' ';
        else --未评级或无评级条件
            v_ratingClause := ' ';
        end if;

        --发行人参与主体小类条件
        v_col_clause :=  v_cc_qlifd.rating_result_col
                       ||v_cc_qlifd.issue_intent_type_col
                       ||v_cc_qlifd.client_type_col
                       ||v_ratingClause
                       ||v_cc_qlifd.collateral_type_col;
        v_sft_clause :=  v_cc_qlifd.rating_result_sft
                       ||v_cc_qlifd.issue_intent_type_sft
                       ||v_cc_qlifd.client_type_sft
                       ||v_ratingClause
                       ||v_cc_qlifd.collateral_type_sft;

        v_qualflagstd := '';
        v_qualflagfirb := '';
        if v_cc_qlifd.qualified_flag = '01' then    --T 权重法内评法都合格 代码:QualificationFlag
            v_qualflagstd := '1';
            v_qualflagfirb := '1';
        elsif v_cc_qlifd.qualified_flag = '02' then --T1 权重法不合格、内评法合格 代码:QualificationFlag
            v_qualflagstd := '0';
            v_qualflagfirb := '1';
        elsif v_cc_qlifd.qualified_flag = '03' then --F 权重法内评法都不合格 代码:QualificationFlag
            v_qualflagstd := '0';
            v_qualflagfirb := '0';
        elsif v_cc_qlifd.qualified_flag = '04' then --T2 权重法合格、内评法不合格 代码:QualificationFlag
            v_qualflagstd := '1';
            v_qualflagfirb := '0';
        else
            v_qualflagstd := '0';
            v_qualflagfirb := '0';
        end if;

        --更新回购类抵质押品不合格
        v_col_sql := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL T1
                         SET T1.QUALFLAGSTD = '''||v_qualflagstd||'''
                            ,T1.QUALFLAGFIRB = '''||v_qualflagfirb||'''
                       WHERE T1.SSYSID = ''HG'' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')'||v_col_clause ;
        --证券融资交易仅买入返售回购证券端需要认定，资金端和卖出回购证券端默认为合格的现金类资产
        v_sft_sql := 'UPDATE RWA_DEV.RWA_EI_SFTDETAIL T1
                         SET T1.QUALFLAGSTD = '''||v_qualflagstd||'''
                            ,T1.QUALFLAGFIRB = '''||v_qualflagfirb||'''
                       WHERE T1.SSYSID = ''HG'' AND T1.SFTDETAILID LIKE ''%MRFSZQ'' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')'||v_sft_clause ;

        v_sqlTab(v_tab_len) := v_col_sql;
        v_tab_len := v_tab_len + 1;
        v_sqlTab(v_tab_len) := v_sft_sql;
        v_tab_len := v_tab_len + 1;
    end loop;

    --添加剩余数据认定未不合格处理sql
    --权重法
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL SET QUALFLAGSTD = ''0'', COLLATERALTYPESTD = '''', COLLATERALSDVSSTD = '''' WHERE SSYSID = ''HG'' AND QUALFLAGSTD IS NULL AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;
    --内评法
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL SET QUALFLAGFIRB=''0'',COLLATERALTYPEIRB = '''',FCTYPE = '''' WHERE SSYSID = ''HG'' AND QUALFLAGFIRB IS NULL AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;
    --仅证券端需要认定
    --权重法
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_SFTDETAIL SET QUALFLAGSTD = ''0'', COLLATERALSDVSSTD = '''' WHERE SSYSID = ''HG'' AND SFTDETAILID LIKE ''%MRFSZQ'' AND QUALFLAGSTD IS NULL AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;
    --内评法
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_SFTDETAIL SET QUALFLAGFIRB = ''0'', FCTYPE = '''' WHERE SSYSID = ''HG'' AND SFTDETAILID LIKE ''%MRFSZQ'' AND QUALFLAGFIRB IS NULL AND DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;

    --2.2 获取已合格抵质押品类型
    for v_cc_type in cc_type loop
        v_col_clause :=  v_cc_type.collateral_type_col
                       ||v_cc_type.client_type_col||'';
        v_sft_clause :=  v_cc_type.collateral_type_sft
                       ||v_cc_type.client_type_sft||'';
        v_col_sql := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL T1 SET T1.COLLATERALTYPESTD = ''' || v_cc_type.std_code || ''' WHERE T1.SSYSID = ''HG'' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'') AND T1.QUALFLAGSTD = ''1''';
        v_sft_sql := 'UPDATE RWA_DEV.RWA_EI_SFTDETAIL T1 SET T1.FCTYPE = ''' || v_cc_type.irb_financial_code || ''' WHERE T1.SSYSID = ''HG'' AND T1.SFTDETAILID LIKE ''%MRFSZQ'' AND T1.QUALFLAGFIRB = ''1'' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';

        v_sqlTab(v_tab_len) := v_col_sql||v_col_clause;
        v_tab_len := v_tab_len + 1;
        v_sqlTab(v_tab_len) := v_sft_sql||v_sft_clause;
        v_tab_len := v_tab_len + 1;

        v_col_sql := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL T1 SET T1.COLLATERALTYPEIRB='''||v_cc_type.irb_code||''',T1.FCTYPE = ''' || v_cc_type.irb_financial_code || ''' WHERE T1.SSYSID = ''HG'' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'') AND T1.QUALFLAGFIRB = ''1''';

        v_sqlTab(v_tab_len) := v_col_sql||v_col_clause;
        v_tab_len := v_tab_len + 1;
    end loop;

    --2.3 获取已合格抵质押品细分
    for v_cc_sdvs in cc_sdvs loop
        --注册地国家评级条件
        if v_cc_sdvs.country_level = '01' then        --AA-级及以上
            v_ratingClause := ' AND T1.RCERATING <= ''0104'' ';
        elsif v_cc_sdvs.country_level = '02' then    --AA-级以下，A-级及以上
            v_ratingClause := ' AND T1.RCERATING > ''0104'' AND T1.RCERATING <= ''0107'' ';
        elsif v_cc_sdvs.country_level = '03' then    --A-级以下，BBB-级及以上
            v_ratingClause := ' AND T1.RCERATING > ''0107'' AND T1.RCERATING <= ''0110'' ';
        elsif v_cc_sdvs.country_level = '04' then    --BBB-级以下，B-级及以上
            v_ratingClause := ' AND T1.RCERATING > ''0110'' AND T1.RCERATING <= ''0116'' ';
        elsif v_cc_sdvs.country_level = '05' then    --B-级以下
            v_ratingClause := ' AND T1.RCERATING > ''0116'' ';
        elsif v_cc_sdvs.country_level = '06' then    --未评级
            v_ratingClause := ' AND NVL(T1.RCERATING,''0124'') IN (''0124'',''0207'') ';
        elsif v_cc_sdvs.country_level = '07' then    --A-级及以上
            v_ratingClause := ' AND T1.RCERATING <= ''0107'' ';
        elsif v_cc_sdvs.country_level = '08' then    --BBB-级及以上
            v_ratingClause := ' AND T1.RCERATING <= ''0110'' ';
        elsif v_cc_sdvs.country_level = '09' then    --B-级及以上，A-级以下
            v_ratingClause := ' AND T1.RCERATING <=''0116'' and T1.RCERATING > ''0107'' ';
        else --未评级或无评级条件
            v_ratingClause := ' ';
        end if;

        --发行人参与主体小类条件
        v_col_clause :=  v_cc_sdvs.client_type_col
                       ||v_ratingClause
                       ||v_cc_sdvs.collateral_type_col;
        v_sft_clause :=  v_cc_sdvs.client_type_sft
                       ||v_ratingClause
                       ||v_cc_sdvs.collateral_type_sft;
        --更新前置语句
        v_col_sql := 'UPDATE RWA_DEV.RWA_EI_COLLATERAL T1 SET T1.COLLATERALSDVSSTD = ''' || v_cc_sdvs.collateral_std_detail || ''' WHERE T1.QUALFLAGSTD = ''1'' AND T1.SSYSID = ''HG'' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')'||v_col_clause;
        --证券融资交易不合格也更新细分
        v_sft_sql := 'UPDATE RWA_DEV.RWA_EI_SFTDETAIL T1 SET T1.COLLATERALSDVSSTD = ''' || v_cc_sdvs.collateral_std_detail || ''' WHERE T1.QUALFLAGSTD = ''1'' AND T1.SSYSID = ''HG'' AND T1.SFTDETAILID LIKE ''%MRFSZQ'' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')'||v_sft_clause;

        v_sqlTab(v_tab_len) := v_col_sql||v_col_clause;
        v_tab_len := v_tab_len + 1;
        v_sqlTab(v_tab_len) := v_sft_sql||v_sft_clause;
        v_tab_len := v_tab_len + 1;
    end loop;

    --3.执行更新sql
    --delete from qhjiang_test;
    for i in 1..v_sqlTab.count loop
        if v_sqlTab(i) IS NOT NULL THEN
            --insert into qhjiang_test(num1,str) values(i,v_sqlTab(i));
            --commit;
            --Dbms_output.Put_line(v_sqlTab(i));
            --v_beginTime := DBMS_UTILITY.GET_TIME;
            v_err_sql := v_sqlTab(i);
            EXECUTE IMMEDIATE v_sqlTab(i);
            --v_endTime := DBMS_UTILITY.GET_TIME;
            --v_costTime := v_endTime - v_beginTime;
            --update qhjiang_test set str1=v_costTime where num1=i;
            COMMIT;
        end if;
    end loop;

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_COLLATERAL WHERE QUALFLAGSTD = '1' AND SSYSID = 'HG' AND DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_COLLATERAL表认定权重法/内评法下合格的回购类抵质押品数据记录为: ' || v_count || ' 条');

    SELECT COUNT(1)+v_count INTO v_count FROM RWA_DEV.RWA_EI_SFTDETAIL WHERE QUALFLAGSTD = '1' AND SSYSID = 'HG' AND SFTDETAILID LIKE '%MRFSZQ' AND DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_SFTDETAIL表认定权重法/内评法下合格的回购类抵质押品数据记录为: ' || v_count || ' 条');


    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始6 】:' || to_char(systimestamp, 'yyyy-mm-dd hh24:mi:ss.ff '));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功' || '-' || v_count;
        --定义异常
        EXCEPTION
    WHEN OTHERS THEN
                 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
                 Dbms_output.Put_line(v_err_sql);
            ROLLBACK;
                p_po_rtncode := sqlcode;
                p_po_rtnmsg  := '权重法/内评法回购类合格抵质押品('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_CD_HGCOLLATERAL;
/

