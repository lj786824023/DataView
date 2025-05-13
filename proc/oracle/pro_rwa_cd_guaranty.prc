CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_CD_GUARANTY(
                                                            p_data_dt_str    IN    VARCHAR2,        --数据日期 yyyyMMdd
                                                            p_po_rtncode    OUT    VARCHAR2,        --返回编号 1 成功,0 失败
                                                            p_po_rtnmsg     OUT    VARCHAR2         --返回描述
                                                            )
  /*
    存储过程名称:RWA_DEV.PRO_RWA_CD_GUARANTY
    实现功能:RWA系统-合规数据集-权重法/内评法合格缓释物认定(保证)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-04-21
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.RWA_EI_GUARANTEE|保证表
    源  表2 :RWA_DEV.RWA_EI_CLIENT|参与主体表
    源  表3 :RWA_DEV.RWA_EI_CMRELEVENCE|合同缓释物关联表
    源  表4 :RWA_DEV.RWA_EI_CONTRACT|合同表
    源  表5 :RWA_DEV.RWA_CD_GUARANTY_QUALIFIED|保证合格映射表
    源  表6 :RWA_DEV.RWA_CD_GUARANTY_TYPE|保证担保方式表
    源  表7 :RWA_DEV.RWA_CD_GUARANTY_STD|合格保证权重法映射表
    目标表  :RWA_DEV.RWA_EI_GUARANTEE|保证表
    变更记录(修改人|修改时间|修改内容):
             QHJIANG|2017-04-17|根据二期新需求做相应的改造:二期权重法重新计算逻辑重新调整，并加入内评法的计算逻辑
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_CD_GUARANTY';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;
  --定义varchar2(4000)数组
  type varchartab is table of varchar2(4000) index by binary_integer;
  v_sqlTab varchartab;
  --定义数组最大长度
  v_tab_len INTEGER := 1;
  --定义权重法合格标识
  v_qualflagstd VARCHAR2(200) := '';
  --定义内评初级法合格标识
  v_qualflagfirb VARCHAR2(200) := '';
  --定义执行错误的sql
  v_err_sql VARCHAR2(4000) := '';


  --定义游标,权重法/内评法合格保证认定规则(新押品目录)
  cursor cc_qlifd is
      select
            --保证编号
            case when guaranty_type is not null
                 then ' AND T1.GUARANTEEWAY='''||guaranty_type||''' '
                 else ' ' end as guaranty_type
            --参与主体小类代码
            ,case when client_sub_type is not null
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.CLIENTSUBTYPE = ''' || client_sub_type || ''' AND T1.DATADATE = T2.DATADATE) '
                  else ' ' end as client_sub_type
            --注册国家地区评级代码
            ,case when country_level_no is not null and country_level_no = '01'--AA-级及以上
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.RCERATING <= ''0104'' AND T1.DATADATE = T2.DATADATE) '
                  when country_level_no is not null and country_level_no = '02'--AA-级以下，A-级及以上
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.RCERATING > ''0104'' AND T2.RCERATING <= ''0107'' AND T1.DATADATE = T2.DATADATE) '
                  when country_level_no is not null and country_level_no = '03'--A-级以下，BBB-级及以上
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.RCERATING > ''0107'' AND T2.RCERATING <= ''0110'' AND T1.DATADATE = T2.DATADATE) '
                  when country_level_no is not null and country_level_no = '04'--BBB-级以下，B-级及以上
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.RCERATING > ''0110'' AND T2.RCERATING <= ''0116'' AND T1.DATADATE = T2.DATADATE) '
                  when country_level_no is not null and country_level_no = '05'--B-级以下
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.RCERATING > ''0116'' AND T1.DATADATE = T2.DATADATE) '
                  when country_level_no is not null and country_level_no='06'--未评级
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND NVL(T2.RCERATING,''0124'') IN (''0124'',''0207'') AND T1.DATADATE = T2.DATADATE) '
                  when country_level_no is not null and country_level_no = '07'--A-级及以上
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.RCERATING <= ''0107'' AND T1.DATADATE = T2.DATADATE) '
                  when country_level_no is not null and country_level_no = '08'--BBB-级及以上
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.RCERATING <= ''0110'' AND T1.DATADATE = T2.DATADATE) '
                  when country_level_no is not null and country_level_no='09'--B-级及以上，A-级以下
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.RCERATING <= ''0116'' AND T2.RCERATING>''0107'' AND T1.DATADATE = T2.DATADATE) '
                  else ' ' end as country_level_no
            --保证担保形式是否企业担保  码值:YesOrNo
            ,case when guarantee_form is not null and guarantee_form='1'
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.CLIENTSUBTYPE NOT LIKE ''04%'' AND T1.DATADATE = T2.DATADATE) '
                  when guarantee_form is not null and guarantee_form='0'
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.CLIENTSUBTYPE LIKE ''04%'' AND T1.DATADATE = T2.DATADATE) '
                  else '' end as guarantee_form
            --是否保证人PD<借款人PD  码值:YesOrNo
            ,case when pd_type is not null and pd_type='1'
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_TMP_GUARANTORPD T2 WHERE T1.GUARANTORID = T2.GUARANTORID AND T2.FLAG = ''1'') '
                  when pd_type is not null and pd_type='0'
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_TMP_GUARANTORPD T2 WHERE T1.GUARANTORID = T2.GUARANTORID AND T2.FLAG = ''0'') '

                  else '' end as pd_type
            ,qualified_flag
        from RWA_DEV.RWA_CD_GUARANTY_QUALIFIED order by serial_no asc;

  v_cc_qlifd cc_qlifd%rowtype;
  --定义游标,权重法/内评法合格保证类型规则(老押品目录)
  cursor cc_type is
      select guaranty_id,std_code,irb_code from RWA_DEV.RWA_CD_GUARANTY_TYPE ;
  v_cc_type cc_type%rowtype;

  --定义游标,权重法/内评法合格保证人细分规则(老押品目录)
  cursor cc_sdvs is
      select --保证编号
            case when guaranty_type is not null
                 then ' AND T1.GUARANTEEWAY='''||guaranty_type||''' '
                 else ' ' end as guaranty_type
            --参与主体小类代码
            ,case when client_sub_type is not null
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.CLIENTSUBTYPE = ''' || client_sub_type || ''' AND T1.DATADATE = T2.DATADATE) '
                  else ' ' end as client_sub_type
            --注册国家地区评级代码
            ,case when country_level is not null and country_level = '01'--AA-级及以上
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.RCERATING <= ''0104'' AND T1.DATADATE = T2.DATADATE) '
                  when country_level is not null and country_level = '02'--AA-级以下，A-级及以上
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.RCERATING > ''0104'' AND T2.RCERATING <= ''0107'' AND T1.DATADATE = T2.DATADATE) '
                  when country_level is not null and country_level = '03'--A-级以下，BBB-级及以上
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.RCERATING > ''0107'' AND T2.RCERATING <= ''0110'' AND T1.DATADATE = T2.DATADATE) '
                  when country_level is not null and country_level = '04'--BBB-级以下，B-级及以上
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.RCERATING > ''0110'' AND T2.RCERATING <= ''0116'' AND T1.DATADATE = T2.DATADATE) '
                  when country_level is not null and country_level = '05'--B-级以下
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.RCERATING > ''0116'' AND T1.DATADATE = T2.DATADATE) '
                  when country_level is not null and country_level='06'--未评级
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND NVL(T2.RCERATING,''0124'') IN (''0124'',''0207'') AND T1.DATADATE = T2.DATADATE) '
                  when country_level is not null and country_level = '07'--A-级及以上
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.RCERATING <= ''0107'' AND T1.DATADATE = T2.DATADATE) '
                  when country_level is not null and country_level = '08'--BBB-级及以上
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND T2.RCERATING <= ''0110'' AND T1.DATADATE = T2.DATADATE) '
                  when country_level is not null and country_level = '09'--B-级及以上，A-级以下
                  then ' AND EXISTS (SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T2 WHERE T1.GUARANTORID = T2.CLIENTID AND AND T2.RCERATING <=''0116'' and T2.RCERATING>''0107'' AND T1.DATADATE = T2.DATADATE) '
                  else ' ' end as country_level
            ,guaranty_std_detail
        from RWA_DEV.RWA_CD_GUARANTY_STD order by serial_no asc;

  v_cc_sdvs cc_sdvs%rowtype;
  --定义更新sql前缀
  v_sql_pre varchar2(4000);
  --定义更新where条件
  v_clause varchar2(4000);

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    --清空保证人与借款人PD关系表
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TMP_GUARANTORPD';

    --重新初始化保证人与借款人PD关系表
    INSERT INTO RWA_DEV.RWA_TMP_GUARANTORPD
	  (GUARANTORID, GUARANTORPD, CUSTOMERID, CUSTOMERPD, FLAG)
	  SELECT T1.GUARANTORID AS GUARANTORID,
	         T1.GUARANTORPD AS GUARANTORPD,
	         '' 						AS CUSTOMERID,
	         MIN(T5.PD) 		AS CUSTOMERPD,
	         CASE
	           WHEN T1.GUARANTORPD < MIN(T5.PD) THEN
	            '1'
	           ELSE
	            '0'
	         END AS FLAG
	    FROM RWA_EI_GUARANTEE T1
	   INNER JOIN RWA_EI_CMRELEVENCE T2
	      ON T1.GUARANTEEID = T2.MITIGATIONID
	     AND T1.DATADATE = T2.DATADATE
	   INNER JOIN RWA_EI_CONTRACT T3
	      ON T2.CONTRACTID = T3.CONTRACTID
	     AND T2.DATADATE = T3.DATADATE
	    LEFT JOIN RWA_EI_CLIENT T5
	      ON T3.CLIENTID = T5.CLIENTID
	     AND T3.DATADATE = T5.DATADATE
	   WHERE T1.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
	     AND T1.GUARANTORID IS NOT NULL
	   GROUP BY T1.GUARANTORID, T1.GUARANTORPD
		;

		COMMIT;
		--整理表信息
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_TMP_GUARANTORPD',cascade => true);

    --1.更新保证表的合格认定状态为空
    --v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_GUARANTEE T1 SET T1.QUALFLAGSTD = '''',T1.GUARANTEETYPESTD='''',T1.GUARANTORSDVSSTD='''',T1.QUALFLAGFIRB='''',T1.GUARANTEETYPEIRB='''' WHERE T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    --仅重置权重法部分，内评法从原系统获取，RWA系统仅兜底
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_GUARANTEE T1 SET T1.QUALFLAGSTD = '''',T1.GUARANTEETYPESTD='''',T1.GUARANTORSDVSSTD='''' WHERE T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;

    --2.获取待执行的更新sql
    --2.1 获取合格认定sql
    for v_cc_qlifd in cc_qlifd loop
        v_qualflagstd := '';
        v_qualflagfirb := '';
        v_clause :=  v_cc_qlifd.guaranty_type     --保证编号
                   ||v_cc_qlifd.client_sub_type   --权重法.参与主体小类代码
                   ||v_cc_qlifd.country_level_no  --权重法.注册国家地区评级代码
                   ||v_cc_qlifd.guarantee_form    --内评法.保证担保形式＝企业担保 满足条件 01 不满足 02
                   ||v_cc_qlifd.pd_type;          --内评法.保证人PD<借款人PD 满足条件 01 不满足 02
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
        v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_GUARANTEE T1
                                   SET T1.QUALFLAGSTD = '''||v_qualflagstd||'''
                                      ,T1.GUARANTEETYPESTD = ''''
                                      ,T1.GUARANTORSDVSSTD = ''''
                                      ,T1.QUALFLAGFIRB = NVL(T1.QUALFLAGFIRB,'''||v_qualflagfirb||''')
                                 WHERE T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'') 
                                 '||v_clause;
        v_tab_len := v_tab_len + 1;
    end loop;
    --添加剩余数据认定未不合格处理sql
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_GUARANTEE T1 SET T1.QUALFLAGSTD = ''0'', T1.GUARANTEETYPESTD = '''', T1.GUARANTORSDVSSTD = '''' WHERE T1.QUALFLAGSTD IS NULL AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;
    v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_GUARANTEE T1 SET T1.QUALFLAGFIRB = ''0'', T1.GUARANTEETYPEIRB = '''' WHERE T1.QUALFLAGFIRB IS NULL AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
    v_tab_len := v_tab_len + 1;

    --2.2 获取已合格保证类型
    for v_cc_type in cc_type loop
        v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_GUARANTEE T1 SET T1.GUARANTEETYPESTD = ''' || v_cc_type.std_code || ''' WHERE T1.GUARANTEEWAY = ''' || v_cc_type.guaranty_id || ''' AND T1.QUALFLAGSTD = ''1'' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
        v_tab_len := v_tab_len + 1;
        v_sqlTab(v_tab_len) := 'UPDATE RWA_DEV.RWA_EI_GUARANTEE T1 SET T1.GUARANTEETYPEIRB = ''' || v_cc_type.irb_code || ''' WHERE T1.GUARANTEEWAY = ''' || v_cc_type.guaranty_id || ''' AND T1.QUALFLAGFIRB = ''1'' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
        v_tab_len := v_tab_len + 1;
    end loop;


    --2.3 获取已合格保证人细分
    for v_cc_sdvs in cc_sdvs loop
        v_clause :=  v_cc_sdvs.guaranty_type     --保证编号
                   ||v_cc_sdvs.client_sub_type   --权重法.参与主体小类代码
                   ||v_cc_sdvs.country_level;    --权重法.注册国家地区评级代码

        v_sql_pre := 'UPDATE RWA_DEV.RWA_EI_GUARANTEE T1 SET T1.GUARANTORSDVSSTD = ''' || v_cc_sdvs.guaranty_std_detail || ''' WHERE T1.QUALFLAGSTD = ''1''';

        v_sqlTab(v_tab_len) := v_sql_pre || v_clause || ' AND T1.DATADATE = TO_DATE('''|| p_data_dt_str || ''',''YYYYMMDD'')';
        v_tab_len := v_tab_len + 1;
    end loop;

    --3.执行更新sql
    --delete from qhjiang_test;
    for i in 1..v_sqlTab.count loop
        if v_sqlTab(i) IS NOT NULL THEN
            --insert into qhjiang_test(num1,str) values(i,v_sqlTab(i));
            --commit;
            v_err_sql := v_sqlTab(i);
            EXECUTE IMMEDIATE v_sqlTab(i);

            COMMIT;
        end if;
    end loop;

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_GUARANTEE WHERE (QUALFLAGSTD IS NOT NULL OR QUALFLAGFIRB IS NOT NULL) AND DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_GUARANTEE表认定权重法/内评法下合格的保证数据记录为: ' || v_count || ' 条');



    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功' || '-' || v_count;
        --定义异常
        EXCEPTION
    WHEN OTHERS THEN
                 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
                 Dbms_output.Put_line('出错了,错误SQL为:'||v_err_sql);
            ROLLBACK;
                p_po_rtncode := sqlcode;
                p_po_rtnmsg  := '权重法/内评法合格保证('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_CD_GUARANTY;
/

