CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_CD_IRB_EXPOSURE_TYPE(
                                 p_data_dt_str  IN   VARCHAR2,    --数据日期
                                 p_po_rtncode   OUT  VARCHAR2,    --返回编号
                                 p_po_rtnmsg    OUT  VARCHAR2     --返回描述
                                 )
AS
/*
    存储过程名称:PRO_RWA_CD_IRB_EXPOSURE_TYPE
    实现功能:更新暴露表中内评法暴露大类和内评法暴露小类
    数据口径:全量
    跑批频率:月初
    版  本  :V1.0.0
    编写人  :QHJIANG
    编写时间:2017-04-05
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :信用风险暴露表       RWA_EI_EXPOSURE
    源  表2 :信用风险合同表       RWA_EI_CONTRACT
    源  表4 :参与主体表           RWA_EI_CLIENT
    目标表  :内评法暴露分类(RWA系统内部计算)表       RWA_IRB_EXPOSURE_TYPE
    辅助表  :内评法暴露分类映射表 RWA_CD_IRB_EXPOSURE_TYPE
    备   注 :资产证券化风险暴露,由补录数据时初始化完成，在此不需要做计算
             其他风险暴露,合格购入公司应收账款（暂无此业务，将来有的话将纳入）
    变更记录(修改人|修改时间|修改内容)：
  */
    --定义异常变量
  v_raise EXCEPTION;
  --定义更新的sql语句
  v_update_sql VARCHAR2(4000);
  --定义匹配条件的记录
  v_count number(18);
  --定义用于存放判断条件

  --参与主体小类
  CLIENT_SUB_TYPE VARCHAR2(4000);
  --专业贷款类型
  SL_TYPE VARCHAR2(4000);
  --业务品种
  BUSINESS_TYPE VARCHAR2(4000);
  --符合监管标准的小微企业
  SSMB_FLAG VARCHAR2(4000);
  --科目
  SUBJECT_NO VARCHAR2(4000);
  --信用卡额度+合同金额
  BALANCE VARCHAR2(4000);

  --内评法暴露大类
  EXPO_CLASS_IRB VARCHAR2(100);
  --内评法暴露小类
  EXPO_SUBCLASS_IRB VARCHAR2(100);
  --定义名称变量
  v_pro_name VARCHAR2(200) := 'RWA_DEV.RWA_IRB_EXPOSURE_TYPE';


BEGIN

     --清理当期数据，支持重跑
     --DELETE FROM RWA_DEV.RWA_TEMP_CRED_LIMIT;
     --COMMIT;

     EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TEMP_CRED_LIMIT';

     BEGIN
     --内平法对比结果表存了多期数据，要建成分区表
     --删除不存在的分区，如果此存储过程是第一次跑抛出异常，可以忽略
     EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_IRB_EXPOSURE_TYPE DROP PARTITION RWA_IRB_EXPOSURE_TYPE' || p_data_dt_str;

     --COMMIT;

     EXCEPTION
      WHEN OTHERS THEN
        IF (SQLCODE <> '-2149') THEN
          --首次分区truncate会出现2149异常
         p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '内评法对比结果表('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
        END IF;
    END;

    --新增一个当前日期下的分区
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_IRB_EXPOSURE_TYPE ADD PARTITION RWA_IRB_EXPOSURE_TYPE' || p_data_dt_str || ' VALUES(TO_DATE('|| p_data_dt_str || ',''YYYYMMDD''))';
    --COMMIT;

     INSERT INTO RWA_DEV.RWA_TEMP_CRED_LIMIT(CLIENTID,CRED_LIMIT)
     SELECT CLIENTID,SUM(CRED_LIMIT) AS CRED_LIMIT FROM (
            SELECT T1.CLIENTID AS CLIENTID,T1.CONTRACTAMOUNT AS CRED_LIMIT
              FROM RWA_DEV.RWA_EI_CONTRACT T1
             WHERE T1.DATADATE = TO_DATE(P_DATA_DT_STR,'YYYYMMDD')
               AND T1.GUARANTEETYPE='005'
               AND T1.BUSINESSTYPEID in('11103018','11103015','11106010')--11103018 长江卡循环贷款 11103015 接利贷 11106010 信用卡垫款
            )
      GROUP BY CLIENTID;
    COMMIT;

    --未使用额度客户，但是不在上面当中
    INSERT INTO RWA_DEV.RWA_TEMP_CRED_LIMIT(CLIENTID,CRED_LIMIT)
     SELECT CLIENTID,SUM(CRED_LIMIT) AS CRED_LIMIT FROM (
            SELECT T1.CLIENTID AS CLIENTID,T1.CONTRACTAMOUNT AS CRED_LIMIT
              FROM RWA_DEV.RWA_EI_CONTRACT T1
             WHERE T1.DATADATE = TO_DATE(P_DATA_DT_STR,'YYYYMMDD')
               AND T1.GUARANTEETYPE='005'
               AND T1.BUSINESSTYPEID in('11103018','11103015','11106020')--11103018 长江卡循环贷款 11103015 接利贷 11106020 信用卡垫款_未使用额度
            ) T
      WHERE NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_TEMP_CRED_LIMIT T2 WHERE T.CLIENTID=T2.CLIENTID)
      GROUP BY T.CLIENTID;
    COMMIT;

	dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_TEMP_CRED_LIMIT',cascade => true);

  DECLARE
   --同过游标读取需要的判断条件
  CURSOR c_cursor IS
    SELECT
         --参与主体小类
         CASE WHEN CLIENTSUBTYPE IS NOT NULL
              THEN ' AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T1 WHERE T.CLIENTID = T1.CLIENTID AND T.DATADATE = T1.DATADATE AND T1.CLIENTSUBTYPE like '''||CLIENTSUBTYPE||'%'') '
              ELSE ''
         END  AS CLIENT_SUB_TYPE
         --专业贷款类型
        ,CASE WHEN SLTYPE IS NOT NULL
              THEN ' AND T.SLTYPE= '''||SLTYPE||''' '
              ELSE ''
         END  AS SL_TYPE
         --业务品种
        ,CASE WHEN BUSINESSTYPE IS NOT NULL
              THEN ' AND T.BUSINESSTYPEID= '''||BUSINESSTYPE||''' '
              ELSE ''
         END  AS BUSINESS_TYPE
         --符合监管标准的小微企业
        ,CASE WHEN SSMBFLAG IS NOT NULL
              THEN ' AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T1 WHERE T.CLIENTID = T1.CLIENTID AND T.DATADATE = T1.DATADATE AND T1.SSMBFLAG= '''||SSMBFLAG||''') '
              ELSE ''
         END  AS SSMB_FLAG
         --科目
        ,CASE WHEN SUBJECTNO IS NOT NULL
              THEN ' AND T.ACCSUBJECT1 like '''||SUBJECTNO||'%'' '
              ELSE ''
         END  AS SUBJECT_NO
         --担保方式 信用卡额度+合同金额
        ,CASE WHEN BALANCE IS NOT NULL   --代码:CreditAmountType 01 <=100万
               AND (   BUSINESSTYPE='11103018'--11103018 长江卡循环贷款
                    OR BUSINESSTYPE='11103015'--11103015 接利贷
                    OR BUSINESSTYPE='11106010'--11106010 信用卡垫款
                    OR BUSINESSTYPE='11106020'--11106020 信用卡垫款_未使用额度
                   )
               AND BALANCE='01'
              THEN ' AND T.BUSINESSTYPEID='''||BUSINESSTYPE||'''
                     AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_EI_CONTRACT T1 WHERE T.DATADATE=T1.DATADATE AND T.CONTRACTID = T1.CONTRACTID AND ((T1.GUARANTEETYPE=''005'' AND T1.BUSINESSTYPEID in(''11103018'',''11103015'')) OR T1.BUSINESSTYPEID in(''11106010'',''11106020'') ))
                     AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_TEMP_CRED_LIMIT T1 WHERE T1.CLIENTID = T.CLIENTID AND T1.CRED_LIMIT<=1000000) '
              ELSE ''
         END  AS BALANCE
         --内评法暴露大类
        ,EXPOCLASSIRB    AS EXPO_CLASS_IRB
         --内评法暴露小类
        ,EXPOSUBCLASSIRB AS EXPO_SUBCLASS_IRB
    FROM RWA_CD_IRB_EXPOSURE_TYPE
    ORDER BY SORTNO ASC
    ;

  BEGIN
    --开启游标
    OPEN c_cursor;
    --通过循环来遍历检索游标
    LOOP
      --将游标获取的值赋予定义的匹配条件
      FETCH c_cursor INTO
        CLIENT_SUB_TYPE    --参与主体小类
       ,SL_TYPE            --专业贷款类型
       ,BUSINESS_TYPE      --业务品种
       ,SSMB_FLAG          --符合监管标准的小微企业
       ,SUBJECT_NO         --科目
       ,BALANCE            --信用卡额度+合同金额
       ,EXPO_CLASS_IRB     --内评法暴露大类
       ,EXPO_SUBCLASS_IRB  --内评法暴露小类
       ;
      --档游标检索完成后退出游标
      EXIT WHEN c_cursor%NOTFOUND;
      v_update_sql:='INSERT INTO RWA_DEV.RWA_IRB_EXPOSURE_TYPE(DATADATE,DATANO,EXPOSUREID,EXPOCLASSIRB,EXPOSUBCLASSIRB,RWAEXPOCLASSIRB,RWAEXPOSUBCLASSIRB)
                     SELECT  T.DATADATE                     AS DATADATE
                            ,T.DATANO                       AS DATANO
                            ,T.EXPOSUREID                   AS EXPOSUREID
                            ,T.EXPOCLASSIRB                 AS EXPOCLASSIRB
                            ,T.EXPOSUBCLASSIRB              AS EXPOSUBCLASSIRB
                            ,'''||EXPO_CLASS_IRB||'''       AS RWAEXPOCLASSIRB
                            ,'''||EXPO_SUBCLASS_IRB||'''    AS RWAEXPOSUBCLASSIRB
                       FROM RWA_DEV.RWA_EI_EXPOSURE T
                      WHERE T.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'')
                        AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_IRB_EXPOSURE_TYPE RIET WHERE RIET.DATANO=T.DATANO AND RIET.EXPOSUREID=T.EXPOSUREID) ';

      --合并sql，拼接where条件
      v_update_sql := v_update_sql
      ||CLIENT_SUB_TYPE    --参与主体小类
      ||SL_TYPE            --专业贷款类型
      ||BUSINESS_TYPE      --业务品种
      ||SSMB_FLAG          --符合监管标准的小微企业
      ||SUBJECT_NO         --科目
      ||BALANCE            --信用卡额度+合同金额
      ;

      EXECUTE IMMEDIATE v_update_sql;
      COMMIT;
      --结束循环
    END LOOP;
    --关闭游标
    CLOSE c_cursor;
  END;
    ----整理表信息
    --dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_IRB_EXPOSURE_TYPE',cascade => true);
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_IRB_EXPOSURE_TYPE',partname => 'RWA_IRB_EXPOSURE_TYPE'||p_data_dt_str,granularity => 'PARTITION',cascade => true);

    --执行更新暴露大小类之前，把所有暴露大小类置空（使用我们的规则进行内评法暴露分类）
    UPDATE RWA_DEV.RWA_EI_EXPOSURE SET EXPOCLASSIRB=NULL,EXPOSUBCLASSIRB=NULL
    WHERE DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND SSYSID <> 'LC'
    AND EXPOSUREID <> 'B200801010095';

    COMMIT;

    --更新内评法暴露大小类(内评法暴露大小类为空的情况)
    MERGE INTO (SELECT DATADATE,DATANO,EXPOSUREID,EXPOCLASSIRB,EXPOSUBCLASSIRB
                  FROM RWA_DEV.RWA_EI_EXPOSURE
                 WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
                   AND EXPOCLASSIRB IS NULL) T
    USING (SELECT T1.EXPOSUREID,T1.DATADATE,T1.DATANO,T1.RWAEXPOCLASSIRB,T1.RWAEXPOSUBCLASSIRB
             FROM RWA_DEV.RWA_IRB_EXPOSURE_TYPE T1
            WHERE T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')) T2
    ON(T2.EXPOSUREID = T.EXPOSUREID AND T2.DATADATE = T.DATADATE AND T2.DATANO=T.DATANO )
    WHEN MATCHED THEN
      UPDATE SET T.EXPOCLASSIRB = T2.RWAEXPOCLASSIRB,T.EXPOSUBCLASSIRB = T2.RWAEXPOSUBCLASSIRB;

		COMMIT;

    --无内评法暴露分类的全部放到一般公司
    UPDATE RWA_DEV.RWA_EI_EXPOSURE T SET T.EXPOCLASSIRB = '0203',T.EXPOSUBCLASSIRB = '020301'
    WHERE T.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND T.EXPOSUBCLASSIRB IS NULL;

    COMMIT;

    --更新内评法标准小微企业标识为1的客户的内评法暴露类别为其他零售风险暴露
		UPDATE RWA_DEV.RWA_EI_CLIENT SET EXPOCATEGORYIRB = '020403'
		WHERE  DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')
		AND		 CLIENTTYPE = '03'
		AND		 SSMBFLAG = '1'
		;

		COMMIT;

		--更新参与主体内评法暴露大小类
    MERGE INTO (SELECT CLIENTID
    									,EXPOCATEGORYIRB
    							FROM RWA_DEV.RWA_EI_CLIENT
    						 WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
    						 	 AND EXPOCATEGORYIRB IS NULL) T
    	USING (SELECT T1.CLIENTID,
              MAX(T1.EXPOSUBCLASSIRB) AS EXPOSUBCLASSIRB
         FROM RWA_DEV.RWA_EI_EXPOSURE T1
        WHERE T1.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
        GROUP BY T1.CLIENTID) T2
     	ON (T.CLIENTID = T2.CLIENTID)
		WHEN MATCHED THEN
  	UPDATE SET T.EXPOCATEGORYIRB = T2.EXPOSUBCLASSIRB;

		COMMIT;


		--更新没有暴露的参与主体内评法暴露类别
	  MERGE INTO (SELECT CLIENTSUBTYPE,EXPOCATEGORYIRB FROM RWA_DEV.RWA_EI_CLIENT WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD') AND EXPOCATEGORYIRB IS NULL) T
	  	USING (SELECT T1.CLIENTSUBTYPE,MIN(T1.EXPOSUBCLASSIRB) AS EXPOSUBCLASSIRB
	         FROM RWA_DEV.RWA_CD_IRB_EXPOSURE_TYPE T1
	        GROUP BY T1.CLIENTSUBTYPE) T2
	  	ON (T.CLIENTSUBTYPE = T2.CLIENTSUBTYPE)
	  WHEN MATCHED THEN
	  UPDATE SET T.EXPOCATEGORYIRB = T2.EXPOSUBCLASSIRB;

		COMMIT;
    
    
   

    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_IRB_EXPOSURE_TYPE WHERE DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD');
    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功'||'-'||v_count;
    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '更新内评法暴露大小类(PRO_RWA_CD_IRB_EXPOSURE_TYPE)ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_CD_IRB_EXPOSURE_TYPE;
/

