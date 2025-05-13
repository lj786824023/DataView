CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_CD_STD_EXPOSURE_TYPE(
                                 p_data_dt_str  IN   VARCHAR2,   --数据日期
                                 p_po_rtncode    OUT  VARCHAR2,   --返回编号
                                 p_po_rtnmsg    OUT  VARCHAR2     --返回描述
                                 )
AS
/*
    存储过程名称:PRO_RWA_CD_SUBJECT_ASSET
    实现功能:更新暴露表中的暴露大类和暴露小类
    数据口径:全量
    跑批频率:月初
    版  本  :V1.0.0
    编写人  :YUSJ
    编写时间:2016-08-23
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :信用风险暴露表
    目标表  :RWA_CD_STD_EXPOSURE_TYPE
    辅助表  :无
    备   注：
    变更记录(修改人|修改时间|修改内容)：
  */
    --定义异常变量
  v_raise EXCEPTION;
  --定义更新的sql语句
  v_update_sql VARCHAR2(4000);
  --定义匹配条件的记录
  v_count number(18);
  --定义用于存放判断条件

  --资产小类代码
  ASSET_SUB_TYPE VARCHAR2(200);
  --参与主体小类
  CLIENT_SUB_TYPE VARCHAR2(4000);
   --注册地国家评级
  COUNTRY_RATTING VARCHAR2(4000);
  --债权等级
  CLAIMSLEVEL VARCHAR2(200);
  --债券发行目的
  BONDISSUEINTENT VARCHAR2(200);
  --原始期限
  ORIGINAL_MATURITY VARCHAR2(200);
  --业务品种
  BUSINESS_TYPE VARCHAR2(200);
  --股权投资形成原因
  EQUITYINVESTCAUSE VARCHAR2(200);
  --科目号
  SUBJECT_NO VARCHAR2(200);
  --是否非自用不动产
  NSUREALPROPERTYFLAG VARCHAR2(200);
  --暴露大类
  EXPOCLASSSTD VARCHAR2(100);
  --暴露小类
  EXPOSUBCLASSSTD VARCHAR2(100);


BEGIN

     --执行更新暴露大小类之前，把所有暴露大小类置空
     /*需要排除的情况太多了，干脆就直接不置空，因为每次执行暴露分类，都是要全部重新执行ETL
     UPDATE RWA_DEV.RWA_EI_EXPOSURE
		    SET EXPOCLASSSTD = NULL, EXPOSUBCLASSSTD = NULL
		  WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		    AND CASE
		          WHEN SSYSID NOT IN ('XYK', 'ABS', 'TZ', 'XD', 'TY','GQ','JDYS') THEN
		           '1'
		          WHEN SSYSID = 'TZ' AND CLIENTID NOT IN ('MZZXZ', 'XN-YBGS') THEN
		           '1'
		          WHEN SSYSID IN ('XD', 'TY') AND CLIENTID NOT IN ('XN-GEKH', 'XN-YBGS') THEN
		           '1'
		          WHEN EXPOSUREID LIKE 'YPWF%' OR EXPOSUREID LIKE 'MDSP%' OR EXPOSUREID LIKE 'MDYP%' THEN
		           '0'
		          ELSE
		           '0'
		        END = '1'
     ;
     COMMIT;
     */

  DECLARE
   --同过游标读取需要的判断条件
  CURSOR c_cursor IS
    SELECT
    CASE WHEN ASSET_SUB_TYPE IS NOT NULL
         THEN ' AND AssetSubType= '''||ASSET_SUB_TYPE||''' '
         ELSE ''
    END  AS ASSET_SUB_TYPE    --资产小类
    ,CASE WHEN CLAIMSLEVEL IS NOT NULL
         THEN ' AND ClaimsLevel= '''||CLAIMSLEVEL||''' '
         ELSE ''
    END  AS CLAIMSLEVEL    --债权等级
    ,CASE WHEN BONDISSUEINTENT IS NOT NULL
         THEN ' AND BONDISSUEINTENT= '''||BONDISSUEINTENT||''' '
         ELSE ''
    END  AS BONDISSUEINTENT    --债券发行目的
    ,CASE WHEN ORIGINAL_MATURITY='01'
         THEN ' AND OriginalMaturity<=0.25 '
         WHEN ORIGINAL_MATURITY='02'
         THEN ' AND OriginalMaturity>0.25 '
         WHEN ORIGINAL_MATURITY='05'
         THEN ' AND OriginalMaturity<=2 '
         WHEN ORIGINAL_MATURITY='06'
         THEN ' AND OriginalMaturity>2 '
         ELSE ''
    END  AS ORIGINAL_MATURITY    --原始期限
    ,CASE WHEN BUSINESS_TYPE IS NOT NULL
         THEN ' AND BusinessTypeID='''||BUSINESS_TYPE||''' '
         ELSE ''
    END  AS BUSINESS_TYPE    --业务品种
    ,CASE WHEN EQUITYINVESTCAUSE IS NOT NULL
         THEN ' AND EQUITYINVESTCAUSE= '''||EQUITYINVESTCAUSE||''' '
         ELSE ''
    END  AS EQUITYINVESTCAUSE    --股权投资形成原因
    ,CASE WHEN SUBJECT_NO IS NOT NULL
         THEN ' AND AccSubject1 LIKE '''||SUBJECT_NO||'%'' '
         ELSE ''
    END  AS SUBJECT_NO    --科目号
    ,CASE WHEN NSUREALPROPERTYFLAG IS NOT NULL
         THEN ' AND NSUREALPROPERTYFLAG= '''||NSUREALPROPERTYFLAG||''' '
         ELSE ''
    END  AS NSUREALPROPERTYFLAG    --是否非自用不动产
    ,CASE WHEN CLIENT_SUB_TYPE IS NOT NULL AND SSMBFlag IS NOT NULL AND ORGANIZATIONCODE IS NOT NULL
          THEN ' AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T1 WHERE T.ClientID = T1.ClientID AND T1.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'') AND T1.ClientSubType LIKE '''||CLIENT_SUB_TYPE||'%'' AND T1.SSMBFLAGSTD='''||SSMBFlag||''' AND REPLACE(T1.ORGANIZATIONCODE,''-'','''')='''||ORGANIZATIONCODE||''') '
          WHEN CLIENT_SUB_TYPE IS NOT NULL AND SSMBFlag IS NOT NULL AND ORGANIZATIONCODE IS  NULL
          THEN ' AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T1 WHERE T.ClientID = T1.ClientID AND T1.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'') AND T1.ClientSubType LIKE '''||CLIENT_SUB_TYPE||'%'' AND T1.SSMBFLAGSTD='''||SSMBFlag||''') '
          WHEN CLIENT_SUB_TYPE IS NOT NULL AND SSMBFlag IS  NULL AND ORGANIZATIONCODE IS NOT  NULL
          THEN ' AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T1 WHERE T.ClientID = T1.ClientID AND T1.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'') AND T1.ClientSubType LIKE '''||CLIENT_SUB_TYPE||'%'' AND REPLACE(T1.ORGANIZATIONCODE,''-'','''')='''||ORGANIZATIONCODE||''') '
          WHEN CLIENT_SUB_TYPE IS  NULL AND SSMBFlag IS  NULL AND ORGANIZATIONCODE IS NOT  NULL
          THEN ' AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T1 WHERE T.ClientID = T1.ClientID AND T1.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'') AND REPLACE(T1.ORGANIZATIONCODE,''-'','''')='''||ORGANIZATIONCODE||''') '
          WHEN CLIENT_SUB_TYPE IS NOT NULL AND SSMBFlag IS  NULL AND ORGANIZATIONCODE IS  NULL
          THEN ' AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T1 WHERE T.ClientID = T1.ClientID AND T1.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'') AND T1.ClientSubType LIKE '''||CLIENT_SUB_TYPE||'%'')'
          ELSE ''
    END  AS CLIENT_SUB_TYPE    --参与主体小类,标准小微企业标识
    ,CASE WHEN COUNTRY_RATTING='01'
          THEN  ' AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T1 WHERE T.ClientID = T1.ClientID AND T1.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'') AND T1.RCERating<=''0104'') '  -- AA-级以上
          WHEN COUNTRY_RATTING='02'
          THEN  ' AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T1 WHERE T.ClientID = T1.ClientID AND T1.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'') AND T1.RCERating>''0104'' AND T1.RCERating<=''0107'') ' --AA-级以下，A-级及以上
          WHEN COUNTRY_RATTING='03'
          THEN  ' AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T1 WHERE T.ClientID = T1.ClientID AND T1.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'') AND T1.RCERating>''0107'' AND T1.RCERating<=''0110'') ' --A-级以下，BBB-级及以上
          WHEN COUNTRY_RATTING='04'
          THEN  ' AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T1 WHERE T.ClientID = T1.ClientID AND T1.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'') AND T1.RCERating>''0110'' AND T1.RCERating<=''0116'') ' --BBB-级以下，B-级及以上
          WHEN COUNTRY_RATTING='09'
          THEN  ' AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T1 WHERE T.ClientID = T1.ClientID AND T1.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'') AND T1.RCERating>''0107'' AND T1.RCERating<=''0116'') ' --A-级以下，B-级及以上
          WHEN COUNTRY_RATTING='05'
          THEN  ' AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T1 WHERE T.ClientID = T1.ClientID AND T1.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'') AND T1.RCERating>''0116'' AND T1.RCERating<''0124'') ' --B-级以下
          WHEN COUNTRY_RATTING='06'
          THEN  ' AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T1 WHERE T.ClientID = T1.ClientID AND T1.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'') AND T1.RCERating=''0124'') ' --未评级
          ELSE ''
     END AS COUNTRY_RATTING
    ,EXPOCLASSSTD     --暴露大类代码
    ,EXPOSUBCLASSSTD  --暴露小类代码
    FROM RWA_CD_STD_EXPOSURE_TYPE
          --  WHERE SERIALNO<>'20160819000000000203'

        ORDER BY SORTNO
    ;

  BEGIN
    --开启游标
    OPEN c_cursor;
    --通过循环来遍历检索游标
    LOOP
      --将游标获取的值赋予定义的匹配条件
      FETCH c_cursor INTO
        ASSET_SUB_TYPE
       ,CLIENT_SUB_TYPE
       ,COUNTRY_RATTING
       ,CLAIMSLEVEL
       ,BONDISSUEINTENT
       ,ORIGINAL_MATURITY
       ,BUSINESS_TYPE
       ,EQUITYINVESTCAUSE
       ,SUBJECT_NO
       ,NSUREALPROPERTYFLAG
       ,EXPOCLASSSTD
       ,EXPOSUBCLASSSTD
       ;
      --档游标检索完成后退出游标
      EXIT WHEN c_cursor%NOTFOUND;
      v_update_sql:='UPDATE RWA_DEV.RWA_EI_EXPOSURE T SET T.EXPOCLASSSTD='''||EXPOCLASSSTD||''', T.EXPOSUBCLASSSTD='''||EXPOSUBCLASSSTD||''' WHERE T.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'') 
                AND T.EXPOCLASSSTD IS NULL AND T.EXPOSUBCLASSSTD IS NULL ';

      --合并sql，拼接where条件
      v_update_sql := v_update_sql
      ||ASSET_SUB_TYPE
      ||CLIENT_SUB_TYPE
      ||COUNTRY_RATTING
      ||CLAIMSLEVEL
      ||BONDISSUEINTENT
      ||ORIGINAL_MATURITY
      ||BUSINESS_TYPE
      ||EQUITYINVESTCAUSE
      ||SUBJECT_NO
      ||NSUREALPROPERTYFLAG
      ;
      --执行sql
      EXECUTE IMMEDIATE v_update_sql;
      COMMIT;
      --结束循环
    END LOOP;
      --关闭游标
    CLOSE c_cursor;
  END;
    --通过暴露小类更新权重法业务类型
    UPDATE RWA_DEV.RWA_EI_EXPOSURE T1
        SET T1.BUSINESSTYPESTD=(SELECT T2.BUSINESSSTDTYPE
                             FROM RWA_DEV.RWA_CD_STD_BUSINESS_TYPE T2
                             WHERE T1.EXPOSUBCLASSSTD=T2.EXPOSUBCLASSSTD
                             )
    WHERE T1.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;
    
    
     -----------------更新场外净额结算工具暴露类别   BY WZB 20191130---
    MERGE INTO RWA_EI_OTCNETTING A
USING (SELECT T.VALIDNETAGREEMENTID AS VALIDNETAGREEMENTID,
              MAX(T.EXPOCLASSSTD) AS EXPOCLASSSTD ,
              MAX(T.EXPOSUBCLASSSTD) AS EXPOSUBCLASSSTD,
              MAX(T.EXPOCLASSIRB) AS EXPOCLASSIRB,
              MAX(T.EXPOSUBCLASSIRB) AS EXPOSUBCLASSIRB
         FROM rwa_ei_exposure T
        WHERE SSYSID = 'YSP'
          AND DATANO = p_data_dt_str
        GROUP BY VALIDNETAGREEMENTID) B
on (A.VALIDNETAGREEMENTID = B.VALIDNETAGREEMENTID AND A.DATANO = p_data_dt_str)
 WHEN MATCHED 
   THEN UPDATE SET A.EXPOCLASSSTD=B.EXPOCLASSSTD,
                   A.EXPOSUBCLASSSTD=B.EXPOSUBCLASSSTD,
                   A.EXPOCLASSIRB=B.EXPOCLASSIRB,
                   A.EXPOSUBCLASSIRB=B.EXPOSUBCLASSIRB;
     COMMIT;


    
    
    --统计没有更新成功的暴露大小类条数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_EXPOSURE WHERE DATANO=p_data_dt_str AND ExpoSubClassSTD IS  NULL;
    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功'||'-'||v_count;
    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '更新权重法暴露大小类(PRO_RWA_CD_STD_EXPOSURE_TYPE)ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_CD_STD_EXPOSURE_TYPE;
/

