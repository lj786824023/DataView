CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_CD_SUBJECT_ASSET(
                                 p_data_dt_str  IN   VARCHAR2,   --数据日期
                                 p_po_rtncode    OUT  VARCHAR2,   --返回编号
                                 p_po_rtnmsg    OUT  VARCHAR2     --返回描述
                                 )
AS
/*
    存储过程名称:PRO_RWA_CD_SUBJECT_ASSET
    实现功能:更新暴露表和合同表中的资产大小类
    数据口径:全量
    跑批频率:月初
    版  本  :V1.0.0
    编写人  :YUSJ
    编写时间:2016-08-18
    单  位  :上海安硕信息技术股份有限公司
    源  表1  :信用风险暴露表
    源  表2  :信用风险合同表
    源  表3  :参与主体表
    目标表  :RWA_CD_SUBJECT_ASSET
    辅助表  :无
    备   注：
    变更记录(修改人|修改时间|修改内容)：
  */
    --定义异常变量
  v_raise EXCEPTION;
  --定义更新的sql语句
  v_update_sql VARCHAR2(2000);
  --定义匹配条件的记录
  v_count number(18);
  --定义用于存放判断条件

  --科目号
  SUBJECT_NO VARCHAR2(100);
  --参与主体小类
  CLIENT_SUB_TYPE VARCHAR2(300);
   --原始期限
  ORIGINAL_MATURITY VARCHAR2(300);
  --业务品种代码
  BUSINESS_TYPE VARCHAR2(300);
  --资产大类代码
  ASSET_TYPE VARCHAR2(50);
  --资产小类代码
  ASSET_SUB_TYPE VARCHAR2(50);

BEGIN

     --执行更新暴露层面  资产大小类之前，把所有资产大小类置空
     UPDATE RWA_DEV.RWA_EI_EXPOSURE SET AssetType=NULL,AssetSubType=NULL
     WHERE DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
     AND SSYSID NOT IN ('XYK','ABS','DZ')
     ;
     COMMIT;
  DECLARE
      --同过游标读取需要的判断条件
      CURSOR c_cursor IS
        SELECT
        CASE WHEN SUBJECT_NO IS NOT NULL
             THEN ' AND AccSubject1 LIKE '''||SUBJECT_NO||'%'' '
             ELSE ''
        END  AS SUBJECT_NO    --科目号
        ,CASE WHEN CLIENT_SUB_TYPE IS NOT NULL
              THEN ' AND EXISTS(SELECT 1 FROM RWA_DEV.RWA_EI_CLIENT T1 WHERE T.ClientID = T1.ClientID AND T1.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'') AND T1.ClientSubType='''||CLIENT_SUB_TYPE||''') '
               ELSE ''
        END  AS CLIENT_SUB_TYPE    --参与主体小类
        ,CASE WHEN ORIGINAL_MATURITY ='03'
              THEN ' AND OriginalMaturity<=1 '
              WHEN ORIGINAL_MATURITY ='04'
              THEN ' AND OriginalMaturity>1 '
              ELSE ''
         END AS ORIGINAL_MATURITY     --原始期限
        ,CASE WHEN BUSINESS_TYPE IS NOT NULL
             THEN ' AND BusinessTypeID='''||BUSINESS_TYPE||''' '
             ELSE ''
        END AS BUSINESS_TYPE     --原始期限
        ,ASSET_TYPE   --暴露大类代码
        ,ASSET_SUB_TYPE  --暴露小类代码
        FROM RWA_CD_SUBJECT_ASSET
        ORDER BY SUBJECT_NO,CLIENT_SUB_TYPE,BUSINESS_TYPE ASC
        ;

  BEGIN
    
    --开启游标
    OPEN c_cursor;
    --通过循环来遍历检索游标
    DBMS_OUTPUT.PUT_LINE('>>>>>>Update语句开始执行中>>>>>>>');
    LOOP
      --v_count := v_count + 1;
      --将游标获取的值赋予定义的匹配条件
      FETCH c_cursor INTO
        SUBJECT_NO
       ,CLIENT_SUB_TYPE
       ,ORIGINAL_MATURITY
       ,BUSINESS_TYPE
       ,ASSET_TYPE
       ,ASSET_SUB_TYPE
       ;
      --档游标检索完成后退出游标
      EXIT WHEN c_cursor%NOTFOUND;
      v_update_sql:='UPDATE RWA_DEV.RWA_EI_EXPOSURE T SET T.AssetType='''||ASSET_TYPE||''', T.AssetSubType='''||ASSET_SUB_TYPE||''' WHERE T.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'') AND T.AssetType IS NULL AND T.AssetSubType IS NULL ';

      --合并sql，拼接where条件
      v_update_sql := v_update_sql
      ||SUBJECT_NO
      ||CLIENT_SUB_TYPE
      ||ORIGINAL_MATURITY
      ||BUSINESS_TYPE
      ;

      --执行sql
      EXECUTE IMMEDIATE v_update_sql;
      COMMIT;
      --结束循环
    END LOOP;
      --关闭游标
    CLOSE c_cursor;
  END;
  
  
  
  
  
    --执行更新合同层面  资产大小类之前，把所有资产大小类置空
     UPDATE RWA_DEV.RWA_EI_CONTRACT SET AssetType=NULL,AssetSubType=NULL
     WHERE DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
     AND SSYSID NOT IN('XYK','ABS','DZ')
     ;
     COMMIT;


    --通过暴露的资产大小类更新合同的资产大小类
    MERGE INTO (SELECT ContractID
    									,ASSETTYPE
    									,ASSETSUBTYPE
    							FROM RWA_DEV.RWA_EI_CONTRACT
    						 WHERE DATADATE=TO_DATE(P_DATA_DT_STR,'YYYYMMDD')
    							 AND SSYSID NOT IN ('XYK','ABS','DZ')) T1
    USING (SELECT CONTRACTID, MIN(ASSETTYPE) AS ASSETTYPE,MIN(ASSETSUBTYPE) AS ASSETSUBTYPE
            FROM RWA_DEV.RWA_EI_EXPOSURE
            WHERE DATADATE=TO_DATE(P_DATA_DT_STR,'YYYYMMDD')
            AND SSYSID NOT IN('XYK','ABS','DZ')
            GROUP BY CONTRACTID) T2
    ON (T1.ContractID=T2.ContractID)
    WHEN MATCHED THEN
    UPDATE SET T1.ASSETTYPE=T2.ASSETTYPE,T1.ASSETSUBTYPE=T2.ASSETSUBTYPE
    ;
    COMMIT;
    UPDATE RWA_EI_CONTRACT SET
    ASSETTYPE='121',
    ASSETSUBTYPE='12101'
    WHERE DATANO=p_data_dt_str AND ASSETSUBTYPE IS NULL;
    
    COMMIT;


    --统计没有更新到暴露条数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_EXPOSURE WHERE DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD') AND AssetType IS NULL;
    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功'||'-'||v_count;


    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '更新资产分类(PRO_RWA_CD_SUBJECT_ASSET)ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_CD_SUBJECT_ASSET;
/

