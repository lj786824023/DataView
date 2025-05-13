CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_CD_OFF_EXPOSURE_TYPE(
                                 p_data_dt_str  IN   VARCHAR2,   --数据日期
                                 p_po_rtncode    OUT  VARCHAR2,   --返回编号
                                 p_po_rtnmsg    OUT  VARCHAR2     --返回描述
                                 )
AS
/*
    存储过程名称:PRO_RWA_CD_SUBJECT_ASSET
    实现功能:更新暴露表中表外业务类型和表外业务类型细分
    数据口径:全量
    跑批频率:月初
    版  本  :V1.0.0
    编写人  :YUSJ
    编写时间:2016-08-26
    单  位  :上海安硕信息技术股份有限公司
    源  表1  :信用风险暴露表
    源  表2  :信用风险合同表
    源  表3  :参与主体表
    目标表  :RWA_CD_OFF_EXPOSURE_TYPE
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

  --资产小类代码
  ASSET_SUB_TYPE VARCHAR2(50);
  --原始期限
  ORIGINAL_MATURITY VARCHAR2(100);
  --表外业务类型
  OFFBUSINESSTYPE VARCHAR2(50);
  --表外业务类型细分
  OFFBUSINESSSDVSSTD VARCHAR2(50);

BEGIN

  DECLARE
   --同过游标读取需要的判断条件
  CURSOR c_cursor IS
    SELECT
    CASE WHEN ASSET_SUB_TYPE IS NOT NULL
         THEN ' AND AssetSubType= '''||ASSET_SUB_TYPE||''' '
         ELSE ''
    END  AS ASSET_SUB_TYPE    --资产小类
    ,CASE WHEN ORIGINAL_MATURITY='01'
          THEN ' AND OriginalMaturity<=1'
          WHEN ORIGINAL_MATURITY='02'
          THEN ' AND OriginalMaturity>1'
          ELSE ''
     END AS ORIGINAL_MATURITY     --原始期限
    ,OFFBUSINESSTYPE   --表外业务类型
    ,OFFBUSINESSSDVSSTD  --表外业务类型细分
    FROM RWA_CD_OFF_EXPOSURE_TYPE
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
        ASSET_SUB_TYPE
       ,ORIGINAL_MATURITY
       ,OFFBUSINESSTYPE
       ,OFFBUSINESSSDVSSTD
       ;
      --档游标检索完成后退出游标
      EXIT WHEN c_cursor%NOTFOUND;
      v_update_sql:='UPDATE RWA_DEV.RWA_EI_EXPOSURE T SET T.OFFBUSINESSTYPE='''||OFFBUSINESSTYPE||''', T.OFFBUSINESSSDVSSTD='''||OFFBUSINESSSDVSSTD||''' WHERE T.DATADATE=TO_DATE('''||p_data_dt_str||''',''YYYYMMDD'') AND T.OFFBUSINESSSDVSSTD IS NULL ';

      --合并sql，拼接where条件
      v_update_sql := v_update_sql
      ||ASSET_SUB_TYPE
      ||ORIGINAL_MATURITY
      ;

      --执行sql
      EXECUTE IMMEDIATE v_update_sql;
      COMMIT;
      --结束循环
    END LOOP;
      --关闭游标
    CLOSE c_cursor;
  END;
    --统计没有更新的条数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_EXPOSURE WHERE DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD') AND OFFBUSINESSSDVSSTD IS NOT NULL;
    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功'||'-'||v_count;

    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '更新表外业务类型及表外业务类型细分(PRO_RWA_CD_OFF_EXPOSURE_TYPE)ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_CD_OFF_EXPOSURE_TYPE;
/

