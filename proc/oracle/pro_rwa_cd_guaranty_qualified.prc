CREATE OR REPLACE PROCEDURE RWA_DEV.pro_rwa_cd_guaranty_qualified(
                                       v_data_dt_str  IN   VARCHAR2,   --数据日期
                                       v_po_rtncode  OUT  VARCHAR2,     --返回编号
                                       v_po_rtnmsg    OUT  VARCHAR2     --返回描述
                                        ) AS
/*
    存储过程名称:pro_rwa_cd_guaranty_qualified
    实现功能:实现(UPDATE)各相关业务的合格保证缓释认定
    数据口径:全量
    跑批频率:月末
    版  本  :V1.0.0
    编写人  :YUSJ
    编写时间:2015-07-03
    单  位  :上海安硕信息技术股份有限公司
    源  表1  :合格保证代码表
    目标表  :保证表表
    辅助表  :无
    备   注：
    变更记录(修改人|修改时间|修改内容)：
  */
  --定义更新的sql语句
  v_update_sql VARCHAR2(4000);
  --定义匹配条件的记录
  v_count number(18) := 0;
  --保证编号
  GUARANTY_TYPE VARCHAR2(300);
  --册国家或地区代码
  REGIST_STATE_CODE VARCHAR2(600);
  --合格标识
  QUALIFIED_FLAG VARCHAR2(10);
BEGIN
  DECLARE
   --同过游标读取需要的判断条件
  CURSOR c_cursor IS
  SELECT
    CASE WHEN GUARANTY_TYPE IS NOT NULL
         THEN ' AND EXISTS(SELECT 1 FROM LOAN_APPEND  T1 WHERE T.GuaranteeID = T1.append_no AND T.GuaranteeConID= T1.APPEND_NO AND T1.REG_VALID = 0 AND T1.APPEND_TYPE='''||GUARANTY_TYPE||''') '
         ELSE ''
    END AS GUARANTY_TYPE    --保证编号
     ,CASE WHEN REGIST_STATE_CODE IS NOT NULL and CLIENT_SUB_TYPE IS NOT NULL and  COUNTRY_LEVEL_NO IS NOT NULL
         THEN ' AND EXISTS(SELECT 1 FROM RWA_EI_CLIENT T2 WHERE T.GuarantorID=T2.ClientID AND T.DATANO=T2.DATANO AND T2.RegistState='''||REGIST_STATE_CODE||''' AND T2.ClientSubType='''||CLIENT_SUB_TYPE||''' AND T2.RCERating'||COUNTRY_LEVEL_NO||') '
         WHEN REGIST_STATE_CODE IS NULL and CLIENT_SUB_TYPE IS NOT NULL and  COUNTRY_LEVEL_NO IS NOT NULL
         THEN ' AND EXISTS(SELECT 1 FROM RWA_EI_CLIENT T2 WHERE T.GuarantorID=T2.ClientID AND T.DATANO=T2.DATANO AND T2.ClientSubType='''||CLIENT_SUB_TYPE||''' AND T2.RCERating'||COUNTRY_LEVEL_NO||') '
         WHEN REGIST_STATE_CODE IS NOT NULL and CLIENT_SUB_TYPE IS NULL and  COUNTRY_LEVEL_NO IS NOT NULL
         THEN ' AND EXISTS(SELECT 1 FROM RWA_EI_CLIENT T2 WHERE T.GuarantorID=T2.ClientID AND T.DATANO=T2.DATANO AND T2.RegistState='''||REGIST_STATE_CODE||''' AND T2.RCERating'||COUNTRY_LEVEL_NO||') '
         WHEN REGIST_STATE_CODE IS NOT NULL and CLIENT_SUB_TYPE IS NOT NULL and  COUNTRY_LEVEL_NO IS NULL
         THEN ' AND EXISTS(SELECT 1 FROM RWA_EI_CLIENT T2 WHERE T.GuarantorID=T2.ClientID AND T.DATANO=T2.DATANO AND T2.RegistState='''||REGIST_STATE_CODE||''' AND T2.ClientSubType='''||CLIENT_SUB_TYPE||''') '
         WHEN REGIST_STATE_CODE IS NULL and CLIENT_SUB_TYPE IS NULL and  COUNTRY_LEVEL_NO IS NOT NULL
         THEN ' AND EXISTS(SELECT 1 FROM RWA_EI_CLIENT T2 WHERE T.GuarantorID=T2.ClientID AND T.DATANO=T2.DATANO AND T2.RCERating'||COUNTRY_LEVEL_NO||') '
         WHEN REGIST_STATE_CODE IS NULL and CLIENT_SUB_TYPE IS NOT NULL and  COUNTRY_LEVEL_NO IS NULL
         THEN ' AND EXISTS(SELECT 1 FROM RWA_EI_CLIENT T2 WHERE T.GuarantorID=T2.ClientID AND T.DATANO=T2.DATANO AND T2.ClientSubType='''||CLIENT_SUB_TYPE||''') '
         WHEN REGIST_STATE_CODE IS NOT NULL and CLIENT_SUB_TYPE IS NULL and  COUNTRY_LEVEL_NO IS NULL
         THEN ' AND EXISTS(SELECT 1 FROM RWA_EI_CLIENT T2 WHERE T.GuarantorID=T2.ClientID AND T.DATANO=T2.DATANO AND T2.RegistState='''||REGIST_STATE_CODE||''' ) '
         ELSE ''
    END AS REGIST_STATE_CODE    --注册国家地区代码
    ,QUALIFIED_FLAG
    FROM RWA_CD_GUARANTY_QUALIFIED;
  BEGIN
    --开启游标
    OPEN c_cursor;
    --通过循环来遍历检索游标
    DBMS_OUTPUT.PUT_LINE('>>>>>>Update语句开始执行中>>>>>>>');
    LOOP
      v_count := v_count + 1;
      -- 将游标获取的值赋予定义的匹配条件
      FETCH c_cursor INTO
        GUARANTY_TYPE
       ,REGIST_STATE_CODE
       ,QUALIFIED_FLAG
       ;
      --档游标检索完成后退出游标
      EXIT WHEN c_cursor%NOTFOUND;
      IF QUALIFIED_FLAG='01' THEN
        v_update_sql:='UPDATE RWA_EI_GUARANTEE T SET T.QUALFLAGSTD=''1'', T.QUALFLAGFIRB=''1''  WHERE T.datadate=TO_DATE('''||v_data_dt_str||''',''YYYYMMDD'')';
      ELSIF QUALIFIED_FLAG ='02' THEN
        v_update_sql:='UPDATE RWA_EI_GUARANTEE T SET T.QUALFLAGSTD=''0'', T.QUALFLAGFIRB=''1''  WHERE T.datadate=TO_DATE('''||v_data_dt_str||''',''YYYYMMDD'')';
      ELSE
        v_update_sql:='UPDATE RWA_EI_GUARANTEE T SET T.QUALFLAGSTD=''0'', T.QUALFLAGFIRB=''0''  WHERE T.datadate=TO_DATE('''||v_data_dt_str||''',''YYYYMMDD'')';
      END IF;
      --合并sql，拼接where条件
      v_update_sql := v_update_sql
      ||GUARANTY_TYPE
      ||REGIST_STATE_CODE
      ;
      DBMS_OUTPUT.PUT_LINE(v_update_sql);
      --执行sql
      EXECUTE IMMEDIATE v_update_sql;
      COMMIT;
      --结束循环
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('遍历的总记录为：'|| v_count);
    DBMS_OUTPUT.PUT_LINE('Update语句已经执行结束！！！');
      --关闭游标
    CLOSE c_cursor;
  END;
    UPDATE RWA_EI_GUARANTEE SET QUALFLAGSTD='0' WHERE datadate=TO_DATE(v_data_dt_str,'YYYYMMDD')AND QUALFLAGSTD IS NULL;
    UPDATE RWA_EI_GUARANTEE SET QUALFLAGFIRB='0' WHERE datadate=TO_DATE(v_data_dt_str,'YYYYMMDD') AND QUALFLAGFIRB IS NULL;
    COMMIT;
  v_po_rtncode := '1';
  v_po_rtnmsg  := '成功';
    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
         Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
         v_po_rtncode := sqlcode;
         v_po_rtnmsg  := '合格保证映射出错：'|| sqlerrm;
         RETURN;
END pro_rwa_cd_guaranty_qualified;
/

