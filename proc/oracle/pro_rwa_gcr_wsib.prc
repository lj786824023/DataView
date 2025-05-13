CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_GCR_WSIB(p_data_dt_str IN VARCHAR2, --数据日期 yyyyMMdd
                                             p_po_rtncode  OUT VARCHAR2, --返回编号 1 成功,0 失败
                                             p_po_rtnmsg   OUT VARCHAR2 --返回描述
                                             )
/*
  存储过程名称:RWA_DEV.PRO_RWA_WSIB_GCR
  实现功能:利率互换的观察日补录铺底
  数据口径:全量
  跑批频率:月初运行
  版  本  :V1.0.0
  编写人  :CHENGANG
  编写时间:2020-04-26
  单  位  :上海安硕信息技术股份有限公司
  变更记录(修改人|修改时间|修改内容):
  */
 AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_WSIB_GCR';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

BEGIN
  --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

  /*如果是全量数据加载需清空目标表*/
  --1.清除目标表中的原有记录
  --EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA.RWA_WSIB_GCR';
  DELETE FROM RWA.RWA_WSIB_GCR
   WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD');

  COMMIT;

  --2.将满足条件的数据从源表插入到目标表中

  INSERT INTO RWA.RWA_WSIB_GCR
    (DATADATE --数据日期
    ,
     ORGID --机构ID
    ,
     DEALNO --流水号
    ,
     CFN1 --客户名称
    ,
     GCRQ --观察日起
    ,
     GCRZ --观察日止
     )
    select TO_DATE(p_data_dt_str, 'YYYYMMDD') AS DATADATE --数据日期
          ,
           '9998' AS ORGID --机构ID
          ,
           TT.DEALNO,--流水号
           TT.CFN1,--客户名称
           '',
           ''
      from (select DISTINCT T1.DEALNO, T3.CFN1
              FROM RWA_DEV.OPI_SWDT T1 --互换交易
             inner JOIN RWA_DEV.OPI_SWDH T2 --互换报头 
                ON T1.DEALNO = T2.DEALNO
               AND T2.DATANO = p_data_dt_str
               AND T2.PORT <> 'SWDK' --排除结构性存款业务
              LEFT JOIN RWA_DEV.OPI_CUST T3 --客户信息
                ON trim(T2.CNO) = trim(T3.CNO)
               AND T3.DATANO = p_data_dt_str
             WHERE T1.DATANO = p_data_dt_str
               AND SUBSTR(T2.COST, 1, 1) = '3' --第一位=3  --数据为利率/货币掉期业务
                  --AND SUBSTR(T2.COST, 4, 1) <> '3' --取交易账户下，台账是包含了这两种业务的，所以这个条件去除掉
               AND SUBSTR(T2.COST, 6, 1) IN ('1', '2', '3') --第六位=1  --利率掉期
               AND T2.VERIND = 1
               AND TRIM(T2.REVDATE) IS NULL
            -- ORDER BY T1.DEALNO
            )   TT;

  commit;

  dbms_stats.gather_table_stats(ownname => 'RWA',
                                tabname => 'RWA_WSIB_GCR',
                                cascade => true);

  /*目标表数据统计*/
  --统计插入的记录数
  SELECT COUNT(1)
    INTO v_count
    FROM RWA.RWA_WSIB_GCR
   WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD');
  --Dbms_output.Put_line('RWA.RWA_WSIB_GCR表当前插入的核心系统-直销银行垫款铺底数据记录为: ' || v_count || ' 条');

  --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

  p_po_rtncode := '1';
  p_po_rtnmsg  := '成功' || '-' || v_count;
  --定义异常
EXCEPTION
  WHEN OTHERS THEN
    --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
    ROLLBACK;
    p_po_rtncode := sqlcode;
    p_po_rtnmsg  := '观察日信息数据铺底(' || v_pro_name || ')处理失败！' || sqlerrm ||
                    ';错误行数为:' || dbms_utility.format_error_backtrace;
    RETURN;
END PRO_RWA_GCR_WSIB;
/

