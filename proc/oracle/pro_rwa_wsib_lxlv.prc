CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_WSIB_LXLV(
                            p_data_dt_str IN  VARCHAR2,   --数据日期 yyyyMMdd
                            p_po_rtncode  OUT VARCHAR2,   --返回编号 1 成功,0 失败
                            p_po_rtnmsg   OUT VARCHAR2    --返回描述
        )
  /*
    存储过程名称:RWA_DEV.PRO_RWA_WSIB_LXLV
    实现功能:
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :
    编写时间:2020-01-06
    单  位  :上海安硕信息技术股份有限公司
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_WSIB_LXLV';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    --EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA.RWA_WSIB_LXLV';
    DELETE FROM RWA.RWA_WSIB_LXLV WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    COMMIT;

    --2.将满足条件的数据从源表插入到目标表中

    INSERT INTO RWA.RWA_WSIB_LXLV
    (
    DATADATE                               --数据日期
   ,ORGID                                  --机构ID
   ,CCY                                    --币种
   ,BRPROCDATE                             --起始日期
   ,MATDATE                                --结束日期
   ,MTY                                    --期限
   ,LXLV                                   --零息利率
    )
  select
       TO_DATE(p_data_dt_str, 'YYYYMMDD')AS DATADATE --数据日期
       ,'9998'                           AS ORGID   --机构ID
      ,T.CCY                             AS CCY --币种
      , to_char(T.BRPROCDATE,'yyyy/mm/dd')  AS BRPROCDATE  --起始日期
      , to_char(T.MATDATE,'yyyy/mm/dd')      AS MATDATE --结束日期
      ,T.MTY                             AS  MTY --期限
      ,K2.LXLV                           AS LXLV   --零息利率
       FROM OPI_DDFT T
        INNER JOIN (SELECT BR,
                           CCY,
                           YIELDCURVE,
                           SHIFTSEQ,
                           MAX(BRPROCDATE) BRPROCDATE,
                           MAX(MATDATE) MATDATE,
                           QUOTETYPE,
                           MTY
                      FROM OPI_DDFT
                     WHERE OPI_DDFT.BR = '01'
                       AND OPI_DDFT.QUOTETYPE = 'M'
                       AND OPI_DDFT.YIELDCURVE = 'DISCOUNT'
                       AND OPI_DDFT.Datano=p_data_dt_str
                     GROUP BY BR, CCY, YIELDCURVE, SHIFTSEQ, QUOTETYPE, MTY) OPI_DDFT2
           ON T.CCY = OPI_DDFT2.CCY
          AND T.BRPROCDATE = OPI_DDFT2.BRPROCDATE
          AND T.MATDATE = OPI_DDFT2.MATDATE
      LEFT JOIN(SELECT   K1.CCY,
              K1.BRPROCDATE,
              K1.MATDATE,
              K1.MTY,
              K1.LXLV
           FROM RWA.RWA_WS_LXLV_BL K1
          WHERE DATADATE =
          (SELECT MAX(DATADATE)
          FROM RWA.RWA_WS_LXLV_BL
          WHERE DATADATE < TO_DATE(p_data_dt_str, 'YYYYMMDD'))) K2---取上一期的补录评级信息铺地
           ON     T.CCY=K2.CCY
           AND   T.BRPROCDATE=K2.BRPROCDATE
           AND   T.MATDATE=K2.MATDATE
           AND   T.MTY=K2.MTY
          where T.Datano=p_data_dt_str
          order by T.CCY,T.MTY   ;
     
           commit;

    dbms_stats.gather_table_stats(ownname => 'RWA',tabname => 'RWA_WSIB_LXLV',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA.RWA_WSIB_LXLV WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA.RWA_WSIB_LXLV表当前插入的核心系统-直销银行垫款铺底数据记录为: ' || v_count || ' 条');



    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功' || '-' || v_count;
    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
         ROLLBACK;
         p_po_rtncode := sqlcode;
         p_po_rtnmsg  := '客户评级信息补录数据铺底('|| v_pro_name ||')处理失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_WSIB_LXLV;
/

