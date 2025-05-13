CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_CURCHANGE_MARKET(
                             p_data_dt_str  IN  VARCHAR2,    --数据日期 yyyyMMdd
                             p_po_rtncode  OUT  VARCHAR2,    --返回编号 1 成功,0 失败
                            p_po_rtnmsg    OUT  VARCHAR2    --返回描述
        )
  /*
    存储过程名称:RWA_DEV.PRO_RWA_EI_CURCHANGE_MARKET
    实现功能:市场风险相关表金额汇率转换
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :QHJIANG
    编写时间:2016-09-28
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.TMP_CURRENCY_CHANGE|汇率表
    目标表1 :RWA_DEV.RWA_EI_BONDINFO|汇总债券信息表
    目标表2 :RWA_DEV.RWA_EI_FESPOTPOSITION|汇总外汇现货头寸表
    目标表3 :RWA_DEV.RWA_EI_MARKETEXPOSURESTD|汇总标准法暴露表
    目标表4 :RWA_DEV.RWA_EI_TRADBONDPOSITION|交易债券头寸汇总表
    目标表5 :RWA_DEV.RWA_EI_FEFORWARDSSWAP|外汇远期掉期汇总表
    变更记录(修改人|修改时间|修改内容):
    XLP  20191206  调整RWA_EI_FESPOTPOSITION外汇现货头寸表 其中的损益科目 6开头数据已是本币综合折人民币数据  无需进行折算人民币
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_CURCHANGE_MARKET';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER := 1;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    --汇总债券信息表
    UPDATE RWA_DEV.RWA_EI_BONDINFO TA
             --面额
    SET      TA.DENOMINATION = TA.DENOMINATION*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                    FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                    WHERE  NPQ.DATANO = TO_CHAR(TA.DATADATE,'yyyyMMdd')
                                                    AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
    WHERE   TA.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;
    
    --汇总外汇现货头寸表
    UPDATE RWA_DEV.RWA_EI_FESPOTPOSITION TA
             --头寸
    SET      TA.POSITION = TA.POSITION*NVL((SELECT NPQ.MIDDLEPRICE/100
                                            FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                            WHERE  NPQ.DATANO = TO_CHAR(TA.DATADATE,'yyyyMMdd')
                                            AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
    WHERE   TA.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND   TA.ACCSUBJECTS NOT LIKE '6%' --无需折算损益科目
    ;
    COMMIT;
    
    --汇总标准法暴露表
    UPDATE RWA_DEV.RWA_EI_MARKETEXPOSURESTD TA
             --头寸
    SET      TA.POSITION = TA.POSITION*NVL((SELECT NPQ.MIDDLEPRICE/100
                                            FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                            WHERE  NPQ.DATANO = TO_CHAR(TA.DATADATE,'yyyyMMdd')
                                            AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
    WHERE   TA.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;
    
    --交易债券头寸汇总表
    UPDATE RWA_DEV.RWA_EI_TRADBONDPOSITION TA
             --面额
    SET      TA.DENOMINATION = TA.DENOMINATION*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                    FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                    WHERE  NPQ.DATANO = TO_CHAR(TA.DATADATE,'yyyyMMdd')
                                                    AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --市值
            ,TA.MARKETVALUE = TA.MARKETVALUE*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                  FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                  WHERE  NPQ.DATANO = TO_CHAR(TA.DATADATE,'yyyyMMdd')
                                                  AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --折溢价
            ,TA.DISCOUNTPREMIUM = TA.DISCOUNTPREMIUM*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                          FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                          WHERE  NPQ.DATANO = TO_CHAR(TA.DATADATE,'yyyyMMdd')
                                                          AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --公允价值变动
            ,TA.FAIRVALUECHANGE = TA.FAIRVALUECHANGE*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                          FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                          WHERE  NPQ.DATANO = TO_CHAR(TA.DATADATE,'yyyyMMdd')
                                                          AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --账面余额
            ,TA.BOOKBALANCE = TA.BOOKBALANCE*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                  FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                  WHERE  NPQ.DATANO = TO_CHAR(TA.DATADATE,'yyyyMMdd')
                                                  AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
    WHERE   TA.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;
    
    --远期掉期汇总表
    UPDATE RWA_DEV.RWA_EI_FEFORWARDSSWAP TA
             --买入金额
    SET      TA.BUYAMOUNT = TA.BUYAMOUNT*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                    FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                    WHERE  NPQ.DATANO = TO_CHAR(TA.DATADATE,'yyyyMMdd')
                                                    AND    NPQ.CURRENCYCODE = TA.BUYCURRENCY),1)
            --卖出金额
            ,TA.SELLAMOUNT = TA.SELLAMOUNT*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                  FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                  WHERE  NPQ.DATANO = TO_CHAR(TA.DATADATE,'yyyyMMdd')
                                                  AND    NPQ.CURRENCYCODE = TA.SELLCURRENCY),1)

    WHERE   TA.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功'||'-'||v_count;
    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '市场风险相关表金额汇率转换('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_EI_CURCHANGE_MARKET;
/

