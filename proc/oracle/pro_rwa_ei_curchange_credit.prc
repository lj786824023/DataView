CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_CURCHANGE_CREDIT(
                             p_data_dt_str  IN  VARCHAR2,    --数据日期 yyyyMMdd
                             p_po_rtncode  OUT  VARCHAR2,    --返回编号 1 成功,0 失败
                            p_po_rtnmsg    OUT  VARCHAR2    --返回描述
        )
  /*
    存储过程名称:RWA_DEV.PRO_RWA_EI_CURCHANGE_CREDIT
    实现功能:信用风险相关表金额汇率转换
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :QHJIANG
    编写时间:2016-09-28
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.TMP_CURRENCY_CHANGE|汇率表
    目标表1 :RWA_DEV.RWA_EI_ABSEXPOSURE|风险暴露汇总表
    目标表2 :RWA_DEV.RWA_EI_COLLATERAL|汇总抵质押品表
    目标表3 :RWA_DEV.RWA_EI_CONTRACT|合同汇总表
    目标表4 :RWA_DEV.RWA_EI_EXPOSURE|风险暴露汇总表
    目标表5 :RWA_DEV.RWA_EI_GUARANTEE|汇总保证表
    目标表6 :RWA_DEV.RWA_EI_SFTDETAIL|汇总买断回购表
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'PRO_RWA_EI_CURCHANGE_CREDIT';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER := 1;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    --风险暴露汇总表
    UPDATE RWA_DEV.RWA_EI_ABSEXPOSURE TA
             --资产余额
    SET      TA.ASSETBALANCE = TA.ASSETBALANCE*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                    FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                    WHERE  NPQ.DATANO = TA.DATANO
                                                    AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --减值准备
           /* ,TA.PROVISIONS = TA.PROVISIONS*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                WHERE  NPQ.DATANO = TA.DATANO
                                                AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)*/
    WHERE   TA.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;
    --汇总抵质押品表
    UPDATE RWA_DEV.RWA_EI_COLLATERAL TA
             --抵押总额
    SET      TA.COLLATERALAMOUNT = TA.COLLATERALAMOUNT*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                            FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                            WHERE  NPQ.DATANO = TA.DATANO
                                                            AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
    WHERE   TA.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;
    --合同汇总表
    UPDATE RWA_DEV.RWA_EI_CONTRACT TA
             --合同总金额
    SET      TA.CONTRACTAMOUNT = TA.CONTRACTAMOUNT*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                        FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                        WHERE  NPQ.DATANO = TA.DATANO
                                                        AND    NPQ.CURRENCYCODE = TA.SETTLEMENTCURRENCY),1)
            --合同未提取部分
            ,TA.NOTEXTRACTPART = TA.NOTEXTRACTPART*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                        FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                        WHERE  NPQ.DATANO = TA.DATANO
                                                        AND    NPQ.CURRENCYCODE = TA.SETTLEMENTCURRENCY),1)
    WHERE   TA.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;
    --风险暴露汇总表
    UPDATE RWA_DEV.RWA_EI_EXPOSURE TA
             --正常本金余额
    SET      TA.NORMALPRINCIPAL = TA.NORMALPRINCIPAL*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                          FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                          WHERE  NPQ.DATANO = TA.DATANO
                                                          AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --逾期余额
            ,TA.OVERDUEBALANCE = TA.OVERDUEBALANCE*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                        FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                        WHERE  NPQ.DATANO = TA.DATANO
                                                        AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --非应计余额
            ,TA.NONACCRUALBALANCE = TA.NONACCRUALBALANCE*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                              FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                              WHERE  NPQ.DATANO = TA.DATANO
                                                              AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --表内余额
            ,TA.ONSHEETBALANCE = TA.ONSHEETBALANCE*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                        FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                        WHERE  NPQ.DATANO = TA.DATANO
                                                        AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --正常利息
            ,TA.NORMALINTEREST = TA.NORMALINTEREST*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                        FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                        WHERE  NPQ.DATANO = TA.DATANO
                                                        AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --表内欠息
            ,TA.ONDEBITINTEREST = TA.ONDEBITINTEREST*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                          FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                          WHERE  NPQ.DATANO = TA.DATANO
                                                          AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --表外欠息
            ,TA.OFFDEBITINTEREST = TA.OFFDEBITINTEREST*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                            FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                            WHERE  NPQ.DATANO = TA.DATANO
                                                            AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --应收费用
            ,TA.EXPENSERECEIVABLE = TA.EXPENSERECEIVABLE*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                              FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                              WHERE  NPQ.DATANO = TA.DATANO
                                                              AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --资产余额
            ,TA.ASSETBALANCE = TA.ASSETBALANCE*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                    FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                    WHERE  NPQ.DATANO = TA.DATANO
                                                    AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --专项准备金
           /* ,TA.SPECIALPROVISION = TA.SPECIALPROVISION*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                            FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                            WHERE  NPQ.DATANO = TA.DATANO
                                                            AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --一般准备金
            ,TA.GENERALPROVISION = TA.GENERALPROVISION*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                            FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                            WHERE  NPQ.DATANO = TA.DATANO
                                                            AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --特别准备金
            ,TA.ESPECIALPROVISION = TA.ESPECIALPROVISION*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                              FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                              WHERE  NPQ.DATANO = TA.DATANO
                                                              AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)*/
            --已核销金额
            ,TA.WRITTENOFFAMOUNT = TA.WRITTENOFFAMOUNT*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                            FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                            WHERE  NPQ.DATANO = TA.DATANO
                                                            AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --尚未支付费用
            ,TA.NONPAYMENTFEES = TA.NONPAYMENTFEES*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                        FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                        WHERE  NPQ.DATANO = TA.DATANO
                                                        AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
    WHERE   TA.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;
    --汇总保证表
    UPDATE RWA_DEV.RWA_EI_GUARANTEE TA
             --保证总额
    SET      TA.GUARANTEEAMOUNT = TA.GUARANTEEAMOUNT*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                          FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                          WHERE  NPQ.DATANO = TA.DATANO
                                                          AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
    WHERE   TA.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;
    --汇总买断回购表
    UPDATE RWA_DEV.RWA_EI_SFTDETAIL TA
             --资产余额
    SET      TA.ASSETBALANCE = TA.ASSETBALANCE*NVL((SELECT NPQ.MIDDLEPRICE/100
                                            FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                            WHERE  NPQ.DATANO = TA.DATANO
                                            AND    NPQ.CURRENCYCODE = TA.ASSETCURRENCY),1)
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
          p_po_rtnmsg  := '信用风险相关表金额汇率转换('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_EI_CURCHANGE_CREDIT;
/

