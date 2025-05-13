CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_OPERATIONAL(p_data_dt_str  IN  VARCHAR2, --数据日期
                                                   p_po_rtncode   OUT VARCHAR2, --返回编号
                                                   p_po_rtnmsg    OUT VARCHAR2  --返回描述
)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_EI_OPERATIONAL
    实现功能:将总账余额表(加工表)信息加工导入操作风险暴露表)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :QHJIANG
    编写时间:2016-06-28
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.FNS_GL_BALANCE|总账余额表(加工表)
    源  表2 :RWA.CODE_LIBRARY|代码表
    源  表3 :RWA_DEV.TMP_CURRENCY_CHANGE|汇率表
    源  表4 :RWA.RWA_CD_OPERATIONAL_STAND_MODEL|操作风险科目表-标准法-计算模板
    源  表5 :RWA.ORG_INFO|机构表
    目标表1 :RWA_DEV.RWA_EI_OPERATIONALEXPOSURE|操作风险暴露表
    目标表2 :RWA_DEV.RWA_EI_OPERATIONALACCOUNT|操作风险科目表
    辅助表  :无
    变更记录(修改人|修改时间|修改内容):

    pxl   2019/12/05  使用财务总账RMB折人数据计量操作风险   因为<>RMB数据6开头损益科目的本币存在问题（财务系统问题）

    */

  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_OPERATIONAL';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前遍历的记录数
  v_count INTEGER :=1;
  --定义当前插入的记录数
  v_count1 INTEGER;
  --当前计算日期
  v_data_dt_str VARCHAR2(20);
  --加工几年的数据
  v_run_count INTEGER :=0;
  --对应年份是否需要加工
  v_run_flag INTEGER :=0;
  --总利息收入
  v_INTERESTINCOME NUMBER(24,6) := 0;
  --总利息支出
  v_INTERESTEXPENSE NUMBER(24,6) := 0;

  BEGIN
    
    
    --特殊处理  上线前放开
    --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --年末加工数据
    IF TO_CHAR(TO_DATE(p_data_dt_str,'YYYYMMDD'),'YYYY')||'1231'=p_data_dt_str  THEN
      v_run_count := 1;
      --前1年
      SELECT count(1) INTO v_run_flag FROM RWA_DEV.RWA_EI_OPERATIONALEXPOSURE WHERE DATADATE=ADD_MONTHS(TO_DATE(TO_CHAR(TO_DATE(p_data_dt_str,'YYYYMMDD'),'YYYY')||'1231','YYYYMMDD'),-12*1);
      IF v_run_flag=0 THEN v_run_count := v_run_count+1; END IF;
      v_run_flag := 0;
      --前2年
      SELECT count(1) INTO v_run_flag FROM RWA_DEV.RWA_EI_OPERATIONALEXPOSURE WHERE DATADATE=ADD_MONTHS(TO_DATE(TO_CHAR(TO_DATE(p_data_dt_str,'YYYYMMDD'),'YYYY')||'1231','YYYYMMDD'),-12*2);
      IF v_run_flag=0 THEN v_run_count := v_run_count+1; END IF;
      v_run_flag := 0;
    END IF;

    WHILE v_count<=v_run_count LOOP
        v_data_dt_str := TO_CHAR(ADD_MONTHS(TO_DATE(p_data_dt_str,'YYYYMMDD'),-(v_count-1)*12),'YYYYMMDD');

        BEGIN
           --删除不存在的分区，如果此存储过程是第一次跑抛出异常，可以忽略
           EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_OPERATIONALACCOUNT DROP PARTITION OPERATIONAL' || v_data_dt_str;

           COMMIT;
           EXCEPTION
            WHEN OTHERS THEN
               IF (SQLCODE <> '-2149') THEN
                  --首次分区truncate会出现2149异常
                  p_po_rtncode := sqlcode;
                  p_po_rtnmsg  := '操作风险科目表('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
               RETURN;
            END IF;
        END;

        --新增一个当前日期下的分区
        EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_OPERATIONALACCOUNT ADD PARTITION OPERATIONAL' || v_data_dt_str || ' VALUES(TO_DATE('|| v_data_dt_str || ',''YYYYMMDD''))';

        --DBMS_OUTPUT.PUT_LINE('开始：导入【操作风险科目表】' || v_data_dt_str);
        --操作风险科目表
        --DBMS_OUTPUT.PUT_LINE('开始：导入【操作风险科目表】' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
         INSERT INTO RWA_DEV.RWA_EI_OPERATIONALACCOUNT(
             DATADATE                --数据日期
            ,DATANO                  --数据流水号
            ,ORGSORTNO               --所属机构排序号
            ,ORGID                   --所属机构ID
            ,ORGNAME                 --所属机构名称
            ,ACCOUNTCODE             --科目代码
            ,ACCOUNTNAME             --科目名称
            ,ACCOUNTBALANCE          --科目余额
            ,CURRENCY                --币种
        )
        SELECT
                TEMP.DATADATE                       AS DATADATE              --数据日期
               ,TEMP.DATANO                         AS DATANO                --数据流水号
               ,OI.SORTNO                           AS ORGSORTNO             --所属机构排序号
               ,TEMP.ORGID                          AS ORGID                 --所属机构ID
               ,OI.ORGNAME                          AS ORGNAME               --所属机构名称
               ,ACCOUNTCODE                         AS ACCOUNTCODE           --科目代码
               ,ACCOUNTNAME                         AS ACCOUNTNAME           --科目名称
               ,ACCOUNTBALANCE                      AS ACCOUNTBALANCE        --科目余额
               ,CURRENCY                            AS CURRENCY              --币种
        FROM
            (SELECT
                     TO_DATE(FGB.DATANO,'YYYYMMDD')              AS DATADATE                --数据日期
                    ,FGB.DATANO                                  AS DATANO                  --数据流水号
                    ,FGB.ORG_ID                                  AS ORGID                   --所属机构ID
                    ,FGB.SUBJECT_NO                              AS ACCOUNTCODE             --科目代码
                    ,''                                          AS ACCOUNTNAME             --科目名称
                    ,SUM(FGB.BALANCE)                            AS ACCOUNTBALANCE          --科目余额
                    ,FGB.CURRENCY_CODE                           AS CURRENCY                --币种
             FROM   (SELECT  --NPQ.MIDDLEPRICE
                             100
                            ,T.DATANO
                            ,'9998' AS ORG_ID
                            --,T.CURRENCY_CODE
                            ,'CNY' AS CURRENCY_CODE
                            ,T.SUBJECT_NO
                            ,CL.ITEMNAME
                            ,CASE
                               --WHEN OSM.DCFX = 'D-C' THEN  (T.BALANCE_D - T.BALANCE_C)*NVL(NPQ.MIDDLEPRICE, 100)/100 
                               WHEN OSM.DCFX = 'D-C' THEN  (T.BALANCE_D - T.BALANCE_C) 
                               --WHEN OSM.DCFX = 'C-D' THEN  (T.BALANCE_C - T.BALANCE_D)*NVL(NPQ.MIDDLEPRICE, 100)/100 
                               WHEN OSM.DCFX = 'C-D' THEN  (T.BALANCE_C - T.BALANCE_D)
                               --ELSE (T.BALANCE_D - T.BALANCE_C)*NVL(NPQ.MIDDLEPRICE, 100)/100
                               ELSE (T.BALANCE_D - T.BALANCE_C)
                             END  AS BALANCE
                     FROM RWA_DEV.FNS_GL_BALANCE T
                     LEFT JOIN RWA.CODE_LIBRARY CL
                     ON     CL.CODENO='NewSubject'
                     AND    T.SUBJECT_NO=CL.ITEMNO
                     AND    CL.ISINUSE='1'
                     --LEFT JOIN RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                     --ON     NPQ.DATANO = T.DATANO
                     --AND    NPQ.CURRENCYCODE = T.CURRENCY_CODE
                     LEFT JOIN RWA.RWA_CD_OPERATIONAL_STAND_MODEL OSM
                       ON T.SUBJECT_NO = OSM.SUBJECT_NO
                     WHERE  T.DATANO = v_data_dt_str
                     --AND    T.CURRENCY_CODE <> 'RMB'
                     AND    T.CURRENCY_CODE = 'RMB'
               ) FGB
            INNER JOIN RWA.RWA_CD_OPERATIONAL_STAND_MODEL OSM
              ON  FGB.SUBJECT_NO = OSM.SUBJECT_NO                 
            GROUP BY FGB.DATANO, FGB.ORG_ID, FGB.SUBJECT_NO, FGB.CURRENCY_CODE
            
       ) TEMP
        LEFT JOIN RWA.ORG_INFO OI
        ON     TEMP.ORGID=OI.ORGID
        AND    OI.STATUS='1'
        ;

        COMMIT;

        --清除目标表中的原有记录
        EXECUTE IMMEDIATE 'DELETE FROM RWA_DEV.RWA_EI_OPERATIONALEXPOSURE WHERE DATADATE = TO_DATE('||v_data_dt_str||',''YYYYMMDD'')';

        COMMIT;

        --操作风险暴露表
        --DBMS_OUTPUT.PUT_LINE('开始：导入【操作风险暴露表】' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
        --导入【操作风险暴露表】的数据
        INSERT INTO RWA_DEV.RWA_EI_OPERATIONALEXPOSURE(
             DATADATE                --数据日期
            ,DATANO                  --数据流水号
            ,BUSINESSLINE            --业务条线
            ,ORGSORTNO               --所属机构排序号
            ,ORGID                   --所属机构ID
            ,ORGNAME                 --所属机构名称
            ,INTERESTINCOME          --利息收入
            ,INTERESTEXPENSE         --利息支出
            ,NETFEECOMMINCOME        --手续费和佣金净收入
            ,NETTRADPROFITLOSS       --净交易损益
            ,NETINVESECUPROFITLOSS   --证券投资净损益
            ,OTHEROPERATINGINCOME    --其他营业收入
        )
        SELECT
                TEMP.DATADATE                       AS DATADATE              --数据日期
               ,TEMP.DATANO                         AS DATANO                --数据流水号
               ,TEMP.BUSINESSLINE                   AS BUSINESSLINE          --业务条线
               ,OI.SORTNO                           AS ORGSORTNO             --所属机构排序号
               ,TEMP.ORGID                          AS ORGID                 --所属机构ID
               ,OI.ORGNAME                          AS ORGNAME               --所属机构名称
               ,TEMP.BALANCE010                     AS INTERESTINCOME        --利息收入
               ,TEMP.BALANCE020                AS INTERESTEXPENSE       --利息支出
               ,TEMP.BALANCE050-TEMP.BALANCE060     AS NETFEECOMMINCOME      --手续费和佣金净收入=050手续费及佣金收入-060 手续费及佣金支出
               ,TEMP.BALANCE031+TEMP.BALANCE032     AS NETTRADPROFITLOSS     --净交易损益=031 交易收益-032 交易损失
               ,TEMP.BALANCE070                     AS NETINVESECUPROFITLOSS --证券投资净损益
               ,TEMP.BALANCE040                     AS OTHEROPERATINGINCOME  --其他营业收入
        FROM
            (SELECT
                     TO_DATE(FGB.DATANO,'YYYYMMDD')                                                         AS DATADATE     --数据日期
                    ,FGB.DATANO                                                                             AS DATANO       --数据流水号
                    ,OSM.BUSINESSLINE                                                                       AS BUSINESSLINE --业务条线
                    ,FGB.ORG_ID                                                                             AS ORGID        --所属机构ID
                    ,SUM(DECODE(OSM.SUMTYPE,'010',FGB.BALANCE,0)*NVL(FGB.MIDRATE, 100)/100)                   AS BALANCE010   --010 利息收入
                    ,SUM(DECODE(OSM.SUMTYPE,'020',FGB.BALANCE,0)*NVL(FGB.MIDRATE, 100)/100)                   AS BALANCE020   --020 利息支出
                    ,SUM(DECODE(OSM.SUMTYPE,'031',FGB.BALANCE,0)*NVL(FGB.MIDRATE, 100)/100)                   AS BALANCE031   --031 交易收益
                    ,SUM(DECODE(OSM.SUMTYPE,'032',FGB.BALANCE,0)*NVL(FGB.MIDRATE, 100)/100)                   AS BALANCE032   --032 交易损失
                    ,SUM(DECODE(OSM.SUMTYPE,'040',FGB.BALANCE,0)*NVL(FGB.MIDRATE, 100)/100)                   AS BALANCE040   --040 其他营业收入
                    ,SUM(DECODE(OSM.SUMTYPE,'050',FGB.BALANCE,0)*NVL(FGB.MIDRATE, 100)/100)                   AS BALANCE050   --050 手续费及佣金收入
                    ,SUM(DECODE(OSM.SUMTYPE,'060',FGB.BALANCE,0)*NVL(FGB.MIDRATE, 100)/100)                   AS BALANCE060   --060 手续费及佣金支出
                    ,SUM(DECODE(OSM.SUMTYPE,'070',FGB.BALANCE,0)*NVL(FGB.MIDRATE, 100)/100)                   AS BALANCE070   --070 证券投资净损益
               FROM   (
                      SELECT  --NPQ.MIDDLEPRICE
                             100 MIDRATE
                            ,T.DATANO
                            ,'9998' AS ORG_ID
                            --,T.CURRENCY_CODE
                            ,'CNY' AS CURRENCY_CODE
                            ,T.SUBJECT_NO
                            ,CL.ITEMNAME
                            ,CASE
                               --WHEN OSM.DCFX = 'D-C' THEN  (T.BALANCE_D - T.BALANCE_C)*NVL(NPQ.MIDDLEPRICE, 100)/100 
                               WHEN OSM.DCFX = 'D-C' THEN  (T.BALANCE_D - T.BALANCE_C) 
                               --WHEN OSM.DCFX = 'C-D' THEN  (T.BALANCE_C - T.BALANCE_D)*NVL(NPQ.MIDDLEPRICE, 100)/100 
                               WHEN OSM.DCFX = 'C-D' THEN  (T.BALANCE_C - T.BALANCE_D)
                               --ELSE (T.BALANCE_D - T.BALANCE_C)*NVL(NPQ.MIDDLEPRICE, 100)/100
                               ELSE (T.BALANCE_D - T.BALANCE_C)
                             END  AS BALANCE
                     FROM RWA_DEV.FNS_GL_BALANCE T
                     LEFT JOIN RWA.CODE_LIBRARY CL
                     ON     CL.CODENO='NewSubject'
                     AND    T.SUBJECT_NO=CL.ITEMNO
                     AND    CL.ISINUSE='1'
                     --LEFT JOIN RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                     --ON     NPQ.DATANO = T.DATANO
                     --AND    NPQ.CURRENCYCODE = T.CURRENCY_CODE
                     LEFT JOIN RWA.RWA_CD_OPERATIONAL_STAND_MODEL OSM
                       ON T.SUBJECT_NO = OSM.SUBJECT_NO
                     WHERE  T.DATANO = v_data_dt_str
                     --AND    T.CURRENCY_CODE <> 'RMB'
                     AND    T.CURRENCY_CODE = 'RMB'
                    ) FGB
                    INNER JOIN RWA.RWA_CD_OPERATIONAL_STAND_MODEL OSM
                          ON  FGB.SUBJECT_NO = OSM.SUBJECT_NO     
             GROUP BY FGB.DATANO,OSM.BUSINESSLINE,FGB.ORG_ID) TEMP
        LEFT JOIN RWA.ORG_INFO OI
        ON     TEMP.ORGID=OI.ORGID
        AND    OI.STATUS='1'
        ;

				COMMIT;



        /***************     商业银行计算业务条线净利息收入时, 应按各业务条线的资金占用比例分摊利息成本。     ***********/

        --1. 利息收入汇总          
        SELECT SUM(T1.INTERESTINCOME) INTO v_INTERESTINCOME --利息收入科目汇总
          FROM RWA_DEV.RWA_EI_OPERATIONALEXPOSURE T1
         WHERE T1.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
        ;
        
        --2. 利息支出汇总  
        SELECT SUM(T1.INTERESTEXPENSE) INTO v_INTERESTEXPENSE  --利息支出科目汇总
        FROM   RWA_DEV.RWA_EI_OPERATIONALEXPOSURE T1
        WHERE  T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
        ;
        
                
        --3.各个条线的值=X * 各个条线的占比        
        MERGE INTO RWA_DEV.RWA_EI_OPERATIONALEXPOSURE T1 
        USING (
                    
            SELECT T2.BUSINESSLINE,
                   v_INTERESTEXPENSE * (SUM(T2.INTERESTINCOME) / v_INTERESTINCOME)  INTERESTEXPENSE-- X 非净利息值
              FROM RWA_DEV.RWA_EI_OPERATIONALEXPOSURE T2
             WHERE T2.DATANO = p_data_dt_str
             GROUP BY T2.BUSINESSLINE
        ) T2 ON (T2.BUSINESSLINE = T1.BUSINESSLINE AND T1.DATANO = p_data_dt_str)
        WHEN MATCHED THEN 
           UPDATE SET T1.INTERESTEXPENSE = T2.INTERESTEXPENSE
        ;


        /*  弃用一期逻辑
        SELECT SUM(TEMP2.INTERESTINCOME) INTO v_INTERESTINCOME
        FROM   RWA_DEV.RWA_EI_OPERATIONALEXPOSURE TEMP2
        WHERE  TEMP2.DATADATE = TO_DATE(v_data_dt_str,'YYYYMMDD')
        ;

        UPDATE RWA_DEV.RWA_EI_OPERATIONALEXPOSURE TEMP1
        SET    INTERESTEXPENSE=(CASE WHEN NVL(v_INTERESTINCOME,0)=0
                                     THEN 0
                                     ELSE (NVL(TEMP1.INTERESTEXPENSE,0)*NVL(TEMP1.INTERESTINCOME,0)/NVL(v_INTERESTINCOME,0)) END)
        WHERE  TEMP1.DATANO = v_data_dt_str
        ;

				COMMIT;
        */

        v_INTERESTINCOME :=0;

        --净利息收入[NETINTERESTINCOME]=利息收入[INTERESTINCOME]-利息支出[INTERESTEXPENSE]
        --净非利息收入[NETNONINTERESTINCOME]=手续费和佣金净收入[NETFEECOMMINCOME]+净交易损益[NETTRADPROFITLOSS]+证券投资净损益[NETINVESECUPROFITLOSS]+其他营业收入[OTHEROPERATINGINCOME]
        --总收入[GROSSINCOME]=净利息收入[NETINTERESTINCOME]+净非利息收入[NETNONINTERESTINCOME]
        UPDATE RWA_DEV.RWA_EI_OPERATIONALEXPOSURE OES
        SET     OES.NETINTERESTINCOME=(NVL(OES.INTERESTINCOME,0)-NVL(OES.INTERESTEXPENSE,0))
               ,OES.NETNONINTERESTINCOME=(NVL(OES.NETFEECOMMINCOME,0)+NVL(OES.NETTRADPROFITLOSS,0)+NVL(OES.NETINVESECUPROFITLOSS,0)+NVL(OES.OTHEROPERATINGINCOME,0))
               ,OES.GROSSINCOME=(NVL(OES.INTERESTINCOME,0)-NVL(OES.INTERESTEXPENSE,0))+(NVL(OES.NETFEECOMMINCOME,0)+NVL(OES.NETTRADPROFITLOSS,0)+NVL(OES.NETINVESECUPROFITLOSS,0)+NVL(OES.OTHEROPERATINGINCOME,0))
        WHERE  OES.DATANO = v_data_dt_str;

        COMMIT;

        v_count := v_count+1;
    END LOOP;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_OPERATIONALACCOUNT',cascade => true);
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_OPERATIONALEXPOSURE',cascade => true);

    --DBMS_OUTPUT.PUT_LINE('结束：导入【操作风险暴露表】' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_EI_OPERATIONALEXPOSURE;
    --DBMS_OUTPUT.PUT_LINE('RWA_DEV.RWA_EI_OPERATIONALEXPOSURE-操作风险暴露表，中插入数量为：' || v_count1 || '条');

    --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功';

    --定义异常
    EXCEPTION WHEN OTHERS THEN
        --DBMS_OUTPUT.PUT_LINE('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '导入【操作风险暴露表】('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;


    RETURN;

END PRO_RWA_EI_OPERATIONAL;
/

