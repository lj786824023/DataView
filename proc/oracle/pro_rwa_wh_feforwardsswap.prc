CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_WH_FEFORWARDSSWAP(
                                                               p_data_dt_str IN  VARCHAR2,--数据日期 yyyyMMdd
                                                               p_po_rtncode  OUT VARCHAR2,--返回编号 1 成功,0 失败
                                                               p_po_rtnmsg   OUT VARCHAR2 --返回描述
)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_WH_FEFORWARDSSWAP
    实现功能:国结系统-市场风险-外汇远期掉期(从数据财务系统将业务相关信息全量导入RWA市场风险外汇接口表外汇远期掉期表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-08-02
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.NSS_FI_FOREXCHANGEINFO|国结系统外汇买卖交易表
    源  表2 :RWA.RWA_WP_DATASUPPLEMENT|补录任务明细信息表
    源  表3 :RWA.RWA_WS_FOREIGN_EXCHANGE|外汇买卖补录表
    目标表1 :RWA_DEV.RWA_WH_FEFORWARDSSWAP|外汇远期掉期表（掉期）
    变更记录(修改人|修改时间|修改内容):
    xlp  20190909  结构性敞口  
    xlp  20191121  新增衍生品业务  外汇远期、掉期 市场风险计量规则 
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_WH_FEFORWARDSSWAP';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_WH_FEFORWARDSSWAP';

    --2.将满足条件的数据从源表插入到目标表中
    --2.1 财务总账  获取结构性存款
    INSERT INTO RWA_DEV.RWA_WH_FEFORWARDSSWAP(
                 DATADATE                              --数据日期
                ,TRANID                                --交易ID
                ,TRANORGID                             --交易机构ID
                ,ACCORGID                              --账务机构ID
                ,INSTRUMENTSTYPE                       --金融工具类型
                ,ACCSUBJECTS                           --会计科目
                ,BOOKTYPE                              --账户类别
                ,STRUCTURALEXPOFLAG                    --是否结构性敞口
                ,BUYCURRENCY                           --买入币种
                ,BUYAMOUNT                             --买入金额
                ,SELLCURRENCY                          --卖出币种
                ,SELLAMOUNT                            --卖出金额
                ,STARTDATE                             --起始日期
                ,DUEDATE                               --到期日期
                ,ORIGINALMATURITY                      --原始期限
                ,RESIDUALM                             --剩余期限
                ,BUYZERORATE                           --买入币种零息利率
                ,BUYDISCOUNTRATE                       --买入折现因子
                ,SELLZERORATE                          --卖出币种零息利率
                ,SELLDISCOUNTRATE                      --卖出折现因子

    )
    SELECT
                 DATADATE                              --数据日期
                ,p_data_dt_str||lpad(rownum, 10, '0')
                                            AS TRANID  --交易ID
                ,TRANORGID                             --交易机构ID
                ,ACCORGID                              --账务机构ID
                ,INSTRUMENTSTYPE                       --金融工具类型
                ,ACCSUBJECTS                           --会计科目
                ,BOOKTYPE                              --账户类别
                ,STRUCTURALEXPOFLAG                    --是否结构性敞口
                ,BUYCURRENCY                           --买入币种
                ,BUYAMOUNT                             --买入金额
                ,SELLCURRENCY                          --卖出币种
                ,SELLAMOUNT                            --卖出金额
                ,STARTDATE                             --起始日期
                ,DUEDATE                               --到期日期
                ,ORIGINALMATURITY                      --原始期限
                ,RESIDUALM                             --剩余期限
                ,BUYZERORATE                           --买入币种零息利率
                ,BUYDISCOUNTRATE                       --买入折现因子
                ,SELLZERORATE                          --卖出币种零息利率
                ,SELLDISCOUNTRATE                      --卖出折现因子
    FROM (
        SELECT
                    TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                    ,'9998'                                  AS TRANORGID                --交易机构ID       默认为“10000000-总行”
                    ,'9998'                                  AS ACCORGID                 --账务机构ID       默认为“10000000-总行”
                    ,'0506'                                      AS INSTRUMENTSTYPE          --金融工具类型     默认为“0506-外汇即期”
                    ,T1.ACCSUBJECTS                              AS ACCSUBJECTS              --会计科目
                    ,T1.BOOKTYPE                                 AS BOOKTYPE                 --账户类别
                    ,T1.STRUCTURALEXPOFLAG                       AS STRUCTURALEXPOFLAG       --是否结构性敞口
                    ,T1.BUYCURRENCY                              AS BUYCURRENCY              --买入币种
                    ,T1.BUYAMOUNT                                AS BUYAMOUNT                --买入金额
                    ,T1.SELLCURRENCY                             AS SELLCURRENCY             --卖出币种
                    ,T1.SELLAMOUNT                               AS SELLAMOUNT               --卖出金额
                    ,TO_CHAR(TO_DATE(p_data_dt_str,'yyyyMMdd'),'yyyy-MM-dd')
                                                                 AS STARTDATE                --起始日期
                    ,TO_CHAR(TO_DATE(p_data_dt_str,'yyyyMMdd'),'yyyy-MM-dd')
                                                                 AS DUEDATE                  --到期日期
                    ,0                                           AS ORIGINALMATURITY         --原始期限
                    ,0                                           AS RESIDUALM                --剩余期限
                    ,NULL                                        AS BUYZERORATE              --买入币种零息利率
                    ,NULL                                        AS BUYDISCOUNTRATE          --买入折现因子
                    ,NULL                                        AS SELLZERORATE             --卖出币种零息利率
                    ,NULL                                        AS SELLDISCOUNTRATE         --卖出折现因子
        FROM (
              SELECT
                     T.SUBJECT_NO                                AS ACCSUBJECTS              --会计科目
                    ,CASE WHEN T.SUBJECT_NO LIKE '1101%' THEN '02'
                          ELSE '01' END                          AS BOOKTYPE                 --账户类别         通过会计科目映射，若科目号为1101-交易性金融资产为交易账户(02)，其他默认为银行账户(01)
                    ,CASE WHEN T.SUBJECT_NO = '10030102' OR T.SUBJECT_NO LIKE '4001%' THEN '1'
                     ELSE '0' END                                AS STRUCTURALEXPOFLAG       --是否结构性敞口   通过会计科目映射，若科目号为4001-股本、科目10030102-存放中央银行款项-存放中央银行法定准备金为结构性敞口(1)，其他默认为否(0)
                    ,T.CURRENCY_CODE                             AS BUYCURRENCY              --买入币种
                    ,CASE WHEN CL.ATTRIBUTE8='D-C' AND T.BALANCE_D - T.BALANCE_C>0 THEN T.BALANCE_D - T.BALANCE_C
                          WHEN CL.ATTRIBUTE8='C-D' AND T.BALANCE_C - T.BALANCE_D>0 THEN T.BALANCE_C - T.BALANCE_D
                          ELSE 0 END                             AS BUYAMOUNT                --买入金额         >0
                    ,T.CURRENCY_CODE                             AS SELLCURRENCY             --卖出币种          <0
                    ,CASE WHEN CL.ATTRIBUTE8='D-C' AND T.BALANCE_D - T.BALANCE_C<0 THEN T.BALANCE_D - T.BALANCE_C
                          WHEN CL.ATTRIBUTE8='C-D' AND T.BALANCE_C - T.BALANCE_D<0 THEN T.BALANCE_C - T.BALANCE_D
                          ELSE 0 END                             AS SELLAMOUNT               --卖出金额
              FROM  RWA_DEV.FNS_GL_BALANCE T
              LEFT JOIN RWA.CODE_LIBRARY CL
              ON    CL.CODENO='NewSubject'
              AND   T.SUBJECT_NO=CL.ITEMNO
              AND   CL.ISINUSE='1'
              WHERE T.CURRENCY_CODE IS NOT NULL
              AND   T.CURRENCY_CODE <> 'RMB'
              AND   T.CURRENCY_CODE <> 'CNY'
              AND   T.DATANO = p_data_dt_str
              AND   (T.SUBJECT_NO = '10030102' OR T.SUBJECT_NO LIKE '4001%')
             ) T1
             WHERE T1.BUYAMOUNT <> 0 OR T1.SELLAMOUNT <> 0
   )
  ;

    COMMIT;    
    
    /*  根据 G4C-1(e)《市场风险标准法资本要求情况表（外汇风险）》填报说明 中规定 
    净头寸是每一币种的多头头寸和空头头寸轧差后的余额，包括即期净头寸、远期净头寸和期权合约得尔塔（Delta）净额、无法撤销的保证、
    以外币计值的损益之和。
    
    只需要计量即期净头寸、远期净头寸和期权合约得尔塔（Delta）净额
    
    --2.1 OPICS系统  获取外汇掉期 
    INSERT INTO RWA_DEV.RWA_WH_FEFORWARDSSWAP(
                 DATADATE                              --数据日期
                ,TRANID                                --交易ID
                ,TRANORGID                             --交易机构ID
                ,ACCORGID                              --账务机构ID
                ,INSTRUMENTSTYPE                       --金融工具类型
                ,ACCSUBJECTS                           --会计科目
                ,BOOKTYPE                              --账户类别
                ,STRUCTURALEXPOFLAG                    --是否结构性敞口
                ,BUYCURRENCY                           --买入币种
                ,BUYAMOUNT                             --买入金额
                ,SELLCURRENCY                          --卖出币种
                ,SELLAMOUNT                            --卖出金额
                ,STARTDATE                             --起始日期
                ,DUEDATE                               --到期日期
                ,ORIGINALMATURITY                               --原始期限
                ,RESIDUALM                             --剩余期限
                ,BUYZERORATE                           --买入币种零息利率
                ,BUYDISCOUNTRATE                       --买入折现因子
                ,SELLZERORATE                          --卖出币种零息利率
                ,SELLDISCOUNTRATE                      --卖出折现因子

    )
    WITH YSP_WHDQ AS (                
             SELECT         
                  PS || DEALNO AS DEALNO, --流水号       
                  COST AS COST, --成本中心        
                  PORT AS PORT, --产品        
                  CUST AS CUST, --客户        
                  PS AS PS, --方向        
                  DEALDATE AS DEALDATE, --交易日期        
                  VDATE AS VDATE, --交易到期日期       
                  SWAPDEAL AS SWAPDEAL, --掉期、非掉期标识        
                  TENOR AS TENOR, --即期远期标识                
                  CCY AS CCY, --买入/卖出币种       
                  ABS(CCYAMT) AS AMT, --买入/卖出币种金额        
                  CCYRATE_8 AS RATE, --买入/卖出汇率                        
                  CTRCCY AS CTRCCY, --卖出币种        
                  ABS(CTRAMT) AS CTRAMT, --卖出金额       
                  CTRBRATE_8 AS CTRBRATE_8, --汇率
                  ABS(CCYNPVAMT + CTRNPVAMT) AS CCYNPVAMT, --盯市价值   
                  CASE WHEN SUBSTR(COST, 1, 4) = '3' THEN '01' ELSE '02' END  BOOKTYPE --账户类型
             FROM RWA_DEV.OPI_FXDH W --外汇信息
            WHERE SUBSTR(W.COST, 1, 1) = '2' --外汇业务
              AND SUBSTR(W.COST, 6, 1) = '3' --掉期
              AND (W.CCY <> 'CNY' AND W.CTRCCY <> 'CNY') --不考虑结售汇业务 即人民币对外币或外币对人民币        
              AND W.VDATE >= p_data_dt_str  --未到期数据        
              AND W.DATANO = p_data_dt_str             
    )         
        SELECT
                    TO_DATE(p_data_dt_str,'YYYYMMDD')               AS DATADATE                 --数据日期
                    ,DEALNO                                         AS TRANID  --交易ID
                    ,'9998'                                      AS TRANORGID                --交易机构ID       默认为“10000000-总行”
                    ,'9998'                                      AS ACCORGID                 --账务机构ID       默认为“10000000-总行”
                    ,'0503'                                      AS INSTRUMENTSTYPE          --金融工具类型     默认为“0503	外汇掉期”
                    ,''                                          AS ACCSUBJECTS              --会计科目
                    ,T1.BOOKTYPE                                 AS BOOKTYPE                 --账户类别
                    ,'0'                                         AS STRUCTURALEXPOFLAG       --是否结构性敞口
                    ,T1.CCY                                      AS BUYCURRENCY              --买入币种
                    ,T1.AMT                                      AS BUYAMOUNT                --买入金额
                    ,T1.CTRCCY                                   AS SELLCURRENCY             --卖出币种
                    ,T1.CTRAMT                                   AS SELLAMOUNT               --卖出金额
                    ,TO_CHAR(TO_DATE(T1.DEALDATE,'yyyyMMdd'),'yyyy-MM-dd')
                                                                 AS STARTDATE                --起始日期
                    ,TO_CHAR(TO_DATE(T1.VDATE,'yyyyMMdd'),'yyyy-MM-dd')
                                                                 AS DUEDATE                  --到期日期
                    ,CASE WHEN (TO_DATE(T1.VDATE,'YYYY-MM-DD') - TO_DATE(T1.DEALDATE,'YYYY-MM-DD')) / 365<0
                                  THEN 0
                                  ELSE (TO_DATE(T1.VDATE,'YYYY-MM-DD') - TO_DATE(T1.DEALDATE,'YYYY-MM-DD')) / 365
                            END   --原始期限
                    ,CASE WHEN (TO_DATE(T1.VDATE,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                                  THEN 0
                                  ELSE (TO_DATE(T1.VDATE,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                            END   --剩余期限
                    ,NULL                                        AS BUYZERORATE              --买入币种零息利率
                    ,NULL                                        AS BUYDISCOUNTRATE          --买入折现因子
                    ,NULL                                        AS SELLZERORATE             --卖出币种零息利率
                    ,NULL                                        AS SELLDISCOUNTRATE         --卖出折现因子
        FROM YSP_WHDQ T1                
        ;
        
    COMMIT;*/
    
    
               
    --2.2 OPICS系统  获取外汇即期业务 剔除结售汇业务  默认折现因子为1 因为为OPICS系统无
    INSERT INTO RWA_DEV.RWA_WH_FEFORWARDSSWAP(
                 DATADATE                              --数据日期
                ,TRANID                                --交易ID
                ,TRANORGID                             --交易机构ID
                ,ACCORGID                              --账务机构ID
                ,INSTRUMENTSTYPE                       --金融工具类型
                ,ACCSUBJECTS                           --会计科目
                ,BOOKTYPE                              --账户类别
                ,STRUCTURALEXPOFLAG                    --是否结构性敞口
                ,BUYCURRENCY                           --买入币种
                ,BUYAMOUNT                             --买入金额
                ,SELLCURRENCY                          --卖出币种
                ,SELLAMOUNT                            --卖出金额
                ,STARTDATE                             --起始日期
                ,DUEDATE                               --到期日期
                ,ORIGINALMATURITY                               --原始期限
                ,RESIDUALM                             --剩余期限
                ,BUYZERORATE                           --买入币种零息利率
                ,BUYDISCOUNTRATE                       --买入折现因子
                ,SELLZERORATE                          --卖出币种零息利率
                ,SELLDISCOUNTRATE                      --卖出折现因子

    )
    WITH YSP_WHYQ AS (

      SELECT         
          W.DEALNO || 'P' AS DEALNO, --流水号       
          COST AS COST,   --成本中心        
          W.PORT AS PORT, --产品        
          W.CUST AS CUST, --客户        
          'P' AS PS, --方向        
          W.DEALDATE AS DEALDATE, --交易日期        
          W.VDATE AS VDATE, --交易到期日期       
          W.SWAPDEAL AS SWAPDEAL, --掉期、非掉期标识        
          W.TENOR AS TENOR, --即期远期标识                
          W.CCY AS CCY, --买入币种       
          ABS(W.CCYAMT) AS AMT, --买入币种金额        
          W.CCYRATE_8 AS RATE, --买入汇率              
          W.CCYNPVAMT + W.CTRNPVAMT AS CCYNPVAMT, --盯市价值
          W.CCYNPVAMT AS AMT1,      --买入现值               
          CASE WHEN SUBSTR(W.COST, 1, 4) = '3' THEN '01' ELSE '02' END  BOOKTYPE --账户类型
          --NULL BUYZERORATE,                           --买入币种零息利率
          --1 BUYDISCOUNTRATE,    --买入折现因子
          --NULL SELLZERORATE,                          --卖出币种零息利率
          --NULL SELLDISCOUNTRATE                       --卖出折现因子
     FROM RWA_DEV.OPI_FXDH W --外汇信息      
    WHERE SUBSTR(W.COST, 1, 1) = '2' --外汇业务   
      AND SUBSTR(W.COST, 4, 1) <> '3' --第四位<>3 --取交易账户下    
      AND SUBSTR(W.COST, 6, 1) = '1' --即期
      AND (W.CCY <> 'CNY' AND W.CTRCCY <> 'CNY') --不考虑结售汇业务 即人民币对外币或外币对人民币        
      AND W.VDATE >= p_data_dt_str  --未到期数据        
      AND W.DATANO = p_data_dt_str 
      AND W.VERIND = '1'   
      AND TRIM(W.REVDATE) IS NULL  
      
      UNION ALL
      
       SELECT         
          W.DEALNO || 'S' AS DEALNO, --流水号       
          W.COST AS COST, --成本中心        
          W.PORT AS  PORT,  --产品        
          W.CUST AS CUST, --客户        
          'S' AS PS, --方向        
          W.DEALDATE AS DEALDATE, --交易日期        
          W.VDATE AS VDATE, --交易到期日期       
          W.SWAPDEAL AS SWAPDEAL, --掉期、非掉期标识        
          W.TENOR AS TENOR, --即期远期标识                    
          W.CTRCCY AS CCY, --卖出币种        
          ABS(W.CTRAMT) AS AMT, --卖出金额       
          W.CTRBRATE_8 AS RATE, --汇率             
          W.CCYNPVAMT + W.CTRNPVAMT AS CCYNPVAMT, --盯市价值
          W.CTRNPVAMT  AS AMT1,     --卖出现值        
          CASE WHEN SUBSTR(W.COST, 1, 4) = '3' THEN '01' ELSE '02' END  BOOKTYPE --账户类型
          --NULL BUYZERORATE,                           --买入币种零息利率
          --NULL BUYDISCOUNTRATE,    --买入折现因子
          --NULL SELLZERORATE,                          --卖出币种零息利率
          --1 SELLDISCOUNTRATE                       --卖出折现因子
     FROM RWA_DEV.OPI_FXDH W --外汇信息    
    WHERE SUBSTR(W.COST, 1, 1) = '2' --外汇业务   
      AND SUBSTR(W.COST, 4, 1) <> '3' --第四位<>3 --取交易账户下    
      AND SUBSTR(W.COST, 6, 1) = '1' --即期
      AND (W.CCY <> 'CNY' AND W.CTRCCY <> 'CNY') --不考虑结售汇业务 即人民币对外币或外币对人民币        
      AND W.VDATE >= p_data_dt_str  --未到期数据        
      AND W.DATANO = p_data_dt_str 
      AND W.VERIND = '1'   
      AND TRIM(W.REVDATE) IS NULL  
      
    )         
        SELECT
                    TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                    ,DEALNO      AS TRANID  --交易ID
                    ,'9998'                                  AS TRANORGID                --交易机构ID       默认为“10000000-总行”
                    ,'9998'                                  AS ACCORGID                 --账务机构ID       默认为“10000000-总行”
                    ,'0502'                                      AS INSTRUMENTSTYPE          --金融工具类型     默认为“0503  外汇远期”
                    ,''                                          AS ACCSUBJECTS              --会计科目
                    ,T1.BOOKTYPE                                 AS BOOKTYPE                 --账户类别
                    ,'0'                                         AS STRUCTURALEXPOFLAG       --是否结构性敞口
                    ,CASE 
                       WHEN T1.PS = 'P' THEN T1.CCY
                       ELSE NULL   
                     END                                     AS BUYCURRENCY              --买入币种
                    ,CASE 
                       WHEN T1.PS = 'P' THEN T1.AMT1
                       ELSE NULL   
                     END                                     AS BUYAMOUNT                --买入金额
                    ,CASE 
                       WHEN T1.PS = 'S' THEN T1.CCY
                       ELSE NULL   
                     END                                     AS SELLCURRENCY             --卖出币种
                    ,CASE 
                       WHEN T1.PS = 'S' THEN T1.AMT1
                       ELSE NULL   
                     END                                     AS SELLAMOUNT               --卖出金额
                    ,TO_CHAR(TO_DATE(p_data_dt_str,'yyyyMMdd'),'yyyy-MM-dd') AS STARTDATE                --起始日期
                    ,TO_CHAR(TO_DATE(T1.VDATE,'yyyyMMdd'),'yyyy-MM-dd') AS DUEDATE                  --到期日期
                    ,CASE 
                          WHEN (TO_DATE(T1.VDATE,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYY-MM-DD')) / 365<0
                                  THEN 0
                                  ELSE (TO_DATE(T1.VDATE,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYY-MM-DD')) / 365
                      END   --原始期限
                    ,CASE WHEN (TO_DATE(T1.VDATE,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                                  THEN 0
                                  ELSE (TO_DATE(T1.VDATE,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                      END   --剩余期限
                    ,1              --买入币种零息利率   上面的金额直接就是现值，所以这些直接赋值1
                    ,1          --买入折现因子
                    ,1             --卖出币种零息利率
                    ,1         --卖出折现因子
        FROM YSP_WHYQ T1 
        ;
  
    COMMIT;
  
    --2.3 OPICS系统  获取外汇远期业务 剔除结售汇业务  取不到时默认折现因子为1 
    INSERT INTO RWA_DEV.RWA_WH_FEFORWARDSSWAP(
                 DATADATE                              --数据日期
                ,TRANID                                --交易ID
                ,TRANORGID                             --交易机构ID
                ,ACCORGID                              --账务机构ID
                ,INSTRUMENTSTYPE                       --金融工具类型
                ,ACCSUBJECTS                           --会计科目
                ,BOOKTYPE                              --账户类别
                ,STRUCTURALEXPOFLAG                    --是否结构性敞口
                ,BUYCURRENCY                           --买入币种
                ,BUYAMOUNT                             --买入金额
                ,SELLCURRENCY                          --卖出币种
                ,SELLAMOUNT                            --卖出金额
                ,STARTDATE                             --起始日期
                ,DUEDATE                               --到期日期
                ,ORIGINALMATURITY                               --原始期限
                ,RESIDUALM                             --剩余期限
                ,BUYZERORATE                           --买入币种零息利率
                ,BUYDISCOUNTRATE                       --买入折现因子
                ,SELLZERORATE                          --卖出币种零息利率
                ,SELLDISCOUNTRATE                      --卖出折现因子

    )
    WITH YSP_WHYQ AS (

      SELECT         
          W.DEALNO || 'P' AS DEALNO, --流水号       
          COST AS COST,   --成本中心        
          W.PORT AS PORT, --产品        
          W.CUST AS CUST, --客户        
          'P' AS PS, --方向        
          W.DEALDATE AS DEALDATE, --交易日期        
          W.VDATE AS VDATE, --交易到期日期       
          W.SWAPDEAL AS SWAPDEAL, --掉期、非掉期标识        
          W.TENOR AS TENOR, --即期远期标识                
          W.CCY AS CCY, --买入币种       
          ABS(W.CCYAMT) AS AMT, --买入币种金额        
          W.CCYRATE_8 AS RATE, --买入汇率              
          W.CCYNPVAMT + W.CTRNPVAMT AS CCYNPVAMT, --盯市价值
          W.CCYNPVAMT AS AMT1,      --买入现值               
          CASE WHEN SUBSTR(W.COST, 1, 4) = '3' THEN '01' ELSE '02' END  BOOKTYPE --账户类型
          --NULL BUYZERORATE,                           --买入币种零息利率
          --1 BUYDISCOUNTRATE,    --买入折现因子
          --NULL SELLZERORATE,                          --卖出币种零息利率
          --NULL SELLDISCOUNTRATE                       --卖出折现因子
     FROM RWA_DEV.OPI_FXDH W --外汇信息      
    WHERE SUBSTR(W.COST, 1, 1) = '2' --外汇业务   
      AND SUBSTR(W.COST, 4, 1) <> '3' --第四位<>3 --取交易账户下    
      AND SUBSTR(W.COST, 6, 1) = '2' --远期
      AND (W.CCY <> 'CNY' AND W.CTRCCY <> 'CNY') --不考虑结售汇业务 即人民币对外币或外币对人民币        
      AND W.VDATE >= p_data_dt_str  --未到期数据        
      AND W.DATANO = p_data_dt_str 
      AND W.VERIND = '1'   
      AND TRIM(W.REVDATE) IS NULL  
      
      UNION ALL
      
       SELECT         
          W.DEALNO || 'S' AS DEALNO, --流水号       
          W.COST AS COST, --成本中心        
          W.PORT AS  PORT,  --产品        
          W.CUST AS CUST, --客户        
          'S' AS PS, --方向        
          W.DEALDATE AS DEALDATE, --交易日期        
          W.VDATE AS VDATE, --交易到期日期       
          W.SWAPDEAL AS SWAPDEAL, --掉期、非掉期标识        
          W.TENOR AS TENOR, --即期远期标识                    
          W.CTRCCY AS CCY, --卖出币种        
          ABS(W.CTRAMT) AS AMT, --卖出金额       
          W.CTRBRATE_8 AS RATE, --汇率             
          W.CCYNPVAMT + W.CTRNPVAMT AS CCYNPVAMT, --盯市价值
          W.CTRNPVAMT  AS AMT1,     --卖出现值        
          CASE WHEN SUBSTR(W.COST, 1, 4) = '3' THEN '01' ELSE '02' END  BOOKTYPE --账户类型
          --NULL BUYZERORATE,                           --买入币种零息利率
          --NULL BUYDISCOUNTRATE,    --买入折现因子
          --NULL SELLZERORATE,                          --卖出币种零息利率
          --1 SELLDISCOUNTRATE                       --卖出折现因子
     FROM RWA_DEV.OPI_FXDH W --外汇信息    
    WHERE SUBSTR(W.COST, 1, 1) = '2' --外汇业务   
      AND SUBSTR(W.COST, 4, 1) <> '3' --第四位<>3 --取交易账户下    
      AND SUBSTR(W.COST, 6, 1) = '2' --远期
      AND (W.CCY <> 'CNY' AND W.CTRCCY <> 'CNY') --不考虑结售汇业务 即人民币对外币或外币对人民币        
      AND W.VDATE >= p_data_dt_str  --未到期数据        
      AND W.DATANO = p_data_dt_str 
      AND W.VERIND = '1'   
      AND TRIM(W.REVDATE) IS NULL  
      
    )         
        SELECT
                    TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                    ,DEALNO      AS TRANID  --交易ID
                    ,'9998'                                  AS TRANORGID                --交易机构ID       默认为“10000000-总行”
                    ,'9998'                                  AS ACCORGID                 --账务机构ID       默认为“10000000-总行”
                    ,'0502'                                      AS INSTRUMENTSTYPE          --金融工具类型     默认为“0503  外汇远期”
                    ,''                                          AS ACCSUBJECTS              --会计科目
                    ,T1.BOOKTYPE                                 AS BOOKTYPE                 --账户类别
                    ,'0'                                         AS STRUCTURALEXPOFLAG       --是否结构性敞口
                    ,CASE 
                       WHEN T1.PS = 'P' THEN T1.CCY
                       ELSE NULL   
                     END                                     AS BUYCURRENCY              --买入币种
                    ,CASE 
                       WHEN T1.PS = 'P' THEN T1.AMT1
                       ELSE NULL   
                     END                                     AS BUYAMOUNT                --买入金额
                    ,CASE 
                       WHEN T1.PS = 'S' THEN T1.CCY
                       ELSE NULL   
                     END                                     AS SELLCURRENCY             --卖出币种
                    ,CASE 
                       WHEN T1.PS = 'S' THEN T1.AMT1
                       ELSE NULL   
                     END                                     AS SELLAMOUNT               --卖出金额
                    ,TO_CHAR(TO_DATE(p_data_dt_str,'yyyyMMdd'),'yyyy-MM-dd') AS STARTDATE                --起始日期
                    ,TO_CHAR(TO_DATE(T1.VDATE,'yyyyMMdd'),'yyyy-MM-dd') AS DUEDATE                  --到期日期
                    ,CASE 
                          WHEN (TO_DATE(T1.VDATE,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYY-MM-DD')) / 365<0
                                  THEN 0
                                  ELSE (TO_DATE(T1.VDATE,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYY-MM-DD')) / 365
                      END   --原始期限
                    ,CASE WHEN (TO_DATE(T1.VDATE,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                                  THEN 0
                                  ELSE (TO_DATE(T1.VDATE,'YYYY-MM-DD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                      END   --剩余期限
                    ,1              --买入币种零息利率
                    ,NULL          --买入折现因子
                    ,1             --卖出币种零息利率
                    ,NULL         --卖出折现因子
        FROM YSP_WHYQ T1 
        ;
        
    COMMIT;
    
   /* */
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_WH_FEFORWARDSSWAP',cascade => true);


    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_WH_FEFORWARDSSWAP;

  p_po_rtncode := '1';
  p_po_rtnmsg  := '成功' || '-' || v_count;
--定义异常
EXCEPTION
    WHEN OTHERS THEN
 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
    p_po_rtncode := sqlcode;
    p_po_rtnmsg  := '外汇远期掉期('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_WH_FEFORWARDSSWAP;
/

