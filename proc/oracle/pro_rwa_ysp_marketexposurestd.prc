CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_YSP_MARKETEXPOSURESTD(
                                                   p_data_dt_str    IN    VARCHAR2,        --数据日期 yyyyMMdd
                                                   p_po_rtncode    OUT    VARCHAR2,        --返回编号 1 成功,0 失败
                                                   p_po_rtnmsg     OUT    VARCHAR2         --返回描述
                )
  /*
    存储过程名称:RWA_DEV.PRO_RWA_YSP_MARKETEXPOSURESTD
    实现功能:OPTIC-市场风险-标准法暴露表(从数据源外汇现货头寸表全量导入RWA市场风险国结接口表外汇标准法暴露表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0l
    编写人  :YUSJ
    编写时间:2019-12-25
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.RWA_WH_FESPOTPOSITION|外汇现货头寸表
    源  表2 :RWA.ORG_INFO|RWA机构表
    源  表3 :RWA_DEV.RWA_WH_FEFORWARDSSWAP|外汇远期掉期表（掉期）
    目标表  :RWA_DEV.RWA_WH_MARKETEXPOSURESTD|国结系统外汇标准法暴露表
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_YSP_MARKETEXPOSURESTD';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_YSP_MARKETEXPOSURESTD';

    --2.将满足条件的数据从源表插入到目标表中
    
    --利率互换业务       
     INSERT INTO RWA_DEV.RWA_YSP_MARKETEXPOSURESTD(
                DATADATE                               --数据日期
                ,DATANO                                --数据流水号
                ,EXPOSUREID                            --风险暴露ID
                ,BOOKTYPE                              --账户类别
                ,INSTRUMENTSID                         --金融工具ID
                ,INSTRUMENTSTYPE                       --金融工具类型
                ,ORGID                                 --所属机构ID
                ,ORGNAME                               --所属机构名称
                ,ORGTYPE                               --所属机构类型
                ,MARKETRISKTYPE                        --市场风险类型
                ,INTERATERISKTYPE                      --利率风险类型
                ,EQUITYRISKTYPE                        --股票风险类型
                ,EXCHANGERISKTYPE                      --外汇风险类型
                ,COMMODITYNAME                         --商品种类名称
                ,OPTIONRISKTYPE                        --期权风险类型
                ,ISSUERID                              --发行人ID
                ,ISSUERNAME                            --发行人名称
                ,ISSUERTYPE                            --发行人大类
                ,ISSUERSUBTYPE                         --发行人小类
                ,ISSUERREGISTSTATE                     --发行人注册国家
                ,ISSUERRCERATING                       --发行人境外注册地外部评级
                ,SMBFLAG                               --小微企业标识
                ,UNDERBONDFLAG                         --是否承销债券
                ,PAYMENTDATE                           --缴款日
                ,SECURITIESTYPE                        --证券类别
                ,BONDISSUEINTENT                       --债券发行目的
                ,CLAIMSLEVEL                           --债权级别
                ,REABSFLAG                             --再资产证券化标识
                ,ORIGINATORFLAG                        --是否发起机构
                ,SECURITIESERATING                     --证券外部评级
                ,STOCKCODE                             --股票/股指代码
                ,STOCKMARKET                           --交易市场
                ,EXCHANGEAREA                          --交易地区
                ,STRUCTURALEXPOFLAG                    --是否结构性敞口
                ,OPTIONUNDERLYINGFLAG                  --是否期权基础工具
                ,OPTIONUNDERLYINGTYPE                  --期权基础工具类型
                ,OPTIONID                              --期权工具ID
                ,VOLATILITY                            --波动率
                ,STARTDATE                             --起始日期
                ,DUEDATE                               --到期日期
                ,ORIGINALMATURITY                      --原始期限
                ,RESIDUALM                             --剩余期限
                ,NEXTREPRICEDATE                       --下次重定价日
                ,NEXTREPRICEM                          --下次重定价期限
                ,RATETYPE                              --利率类型
                ,COUPONRATE                            --票面利率
                ,MODIFIEDDURATION                      --修正久期
                ,POSITIONTYPE                          --头寸属性
                ,POSITION                              --头寸
                ,CURRENCY                              --币种
                ,OPTIONUNDERLYINGNAME                  --期权基础工具名称
                ,ORGSORTNO                             --机构排序号

    )WITH TEMP11 AS (
    SELECT T1.DEALNO AS DEALNO
    ,T1.SEQ AS SEQ
    ,CASE WHEN T1.PAYRECIND='P' THEN TO_DATE(T1.Matdate,'YYYYMMDD')-TO_DATE(p_data_dt_str, 'YYYYMMDD')
          WHEN T1.PAYRECIND='R' AND T2.PORT='SWZS' THEN 
                        CASE WHEN  SUBSTR(T1.RATECODE,-6,1)='M' THEN 30*SUBSTR(RATECODE,1,LENGTH(RATECODE)-6)
                             WHEN  SUBSTR(T1.RATECODE,-6,1)='Y' THEN 365*SUBSTR(RATECODE,1,LENGTH(RATECODE)-6)
                             WHEN  SUBSTR(T1.RATECODE,-6,1)='W' THEN 7*SUBSTR(RATECODE,1,LENGTH(RATECODE)-6)
                             WHEN  SUBSTR(T1.RATECODE,-6,1)='D' THEN 1*SUBSTR(RATECODE,1,LENGTH(RATECODE)-6)
                        END
          WHEN T1.PAYRECIND='R' AND T5.GCRQ IS NULL THEN 
                                              CASE WHEN TO_DATE(T5.GCRZ,'YYYY-MM-DD')> TO_DATE(p_data_dt_str, 'YYYYMMDD')
                                                   THEN TO_DATE(T5.GCRZ,'YYYY-MM-DD')-TO_DATE(p_data_dt_str, 'YYYYMMDD')
                                                   ELSE TO_DATE(T1.Matdate,'YYYYMMDD')-TO_DATE(p_data_dt_str,'YYYYMMDD')
                                              END
         WHEN T1.PAYRECIND='R' AND T5.GCRQ IS NOT NULL  
                               AND TO_DATE(T5.GCRZ,'YYYY-MM-DD')>TO_DATE(p_data_dt_str, 'YYYYMMDD')
                               THEN 1       --这种情况默认为1天            
         ELSE TO_DATE(T1.Matdate,'YYYYMMDD')-TO_DATE(p_data_dt_str,'YYYYMMDD')
      END   AS RESIDUALM                                 --剩余期限，转换为天的，算折现因子需要
    FROM RWA_DEV.OPI_SWDT T1 --互换交易
    inner JOIN RWA_DEV.OPI_SWDH T2 --互换报头 
    ON T1.DEALNO = T2.DEALNO
    AND T2.DATANO = p_data_dt_str
    AND T2.PORT<>'SWDK'        --排除结构性存款业务
    LEFT JOIN RWA.RWA_WS_GCR_BL T5
    ON T1.DEALNO=T5.DEALNO
    AND T5.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
    WHERE T1.DATANO = p_data_dt_str
    AND SUBSTR(T2.COST, 1, 1) = '3' --第一位=3  --数据为利率/货币掉期业务
    --AND SUBSTR(T2.COST, 4, 1) <> '3' --取交易账户下，台账是包含了这两种业务的，所以这个条件去除掉
    AND SUBSTR(T2.COST, 6, 1) IN ('1','2','3') --第六位=1  --利率掉期
    AND T2.VERIND = 1 
    AND TRIM(T2.REVDATE) IS NULL
    AND TO_DATE(T1.MATDATE,'YYYYMMDD')>=TO_DATE(p_data_dt_str,'YYYYMMDD')  
    )
    SELECT
                 TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,p_data_dt_str                                AS DATANO                   --数据流水号
                ,'YSP'||T1.DEALNO||T1.SEQ||T1.PAYRECIND       AS EXPOSUREID               --风险暴露ID
                ,'02'                                         AS BOOKTYPE                 --账户类别
                ,T1.DEALNO                                    AS INSTRUMENTSID            --金融工具ID
                ,'0201'                                       AS INSTRUMENTSTYPE          --金融工具类型
                ,'6001'                                       AS ORGID                    --所属机构ID
                ,'重庆银行股份有限公司国际业务部'             AS ORGNAME                  --所属机构名称
                ,'01'                                         AS ORGTYPE                  --所属机构类型                                 默认：境内机构(01)
                ,'01'                                         AS MARKETRISKTYPE           --市场风险类型                                 默认：外汇风险(03)
                ,'02'                                         AS INTERATERISKTYPE         --利率风险类型                                 默认：空
                ,''                                           AS EQUITYRISKTYPE           --股票风险类型                                 默认：空
                ,''                                           AS EXCHANGERISKTYPE         --外汇风险类型                                 如果币种<>人民币，则需根据币种映射；人民币不需映射
                ,''                                           AS COMMODITYNAME            --商品种类名称                                 默认：空
                ,''                                           AS OPTIONRISKTYPE           --期权风险类型                                 默认：空
                ,'OPI'||TRIM(T3.Cno)                                       AS ISSUERID          --发行人ID                                     默认：空
                ,T3.Cfn1                                        AS ISSUERNAME               --发行人名称                                   默认：空
                ,''                                           AS ISSUERTYPE               --发行人大类                                   默认：空
                ,''                                           AS ISSUERSUBTYPE            --发行人小类                                   默认：空
                ,CASE WHEN T3.CCODE='CN' THEN '01' 
                ELSE '02' END                                 AS ISSUERREGISTSTATE        --发行人注册国家                               默认：空
                ,''                                           AS ISSUERRCERATING          --发行人境外注册地外部评级                     默认：空
                ,'0'                                          AS SMBFLAG                  --小微企业标识                                 默认：空
                ,''                                           AS UNDERBONDFLAG            --是否承销债券                                 默认：空
                ,''                                           AS PAYMENTDATE              --缴款日                                       默认：空
                ,''                                           AS SECURITIESTYPE           --证券类别                                     默认：空
                ,''                                           AS BONDISSUEINTENT          --债券发行目的                                 默认：空
                ,''                                           AS CLAIMSLEVEL              --债权级别                                     默认：空
                ,''                                           AS REABSFLAG                --再资产证券化标识                             默认：空
                ,''                                           AS ORIGINATORFLAG           --是否发起机构                                 默认：空
                ,''                                           AS SECURITIESERATING        --证券外部评级                                 默认：空
                ,''                                           AS STOCKCODE                --股票/股指代码                                默认：空
                ,''                                           AS STOCKMARKET              --交易市场                                     默认：空
                ,''                                           AS EXCHANGEAREA             --交易地区                                     默认：空
                ,'0'                                          AS STRUCTURALEXPOFLAG       --是否结构性敞口
                ,'0'                                          AS OPTIONUNDERLYINGFLAG     --是否期权基础工具                             默认：否(0)
                ,''                                           AS OPTIONUNDERLYINGTYPE     --期权基础工具类型                             默认：空
                ,''                                           AS OPTIONID                 --期权工具ID                                   默认：空
                ,NULL                                         AS VOLATILITY               --波动率                                       默认：空
                ,T1.Startdate                                 AS STARTDATE                --起始日期                                     默认：空
                ,T1.MATDATE                                   AS DUEDATE                  --到期日期                                     默认：空
                ,(TO_DATE(T1.Matdate , 'YYYYMMDD')-TO_DATE(T1.Startdate, 'YYYYMMDD')) / 365 AS ORIGINALMATURITY         --原始期限                                     默认：空
                ,T5.RESIDUALM/365                             AS RESIDUALM                --剩余期限                                     默认：空
                ,CASE WHEN T1.FIXFLOATIND='L' THEN NVL(TO_CHAR(T4.RATEREVDTE,'YYYYMMDD'),T1.Matdate)
                      ELSE NULL                      --利率类型为浮动利率才需要重定价日
                 END                                AS NEXTREPRICEDATE          --下次重定价日                                 默认：空
                ,CASE WHEN (TO_DATE(CASE WHEN T1.FIXFLOATIND='L' THEN NVL(TO_CHAR(T4.RATEREVDTE,'YYYYMMDD'),T1.Matdate)
                                         ELSE NULL                  
                                     END, 'YYYY-MM-DD') -TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')) / 365 < 0 THEN  0
                      ELSE (TO_DATE(CASE WHEN T1.FIXFLOATIND='L' THEN NVL(TO_CHAR(T4.RATEREVDTE,'YYYYMMDD'),T1.Matdate)
                                         ELSE NULL                  
                                    END, 'YYYY-MM-DD')- TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')) / 365
                  END                                       AS NEXTREPRICEM             --下次重定价期限                               默认：空
                ,CASE WHEN T1.FIXFLOATIND='L' THEN '02'
                      ELSE '01'
                 END                                          AS RATETYPE                 --利率类型                                     默认：空
                ,T1.INTRATE_8                                 AS COUPONRATE               --票面利率                                     默认：空
                ,''                                           AS MODIFIEDDURATION         --修正久期                                     默认：空
                ,CASE WHEN T1.PAYRECIND = 'R' THEN '01' --多头  
                      WHEN T1.PAYRECIND = 'P' THEN '02' --空头
                 ELSE '01' --多头
                 END                                          AS POSITIONTYPE             --头寸属性
                ,ABS(T1.NOTCCYAMT)*NVL(T6.JZRAT/100,1)*CALCULATE(T5.RESIDUALM,T1.NOTCCY,p_data_dt_str)+NVL(ABS(T4.PVAMT),0)        AS POSITION                 --头寸
                ,'CNY'                                       AS CURRENCY                 --币种,头寸中已经乘以汇率，这里默认人民币
                ,''                                           AS OPTIONUNDERLYINGNAME    --期权基础工具名称
                ,'1290'                                       AS ORGSORTNO                 --机构排序号
       FROM RWA_DEV.OPI_SWDT T1 --互换交易
       inner JOIN RWA_DEV.OPI_SWDH T2 --互换报头 
        ON T1.DEALNO = T2.DEALNO
       AND T2.DATANO = p_data_dt_str
       AND T2.PORT<>'SWDK'        --排除结构性存款业务
       INNER JOIN TEMP11  T5  --上面临时表
       ON T1.DEALNO=T5.DEALNO
       AND T1.SEQ=T5.SEQ
       LEFT JOIN RWA_DEV.OPI_CUST T3 --客户信息
       ON trim(T2.CNO) = trim(T3.CNO)
       AND T3.DATANO = p_data_dt_str
       LEFT JOIN (
            --获取下次重定价日期
            SELECT DEALNO,SEQ,PAYRECIND, TO_DATE(MAX(RATEREVDTE)) RATEREVDTE,SUM(PVAMT) PVAMT  --利率互换，头寸支取利息的现值
            FROM OPI_SWDS
            WHERE DATANO = p_data_dt_str
            --AND RATEREVDTE IS NOT NULL 
            GROUP BY DEALNO,SEQ,PAYRECIND
            ) T4
       ON T1.DEALNO = T4.DEALNO 
       AND T1.SEQ = T4.SEQ
       AND T1.PAYRECIND=T4.PAYRECIND
       LEFT JOIN RWA_DEV.NNS_JT_EXRATE T6
       ON T1.NOTCCY=T6.CCY
       AND T6.DATANO=p_data_dt_str
       WHERE T1.DATANO = p_data_dt_str
       AND SUBSTR(T2.COST, 1, 1) = '3' --第一位=3  --数据为利率/货币掉期业务
       --AND SUBSTR(T2.COST, 4, 1) <> '3' --取交易账户下，台账是包含了这两种业务的，所以这个条件去除掉
       AND SUBSTR(T2.COST, 6, 1) IN ('1','2','3') --第六位=1  --利率掉期,2,3为货币远掉期业务
       AND T2.VERIND = 1 
       AND TRIM(T2.REVDATE) IS NULL
       AND TO_DATE(T1.MATDATE,'YYYYMMDD')>=TO_DATE(p_data_dt_str,'YYYYMMDD')
       ;
    COMMIT;
    
    /*--货币互换业务       
     INSERT INTO RWA_DEV.RWA_YSP_MARKETEXPOSURESTD(
                DATADATE                               --数据日期
                ,DATANO                                --数据流水号
                ,EXPOSUREID                            --风险暴露ID
                ,BOOKTYPE                              --账户类别
                ,INSTRUMENTSID                         --金融工具ID
                ,INSTRUMENTSTYPE                       --金融工具类型
                ,ORGID                                 --所属机构ID
                ,ORGNAME                               --所属机构名称
                ,ORGTYPE                               --所属机构类型
                ,MARKETRISKTYPE                        --市场风险类型
                ,INTERATERISKTYPE                      --利率风险类型
                ,EQUITYRISKTYPE                        --股票风险类型
                ,EXCHANGERISKTYPE                      --外汇风险类型
                ,COMMODITYNAME                         --商品种类名称
                ,OPTIONRISKTYPE                        --期权风险类型
                ,ISSUERID                              --发行人ID
                ,ISSUERNAME                            --发行人名称
                ,ISSUERTYPE                            --发行人大类
                ,ISSUERSUBTYPE                         --发行人小类
                ,ISSUERREGISTSTATE                     --发行人注册国家
                ,ISSUERRCERATING                       --发行人境外注册地外部评级
                ,SMBFLAG                               --小微企业标识
                ,UNDERBONDFLAG                         --是否承销债券
                ,PAYMENTDATE                           --缴款日
                ,SECURITIESTYPE                        --证券类别
                ,BONDISSUEINTENT                       --债券发行目的
                ,CLAIMSLEVEL                           --债权级别
                ,REABSFLAG                             --再资产证券化标识
                ,ORIGINATORFLAG                        --是否发起机构
                ,SECURITIESERATING                     --证券外部评级
                ,STOCKCODE                             --股票/股指代码
                ,STOCKMARKET                           --交易市场
                ,EXCHANGEAREA                          --交易地区
                ,STRUCTURALEXPOFLAG                    --是否结构性敞口
                ,OPTIONUNDERLYINGFLAG                  --是否期权基础工具
                ,OPTIONUNDERLYINGTYPE                  --期权基础工具类型
                ,OPTIONID                              --期权工具ID
                ,VOLATILITY                            --波动率
                ,STARTDATE                             --起始日期
                ,DUEDATE                               --到期日期
                ,ORIGINALMATURITY                      --原始期限
                ,RESIDUALM                             --剩余期限
                ,NEXTREPRICEDATE                       --下次重定价日
                ,NEXTREPRICEM                          --下次重定价期限
                ,RATETYPE                              --利率类型
                ,COUPONRATE                            --票面利率
                ,MODIFIEDDURATION                      --修正久期
                ,POSITIONTYPE                          --头寸属性
                ,POSITION                              --头寸
                ,CURRENCY                              --币种
                ,OPTIONUNDERLYINGNAME                  --期权基础工具名称
                ,ORGSORTNO                             --机构排序号

    )
    SELECT
                 TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,p_data_dt_str                                AS DATANO                   --数据流水号
                ,'YSP'||T1.DEALNO||T1.SEQ||T1.PAYRECIND              AS EXPOSUREID               --风险暴露ID
                ,'02'                                         AS BOOKTYPE                 --账户类别
                ,T1.DEALNO                                   AS INSTRUMENTSID            --金融工具ID
                ,'0201'                                       AS INSTRUMENTSTYPE          --金融工具类型
                ,'6001'                                       AS ORGID                    --所属机构ID
                ,'重庆银行股份有限公司国际业务部'             AS ORGNAME                  --所属机构名称
                ,'01'                                         AS ORGTYPE                  --所属机构类型                                 默认：境内机构(01)
                ,'01'                                         AS MARKETRISKTYPE           --市场风险类型                                 默认：外汇风险(03)
                ,'02'                                         AS INTERATERISKTYPE         --利率风险类型                                 默认：空
                ,''                                           AS EQUITYRISKTYPE           --股票风险类型                                 默认：空
                ,''                                           AS EXCHANGERISKTYPE         --外汇风险类型                                 如果币种<>人民币，则需根据币种映射；人民币不需映射
                ,''                                           AS COMMODITYNAME            --商品种类名称                                 默认：空
                ,''                                           AS OPTIONRISKTYPE           --期权风险类型                                 默认：空
                ,'OPI'||TRIM(T3.Cno)                                       AS ISSUERID          --发行人ID                                     默认：空
                ,T3.Cfn1                                        AS ISSUERNAME               --发行人名称                                   默认：空
                ,''                                           AS ISSUERTYPE               --发行人大类                                   默认：空
                ,''                                           AS ISSUERSUBTYPE            --发行人小类                                   默认：空
                ,CASE WHEN T3.CCODE='CN' THEN '01' 
                ELSE '02' END                                 AS ISSUERREGISTSTATE        --发行人注册国家                               默认：空
                ,''                                           AS ISSUERRCERATING          --发行人境外注册地外部评级                     默认：空
                ,'0'                                          AS SMBFLAG                  --小微企业标识                                 默认：空
                ,''                                           AS UNDERBONDFLAG            --是否承销债券                                 默认：空
                ,''                                           AS PAYMENTDATE              --缴款日                                       默认：空
                ,''                                           AS SECURITIESTYPE           --证券类别                                     默认：空
                ,''                                           AS BONDISSUEINTENT          --债券发行目的                                 默认：空
                ,''                                           AS CLAIMSLEVEL              --债权级别                                     默认：空
                ,''                                           AS REABSFLAG                --再资产证券化标识                             默认：空
                ,''                                           AS ORIGINATORFLAG           --是否发起机构                                 默认：空
                ,''                                           AS SECURITIESERATING        --证券外部评级                                 默认：空
                ,''                                           AS STOCKCODE                --股票/股指代码                                默认：空
                ,''                                           AS STOCKMARKET              --交易市场                                     默认：空
                ,''                                           AS EXCHANGEAREA             --交易地区                                     默认：空
                ,'0'                                          AS STRUCTURALEXPOFLAG       --是否结构性敞口
                ,'0'                                          AS OPTIONUNDERLYINGFLAG     --是否期权基础工具                             默认：否(0)
                ,''                                           AS OPTIONUNDERLYINGTYPE     --期权基础工具类型                             默认：空
                ,''                                           AS OPTIONID                 --期权工具ID                                   默认：空
                ,NULL                                         AS VOLATILITY               --波动率                                       默认：空
                ,T1.Startdate                                 AS STARTDATE                --起始日期                                     默认：空
                ,CASE WHEN T1.FIXFLOATIND = 'L' AND T4.RATEREVDTE IS NOT NULL THEN TO_CHAR(T4.RATEREVDTE,'YYYYMMDD') 
                      ELSE T1.Matdate 
                 END                                          AS DUEDATE                  --到期日期                                     默认：空
                ,CASE WHEN (TO_DATE(CASE WHEN T1.FIXFLOATIND = 'L' AND T4.RATEREVDTE IS NOT NULL THEN TO_CHAR(T4.RATEREVDTE,'YYYYMMDD') ELSE T1.Matdate END, 'YYYY-MM-DD') -
                  TO_DATE(T1.Startdate, 'YYYY-MM-DD')) / 365 < 0 THEN 0
                ELSE (TO_DATE(CASE WHEN T1.FIXFLOATIND = 'L' AND T4.RATEREVDTE IS NOT NULL THEN TO_CHAR(T4.RATEREVDTE,'YYYYMMDD') ELSE T1.Matdate END, 'YYYY-MM-DD')
                -TO_DATE(T1.Startdate, 'YYYY-MM-DD')) / 365
                END                                           AS ORIGINALMATURITY         --原始期限                                     默认：空
                ,CASE WHEN (TO_DATE(CASE WHEN T1.FIXFLOATIND = 'L' AND T4.RATEREVDTE IS NOT NULL THEN TO_CHAR(T4.RATEREVDTE,'YYYYMMDD') ELSE T1.Matdate END, 'YYYY-MM-DD') -
                  TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')) / 365 < 0 THEN  0
                ELSE (TO_DATE(CASE WHEN T1.FIXFLOATIND = 'L' AND T4.RATEREVDTE IS NOT NULL THEN TO_CHAR(T4.RATEREVDTE,'YYYYMMDD') 
                  ELSE T1.Matdate END, 'YYYY-MM-DD') - TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')) / 365
                END                                           AS RESIDUALM                --剩余期限                                     默认：空
                ,CASE WHEN T1.FIXFLOATIND='L' THEN NVL(TO_CHAR(T4.RATEREVDTE,'YYYYMMDD'),T1.Matdate)
                      ELSE NULL                      --利率类型为浮动利率才需要重定价日
                 END                                AS NEXTREPRICEDATE          --下次重定价日                                 默认：空
                ,CASE WHEN (TO_DATE(CASE WHEN T1.FIXFLOATIND='L' THEN NVL(TO_CHAR(T4.RATEREVDTE,'YYYYMMDD'),T1.Matdate)
                                         ELSE NULL                  
                                         END, 'YYYY-MM-DD') -
                  TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')) / 365 < 0 THEN  0
                ELSE (TO_DATE(CASE WHEN T1.FIXFLOATIND='L' THEN NVL(TO_CHAR(T4.RATEREVDTE,'YYYYMMDD'),T1.Matdate)
                                         ELSE NULL                  
                                         END, 'YYYY-MM-DD') -
                TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')) / 365
                END                                       AS NEXTREPRICEM             --下次重定价期限                               默认：空
                ,CASE WHEN T1.FIXFLOATIND='L' THEN '02'
                      ELSE '01'
                 END                                          AS RATETYPE                 --利率类型                                     默认：空
                ,T1.INTRATE_8                                 AS COUPONRATE               --票面利率                                     默认：空
                ,''                                           AS MODIFIEDDURATION         --修正久期                                     默认：空
                ,CASE WHEN T1.PAYRECIND = 'R' THEN '01' --多头  
                      WHEN T1.PAYRECIND = 'P' THEN '02' --空头
                 ELSE '01' --多头
                 END                                          AS POSITIONTYPE             --头寸属性
                ,NVL(T4.PVAMT,0)                              AS POSITION                 --头寸
                ,T1.INTCCY                                    AS CURRENCY                 --币种
                ,''                                           AS OPTIONUNDERLYINGNAME    --期权基础工具名称
                ,'1290'                                       AS ORGSORTNO                 --机构排序号
       FROM RWA_DEV.OPI_SWDT T1 --互换交易
       inner JOIN RWA_DEV.OPI_SWDH T2 --互换报头 
        ON T1.DEALNO = T2.DEALNO
       AND T2.DATANO = p_data_dt_str
       AND T2.PORT<>'SWDK'        --排除结构性存款业务
       LEFT JOIN RWA_DEV.OPI_CUST T3 --客户信息
        ON trim(T2.CNO) = trim(T3.CNO)
       AND T3.DATANO = p_data_dt_str
       LEFT JOIN (
            --获取下次重定价日期
            SELECT DEALNO, SEQ,PAYRECIND, TO_DATE(MAX(RATEREVDTE)) RATEREVDTE,SUM(PVAMT) PVAMT   --利息现值
            FROM OPI_SWDS
            WHERE RATEREVDTE IS NOT NULL
            AND  DATANO = p_data_dt_str
            GROUP BY DEALNO,SEQ
            ) T4
       ON T1.DEALNO = T4.DEALNO 
       AND T1.SEQ = T4.SEQ
       AND T1.PAYRECIND=T4.PAYRECIND
      WHERE T1.DATANO = p_data_dt_str
        AND SUBSTR(T2.COST, 1, 1) = '3' --第一位=3  --数据为利率/货币掉期业务
        --AND SUBSTR(T2.COST, 4, 1) <> '3' ---取交易账户下，台账是包含了这两种业务的，所以这个条件去除掉
        AND SUBSTR(T2.COST, 6, 1) IN( '2','3') --第六位=2  --货币掉期，远期业务
        AND T2.VERIND = 1   --上线后放开
        AND TRIM(T2.REVDATE) IS NULL  --上线后放开 
           ;

    COMMIT;*/
           
   --外汇  远期业务 
   INSERT INTO RWA_DEV.RWA_YSP_MARKETEXPOSURESTD(
              DATADATE                               --数据日期
              ,DATANO                                --数据流水号
              ,EXPOSUREID                            --风险暴露ID
              ,BOOKTYPE                              --账户类别
              ,INSTRUMENTSID                         --金融工具ID
              ,INSTRUMENTSTYPE                       --金融工具类型
              ,ORGID                                 --所属机构ID
              ,ORGNAME                               --所属机构名称
              ,ORGTYPE                               --所属机构类型
              ,MARKETRISKTYPE                        --市场风险类型
              ,INTERATERISKTYPE                      --利率风险类型
              ,EQUITYRISKTYPE                        --股票风险类型
              ,EXCHANGERISKTYPE                      --外汇风险类型
              ,COMMODITYNAME                         --商品种类名称
              ,OPTIONRISKTYPE                        --期权风险类型
              ,ISSUERID                              --发行人ID
              ,ISSUERNAME                            --发行人名称
              ,ISSUERTYPE                            --发行人大类
              ,ISSUERSUBTYPE                         --发行人小类
              ,ISSUERREGISTSTATE                     --发行人注册国家
              ,ISSUERRCERATING                       --发行人境外注册地外部评级
              ,SMBFLAG                               --小微企业标识
              ,UNDERBONDFLAG                         --是否承销债券
              ,PAYMENTDATE                           --缴款日
              ,SECURITIESTYPE                        --证券类别
              ,BONDISSUEINTENT                       --债券发行目的
              ,CLAIMSLEVEL                           --债权级别
              ,REABSFLAG                             --再资产证券化标识
              ,ORIGINATORFLAG                        --是否发起机构
              ,SECURITIESERATING                     --证券外部评级
              ,STOCKCODE                             --股票/股指代码
              ,STOCKMARKET                           --交易市场
              ,EXCHANGEAREA                          --交易地区
              ,STRUCTURALEXPOFLAG                    --是否结构性敞口
              ,OPTIONUNDERLYINGFLAG                  --是否期权基础工具
              ,OPTIONUNDERLYINGTYPE                  --期权基础工具类型
              ,OPTIONID                              --期权工具ID
              ,VOLATILITY                            --波动率
              ,STARTDATE                             --起始日期
              ,DUEDATE                               --到期日期
              ,ORIGINALMATURITY                      --原始期限
              ,RESIDUALM                             --剩余期限
              ,NEXTREPRICEDATE                       --下次重定价日
              ,NEXTREPRICEM                          --下次重定价期限
              ,RATETYPE                              --利率类型
              ,COUPONRATE                            --票面利率
              ,MODIFIEDDURATION                      --修正久期
              ,POSITIONTYPE                          --头寸属性
              ,POSITION                              --头寸
              ,CURRENCY                              --币种
              ,OPTIONUNDERLYINGNAME                  --期权基础工具名称
              ,ORGSORTNO                             --机构排序号

  )
  WITH OPI_FXDH_TEMP AS(
    
         SELECT         
            W.DEALNO || 'P' AS DEALNO, --流水号       
            W.COST AS COST, --成本中心        
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
            CASE WHEN SUBSTR(W.COST, 1, 4) = '3' THEN '01' ELSE '02' END AS BOOKTYPE --账户类型
            --NULL BUYZERORATE,                           --买入币种零息利率
            --NVL(NPVR.CCYDISCNPVFACTOR_10, 1) BUYDISCOUNTRATE,    --买入折现因子
            --NULL SELLZERORATE,                          --卖出币种零息利率
            --NULL SELLDISCOUNTRATE                       --卖出折现因子
       FROM RWA_DEV.OPI_FXDH W --外汇信息      
      WHERE SUBSTR(W.COST, 1, 1) = '2' --外汇业务   
        AND SUBSTR(W.COST, 4, 1) <> '3' --第四位<>3 --取交易账户下    
        AND SUBSTR(W.COST, 6, 1) = '2' --远期
        --AND (W.CCY <> 'CNY' AND W.CTRCCY <> 'CNY') 只有在计量外汇风险时  不考虑结售汇业务 即人民币对外币或外币对人民币        
        AND W.VDATE >= p_data_dt_str  --未到期数据        
        AND W.DATANO = p_data_dt_str 
        AND W.VERIND = '1'   --上线后放开该条件
        AND TRIM(W.REVDATE) IS NULL  --上线后放开该条件
          
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
            W.CCYNPVAMT+W.CTRNPVAMT AS CCYNPVAMT, --盯市价值
            W.CTRNPVAMT  AS AMT1,     --卖出现值        
            CASE WHEN SUBSTR(W.COST, 1, 4) = '3' THEN '01' ELSE '02' END AS  BOOKTYPE --账户类型
            --NULL BUYZERORATE,                           --买入币种零息利率
            --NULL BUYDISCOUNTRATE,    --买入折现因子
            --NULL SELLZERORATE,                          --卖出币种零息利率
            --NVL(NPVR.CTRDISCNPVFACTOR_10, 1) SELLDISCOUNTRATE                       --卖出折现因子
       FROM RWA_DEV.OPI_FXDH W --外汇信息       
      WHERE SUBSTR(W.COST, 1, 1) = '2' --外汇业务   
        AND SUBSTR(W.COST, 4, 1) <> '3' --第四位<>3 --取交易账户下    
        AND SUBSTR(W.COST, 6, 1) = '2' --远期
        --AND (W.CCY <> 'CNY' AND W.CTRCCY <> 'CNY') 只有在计量外汇风险时  不考虑结售汇业务 即人民币对外币或外币对人民币        
        AND W.VDATE >= p_data_dt_str  --未到期数据        
        AND W.DATANO = p_data_dt_str 
        AND W.VERIND = '1'   --上线后放开该条件
        AND TRIM(W.REVDATE) IS NULL  --上线后放开该条件    
  )
      SELECT
               TO_DATE(p_data_dt_str,'YYYYMMDD')               AS DATADATE                 --数据日期
              ,p_data_dt_str                                   AS DATANO                   --数据流水号
              ,'YSP'||T1.DEALNO                             AS EXPOSUREID               --风险暴露ID
              ,'02'                                         AS BOOKTYPE                 --账户类别
              ,T1.DEALNO                                    AS INSTRUMENTSID            --金融工具ID
              ,'0501'                                       AS INSTRUMENTSTYPE          --金融工具类型
              ,'6001'                                       AS ORGID                    --所属机构ID
              ,'重庆银行股份有限公司国际业务部'             AS ORGNAME                  --所属机构名称
              ,'01'                                         AS ORGTYPE                  --所属机构类型                                 默认：境内机构(01)
              ,'01'                                         AS MARKETRISKTYPE           --市场风险类型                                 默认：外汇风险(03)
              ,'02'                                         AS INTERATERISKTYPE         --利率风险类型                                 默认：空
              ,''                                           AS EQUITYRISKTYPE           --股票风险类型                                 默认：空
              ,''                                           AS EXCHANGERISKTYPE         --外汇风险类型                                 如果币种<>人民币，则需根据币种映射；人民币不需映射
              ,''                                           AS COMMODITYNAME            --商品种类名称                                 默认：空
              ,''                                           AS OPTIONRISKTYPE           --期权风险类型                                 默认：空
              ,'OPI'||TRIM(T2.CNO)                                AS ISSUERID                 --发行人ID                                     默认：空
              ,T2.CFN1                                        AS ISSUERNAME               --发行人名称                                   默认：空
              ,''                                           AS ISSUERTYPE               --发行人大类                                   默认：空
              ,''                                           AS ISSUERSUBTYPE            --发行人小类                                   默认：空
              ,CASE WHEN T2.CCODE='CN' THEN '01' 
              ELSE '02' END                                 AS ISSUERREGISTSTATE        --发行人注册国家                               默认：空
              ,''                                           AS ISSUERRCERATING          --发行人境外注册地外部评级                     默认：空
              ,'0'                                           AS SMBFLAG                  --小微企业标识                                 默认：空
              ,''                                           AS UNDERBONDFLAG            --是否承销债券                                 默认：空
              ,''                                           AS PAYMENTDATE              --缴款日                                       默认：空
              ,''                                           AS SECURITIESTYPE           --证券类别                                     默认：空
              ,''                                           AS BONDISSUEINTENT          --债券发行目的                                 默认：空
              ,''                                           AS CLAIMSLEVEL              --债权级别                                     默认：空
              ,''                                           AS REABSFLAG                --再资产证券化标识                             默认：空
              ,''                                           AS ORIGINATORFLAG           --是否发起机构                                 默认：空
              ,''                                           AS SECURITIESERATING        --证券外部评级                                 默认：空
              ,''                                           AS STOCKCODE                --股票/股指代码                                默认：空
              ,''                                           AS STOCKMARKET              --交易市场                                     默认：空
              ,''                                           AS EXCHANGEAREA             --交易地区                                     默认：空
              ,'0'                                          AS STRUCTURALEXPOFLAG       --是否结构性敞口
              ,'0'                                          AS OPTIONUNDERLYINGFLAG     --是否期权基础工具                             默认：否(0)
              ,''                                           AS OPTIONUNDERLYINGTYPE     --期权基础工具类型                             默认：空
              ,''                                           AS OPTIONID                 --期权工具ID                                   默认：空
              ,NULL                                         AS VOLATILITY               --波动率                                       默认：空
              ,T1.DEALDATE                                 AS STARTDATE                --起始日期                                     默认：空
              ,T1.VDATE                                   AS DUEDATE                  --到期日期                                     默认：空
              ,CASE WHEN (TO_DATE(T1.VDATE, 'YYYY-MM-DD') -
                TO_DATE(T1.DEALDATE, 'YYYY-MM-DD')) / 365 < 0 THEN 0
              ELSE (TO_DATE(T1.VDATE, 'YYYY-MM-DD')
              -TO_DATE(T1.DEALDATE, 'YYYY-MM-DD')) / 365
              END                                           AS ORIGINALMATURITY         --原始期限                                     默认：空
              ,CASE WHEN (TO_DATE(T1.VDATE, 'YYYY-MM-DD') -
                TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365 < 0 THEN  0
              ELSE (TO_DATE(T1.VDATE, 'YYYY-MM-DD') -
              TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365
              END                                           AS RESIDUALM                --剩余期限                                     默认：空
              ,''                                           AS NEXTREPRICEDATE          --下次重定价日                                 默认：空
              ,''                                           AS NEXTREPRICEM             --下次重定价期限                               默认：空
              ,'01'                                           AS RATETYPE                 --利率类型                                     默认：空
              ,T1.RATE                                       AS COUPONRATE               --票面利率                                     默认：空
              ,''                                           AS MODIFIEDDURATION         --修正久期                                     默认：空
              ,CASE 
                WHEN T1.PS = 'P' THEN '01' --多头  
                WHEN T1.PS = 'S' THEN '02' --空头
                ELSE '01' --多头
              END                                           AS POSITIONTYPE             --头寸属性
              ,T1.AMT1                                       AS POSITION                 --头寸
              ,T1.CCY                                       AS CURRENCY                 --币种
              ,''                                           AS OPTIONUNDERLYINGNAME    --期权基础工具名称
              ,'1290'                                       AS ORGSORTNO                 --机构排序号
            FROM OPI_FXDH_TEMP T1 --外汇信息  
            LEFT JOIN RWA_DEV.OPI_CUST T2
            ON TRIM(T1.CUST)=TRIM(T2.CNO)  
            AND T2.DATANO=p_data_dt_str
            ;
                        
    COMMIT;
    
    /* */
    
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_YSP_MARKETEXPOSURESTD',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_YSP_MARKETEXPOSURESTD;
    --Dbms_output.Put_line('RWA_DEV.RWA_WH_MARKETEXPOSURESTD表当前插入的国结系统-外汇(市场风险)-标准法暴露记录为: ' || v_count || ' 条');



    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
      p_po_rtnmsg  := '成功' || '-' || v_count;
        --定义异常
        EXCEPTION
    WHEN OTHERS THEN
                 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
            ROLLBACK;
                p_po_rtncode := sqlcode;
                p_po_rtnmsg  := '市场风险标准法暴露信息('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_YSP_MARKETEXPOSURESTD;
/

