CREATE OR REPLACE PROCEDURE RWA_DEV.pro_rwa_xn_exposure(
                                                p_data_dt_str  IN   VARCHAR2,    --数据日期
                                                p_po_rtncode   OUT  VARCHAR2,    --返回编号
                                                p_po_rtnmsg    OUT  VARCHAR2     --返回描述
                                               )
  /*
    存储过程名称:RWA_DEV.pro_rwa_xn_exposure
    实现功能:总账虚拟-信用风险暴露ETL转换
    数据口径:全量
    跑批频率:月末运行
    版  本  :V1.0.0
    编写人  :qpzhong
    编写时间:2016-9-20
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.FNS_GL_BALANCE                   |总账表
    源  表2 :RWA_DEV.NSS_PA_QUOTEPRICE                |汇率转换表
    源  表3 :RWA_DEV.RWA_EI_EXPOSURE                  |汇总-信用风险暴露表
    源  表4 :RWA_DEV.RWA_ARTICULATION_PARAM           |总账勾稽参数表
    源  表5 :RWA.CODE_LIBRARY                         |代码表
    源  表6 :RWA.ORG_INFO OI                          |机构表
    源  表7 :RWA.RWA_WS_XD_UNPUTOUT                   |已批未放业务补录表
    源  表8 :RWA_DEV.RWA_TMP_DERIVATION_SUBJECT       |总账勾稽衍生科目临时表
    源  表9 :RWA_DEV.RWA_TMP_GLBALANCE                |总账勾稽科目余额临时表
    目标表1 :RWA_DEV.RWA_EI_EXPOSURE                  |汇总-信用风险暴露表
    目标表2 :RWA_DEV.RWA_EI_CONTRACT                  |汇总-信用风险暴露表
    目标表3 :RWA_DEV.RWA_EI_CLIENT                    |汇总-信用风险暴露表
    目标表4 :RWA_DEV.RWA_TMP_DERIVATION_SUBJECT       |总账勾稽衍生科目临时表
    目标表5 :RWA_DEV.RWA_TMP_GLBALANCE                |总账勾稽科目余额临时表
    变更记录(修改人|修改时间|修改内容):
  */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.pro_rwa_xn_exposure';
  v_datadate date := to_date(p_data_dt_str,'yyyy/mm/dd');       --数据日期
  v_datano VARCHAR2(8) := to_char(v_datadate, 'yyyymmdd');      --数据流水号
  v_startdate VARCHAR2(10) := to_char(v_datadate,'yyyy-mm-dd'); --起始日期
  V_ILDDEBT NUMBER(24,6) := 0;
  --定义插入的记录数
  v_count INTEGER;


  CURSOR C_SERIALNO IS
  select T.SERIALNO,T.THIRD_SUBJECT_NO AS SUBJECT_NO
    from RWA_DEV.RWA_ARTICULATION_PARAM T
   where T.ARTICULATERELATION IS NOT NULL
     AND ARTICULATETYPE IN ('01', '04')
     AND ISCALCULATE = '1' --是否RWA计算 0否 1是
     AND ISINUSE = '1' --启用状态 1启用 0停用
     ;

   V_SERIALNO C_SERIALNO%ROWTYPE;

  BEGIN
    DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TMP_DERIVATION_SUBJECT';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TMP_GLBALANCE';

    --删除目标表当期数据
    DELETE FROM RWA_DEV.RWA_EI_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD') AND SSYSID IN ('XN');
    DELETE FROM RWA_DEV.RWA_EI_CLIENT WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD') AND SSYSID IN ('XN');
    DELETE FROM RWA_DEV.RWA_EI_CONTRACT WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD') AND SSYSID IN ('XN');
    COMMIT;

    --获取无形资产扣减项
    SELECT COUNT(1) INTO V_ILDDEBT FROM RWA_dEV.Rwa_Ei_Profitdist T WHERE T.DATADATE = V_DATADATE;

    IF V_ILDDEBT > 0 THEN
      SELECT T.ILDDEBT INTO V_ILDDEBT FROM RWA_dEV.Rwa_Ei_Profitdist T WHERE T.DATADATE = V_DATADATE;
    END IF;

    --获取存在勾稽关系的科目

    OPEN C_SERIALNO ;
    LOOP FETCH C_SERIALNO INTO V_SERIALNO;
    EXIT WHEN C_SERIALNO%NOTFOUND;

    INSERT INTO RWA_DEV.RWA_TMP_DERIVATION_SUBJECT
      WITH TEMP_SUbJECT1 AS  --提取简单加减的科目
       (
       SELECT DISTINCT DS.ARTICULATERELATION --原始科目号
                       ,REGEXP_SUBSTR(DS.ARTICULATERELATION,'[^-+]+', 1,LEVEL, 'i') AS REL_SUBJECT_NO --衍生科目
          FROM (
              SELECT * FROM RWA_DEV.RWA_ARTICULATION_PARAM    --总账勾稽参数表
              WHERE UPPER(ARTICULATERELATION) NOT LIKE 'MAX%'
               AND SERIALNO = V_SERIALNO.SERIALNO
           ) DS
           CONNECT BY LEVEL <= LENGTH(REGEXP_REPLACE(DS.ARTICULATERELATION, '[^-+]+', '')) + 1
        )
        ,
        TEMP_SUbJECT2 AS    --提取复杂科目
       (SELECT DISTINCT ARTICULATERELATION       --原始科目号
                       ,REGEXP_SUBSTR(ARTICULATERELATION2,'[^-+]+', 1,LEVEL, 'i') AS REL_SUBJECT_NO --衍生科目
          FROM(
             SELECT ARTICULATERELATION,
                    CASE WHEN INSTR(ARTICULATERELATION,',')>6 THEN SUBSTR(ARTICULATERELATION,5,INSTR(ARTICULATERELATION,',')-5)
                         ELSE SUBSTR(ARTICULATERELATION,7,LENGTH(ARTICULATERELATION)-7) END AS ARTICULATERELATION2
              FROM RWA_DEV.RWA_ARTICULATION_PARAM  --总账勾稽参数表
             WHERE UPPER(ARTICULATERELATION) LIKE 'MAX%'
               AND SERIALNO = V_SERIALNO.SERIALNO
           )
        CONNECT BY LEVEL <= LENGTH(REGEXP_REPLACE(ARTICULATERELATION2, '[^-+]+', '')) + 1)
      SELECT RAP.THIRD_SUBJECT_NO, TS.REL_SUBJECT_NO
        FROM RWA_DEV.RWA_ARTICULATION_PARAM RAP
       INNER JOIN (SELECT ARTICULATERELATION,REL_SUBJECT_NO FROM TEMP_SUBJECT1
                   UNION
                   SELECT ARTICULATERELATION,REL_SUBJECT_NO FROM TEMP_SUBJECT2
                  ) TS
          ON TS.ARTICULATERELATION = RAP.ARTICULATERELATION
       ORDER BY RAP.THIRD_SUBJECT_NO, TS.REL_SUBJECT_NO ASC;
    COMMIT;

     --初始化特殊科目信息
    INSERT INTO RWA_DEV.RWA_TMP_GLBALANCE(
           SUBJECT_NO,ORGID,CURRENCY,ACCOUNT_BALANCE
    )
    WITH TEMP_GL AS (
            SELECT FGB.SUBJECT_NO,
                   FGB.ORG_ID AS ORGID,
                   FGB.CURRENCY_CODE AS CURRENCY,
                   CASE WHEN CL.ATTRIBUTE8 = 'C-D'
                        THEN SUM(FGB.BALANCE_C * NVL(NPQ.MIDDLEPRICE/100, 1) - FGB.BALANCE_D * NVL(NPQ.MIDDLEPRICE/100, 1))
                        ELSE SUM(FGB.BALANCE_D * NVL(NPQ.MIDDLEPRICE/100, 1) - FGB.BALANCE_C * NVL(NPQ.MIDDLEPRICE/100, 1))
                    END AS ACCOUNT_BALANCE --科目余额
              FROM RWA_DEV.FNS_GL_BALANCE FGB           --总账表
              LEFT JOIN RWA_DEV.TMP_CURRENCY_CHANGE NPQ   --汇率转换表
                ON NPQ.DATANO = FGB.DATANO
               AND NPQ.CURRENCYCODE = FGB.CURRENCY_CODE
              LEFT JOIN RWA.CODE_LIBRARY CL     --代码表
                ON CL.CODENO = 'NewSubject'
               AND CL.ITEMNO = FGB.SUBJECT_NO
             WHERE FGB.DATANO = V_DATANO
               AND FGB.CURRENCY_CODE <> 'RMB'
               AND FGB.SUBJECT_NO  in (select articulaterelation from rwa_dev.rwa_tmp_derivation_subject where subject_no = v_serialno.subject_no)
             GROUP BY FGB.SUBJECT_NO, FGB.ORG_ID, CURRENCY_CODE,CL.ATTRIBUTE8
             ORDER BY FGB.SUBJECT_NO, FGB.ORG_ID, FGB.CURRENCY_CODE ASC
             )
      SELECT SUBJECT_NO,
             ORGID,
             CURRENCY,
             SUBSTR(COMPLEX_LOGIC_FUNCTION,INSTR(COMPLEX_LOGIC_FUNCTION,'@',1,1)+1) AS ACCOUNT_BALANCE
      FROM (SELECT SUBJECT_NO,
                   ORGID,
                   CURRENCY,
                   FUN_DERIVATION_SUBJECT(REL_SUBJECT_NO || '@' || LOGIC_FUNCTION || '@' || ACCOUNT_BALANCE) AS COMPLEX_LOGIC_FUNCTION
             FROM (
                  SELECT TAP2.SUBJECT_NO as SUBJECT_NO,
                         TAP2.ORGID as ORGID,
                         TAP2.CURRENCY,
                         REPLACE(UPPER(RAP.ARTICULATERELATION),'MAX','GREATEST') AS LOGIC_FUNCTION,
                         TAP2.ARTICULATERELATION AS REL_SUBJECT_NO,
                         NVL(GL.ACCOUNT_BALANCE, 0) AS ACCOUNT_BALANCE
                    FROM (
                          SELECT DISTINCT TAP.SUBJECT_NO,
                                 GL2.ORGID,
                                 GL2.CURRENCY,
                                 TAP.ARTICULATERELATION
                            FROM RWA_DEV.RWA_TMP_DERIVATION_SUBJECT TAP,TEMP_GL GL2
                    ) TAP2
                    LEFT JOIN TEMP_GL GL
                      ON TAP2.ARTICULATERELATION = GL.SUBJECT_NO
                     AND TAP2.CURRENCY = GL.CURRENCY
                     AND TAP2.ORGID = GL.ORGID
                     AND EXISTS (SELECT 1 FROM TEMP_GL GL2
                                  WHERE GL2.ORGID = GL.ORGID
                                    AND GL2.CURRENCY = GL.CURRENCY
                                    AND GL2.SUBJECT_NO = TAP2.SUBJECT_NO)
                   INNER JOIN RWA_DEV.RWA_ARTICULATION_PARAM RAP --总账勾稽参数表
                      ON RAP.THIRD_SUBJECT_NO = TAP2.SUBJECT_NO
                     AND RAP.ISCALCULATE = '1' --是否轧差（1是 0否）
                     AND RAP.ISINUSE = '1' --启用状态 1启用 0停用
                     AND RAP.ISGATHER = '0' --是否汇总到总行
                     AND RAP.ARTICULATETYPE IN ('01','04')
                     AND RAP.SERIALNO = V_SERIALNO.SERIALNO
                   UNION ALL
                   SELECT SUBJECT_NO,
                         '9998' as orgid,
                         'CNY' AS CURRENCY,
                         REPLACE(UPPER(ARTICULATERELATION), 'MAX', 'GREATEST') AS LOGIC_FUNCTION,
                         REL_SUBJECT_NO,
                         SUM(NVL(ACCOUNT_BALANCE, 0)) AS ACCOUNT_BALANCE
                    FROM (SELECT DISTINCT TAP2.SUBJECT_NO,
                                          TAP2.ARTICULATERELATION AS REL_SUBJECT_NO,
                                          GL.ACCOUNT_BALANCE,
                                          RAP.ARTICULATERELATION  AS ARTICULATERELATION
                            FROM (SELECT DISTINCT TAP.SUBJECT_NO,
                                                  TAP.ARTICULATERELATION
                                    FROM RWA_DEV.RWA_TMP_DERIVATION_SUBJECT TAP,
                                         TEMP_GL                            GL2
                                   WHERE GL2.ACCOUNT_BALANCE <> 0) TAP2
                            LEFT JOIN (SELECT SUBJECT_NO,
                                             SUM(ACCOUNT_BALANCE) AS ACCOUNT_BALANCE
                                        FROM TEMP_GL
                                       GROUP BY SUBJECT_NO) GL
                              ON TAP2.ARTICULATERELATION = GL.SUBJECT_NO
                             AND EXISTS (SELECT 1 FROM TEMP_GL GL2 WHERE GL2.SUBJECT_NO = TAP2.SUBJECT_NO)
                           INNER JOIN RWA_DEV.RWA_ARTICULATION_PARAM RAP --总账勾稽参数表
                              ON RAP.THIRD_SUBJECT_NO = TAP2.SUBJECT_NO
                             AND RAP.ISCALCULATE = '1' --是否轧差（1是 0否）
                             AND RAP.ISINUSE = '1' --启用状态 1启用 0停用
                             AND RAP.ISGATHER = '1' --是否汇总到总行
                             AND RAP.ARTICULATETYPE IN ('01', '04')
                             AND RAP.SERIALNO = V_SERIALNO.SERIALNO
                             )
                   GROUP BY SUBJECT_NO, REL_SUBJECT_NO, ARTICULATERELATION
                   )
              GROUP BY SUBJECT_NO, ORGID, CURRENCY
       );
       COMMIT;

END LOOP;
CLOSE C_SERIALNO;

dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_TMP_DERIVATION_SUBJECT',cascade => true);
dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_TMP_GLBALANCE',cascade => true);

    --2.总账虚拟数据插入目标
    INSERT INTO RWA_DEV.RWA_EI_EXPOSURE(
                 DataDate                                                     --数据日期
                ,DataNo                                                       --数据流水号
                ,ExposureID                                                   --风险暴露ID
                ,DueID                                                        --债项ID
                ,SSysID                                                       --源系统ID
                ,ContractID                                                   --合同ID
                ,ClientID                                                     --参与主体ID
                ,SOrgID                                                       --源机构ID
                ,SOrgName                                                     --源机构名称
                ,OrgSortNo                                                    --所属机构排序号
                ,OrgID                                                        --所属机构ID
                ,OrgName                                                      --所属机构名称
                ,AccOrgID                                                     --账务机构ID
                ,AccOrgName                                                   --账务机构名称
                ,IndustryID                                                   --所属行业代码
                ,IndustryName                                                 --所属行业名称
                ,BusinessLine                                                 --业务条线
                ,AssetType                                                    --资产大类
                ,AssetSubType                                                 --资产小类
                ,BusinessTypeID                                               --业务品种代码
                ,BusinessTypeName                                             --业务品种名称
                ,CreditRiskDataType                                           --信用风险数据类型
                ,AssetTypeOfHaircuts                                          --折扣系数对应资产类别
                ,BusinessTypeSTD                                              --权重法业务类型
                ,ExpoClassSTD                                                 --权重法暴露大类
                ,ExpoSubClassSTD                                              --权重法暴露小类
                ,ExpoClassIRB                                                 --内评法暴露大类
                ,ExpoSubClassIRB                                              --内评法暴露小类
                ,ExpoBelong                                                   --暴露所属标识
                ,BookType                                                     --账户类别
                ,ReguTranType                                                 --监管交易类型
                ,RepoTranFlag                                                 --回购交易标识
                ,RevaFrequency                                                --重估频率
                ,Currency                                                     --币种
                ,NormalPrincipal                                              --正常本金余额
                ,OverdueBalance                                               --逾期余额
                ,NonAccrualBalance                                            --非应计余额
                ,OnSheetBalance                                               --表内余额
                ,NormalInterest                                               --正常利息
                ,OnDebitInterest                                              --表内欠息
                ,OffDebitInterest                                             --表外欠息
                ,ExpenseReceivable                                            --应收费用
                ,AssetBalance                                                 --资产余额
                ,AccSubject1                                                  --科目一
                ,AccSubject2                                                  --科目二
                ,AccSubject3                                                  --科目三
                ,StartDate                                                    --起始日期
                ,DueDate                                                      --到期日期
                ,OriginalMaturity                                             --原始期限
                ,ResidualM                                                    --剩余期限
                ,RiskClassify                                                 --风险分类
                ,ExposureStatus                                               --风险暴露状态
                ,OverdueDays                                                  --逾期天数
                ,SpecialProvision                                             --专项准备金
                ,GeneralProvision                                             --一般准备金
                ,EspecialProvision                                            --特别准备金
                ,WrittenOffAmount                                             --已核销金额
                ,OffExpoSource                                                --表外暴露来源
                ,OffBusinessType                                              --表外业务类型
                ,OffBusinessSdvsSTD                                           --权重法表外业务类型细分
                ,UncondCancelFlag                                             --是否可随时无条件撤销
                ,CCFLevel                                                     --信用转换系数级别
                ,CCFAIRB                                                      --高级法信用转换系数
                ,ClaimsLevel                                                  --债权级别
                ,BondFlag                                                     --是否为债券
                ,BondIssueIntent                                              --债券发行目的
                ,NSURealPropertyFlag                                          --是否非自用不动产
                ,RepAssetTermType                                             --抵债资产期限类型
                ,DependOnFPOBFlag                                             --是否依赖于银行未来盈利
                ,IRating                                                      --内部评级
                ,PD                                                           --违约概率
                ,LGDLevel                                                     --违约损失率级别
                ,LGDAIRB                                                      --高级法违约损失率
                ,MAIRB                                                        --高级法有效期限
                ,EADAIRB                                                      --高级法违约风险暴露
                ,DefaultFlag                                                  --违约标识
                ,BEEL                                                         --已违约暴露预期损失比率
                ,DefaultLGD                                                   --已违约暴露违约损失率
                ,EquityExpoFlag                                               --股权暴露标识
                ,EquityInvestType                                             --股权投资对象类型
                ,EquityInvestCause                                            --股权投资形成原因
                ,SLFlag                                                       --专业贷款标识
                ,SLType                                                       --专业贷款类型
                ,PFPhase                                                      --项目融资阶段
                ,ReguRating                                                   --监管评级
                ,CBRCMPRatingFlag                                             --银监会认定评级是否更为审慎
                ,LargeFlucFlag                                                --是否波动性较大
                ,LiquExpoFlag                                                 --是否清算过程中风险暴露
                ,PaymentDealFlag                                              --是否货款对付模式
                ,DelayTradingDays                                             --延迟交易天数
                ,SecuritiesFlag                                               --有价证券标识
                ,SecuIssuerID                                                 --证券发行人ID
                ,RatingDurationType                                           --评级期限类型
                ,SecuIssueRating                                              --证券发行等级
                ,SecuResidualM                                                --证券剩余期限
                ,SecuRevaFrequency                                            --证券重估频率
                ,CCPTranFlag                                                  --是否中央交易对手相关交易
                ,CCPID                                                        --中央交易对手ID
                ,QualCCPFlag                                                  --是否合格中央交易对手
                ,BankRole                                                     --银行角色
                ,ClearingMethod                                               --清算方式
                ,BankAssetFlag                                                --是否银行提交资产
                ,MatchConditions                                              --符合条件情况
                ,SFTFlag                                                      --证券融资交易标识
                ,MasterNetAgreeFlag                                           --净额结算主协议标识
                ,MasterNetAgreeID                                             --净额结算主协议ID
                ,SFTType                                                      --证券融资交易类型
                ,SecuOwnerTransFlag                                           --证券所有权是否转移
                ,OTCFlag                                                      --场外衍生工具标识
                ,ValidNettingFlag                                             --有效净额结算协议标识
                ,ValidNetAgreementID                                          --有效净额结算协议ID
                ,OTCType                                                      --场外衍生工具类型
                ,DepositRiskPeriod                                            --保证金风险期间
                ,MTM                                                          --重置成本
                ,MTMCurrency                                                  --重置成本币种
                ,BuyerOrSeller                                                --买方卖方
                ,QualROFlag                                                   --合格参照资产标识
                ,ROIssuerPerformFlag                                          --参照资产发行人是否能履约
                ,BuyerInsolvencyFlag                                          --信用保护买方是否破产
                ,NonpaymentFees                                               --尚未支付费用
                ,RetailExpoFlag                                               --零售暴露标识
                ,RetailClaimType                                              --零售债权类型
                ,MortgageType                                                 --住房抵押贷款类型
                ,ExpoNumber                                                   --风险暴露个数
                ,LTV                                                          --贷款价值比
                ,Aging                                                        --账龄
                ,NewDefaultDebtFlag                                           --新增违约债项标识
                ,PDPoolModelID                                                --PD分池模型ID
                ,LGDPoolModelID                                               --LGD分池模型ID
                ,CCFPoolModelID                                               --CCF分池模型ID
                ,PDPoolID                                                     --所属PD池ID
                ,LGDPoolID                                                    --所属LGD池ID
                ,CCFPoolID                                                    --所属CCF池ID
                ,ABSUAFlag                                                    --资产证券化基础资产标识
                ,ABSPoolID                                                    --证券化资产池ID
                ,GroupID                                                      --分组编号
                ,DefaultDate                                                  --违约时点
                ,ABSPROPORTION                                                --资产证券化比重
                ,DEBTORNUMBER                                                 --借款人个数
    )
    SELECT
                DISTINCT
                V_DATADATE                                                        AS DATADATE                   --数据日期
                ,V_DATANO                                                         AS DATANO                     --数据流水号
                ,'XN-' || GL.SUBJECT_NO || '-' || GL.ORGID || '-' || GL.CURRENCY  AS EXPOSUREID                 --风险暴露代号              (ZZ- || 科目 || 账务机构 || 币种 )
                ,'XN-' || GL.SUBJECT_NO || '-' || GL.ORGID || '-' || GL.CURRENCY  AS DUEID                      --债项代号
                ,'XN'                                                             AS SSYSID                     --源系统代号
                ,'XN-' || GL.SUBJECT_NO || '-' || GL.ORGID || '-' || GL.CURRENCY  AS CONTRACTID                 --合同代号
                ,'XN-' || GL.SUBJECT_NO || '-' || GL.ORGID || '-' || GL.CURRENCY  AS CLIENTID                   --参与主体代号
                ,GL.ORGID                                                         AS SORGID                     --源机构代号
                ,GL.ORGNAME                                                       AS SORGNAME                   --源机构名称
                ,'1010'                                                              AS ORGSORTNO                  --机构排序号
                ,GL.ORGID                                                         AS ORGID                      --所属机构代号
                ,GL.ORGNAME                                                       AS ORGNAME                    --所属机构名称
                ,GL.ORGID                                                         AS ACCORGID                   --账务机构代号
                ,GL.ORGNAME                                                       AS ACCORGNAME                 --账务机构名称
                ,'999999'                                                         AS INDUSTRYID                 --所属行业代码
                ,'未知'                                                           AS industryname               --所属行业名称              (系统转换)
                ,'0501'                                                           AS BUSINESSLINE               --条线
                ,''                                                               AS ASSETTYPE                  --资产大类                  (根据科目转换)
                ,''                                                               AS ASSETSUBTYPE               --资产小类                  (根据科目转换)
                ,'9010101010'                                                     AS BUSINESSTYPEID             --业务品种代号
                ,'虚拟业务品种'                                                   AS BUSINESSTYPENAME           --业务品种名称
                ,CASE WHEN RETAILFLAG='1' THEN '02' ELSE '01' END                 AS CREDITRISKDATATYPE         --信用风险数据类型          (默认'一般非零售',01一般非零售,02一般零售)
                ,'01'                                                             AS ASSETTYPEOFHAIRCUTS        --折扣系数对应资产类别      (默认'现金及现金等价物',01现金及现金等价物)
                ,''                                                               AS BUSINESSTYPESTD            --权重法业务类型            (系统转换)
                ,''                                                               AS EXPOCLASSSTD               --权重法暴露大类            (系统转换)
                ,''                                                               AS EXPOSUBCLASSSTD            --权重法暴露小类            (系统转换)
                ,''                                                               AS EXPOCLASSIRB               --内评法暴露大类            (系统转换)
                ,''                                                               AS EXPOSUBCLASSIRB            --内评法暴露小类            (系统转换)
                ,CASE WHEN SUBSTR(GL.SUBJECT_NO,1,1) = '7' THEN '02' ELSE '01' END AS EXPOBELONG                 --暴露所属标识
                , '01'                                                            AS BOOKTYPE                   --账户类别
                ,'03'                                                             AS REGUTRANTYPE               --监管交易类型              (默认'抵押贷款',01回购交易,02其他资本市场交易,03抵押贷款)
                ,'0'                                                              AS REPOTRANFLAG               --回购交易标识              (默认为否,1是0否)
                ,1                                                                AS REVAFREQUENCY              --重估频率
                ,GL.CURRENCY                                                      AS CURRENCY                   --币种
                ,CASE WHEN GL.SUBJECT_NO = '17010000' THEN GL.ACCOUNT_BALANCE - NVL(V_ILDDEBT,0)
                ELSE GL.ACCOUNT_BALANCE END                                       AS NORMALPRINCIPAL            --正常本金余额              (金额绝对值进行计算)
                ,0                                                                AS OVERDUEBALANCE             --逾期余额
                ,0                                                                AS NONACCRUALBALANCE          --非应计余额
                ,CASE WHEN GL.SUBJECT_NO = '17010000' THEN GL.ACCOUNT_BALANCE - NVL(V_ILDDEBT,0)
                ELSE GL.ACCOUNT_BALANCE END + 0 + 0                               AS ONSHEETBALANCE             --表内余额
                ,0                                                                AS NORMALINTEREST             --正常利息
                ,0                                                                AS ONDEBITINTEREST            --表内欠息
                ,0                                                                AS OFFDEBITINTEREST           --表外欠息
                ,0                                                                AS EXPENSERECEIVABLE          --应收费用
                ,(CASE WHEN GL.SUBJECT_NO = '17010000' THEN GL.ACCOUNT_BALANCE - NVL(V_ILDDEBT,0)
                ELSE GL.ACCOUNT_BALANCE END+ 0 + 0) + (0 + 0 + 0) + 0             AS ASSETBALANCE               --资产余额
                ,GL.SUBJECT_NO                                                    AS ACCSUBJECT1                --科目一
                ,''                                                               AS ACCSUBJECT2                --科目二
                ,''                                                               AS ACCSUBJECT3                --科目三
                ,V_STARTDATE                                                      AS STARTDATE                  --起始日期
                ,TO_CHAR(ADD_MONTHS(V_DATADATE,1),'YYYY-MM-DD')                   AS DUEDATE                    --到期日期                  (数据日期 + 1个月)
                ,(ADD_MONTHS(V_DATADATE,1) - V_DATADATE)/365                      AS ORIGINALMATURITY           --原始期限                  (单位年)
                ,(ADD_MONTHS(V_DATADATE,1) - V_DATADATE)/365                      AS RESIDUALM                  --剩余期限                  (单位年)
                ,'01'                                                             AS RISKCLASSIFY               --风险分类
                ,''                                                               AS EXPOSURESTATUS             --风险暴露状态               (默认为空)
                ,0                                                                AS OVERDUEDAYS                --逾期天数
                ,0                                                                AS SPECIALPROVISION           --专项准备金
                ,0                                                                AS GENERALPROVISION           --一般准备金
                ,0                                                                AS ESPECIALPROVISION          --特别准备金
                ,0                                                                AS WRITTENOFFAMOUNT           --已核销金额
                ,CASE WHEN SUBSTR(GL.SUBJECT_NO,1,1) = '7' THEN '03' ELSE '' END  AS OFFEXPOSOURCE              --表外暴露来源
                ,''                                                               AS OFFBUSINESSTYPE            --表外业务类型              (系统转换)
                ,''                                                               AS OFFBUSINESSSDVSSTD         --权重法表外业务类型细分    (系统转换)
                ,'0'                                                              AS UNCONDCANCELFLAG           --是否可随时无条件撤销      (默认为否,1是0否)
                ,''                                                               AS CCFLEVEL                   --信用转换系数级别
                ,0                                                                AS CCFAIRB                    --高级法下信用转换系数
                ,'01'                                                             AS CLAIMSLEVEL                --债权级别                  (默认为高级债权;01高级债权,02次级债权)
                ,'0'                                                              AS BONDFLAG                   --是否为债券                (默认为否,1是0否)
                ,''                                                               AS BONDISSUEINTENT            --债券发行目的
                ,'0'                                                              AS NSUREALPROPERTYFLAG        --是否非自用不动产          (默认为否,1是0否)
                ,''                                                               AS REPASSETTERMTYPE           --抵债资产期限类型
                ,'0'                                                              AS DEPENDONFPOBFLAG           --是否依赖于银行未来盈利    (默认为否,1是0否)
                ,''                                                               AS IRATING                    --内部评级
                ,NULL                                                             AS PD                         --违约概率
                ,''                                                               AS LGDLEVEL                   --违约损失率级别
                ,NULL                                                             AS LGDAIRB                    --高级法下违约损失率
                ,NULL                                                             AS MAIRB                      --高级法下有效期限
                ,NULL                                                             AS EADAIRB                    --高级法下违约风险暴露
                ,'0'                                                              AS DEFAULTFLAG                --违约标识                  (默认为否,1是0否)
                ,NULL                                                             AS BEEL                       --已违约暴露预期损失比率
                ,NULL                                                             AS DEFAULTLGD                 --已违约暴露违约损失率
                ,'0'                                                              AS EQUITYEXPOFLAG             --股权暴露标识              (默认为否,1是0否)
                ,''                                                               AS EQUITYINVESTTYPE           --股权投资对象类型
                ,''                                                               AS EQUITYINVESTCAUSE          --股权投资形成原因
                ,'0'                                                              AS SLFLAG                     --专业贷款标识              (默认为否,1是0否)
                ,''                                                               AS SLTYPE                     --专业贷款类型
                ,''                                                               AS PFPHASE                    --项目融资阶段
                ,''                                                               AS REGURATING                 --监管评级
                ,'0'                                                              AS CBRCMPRATINGFLAG           --银监会认定评级是否更为审慎(默认为否,1是0否)
                ,'0'                                                              AS LARGEFLUCFLAG              --是否波动性较大            (默认为否,1是0否)
                ,'0'                                                              AS LIQUEXPOFLAG               --是否清算过程中风险暴露    (默认为否,1是0否)
                ,'0'                                                              AS PAYMENTDEALFLAG            --是否货款对付模式          (默认为否,1是0否)
                ,0                                                                AS DELAYTRADINGDAYS           --延迟交易天数
                ,'0'                                                              AS SECURITIESFLAG             --有价证券标识              (默认为否,1是0否)
                ,''                                                               AS SECUISSUERID               --证券发行人代号
                ,''                                                               AS RATINGDURATIONTYPE         --评级期限类型
                ,''                                                               AS SECUISSUERATING            --证券发行等级
                ,NULL                                                             AS SECURESIDUALM              --证券剩余期限
                ,''                                                               AS SECUREVAFREQUENCY          --证券重估频率
                ,'0'                                                              AS CCPTRANFLAG                --是否中央交易对手相关交易  (默认为否,1是0否)
                ,''                                                               AS CCPID                      --中央交易对手ID
                ,'0'                                                              AS QUALCCPFLAG                --是否合格中央交易对手      (默认为否,1是0否)
                ,''                                                               AS BANKROLE                   --银行角色
                ,''                                                               AS CLEARINGMETHOD             --清算方式
                ,'0'                                                              AS BANKASSETFLAG              --是否银行提交资产          (默认为否,1是0否)
                ,''                                                               AS MATCHCONDITIONS            --符合条件情况
                ,'0'                                                              AS SFTFLAG                    --证券融资交易标识          (默认为否,1是0否)
                ,'0'                                                              AS MASTERNETAGREEFLAG         --净额结算主协议标识        (默认为否,1是0否)
                ,''                                                               AS MASTERNETAGREEID           --净额结算主协议代号
                ,''                                                               AS SFTTYPE                    --证券融资交易类型
                ,'0'                                                              AS SECUOWNERTRANSFLAG         --证券所有权是否转移        (默认为否,1是0否)
                ,'0'                                                              AS OTCFLAG                    --场外衍生工具标识          (默认为否,1是0否)
                ,'0'                                                              AS VALIDNETTINGFLAG           --有效净额结算协议标识      (默认为否,1是0否)
                ,''                                                               AS VALIDNETAGREEMENTID        --有效净额结算协议代号
                ,''                                                               AS OTCTYPE                    --场外衍生工具类型
                ,0                                                                AS DEPOSITRISKPERIOD          --保证金风险期间
                ,0                                                                AS MTM                        --重置成本
                ,''                                                               AS MTMCURRENCY                --重置成本币种
                ,''                                                               AS BUYERORSELLER              --买方卖方
                ,'0'                                                              AS QUALROFLAG                 --合格参照资产标识          (默认为否,1是0否)
                ,'0'                                                              AS ROISSUERPERFORMFLAG        --参照资产发行人是否能履约  (默认为否,1是0否)
                ,'0'                                                              AS BUYERINSOLVENCYFLAG        --信用保护买方是否破产      (默认为否,1是0否)
                ,''                                                               AS NONPAYMENTFEES             --尚未支付费用
                ,RETAILFLAG                                                       AS RETAILEXPOFLAG             --零售暴露标识              (默认为否,1是0否)
                ,''                                                               AS RETAILCLAIMTYPE            --零售债权类型
                ,''                                                               AS MORTGAGETYPE               --住房抵押贷款类型
                ,1                                                                AS ExpoNumber                 --风险暴露个数                默认 1
                ,0.8                                                              AS LTV                        --贷款价值比                 默认 0.8
                ,NULL                                                             AS Aging                      --账龄                        默认 NULL
                ,''                                                               AS NewDefaultDebtFlag         --新增违约债项标识            默认 NULL
                ,''                                                               AS PDPoolModelID              --PD分池模型ID                默认 NULL
                ,''                                                               AS LGDPoolModelID             --LGD分池模型ID               默认 NULL
                ,''                                                               AS CCFPoolModelID             --CCF分池模型ID               默认 NULL
                ,''                                                               AS PDPoolID                   --所属PD池ID                 默认 NULL
                ,''                                                               AS LGDPoolID                  --所属LGD池ID                默认 NULL
                ,''                                                               AS CCFPoolID                  --所属CCF池ID                默认 NULL
                ,'0'                                                              AS ABSUAFlag                  --资产证券化基础资产标识     默认 否(0)
                ,''                                                               AS ABSPoolID                  --证券化资产池ID              默认 NULL
                ,''                                                               AS GroupID                    --分组编号                    默认 NULL
                ,NULL                                                             AS DefaultDate                --违约时点
                ,NULL                                                             AS ABSPROPORTION              --资产证券化比重
                ,NULL                                                             AS DEBTORNUMBER               --借款人个数
     FROM (
            SELECT FGB.SUBJECT_NO,
                   FGB.ORG_ID AS ORGID,
                   OI.ORGNAME ,
                   FGB.CURRENCY_CODE AS CURRENCY,
                   RAP.RETAILFLAG,
                   CASE WHEN CL.ATTRIBUTE8 = 'C-D'
                        THEN SUM(FGB.BALANCE_C * NVL(NPQ.MIDDLEPRICE/100, 1) - FGB.BALANCE_D * NVL(NPQ.MIDDLEPRICE/100, 1))
                        ELSE SUM(FGB.BALANCE_D * NVL(NPQ.MIDDLEPRICE/100, 1) - FGB.BALANCE_C * NVL(NPQ.MIDDLEPRICE/100, 1))
                        END AS ACCOUNT_BALANCE --科目余额
              FROM RWA_DEV.FNS_GL_BALANCE FGB
              LEFT JOIN RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                ON NPQ.DATANO = FGB.DATANO
               AND NPQ.CURRENCYCODE = FGB.CURRENCY_CODE
               LEFT JOIN RWA.ORG_INFO OI
                ON OI.ORGID = FGB.ORG_ID
             INNER JOIN RWA_DEV.RWA_ARTICULATION_PARAM RAP
                ON RAP.THIRD_SUBJECT_NO = FGB.SUBJECT_NO
              LEFT JOIN RWA.CODE_LIBRARY CL
                ON CL.CODENO = 'NewSubject'
               AND cl.itemno = fgb.subject_no
             WHERE FGB.DATANO = V_DATANO
               AND FGB.CURRENCY_CODE <> 'RMB'
               AND RAP.ISGATHER = '0'
               AND RAP.ISCALCULATE = '1'
               AND RAP.ARTICULATETYPE IN ('01','04')
               AND RAP.ARTICULATERELATION IS NULL
             GROUP BY FGB.SUBJECT_NO, FGB.ORG_ID,OI.ORGNAME, CURRENCY_CODE, RAP.RETAILFLAG, CL.ATTRIBUTE8
             UNION ALL
             SELECT FGB.SUBJECT_NO,
                   '9998' AS ORGID,
                   '重庆银行' AS ORGNAME ,
                   FGB.CURRENCY_CODE AS CURRENCY,
                   RAP.RETAILFLAG,
                  CASE WHEN CL.ATTRIBUTE8 = 'C-D'
                   THEN  SUM(FGB.BALANCE_C * NVL(NPQ.MIDDLEPRICE/100, 1) - FGB.BALANCE_D * NVL(NPQ.MIDDLEPRICE/100, 1))
                     ELSE SUM(FGB.BALANCE_D * NVL(NPQ.MIDDLEPRICE/100, 1) - FGB.BALANCE_C * NVL(NPQ.MIDDLEPRICE/100, 1))  END AS ACCOUNT_BALANCE --科目余额
              FROM RWA_DEV.FNS_GL_BALANCE FGB
              LEFT JOIN RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                ON NPQ.DATANO = FGB.DATANO
               AND NPQ.CURRENCYCODE = FGB.CURRENCY_CODE
              LEFT JOIN RWA_DEV.RWA_ARTICULATION_PARAM RAP
                ON RAP.THIRD_SUBJECT_NO = FGB.SUBJECT_NO
              LEFT JOIN RWA.CODE_LIBRARY CL
                ON CL.CODENO = 'NewSubject'
               and cl.itemno = fgb.subject_no
             WHERE FGB.DATANO = V_DATANO
               AND FGB.CURRENCY_CODE <> 'RMB'
               AND RAP.ISGATHER = '1'
               AND RAP.ISCALCULATE = '1'
               AND RAP.ARTICULATETYPE IN ('01','04')
               AND RAP.ARTICULATERELATION IS NULL
             GROUP BY FGB.SUBJECT_NO, CURRENCY_CODE,RAP.RETAILFLAG,CL.ATTRIBUTE8
    ) GL
     WHERE GL.ACCOUNT_BALANCE<>0
       AND GL.SUBJECT_NO NOT IN ( SELECT ACCSUBJECT1
                                    FROM RWA_DEV.RWA_EI_EXPOSURE
                                   WHERE DATADATE = V_DATADATE
                                     AND ACCSUBJECT1 IS NOT NULL
                                     AND SSYSID <> 'GC' )
    ;
    COMMIT;

    /*特殊科目处理*/
    INSERT INTO RWA_DEV.RWA_EI_EXPOSURE(
                 DataDate                                                     --数据日期
                ,DataNo                                                       --数据流水号
                ,ExposureID                                                   --风险暴露ID
                ,DueID                                                        --债项ID
                ,SSysID                                                       --源系统ID
                ,ContractID                                                   --合同ID
                ,ClientID                                                     --参与主体ID
                ,SOrgID                                                       --源机构ID
                ,SOrgName                                                     --源机构名称
                ,OrgSortNo                                                    --所属机构排序号
                ,OrgID                                                        --所属机构ID
                ,OrgName                                                      --所属机构名称
                ,AccOrgID                                                     --账务机构ID
                ,AccOrgName                                                   --账务机构名称
                ,IndustryID                                                   --所属行业代码
                ,IndustryName                                                 --所属行业名称
                ,BusinessLine                                                 --业务条线
                ,AssetType                                                    --资产大类
                ,AssetSubType                                                 --资产小类
                ,BusinessTypeID                                               --业务品种代码
                ,BusinessTypeName                                             --业务品种名称
                ,CreditRiskDataType                                           --信用风险数据类型
                ,AssetTypeOfHaircuts                                          --折扣系数对应资产类别
                ,BusinessTypeSTD                                              --权重法业务类型
                ,ExpoClassSTD                                                 --权重法暴露大类
                ,ExpoSubClassSTD                                              --权重法暴露小类
                ,ExpoClassIRB                                                 --内评法暴露大类
                ,ExpoSubClassIRB                                              --内评法暴露小类
                ,ExpoBelong                                                   --暴露所属标识
                ,BookType                                                     --账户类别
                ,ReguTranType                                                 --监管交易类型
                ,RepoTranFlag                                                 --回购交易标识
                ,RevaFrequency                                                --重估频率
                ,Currency                                                     --币种
                ,NormalPrincipal                                              --正常本金余额
                ,OverdueBalance                                               --逾期余额
                ,NonAccrualBalance                                            --非应计余额
                ,OnSheetBalance                                               --表内余额
                ,NormalInterest                                               --正常利息
                ,OnDebitInterest                                              --表内欠息
                ,OffDebitInterest                                             --表外欠息
                ,ExpenseReceivable                                            --应收费用
                ,AssetBalance                                                 --资产余额
                ,AccSubject1                                                  --科目一
                ,AccSubject2                                                  --科目二
                ,AccSubject3                                                  --科目三
                ,StartDate                                                    --起始日期
                ,DueDate                                                      --到期日期
                ,OriginalMaturity                                             --原始期限
                ,ResidualM                                                    --剩余期限
                ,RiskClassify                                                 --风险分类
                ,ExposureStatus                                               --风险暴露状态
                ,OverdueDays                                                  --逾期天数
                ,SpecialProvision                                             --专项准备金
                ,GeneralProvision                                             --一般准备金
                ,EspecialProvision                                            --特别准备金
                ,WrittenOffAmount                                             --已核销金额
                ,OffExpoSource                                                --表外暴露来源
                ,OffBusinessType                                              --表外业务类型
                ,OffBusinessSdvsSTD                                           --权重法表外业务类型细分
                ,UncondCancelFlag                                             --是否可随时无条件撤销
                ,CCFLevel                                                     --信用转换系数级别
                ,CCFAIRB                                                      --高级法信用转换系数
                ,ClaimsLevel                                                  --债权级别
                ,BondFlag                                                     --是否为债券
                ,BondIssueIntent                                              --债券发行目的
                ,NSURealPropertyFlag                                          --是否非自用不动产
                ,RepAssetTermType                                             --抵债资产期限类型
                ,DependOnFPOBFlag                                             --是否依赖于银行未来盈利
                ,IRating                                                      --内部评级
                ,PD                                                           --违约概率
                ,LGDLevel                                                     --违约损失率级别
                ,LGDAIRB                                                      --高级法违约损失率
                ,MAIRB                                                        --高级法有效期限
                ,EADAIRB                                                      --高级法违约风险暴露
                ,DefaultFlag                                                  --违约标识
                ,BEEL                                                         --已违约暴露预期损失比率
                ,DefaultLGD                                                   --已违约暴露违约损失率
                ,EquityExpoFlag                                               --股权暴露标识
                ,EquityInvestType                                             --股权投资对象类型
                ,EquityInvestCause                                            --股权投资形成原因
                ,SLFlag                                                       --专业贷款标识
                ,SLType                                                       --专业贷款类型
                ,PFPhase                                                      --项目融资阶段
                ,ReguRating                                                   --监管评级
                ,CBRCMPRatingFlag                                             --银监会认定评级是否更为审慎
                ,LargeFlucFlag                                                --是否波动性较大
                ,LiquExpoFlag                                                 --是否清算过程中风险暴露
                ,PaymentDealFlag                                              --是否货款对付模式
                ,DelayTradingDays                                             --延迟交易天数
                ,SecuritiesFlag                                               --有价证券标识
                ,SecuIssuerID                                                 --证券发行人ID
                ,RatingDurationType                                           --评级期限类型
                ,SecuIssueRating                                              --证券发行等级
                ,SecuResidualM                                                --证券剩余期限
                ,SecuRevaFrequency                                            --证券重估频率
                ,CCPTranFlag                                                  --是否中央交易对手相关交易
                ,CCPID                                                        --中央交易对手ID
                ,QualCCPFlag                                                  --是否合格中央交易对手
                ,BankRole                                                     --银行角色
                ,ClearingMethod                                               --清算方式
                ,BankAssetFlag                                                --是否银行提交资产
                ,MatchConditions                                              --符合条件情况
                ,SFTFlag                                                      --证券融资交易标识
                ,MasterNetAgreeFlag                                           --净额结算主协议标识
                ,MasterNetAgreeID                                             --净额结算主协议ID
                ,SFTType                                                      --证券融资交易类型
                ,SecuOwnerTransFlag                                           --证券所有权是否转移
                ,OTCFlag                                                      --场外衍生工具标识
                ,ValidNettingFlag                                             --有效净额结算协议标识
                ,ValidNetAgreementID                                          --有效净额结算协议ID
                ,OTCType                                                      --场外衍生工具类型
                ,DepositRiskPeriod                                            --保证金风险期间
                ,MTM                                                          --重置成本
                ,MTMCurrency                                                  --重置成本币种
                ,BuyerOrSeller                                                --买方卖方
                ,QualROFlag                                                   --合格参照资产标识
                ,ROIssuerPerformFlag                                          --参照资产发行人是否能履约
                ,BuyerInsolvencyFlag                                          --信用保护买方是否破产
                ,NonpaymentFees                                               --尚未支付费用
                ,RetailExpoFlag                                               --零售暴露标识
                ,RetailClaimType                                              --零售债权类型
                ,MortgageType                                                 --住房抵押贷款类型
                ,ExpoNumber                                                   --风险暴露个数
                ,LTV                                                          --贷款价值比
                ,Aging                                                        --账龄
                ,NewDefaultDebtFlag                                           --新增违约债项标识
                ,PDPoolModelID                                                --PD分池模型ID
                ,LGDPoolModelID                                               --LGD分池模型ID
                ,CCFPoolModelID                                               --CCF分池模型ID
                ,PDPoolID                                                     --所属PD池ID
                ,LGDPoolID                                                    --所属LGD池ID
                ,CCFPoolID                                                    --所属CCF池ID
                ,ABSUAFlag                                                    --资产证券化基础资产标识
                ,ABSPoolID                                                    --证券化资产池ID
                ,GroupID                                                      --分组编号
                ,DefaultDate                                                  --违约时点
                ,ABSPROPORTION                                                --资产证券化比重
                ,DEBTORNUMBER                                                 --借款人个数
    )
    SELECT
                V_DATADATE                                                         AS DATADATE                   --数据日期
                ,V_DATANO                                                          AS DATANO                     --数据流水号
                ,'XN-' || GL.SUBJECT_NO || '-' || GL.ORGID || '-' || GL.CURRENCY   AS EXPOSUREID                 --风险暴露代号
                ,'XN-' || GL.SUBJECT_NO || '-' || GL.ORGID || '-' || GL.CURRENCY   AS DUEID                      --债项代号
                ,'XN'                                                              AS SSYSID                     --源系统代号
                ,'XN-' || GL.SUBJECT_NO || '-' || GL.ORGID || '-' || GL.CURRENCY   AS CONTRACTID                 --合同代号
                ,'XN-' || GL.SUBJECT_NO || '-' || GL.ORGID || '-' || GL.CURRENCY   AS CLIENTID                   --参与主体代号
                ,GL.ORGID                                                          AS SORGID                     --源机构代号
                ,OI.ORGNAME                                                        AS SORGNAME                   --源机构名称
                ,OI.SORTNO                                                         AS ORGSORTNO                  --机构排序号
                ,GL.ORGID                                                          AS ORGID                      --所属机构代号
                ,OI.ORGNAME                                                        AS ORGNAME                    --所属机构名称
                ,GL.ORGID                                                          AS ACCORGID                   --账务机构代号
                ,OI.ORGNAME                                                        AS ACCORGNAME                 --账务机构名称
                ,'999999'                                                          AS INDUSTRYID                 --所属行业代码
                ,'未知'                                                            AS INDUSTRYNAME               --所属行业名称
                ,'0501'                                                            AS BUSINESSLINE               --条线
                ,''                                                                AS ASSETTYPE                  --资产大类                  (其他表内资产)
                ,''                                                                AS ASSETSUBTYPE               --资产小类
                ,'9010101010'                                                      AS BUSINESSTYPEID             --业务品种代号
                ,'虚拟业务品种'                                                    AS BUSINESSTYPENAME           --业务品种名称
                ,CASE WHEN RETAILFLAG='1' THEN '02' ELSE '01' END                  AS CREDITRISKDATATYPE         --信用风险数据类型          (默认'一般非零售',01一般非零售,02一般零售)
                ,'01'                                                              AS ASSETTYPEOFHAIRCUTS        --折扣系数对应资产类别      (默认'现金及现金等价物',01现金及现金等价物)
                ,'07'                                                              AS BUSINESSTYPESTD            --权重法业务类型            (默认为07一般资产)
                ,''                                                                AS EXPOCLASSSTD               --权重法暴露大类            (默认为0112其它)
                ,''                                                                AS EXPOSUBCLASSSTD            --权重法暴露小类            (默认为011216其他适用100%风险权重的资产)
                ,''                                                                AS EXPOCLASSIRB               --内评法暴露大类            (默认为空)
                ,''                                                                AS EXPOSUBCLASSIRB            --内评法暴露小类            (默认为空)
                ,'01'                                                              AS EXPOBELONG                 --暴露所属标识              (01表内;02一般表外)
                ,'01'                                                              AS BOOKTYPE                   --账户类别                  (01银行账户;02交易账户)
                ,'03'                                                              AS REGUTRANTYPE               --监管交易类型              (默认'抵押贷款',01回购交易,02其他资本市场交易,03抵押贷款)
                ,'0'                                                               AS REPOTRANFLAG               --回购交易标识              (默认为否,1是0否)
                ,1                                                                 AS REVAFREQUENCY              --重估频率
                ,GL.CURRENCY                                                       AS CURRENCY                   --币种                      (默认为01人民币)
                ,CASE WHEN GL.SUBJECT_NO = '17010000' THEN GL.ACCOUNT_BALANCE - NVL(V_ILDDEBT,0)
                ELSE GL.ACCOUNT_BALANCE END                                        AS NORMALPRINCIPAL            --正常本金余额              (金额绝对值进行计算)
                ,0                                                                 AS OVERDUEBALANCE             --逾期余额
                ,0                                                                 AS NONACCRUALBALANCE          --非应计余额
                ,CASE WHEN GL.SUBJECT_NO = '17010000' THEN GL.ACCOUNT_BALANCE - NVL(V_ILDDEBT,0)
                ELSE GL.ACCOUNT_BALANCE END                                        AS ONSHEETBALANCE             --表内余额
                ,0                                                                 AS NORMALINTEREST             --正常利息
                ,0                                                                 AS ONDEBITINTEREST            --表内欠息
                ,0                                                                 AS OFFDEBITINTEREST           --表外欠息
                ,0                                                                 AS EXPENSERECEIVABLE          --应收费用
                ,CASE WHEN GL.SUBJECT_NO = '17010000' THEN GL.ACCOUNT_BALANCE - NVL(V_ILDDEBT,0)
                ELSE GL.ACCOUNT_BALANCE END                                        AS ASSETBALANCE               --资产余额
                ,GL.SUBJECT_NO                                                     AS ACCSUBJECT1                --科目一
                ,''                                                                AS ACCSUBJECT2                --科目二
                ,''                                                                AS ACCSUBJECT3                --科目三
                ,V_STARTDATE                                                       AS STARTDATE                  --起始日期
                ,TO_CHAR(ADD_MONTHS(V_DATADATE,1),'YYYY-MM-DD')                    AS DUEDATE                    --到期日期                  (数据日期 + 1个月)
                ,(ADD_MONTHS(V_DATADATE,1) - V_DATADATE)/365                       AS ORIGINALMATURITY           --原始期限                  (单位年)
                ,(ADD_MONTHS(V_DATADATE,1) - V_DATADATE)/365                       AS RESIDUALM                  --剩余期限
                ,'01'                                                              AS RISKCLASSIFY               --风险分类
                ,''                                                                AS EXPOSURESTATUS             --风险暴露状态               (默认为空)
                ,0                                                                 AS OVERDUEDAYS                --逾期天数
                ,0                                                                 AS SPECIALPROVISION           --专项准备金
                ,0                                                                 AS GENERALPROVISION           --一般准备金
                ,0                                                                 AS ESPECIALPROVISION          --特别准备金
                ,0                                                                 AS WRITTENOFFAMOUNT           --已核销金额
                ,''                                                                AS OFFEXPOSOURCE              --表外暴露来源
                ,''                                                                AS OFFBUSINESSTYPE            --表外业务类型
                ,''                                                                AS OFFBUSINESSSDVSSTD         --权重法表外业务类型细分
                ,'1'                                                               AS UNCONDCANCELFLAG           --是否可随时无条件撤销      (默认为否,1是0否)
                ,''                                                                AS CCFLEVEL                   --信用转换系数级别
                ,NULL                                                              AS CCFAIRB                    --高级法下信用转换系数
                ,'01'                                                              AS CLAIMSLEVEL                --债权级别                  (默认为高级债权;01高级债权,02次级债权)
                ,'0'                                                               AS BONDFLAG                   --是否为债券                (默认为否,1是0否)
                ,''                                                                AS BONDISSUEINTENT            --债券发行目的
                ,'0'                                                               AS NSUREALPROPERTYFLAG        --是否非自用不动产          (默认为否,1是0否)
                ,''                                                                AS REPASSETTERMTYPE           --抵债资产期限类型
                ,'0'                                                               AS DEPENDONFPOBFLAG           --是否依赖于银行未来盈利    (默认为否,1是0否)
                ,''                                                                AS IRATING                    --内部评级
                ,NULL                                                              AS PD                         --违约概率
                ,''                                                                AS LGDLEVEL                   --违约损失率级别
                ,NULL                                                              AS LGDAIRB                    --高级法下违约损失率
                ,NULL                                                              AS MAIRB                      --高级法下有效期限
                ,NULL                                                              AS EADAIRB                    --高级法下违约风险暴露
                ,'0'                                                               AS DEFAULTFLAG                --违约标识                  (默认为否,1是0否)
                ,NULL                                                              AS BEEL                       --已违约暴露预期损失比率
                ,NULL                                                              AS DEFAULTLGD                 --已违约暴露违约损失率
                ,'0'                                                               AS EQUITYEXPOFLAG             --股权暴露标识              (默认为否,1是0否)
                ,''                                                                AS EQUITYINVESTTYPE           --股权投资对象类型
                ,''                                                                AS EQUITYINVESTCAUSE          --股权投资形成原因
                ,'0'                                                               AS SLFLAG                     --专业贷款标识              (默认为否,1是0否)
                ,''                                                                AS SLTYPE                     --专业贷款类型
                ,''                                                                AS PFPHASE                    --项目融资阶段
                ,''                                                                AS REGURATING                 --监管评级
                ,'0'                                                               AS CBRCMPRATINGFLAG           --银监会认定评级是否更为审慎(默认为否,1是0否)
                ,'0'                                                               AS LARGEFLUCFLAG              --是否波动性较大            (默认为否,1是0否)
                ,'0'                                                               AS LIQUEXPOFLAG               --是否清算过程中风险暴露    (默认为否,1是0否)
                ,'0'                                                               AS PAYMENTDEALFLAG            --是否货款对付模式          (默认为否,1是0否)
                ,0                                                                 AS DELAYTRADINGDAYS           --延迟交易天数
                ,'0'                                                               AS SECURITIESFLAG             --有价证券标识              (默认为否,1是0否)
                ,''                                                                AS SECUISSUERID               --证券发行人代号
                ,''                                                                AS RATINGDURATIONTYPE         --评级期限类型
                ,''                                                                AS SECUISSUERATING            --证券发行等级
                ,NULL                                                              AS SECURESIDUALM              --证券剩余期限
                ,''                                                                AS SECUREVAFREQUENCY          --证券重估频率
                ,'0'                                                               AS CCPTRANFLAG                --是否中央交易对手相关交易  (默认为否,1是0否)
                ,''                                                                AS CCPID                      --中央交易对手ID
                ,'0'                                                               AS QUALCCPFLAG                --是否合格中央交易对手      (默认为否,1是0否)
                ,''                                                                AS BANKROLE                   --银行角色
                ,''                                                                AS CLEARINGMETHOD             --清算方式
                ,'0'                                                               AS BANKASSETFLAG              --是否银行提交资产          (默认为否,1是0否)
                ,''                                                                AS MATCHCONDITIONS            --符合条件情况
                ,'0'                                                               AS SFTFLAG                    --证券融资交易标识          (默认为否,1是0否)
                ,'0'                                                               AS MASTERNETAGREEFLAG         --净额结算主协议标识        (默认为否,1是0否)
                ,''                                                                AS MASTERNETAGREEID           --净额结算主协议代号
                ,''                                                                AS SFTTYPE                    --证券融资交易类型
                ,'0'                                                               AS SECUOWNERTRANSFLAG         --证券所有权是否转移        (默认为否,1是0否)
                ,'0'                                                               AS OTCFLAG                    --场外衍生工具标识          (默认为否,1是0否)
                ,'0'                                                               AS VALIDNETTINGFLAG           --有效净额结算协议标识      (默认为否,1是0否)
                ,''                                                                AS VALIDNETAGREEMENTID        --有效净额结算协议代号
                ,''                                                                AS OTCTYPE                    --场外衍生工具类型
                ,0                                                                 AS DEPOSITRISKPERIOD          --保证金风险期间
                ,0                                                                 AS MTM                        --重置成本
                ,''                                                                AS MTMCURRENCY                --重置成本币种
                ,''                                                                AS BUYERORSELLER              --买方卖方
                ,'0'                                                               AS QUALROFLAG                 --合格参照资产标识          (默认为否,1是0否)
                ,'0'                                                               AS ROISSUERPERFORMFLAG        --参照资产发行人是否能履约  (默认为否,1是0否)
                ,'0'                                                               AS BUYERINSOLVENCYFLAG        --信用保护买方是否破产      (默认为否,1是0否)
                ,''                                                                AS NONPAYMENTFEES             --尚未支付费用
                ,RAP.RETAILFLAG                                                    AS RETAILEXPOFLAG             --零售暴露标识              (默认为否,1是0否)
                ,''                                                                AS RETAILCLAIMTYPE            --零售债权类型
                ,''                                                                AS MORTGAGETYPE               --住房抵押贷款类型
                ,1                                                                 AS ExpoNumber                 --风险暴露个数               默认 1
                ,0.8                                                               AS LTV                        --贷款价值比                  默认 0.8
                ,NULL                                                              AS Aging                      --账龄                       默认 NULL
                ,''                                                                AS NewDefaultDebtFlag         --新增违约债项标识           默认 NULL
                ,''                                                                AS PDPoolModelID              --PD分池模型ID               默认 NULL
                ,''                                                                AS LGDPoolModelID             --LGD分池模型ID              默认 NULL
                ,''                                                                AS CCFPoolModelID             --CCF分池模型ID              默认 NULL
                ,''                                                                AS PDPoolID                   --所属PD池ID                  默认 NULL
                ,''                                                                AS LGDPoolID                  --所属LGD池ID                 默认 NULL
                ,''                                                                AS CCFPoolID                  --所属CCF池ID                 默认 NULL
                ,'0'                                                               AS ABSUAFlag                  --资产证券化基础资产标识      默认 否(0)
                ,''                                                                AS ABSPoolID                  --证券化资产池ID             默认 NULL
                ,''                                                                AS GroupID                    --分组编号                   默认 NULL
                ,NULL                                                              AS DefaultDate                --违约时点
                ,NULL                                                              AS ABSPROPORTION              --资产证券化比重
                ,NULL                                                              AS DEBTORNUMBER               --借款人个数
    FROM RWA_DEV.RWA_TMP_GLBALANCE GL
    LEFT JOIN RWA_DEV.RWA_ARTICULATION_PARAM RAP
      ON RAP.THIRD_SUBJECT_NO = GL.SUBJECT_NO
    LEFT JOIN RWA.ORG_INFO OI
      ON OI.ORGID = GL.ORGID
   WHERE GL.ACCOUNT_BALANCE <> 0
     AND GL.SUBJECT_NO NOT IN
         (SELECT DISTINCT SUBJECT_NO
            FROM RWA_DEV.RWA_TMP_DERIVATION_SUBJECT RTDS
            LEFT JOIN RWA_DEV.RWA_EI_EXPOSURE REE
              ON REE.DATADATE = V_DATADATE
             AND REE.ACCSUBJECT1 = RTDS.ARTICULATERELATION
           WHERE REE.ACCSUBJECT1 IS NOT NULL
             AND REE.SSYSID <> 'GC');
    COMMIT;

    /*特殊科目处理*/
    INSERT INTO RWA_DEV.RWA_EI_EXPOSURE(
                 DataDate                                                     --数据日期
                ,DataNo                                                       --数据流水号
                ,ExposureID                                                   --风险暴露ID
                ,DueID                                                        --债项ID
                ,SSysID                                                       --源系统ID
                ,ContractID                                                   --合同ID
                ,ClientID                                                     --参与主体ID
                ,SOrgID                                                       --源机构ID
                ,SOrgName                                                     --源机构名称
                ,OrgSortNo                                                    --所属机构排序号
                ,OrgID                                                        --所属机构ID
                ,OrgName                                                      --所属机构名称
                ,AccOrgID                                                     --账务机构ID
                ,AccOrgName                                                   --账务机构名称
                ,IndustryID                                                   --所属行业代码
                ,IndustryName                                                 --所属行业名称
                ,BusinessLine                                                 --业务条线
                ,AssetType                                                    --资产大类
                ,AssetSubType                                                 --资产小类
                ,BusinessTypeID                                               --业务品种代码
                ,BusinessTypeName                                             --业务品种名称
                ,CreditRiskDataType                                           --信用风险数据类型
                ,AssetTypeOfHaircuts                                          --折扣系数对应资产类别
                ,BusinessTypeSTD                                              --权重法业务类型
                ,ExpoClassSTD                                                 --权重法暴露大类
                ,ExpoSubClassSTD                                              --权重法暴露小类
                ,ExpoClassIRB                                                 --内评法暴露大类
                ,ExpoSubClassIRB                                              --内评法暴露小类
                ,ExpoBelong                                                   --暴露所属标识
                ,BookType                                                     --账户类别
                ,ReguTranType                                                 --监管交易类型
                ,RepoTranFlag                                                 --回购交易标识
                ,RevaFrequency                                                --重估频率
                ,Currency                                                     --币种
                ,NormalPrincipal                                              --正常本金余额
                ,OverdueBalance                                               --逾期余额
                ,NonAccrualBalance                                            --非应计余额
                ,OnSheetBalance                                               --表内余额
                ,NormalInterest                                               --正常利息
                ,OnDebitInterest                                              --表内欠息
                ,OffDebitInterest                                             --表外欠息
                ,ExpenseReceivable                                            --应收费用
                ,AssetBalance                                                 --资产余额
                ,AccSubject1                                                  --科目一
                ,AccSubject2                                                  --科目二
                ,AccSubject3                                                  --科目三
                ,StartDate                                                    --起始日期
                ,DueDate                                                      --到期日期
                ,OriginalMaturity                                             --原始期限
                ,ResidualM                                                    --剩余期限
                ,RiskClassify                                                 --风险分类
                ,ExposureStatus                                               --风险暴露状态
                ,OverdueDays                                                  --逾期天数
                ,SpecialProvision                                             --专项准备金
                ,GeneralProvision                                             --一般准备金
                ,EspecialProvision                                            --特别准备金
                ,WrittenOffAmount                                             --已核销金额
                ,OffExpoSource                                                --表外暴露来源
                ,OffBusinessType                                              --表外业务类型
                ,OffBusinessSdvsSTD                                           --权重法表外业务类型细分
                ,UncondCancelFlag                                             --是否可随时无条件撤销
                ,CCFLevel                                                     --信用转换系数级别
                ,CCFAIRB                                                      --高级法信用转换系数
                ,ClaimsLevel                                                  --债权级别
                ,BondFlag                                                     --是否为债券
                ,BondIssueIntent                                              --债券发行目的
                ,NSURealPropertyFlag                                          --是否非自用不动产
                ,RepAssetTermType                                             --抵债资产期限类型
                ,DependOnFPOBFlag                                             --是否依赖于银行未来盈利
                ,IRating                                                      --内部评级
                ,PD                                                           --违约概率
                ,LGDLevel                                                     --违约损失率级别
                ,LGDAIRB                                                      --高级法违约损失率
                ,MAIRB                                                        --高级法有效期限
                ,EADAIRB                                                      --高级法违约风险暴露
                ,DefaultFlag                                                  --违约标识
                ,BEEL                                                         --已违约暴露预期损失比率
                ,DefaultLGD                                                   --已违约暴露违约损失率
                ,EquityExpoFlag                                               --股权暴露标识
                ,EquityInvestType                                             --股权投资对象类型
                ,EquityInvestCause                                            --股权投资形成原因
                ,SLFlag                                                       --专业贷款标识
                ,SLType                                                       --专业贷款类型
                ,PFPhase                                                      --项目融资阶段
                ,ReguRating                                                   --监管评级
                ,CBRCMPRatingFlag                                             --银监会认定评级是否更为审慎
                ,LargeFlucFlag                                                --是否波动性较大
                ,LiquExpoFlag                                                 --是否清算过程中风险暴露
                ,PaymentDealFlag                                              --是否货款对付模式
                ,DelayTradingDays                                             --延迟交易天数
                ,SecuritiesFlag                                               --有价证券标识
                ,SecuIssuerID                                                 --证券发行人ID
                ,RatingDurationType                                           --评级期限类型
                ,SecuIssueRating                                              --证券发行等级
                ,SecuResidualM                                                --证券剩余期限
                ,SecuRevaFrequency                                            --证券重估频率
                ,CCPTranFlag                                                  --是否中央交易对手相关交易
                ,CCPID                                                        --中央交易对手ID
                ,QualCCPFlag                                                  --是否合格中央交易对手
                ,BankRole                                                     --银行角色
                ,ClearingMethod                                               --清算方式
                ,BankAssetFlag                                                --是否银行提交资产
                ,MatchConditions                                              --符合条件情况
                ,SFTFlag                                                      --证券融资交易标识
                ,MasterNetAgreeFlag                                           --净额结算主协议标识
                ,MasterNetAgreeID                                             --净额结算主协议ID
                ,SFTType                                                      --证券融资交易类型
                ,SecuOwnerTransFlag                                           --证券所有权是否转移
                ,OTCFlag                                                      --场外衍生工具标识
                ,ValidNettingFlag                                             --有效净额结算协议标识
                ,ValidNetAgreementID                                          --有效净额结算协议ID
                ,OTCType                                                      --场外衍生工具类型
                ,DepositRiskPeriod                                            --保证金风险期间
                ,MTM                                                          --重置成本
                ,MTMCurrency                                                  --重置成本币种
                ,BuyerOrSeller                                                --买方卖方
                ,QualROFlag                                                   --合格参照资产标识
                ,ROIssuerPerformFlag                                          --参照资产发行人是否能履约
                ,BuyerInsolvencyFlag                                          --信用保护买方是否破产
                ,NonpaymentFees                                               --尚未支付费用
                ,RetailExpoFlag                                               --零售暴露标识
                ,RetailClaimType                                              --零售债权类型
                ,MortgageType                                                 --住房抵押贷款类型
                ,ExpoNumber                                                   --风险暴露个数
                ,LTV                                                          --贷款价值比
                ,Aging                                                        --账龄
                ,NewDefaultDebtFlag                                           --新增违约债项标识
                ,PDPoolModelID                                                --PD分池模型ID
                ,LGDPoolModelID                                               --LGD分池模型ID
                ,CCFPoolModelID                                               --CCF分池模型ID
                ,PDPoolID                                                     --所属PD池ID
                ,LGDPoolID                                                    --所属LGD池ID
                ,CCFPoolID                                                    --所属CCF池ID
                ,ABSUAFlag                                                    --资产证券化基础资产标识
                ,ABSPoolID                                                    --证券化资产池ID
                ,GroupID                                                      --分组编号
                ,DefaultDate                                                  --违约时点
                ,ABSPROPORTION                                                --资产证券化比重
                ,DEBTORNUMBER                                                 --借款人个数
    )
    SELECT
                V_DATADATE                                                         AS DATADATE                   --数据日期
                ,V_DATANO                                                          AS DATANO                     --数据流水号
                ,'YPWF-' || V_DATANO                                               AS EXPOSUREID                 --风险暴露代号
                ,'YPWF-' || V_DATANO                                               AS DUEID                      --债项代号
                ,'XN'                                                              AS SSYSID                     --源系统代号
                ,'YPWF-' || V_DATANO                                               AS CONTRACTID                 --合同代号
                ,'YPWF-' || V_DATANO                                               AS CLIENTID                   --参与主体代号
                ,'9998'                                                        AS SORGID                     --源机构代号
                ,'重庆银行'                                                        AS SORGNAME                   --源机构名称
                ,'1'                                                               AS ORGSORTNO                  --机构排序号
                ,'9998'                                                        AS ORGID                      --所属机构代号
                 ,'重庆银行'                                                       AS ORGNAME                    --所属机构名称
                ,'9998'                                                        AS ACCORGID                   --账务机构代号
                 ,'重庆银行'                                                       AS ACCORGNAME                 --账务机构名称
                ,'999999'                                                          AS INDUSTRYID                 --所属行业代码
                ,'未知'                                                            AS INDUSTRYNAME               --所属行业名称
                ,'0501'                                                            AS BUSINESSLINE               --条线
                ,'132'                                                             AS ASSETTYPE                  --资产大类                  (其他表内资产)
                ,'13205'                                                           AS ASSETSUBTYPE               --资产小类
                ,'109080'                                                          AS BUSINESSTYPEID             --业务品种代号
                ,'信贷已批未放'                                                    AS BUSINESSTYPENAME           --业务品种名称
                ,'01'                                                              AS CREDITRISKDATATYPE         --信用风险数据类型          (默认'一般非零售',01一般非零售,02一般零售)
                ,'01'                                                              AS ASSETTYPEOFHAIRCUTS        --折扣系数对应资产类别      (默认'现金及现金等价物',01现金及现金等价物)
                ,'07'                                                              AS BUSINESSTYPESTD            --权重法业务类型            (默认为07一般资产)
                ,'0112'                                                            AS EXPOCLASSSTD               --权重法暴露大类            (默认为0112其它)
                ,'011216'                                                          AS EXPOSUBCLASSSTD            --权重法暴露小类            (默认为011216其他适用100%风险权重的资产)
                ,''                                                                AS EXPOCLASSIRB               --内评法暴露大类            (默认为空)
                ,''                                                                AS EXPOSUBCLASSIRB            --内评法暴露小类            (默认为空)
                ,'02'                                                              AS EXPOBELONG                 --暴露所属标识              (01表内;02一般表外)
                ,'01'                                                              AS BOOKTYPE                   --账户类别                  (01银行账户;02交易账户)
                ,'03'                                                              AS REGUTRANTYPE               --监管交易类型             (默认'抵押贷款',01回购交易,02其他资本市场交易,03抵押贷款)
                ,'0'                                                               AS REPOTRANFLAG               --回购交易标识              (默认为否,1是0否)
                ,1                                                                 AS REVAFREQUENCY              --重估频率
                ,'CNY'                                                             AS CURRENCY                   --币种                      (默认为01人民币)
                ,T.BALANCE                                                         AS NORMALPRINCIPAL            --正常本金余额
                ,0                                                                 AS OVERDUEBALANCE             --逾期余额
                ,0                                                                 AS NONACCRUALBALANCE          --非应计余额
                ,T.BALANCE                                                         AS ONSHEETBALANCE             --表内余额
                ,0                                                                 AS NORMALINTEREST             --正常利息
                ,0                                                                 AS ONDEBITINTEREST            --表内欠息
                ,0                                                                 AS OFFDEBITINTEREST           --表外欠息
                ,0                                                                 AS EXPENSERECEIVABLE          --应收费用
                ,T.BALANCE                                                         AS ASSETBALANCE               --资产余额
                ,''                                                                AS ACCSUBJECT1                --科目一
                ,''                                                                AS ACCSUBJECT2                --科目二
                ,''                                                                AS ACCSUBJECT3                --科目三
                ,V_STARTDATE                                                       AS STARTDATE                  --起始日期
                ,TO_CHAR(ADD_MONTHS(V_DATADATE,1),'YYYY-MM-DD')                    AS DUEDATE                    --到期日期                  (数据日期 + 1个月)
                ,(ADD_MONTHS(V_DATADATE,1) - V_DATADATE)/365                       AS ORIGINALMATURITY           --原始期限                  (单位年)
                ,(ADD_MONTHS(V_DATADATE,1) - V_DATADATE)/365                       AS RESIDUALM                  --剩余期限
                ,'01'                                                              AS RISKCLASSIFY               --风险分类
                ,''                                                                AS EXPOSURESTATUS             --风险暴露状态               (默认为空)
                ,0                                                                 AS OVERDUEDAYS                --逾期天数
                ,0                                                                 AS SPECIALPROVISION           --专项准备金
                ,0                                                                 AS GENERALPROVISION           --一般准备金
                ,0                                                                 AS ESPECIALPROVISION          --特别准备金
                ,0                                                                 AS WRITTENOFFAMOUNT           --已核销金额
                ,'01'                                                              AS OFFEXPOSOURCE              --表外暴露来源
                ,'02'                                                              AS OFFBUSINESSTYPE            --表外业务类型
                ,'0201'                                                            AS OFFBUSINESSSDVSSTD         --权重法表外业务类型细分
                ,'1'                                                               AS UNCONDCANCELFLAG           --是否可随时无条件撤销      (默认为否,1是0否)
                ,''                                                                AS CCFLEVEL                   --信用转换系数级别
                ,NULL                                                              AS CCFAIRB                    --高级法下信用转换系数
                ,'01'                                                              AS CLAIMSLEVEL                --债权级别                  (默认为高级债权;01高级债权,02次级债权)
                ,'0'                                                               AS BONDFLAG                   --是否为债券                (默认为否,1是0否)
                ,''                                                                AS BONDISSUEINTENT            --债券发行目的
                ,'0'                                                               AS NSUREALPROPERTYFLAG        --是否非自用不动产          (默认为否,1是0否)
                ,''                                                                AS REPASSETTERMTYPE           --抵债资产期限类型
                ,'0'                                                               AS DEPENDONFPOBFLAG           --是否依赖于银行未来盈利    (默认为否,1是0否)
                ,''                                                                AS IRATING                    --内部评级
                ,NULL                                                              AS PD                         --违约概率
                ,''                                                                AS LGDLEVEL                   --违约损失率级别
                ,NULL                                                              AS LGDAIRB                    --高级法下违约损失率
                ,NULL                                                              AS MAIRB                      --高级法下有效期限
                ,NULL                                                              AS EADAIRB                    --高级法下违约风险暴露
                ,'0'                                                               AS DEFAULTFLAG                --违约标识                  (默认为否,1是0否)
                ,NULL                                                              AS BEEL                       --已违约暴露预期损失比率
                ,NULL                                                              AS DEFAULTLGD                 --已违约暴露违约损失率
                ,'0'                                                               AS EQUITYEXPOFLAG             --股权暴露标识              (默认为否,1是0否)
                ,''                                                                AS EQUITYINVESTTYPE           --股权投资对象类型
                ,''                                                                AS EQUITYINVESTCAUSE          --股权投资形成原因
                ,'0'                                                               AS SLFLAG                     --专业贷款标识              (默认为否,1是0否)
                ,''                                                                AS SLTYPE                     --专业贷款类型
                ,''                                                                AS PFPHASE                    --项目融资阶段
                ,''                                                                AS REGURATING                 --监管评级
                ,'0'                                                               AS CBRCMPRATINGFLAG           --银监会认定评级是否更为审慎(默认为否,1是0否)
                ,'0'                                                               AS LARGEFLUCFLAG              --是否波动性较大            (默认为否,1是0否)
                ,'0'                                                               AS LIQUEXPOFLAG               --是否清算过程中风险暴露    (默认为否,1是0否)
                ,'0'                                                               AS PAYMENTDEALFLAG            --是否货款对付模式          (默认为否,1是0否)
                ,0                                                                 AS DELAYTRADINGDAYS           --延迟交易天数
                ,'0'                                                               AS SECURITIESFLAG             --有价证券标识              (默认为否,1是0否)
                ,''                                                                AS SECUISSUERID               --证券发行人代号
                ,''                                                                AS RATINGDURATIONTYPE         --评级期限类型
                ,''                                                                AS SECUISSUERATING            --证券发行等级
                ,NULL                                                              AS SECURESIDUALM              --证券剩余期限
                ,''                                                                AS SECUREVAFREQUENCY          --证券重估频率
                ,'0'                                                               AS CCPTRANFLAG                --是否中央交易对手相关交易  (默认为否,1是0否)
                ,''                                                                AS CCPID                      --中央交易对手ID
                ,'0'                                                               AS QUALCCPFLAG                --是否合格中央交易对手      (默认为否,1是0否)
                ,''                                                                AS BANKROLE                   --银行角色
                ,''                                                                AS CLEARINGMETHOD             --清算方式
                ,'0'                                                               AS BANKASSETFLAG              --是否银行提交资产          (默认为否,1是0否)
                ,''                                                                AS MATCHCONDITIONS            --符合条件情况
                ,'0'                                                               AS SFTFLAG                    --证券融资交易标识          (默认为否,1是0否)
                ,'0'                                                               AS MASTERNETAGREEFLAG         --净额结算主协议标识        (默认为否,1是0否)
                ,''                                                                AS MASTERNETAGREEID           --净额结算主协议代号
                ,''                                                                AS SFTTYPE                    --证券融资交易类型
                ,'0'                                                               AS SECUOWNERTRANSFLAG         --证券所有权是否转移        (默认为否,1是0否)
                ,'0'                                                               AS OTCFLAG                    --场外衍生工具标识          (默认为否,1是0否)
                ,'0'                                                               AS VALIDNETTINGFLAG           --有效净额结算协议标识      (默认为否,1是0否)
                ,''                                                                AS VALIDNETAGREEMENTID        --有效净额结算协议代号
                ,''                                                                AS OTCTYPE                    --场外衍生工具类型
                ,0                                                                 AS DEPOSITRISKPERIOD          --保证金风险期间
                ,0                                                                 AS MTM                        --重置成本
                ,''                                                                AS MTMCURRENCY                --重置成本币种
                ,''                                                                AS BUYERORSELLER              --买方卖方
                ,'0'                                                               AS QUALROFLAG                 --合格参照资产标识          (默认为否,1是0否)
                ,'0'                                                               AS ROISSUERPERFORMFLAG        --参照资产发行人是否能履约  (默认为否,1是0否)
                ,'0'                                                               AS BUYERINSOLVENCYFLAG        --信用保护买方是否破产      (默认为否,1是0否)
                ,''                                                                AS NONPAYMENTFEES             --尚未支付费用
                ,'0'                                                               AS RETAILEXPOFLAG             --零售暴露标识              (默认为否,1是0否)
                ,''                                                                AS RETAILCLAIMTYPE            --零售债权类型
                ,''                                                                AS MORTGAGETYPE               --住房抵押贷款类型
                ,1                                                                 AS ExpoNumber                 --风险暴露个数               默认 1
                ,0.8                                                               AS LTV                        --贷款价值比                  默认 0.8
                ,NULL                                                              AS Aging                      --账龄                       默认 NULL
                ,''                                                                AS NewDefaultDebtFlag         --新增违约债项标识           默认 NULL
                ,''                                                                AS PDPoolModelID              --PD分池模型ID               默认 NULL
                ,''                                                                AS LGDPoolModelID             --LGD分池模型ID              默认 NULL
                ,''                                                                AS CCFPoolModelID             --CCF分池模型ID              默认 NULL
                ,''                                                                AS PDPoolID                   --所属PD池ID                  默认 NULL
                ,''                                                                AS LGDPoolID                  --所属LGD池ID                 默认 NULL
                ,''                                                                AS CCFPoolID                  --所属CCF池ID                 默认 NULL
                ,'0'                                                               AS ABSUAFlag                  --资产证券化基础资产标识      默认 否(0)
                ,''                                                                AS ABSPoolID                  --证券化资产池ID             默认 NULL
                ,''                                                                AS GroupID                    --分组编号                   默认 NULL
                ,NULL                                                              AS DefaultDate                --违约时点
                ,NULL                                                              AS ABSPROPORTION              --资产证券化比重
                ,NULL                                                              AS DEBTORNUMBER               --借款人个数
    FROM RWA.RWA_WS_XD_UNPUTOUT T
   WHERE T.DATADATE = V_DATADATE
   ;
    COMMIT;

   /*特殊科目处理*/
    INSERT INTO RWA_DEV.RWA_EI_EXPOSURE(
                 DataDate                                                     --数据日期
                ,DataNo                                                       --数据流水号
                ,ExposureID                                                   --风险暴露ID
                ,DueID                                                        --债项ID
                ,SSysID                                                       --源系统ID
                ,ContractID                                                   --合同ID
                ,ClientID                                                     --参与主体ID
                ,SOrgID                                                       --源机构ID
                ,SOrgName                                                     --源机构名称
                ,OrgSortNo                                                    --所属机构排序号
                ,OrgID                                                        --所属机构ID
                ,OrgName                                                      --所属机构名称
                ,AccOrgID                                                     --账务机构ID
                ,AccOrgName                                                   --账务机构名称
                ,IndustryID                                                   --所属行业代码
                ,IndustryName                                                 --所属行业名称
                ,BusinessLine                                                 --业务条线
                ,AssetType                                                    --资产大类
                ,AssetSubType                                                 --资产小类
                ,BusinessTypeID                                               --业务品种代码
                ,BusinessTypeName                                             --业务品种名称
                ,CreditRiskDataType                                           --信用风险数据类型
                ,AssetTypeOfHaircuts                                          --折扣系数对应资产类别
                ,BusinessTypeSTD                                              --权重法业务类型
                ,ExpoClassSTD                                                 --权重法暴露大类
                ,ExpoSubClassSTD                                              --权重法暴露小类
                ,ExpoClassIRB                                                 --内评法暴露大类
                ,ExpoSubClassIRB                                              --内评法暴露小类
                ,ExpoBelong                                                   --暴露所属标识
                ,BookType                                                     --账户类别
                ,ReguTranType                                                 --监管交易类型
                ,RepoTranFlag                                                 --回购交易标识
                ,RevaFrequency                                                --重估频率
                ,Currency                                                     --币种
                ,NormalPrincipal                                              --正常本金余额
                ,OverdueBalance                                               --逾期余额
                ,NonAccrualBalance                                            --非应计余额
                ,OnSheetBalance                                               --表内余额
                ,NormalInterest                                               --正常利息
                ,OnDebitInterest                                              --表内欠息
                ,OffDebitInterest                                             --表外欠息
                ,ExpenseReceivable                                            --应收费用
                ,AssetBalance                                                 --资产余额
                ,AccSubject1                                                  --科目一
                ,AccSubject2                                                  --科目二
                ,AccSubject3                                                  --科目三
                ,StartDate                                                    --起始日期
                ,DueDate                                                      --到期日期
                ,OriginalMaturity                                             --原始期限
                ,ResidualM                                                    --剩余期限
                ,RiskClassify                                                 --风险分类
                ,ExposureStatus                                               --风险暴露状态
                ,OverdueDays                                                  --逾期天数
                ,SpecialProvision                                             --专项准备金
                ,GeneralProvision                                             --一般准备金
                ,EspecialProvision                                            --特别准备金
                ,WrittenOffAmount                                             --已核销金额
                ,OffExpoSource                                                --表外暴露来源
                ,OffBusinessType                                              --表外业务类型
                ,OffBusinessSdvsSTD                                           --权重法表外业务类型细分
                ,UncondCancelFlag                                             --是否可随时无条件撤销
                ,CCFLevel                                                     --信用转换系数级别
                ,CCFAIRB                                                      --高级法信用转换系数
                ,ClaimsLevel                                                  --债权级别
                ,BondFlag                                                     --是否为债券
                ,BondIssueIntent                                              --债券发行目的
                ,NSURealPropertyFlag                                          --是否非自用不动产
                ,RepAssetTermType                                             --抵债资产期限类型
                ,DependOnFPOBFlag                                             --是否依赖于银行未来盈利
                ,IRating                                                      --内部评级
                ,PD                                                           --违约概率
                ,LGDLevel                                                     --违约损失率级别
                ,LGDAIRB                                                      --高级法违约损失率
                ,MAIRB                                                        --高级法有效期限
                ,EADAIRB                                                      --高级法违约风险暴露
                ,DefaultFlag                                                  --违约标识
                ,BEEL                                                         --已违约暴露预期损失比率
                ,DefaultLGD                                                   --已违约暴露违约损失率
                ,EquityExpoFlag                                               --股权暴露标识
                ,EquityInvestType                                             --股权投资对象类型
                ,EquityInvestCause                                            --股权投资形成原因
                ,SLFlag                                                       --专业贷款标识
                ,SLType                                                       --专业贷款类型
                ,PFPhase                                                      --项目融资阶段
                ,ReguRating                                                   --监管评级
                ,CBRCMPRatingFlag                                             --银监会认定评级是否更为审慎
                ,LargeFlucFlag                                                --是否波动性较大
                ,LiquExpoFlag                                                 --是否清算过程中风险暴露
                ,PaymentDealFlag                                              --是否货款对付模式
                ,DelayTradingDays                                             --延迟交易天数
                ,SecuritiesFlag                                               --有价证券标识
                ,SecuIssuerID                                                 --证券发行人ID
                ,RatingDurationType                                           --评级期限类型
                ,SecuIssueRating                                              --证券发行等级
                ,SecuResidualM                                                --证券剩余期限
                ,SecuRevaFrequency                                            --证券重估频率
                ,CCPTranFlag                                                  --是否中央交易对手相关交易
                ,CCPID                                                        --中央交易对手ID
                ,QualCCPFlag                                                  --是否合格中央交易对手
                ,BankRole                                                     --银行角色
                ,ClearingMethod                                               --清算方式
                ,BankAssetFlag                                                --是否银行提交资产
                ,MatchConditions                                              --符合条件情况
                ,SFTFlag                                                      --证券融资交易标识
                ,MasterNetAgreeFlag                                           --净额结算主协议标识
                ,MasterNetAgreeID                                             --净额结算主协议ID
                ,SFTType                                                      --证券融资交易类型
                ,SecuOwnerTransFlag                                           --证券所有权是否转移
                ,OTCFlag                                                      --场外衍生工具标识
                ,ValidNettingFlag                                             --有效净额结算协议标识
                ,ValidNetAgreementID                                          --有效净额结算协议ID
                ,OTCType                                                      --场外衍生工具类型
                ,DepositRiskPeriod                                            --保证金风险期间
                ,MTM                                                          --重置成本
                ,MTMCurrency                                                  --重置成本币种
                ,BuyerOrSeller                                                --买方卖方
                ,QualROFlag                                                   --合格参照资产标识
                ,ROIssuerPerformFlag                                          --参照资产发行人是否能履约
                ,BuyerInsolvencyFlag                                          --信用保护买方是否破产
                ,NonpaymentFees                                               --尚未支付费用
                ,RetailExpoFlag                                               --零售暴露标识
                ,RetailClaimType                                              --零售债权类型
                ,MortgageType                                                 --住房抵押贷款类型
                ,ExpoNumber                                                   --风险暴露个数
                ,LTV                                                          --贷款价值比
                ,Aging                                                        --账龄
                ,NewDefaultDebtFlag                                           --新增违约债项标识
                ,PDPoolModelID                                                --PD分池模型ID
                ,LGDPoolModelID                                               --LGD分池模型ID
                ,CCFPoolModelID                                               --CCF分池模型ID
                ,PDPoolID                                                     --所属PD池ID
                ,LGDPoolID                                                    --所属LGD池ID
                ,CCFPoolID                                                    --所属CCF池ID
                ,ABSUAFlag                                                    --资产证券化基础资产标识
                ,ABSPoolID                                                    --证券化资产池ID
                ,GroupID                                                      --分组编号
                ,DefaultDate                                                  --违约时点
                ,ABSPROPORTION                                                --资产证券化比重
                ,DEBTORNUMBER                                                 --借款人个数
    )
    SELECT
                V_DATADATE                                                         AS DATADATE                   --数据日期
                ,V_DATANO                                                          AS DATANO                     --数据流水号
                ,CASE WHEN T.WSTYPE = '01' THEN 'MDYP-' || V_DATANO
                      ELSE 'MDSP-'||V_DATANO END                                   AS EXPOSUREID                 --风险暴露代号
                ,CASE WHEN T.WSTYPE = '01' THEN 'MDYP-' || V_DATANO
                      ELSE 'MDSP-'||V_DATANO END                                   AS DUEID                      --债项代号
                ,'XN'                                                              AS SSYSID                     --源系统代号
                ,CASE WHEN T.WSTYPE = '01' THEN 'MDYP-' || V_DATANO
                      ELSE 'MDSP-'||V_DATANO END                                   AS CONTRACTID                 --合同代号
                ,CASE WHEN T.WSTYPE = '01' THEN 'MDYP-' || V_DATANO
                      ELSE 'MDSP-'||V_DATANO END                                   AS CLIENTID                   --参与主体代号
                ,'9998'                                                        AS SORGID                     --源机构代号
                ,'重庆银行'                                                        AS SORGNAME                   --源机构名称
                ,'1'                                                               AS ORGSORTNO                  --机构排序号
                ,'9998'                                                        AS ORGID                      --所属机构代号
                 ,'重庆银行'                                                       AS ORGNAME                    --所属机构名称
                ,'9998'                                                        AS ACCORGID                   --账务机构代号
                 ,'重庆银行'                                                       AS ACCORGNAME                 --账务机构名称
                ,'999999'                                                          AS INDUSTRYID                 --所属行业代码
                ,'未知'                                                            AS INDUSTRYNAME               --所属行业名称
                ,'0501'                                                            AS BUSINESSLINE               --条线
                ,'210'                                                             AS ASSETTYPE                  --资产大类                  (其他表内资产)
                ,'21001'                                                           AS ASSETSUBTYPE               --资产小类
                ,'9010101010'                                                      AS BUSINESSTYPEID             --业务品种代号
                ,'虚拟业务品种'                                                    AS BUSINESSTYPENAME           --业务品种名称
                ,'01'                                                              AS CREDITRISKDATATYPE         --信用风险数据类型          (默认'一般非零售',01一般非零售,02一般零售)
                ,'01'                                                              AS ASSETTYPEOFHAIRCUTS        --折扣系数对应资产类别      (默认'现金及现金等价物',01现金及现金等价物)
                ,'07'                                                              AS BUSINESSTYPESTD            --权重法业务类型            (默认为07一般资产)
                ,case when t.wstype = '01' then '0104' else '0106' end             AS EXPOCLASSSTD               --权重法暴露大类            (默认为0112其它)
                ,case when t.wstype = '01' then '010406' else '010601' end         AS EXPOSUBCLASSSTD            --权重法暴露小类            (默认为011216其他适用100%风险权重的资产)
                ,''                                                                AS EXPOCLASSIRB               --内评法暴露大类            (默认为空)
                ,''                                                                AS EXPOSUBCLASSIRB            --内评法暴露小类            (默认为空)
                ,'02'                                                              AS EXPOBELONG                 --暴露所属标识              (01表内;02一般表外)
                ,'01'                                                              AS BOOKTYPE                   --账户类别                  (01银行账户;02交易账户)
                ,'03'                                                              AS REGUTRANTYPE               --监管交易类型             (默认'抵押贷款',01回购交易,02其他资本市场交易,03抵押贷款)
                ,'0'                                                               AS REPOTRANFLAG               --回购交易标识              (默认为否,1是0否)
                ,1                                                                 AS REVAFREQUENCY              --重估频率
                ,'CNY'                                                             AS CURRENCY                   --币种                      (默认为01人民币)
                ,T.BALANCE                                                         AS NORMALPRINCIPAL            --正常本金余额
                ,0                                                                 AS OVERDUEBALANCE             --逾期余额
                ,0                                                                 AS NONACCRUALBALANCE          --非应计余额
                ,T.BALANCE                                                         AS ONSHEETBALANCE             --表内余额
                ,0                                                                 AS NORMALINTEREST             --正常利息
                ,0                                                                 AS ONDEBITINTEREST            --表内欠息
                ,0                                                                 AS OFFDEBITINTEREST           --表外欠息
                ,0                                                                 AS EXPENSERECEIVABLE          --应收费用
                ,T.BALANCE                                                         AS ASSETBALANCE               --资产余额
                ,''                                                                AS ACCSUBJECT1                --科目一
                ,''                                                                AS ACCSUBJECT2                --科目二
                ,''                                                                AS ACCSUBJECT3                --科目三
                ,V_STARTDATE                                                       AS STARTDATE                  --起始日期
                ,TO_CHAR(ADD_MONTHS(V_DATADATE,4),'YYYY-MM-DD')                    AS DUEDATE                    --到期日期                  (数据日期 + 1个月)
                ,(ADD_MONTHS(V_DATADATE,4) - V_DATADATE)/365                       AS ORIGINALMATURITY           --原始期限                  (单位年)
                ,(ADD_MONTHS(V_DATADATE,4) - V_DATADATE)/365                       AS RESIDUALM                  --剩余期限
                ,'01'                                                              AS RISKCLASSIFY               --风险分类
                ,''                                                                AS EXPOSURESTATUS             --风险暴露状态               (默认为空)
                ,0                                                                 AS OVERDUEDAYS                --逾期天数
                ,0                                                                 AS SPECIALPROVISION           --专项准备金
                ,0                                                                 AS GENERALPROVISION           --一般准备金
                ,0                                                                 AS ESPECIALPROVISION          --特别准备金
                ,0                                                                 AS WRITTENOFFAMOUNT           --已核销金额
                ,'01'                                                              AS OFFEXPOSOURCE              --表外暴露来源
                ,'10'                                                              AS OFFBUSINESSTYPE            --表外业务类型
                ,'1002'                                                            AS OFFBUSINESSSDVSSTD         --权重法表外业务类型细分
                ,'1'                                                               AS UNCONDCANCELFLAG           --是否可随时无条件撤销      (默认为否,1是0否)
                ,''                                                                AS CCFLEVEL                   --信用转换系数级别
                ,NULL                                                              AS CCFAIRB                    --高级法下信用转换系数
                ,'01'                                                              AS CLAIMSLEVEL                --债权级别                  (默认为高级债权;01高级债权,02次级债权)
                ,'0'                                                               AS BONDFLAG                   --是否为债券                (默认为否,1是0否)
                ,''                                                                AS BONDISSUEINTENT            --债券发行目的
                ,'0'                                                               AS NSUREALPROPERTYFLAG        --是否非自用不动产          (默认为否,1是0否)
                ,''                                                                AS REPASSETTERMTYPE           --抵债资产期限类型
                ,'0'                                                               AS DEPENDONFPOBFLAG           --是否依赖于银行未来盈利    (默认为否,1是0否)
                ,''                                                                AS IRATING                    --内部评级
                ,NULL                                                              AS PD                         --违约概率
                ,''                                                                AS LGDLEVEL                   --违约损失率级别
                ,NULL                                                              AS LGDAIRB                    --高级法下违约损失率
                ,NULL                                                              AS MAIRB                      --高级法下有效期限
                ,NULL                                                              AS EADAIRB                    --高级法下违约风险暴露
                ,'0'                                                               AS DEFAULTFLAG                --违约标识                  (默认为否,1是0否)
                ,NULL                                                              AS BEEL                       --已违约暴露预期损失比率
                ,NULL                                                              AS DEFAULTLGD                 --已违约暴露违约损失率
                ,'0'                                                               AS EQUITYEXPOFLAG             --股权暴露标识              (默认为否,1是0否)
                ,''                                                                AS EQUITYINVESTTYPE           --股权投资对象类型
                ,''                                                                AS EQUITYINVESTCAUSE          --股权投资形成原因
                ,'0'                                                               AS SLFLAG                     --专业贷款标识              (默认为否,1是0否)
                ,''                                                                AS SLTYPE                     --专业贷款类型
                ,''                                                                AS PFPHASE                    --项目融资阶段
                ,''                                                                AS REGURATING                 --监管评级
                ,'0'                                                               AS CBRCMPRATINGFLAG           --银监会认定评级是否更为审慎(默认为否,1是0否)
                ,'0'                                                               AS LARGEFLUCFLAG              --是否波动性较大            (默认为否,1是0否)
                ,'0'                                                               AS LIQUEXPOFLAG               --是否清算过程中风险暴露    (默认为否,1是0否)
                ,'0'                                                               AS PAYMENTDEALFLAG            --是否货款对付模式          (默认为否,1是0否)
                ,0                                                                 AS DELAYTRADINGDAYS           --延迟交易天数
                ,'0'                                                               AS SECURITIESFLAG             --有价证券标识              (默认为否,1是0否)
                ,''                                                                AS SECUISSUERID               --证券发行人代号
                ,''                                                                AS RATINGDURATIONTYPE         --评级期限类型
                ,''                                                                AS SECUISSUERATING            --证券发行等级
                ,NULL                                                              AS SECURESIDUALM              --证券剩余期限
                ,''                                                                AS SECUREVAFREQUENCY          --证券重估频率
                ,'0'                                                               AS CCPTRANFLAG                --是否中央交易对手相关交易  (默认为否,1是0否)
                ,''                                                                AS CCPID                      --中央交易对手ID
                ,'0'                                                               AS QUALCCPFLAG                --是否合格中央交易对手      (默认为否,1是0否)
                ,''                                                                AS BANKROLE                   --银行角色
                ,''                                                                AS CLEARINGMETHOD             --清算方式
                ,'0'                                                               AS BANKASSETFLAG              --是否银行提交资产          (默认为否,1是0否)
                ,''                                                                AS MATCHCONDITIONS            --符合条件情况
                ,'0'                                                               AS SFTFLAG                    --证券融资交易标识          (默认为否,1是0否)
                ,'0'                                                               AS MASTERNETAGREEFLAG         --净额结算主协议标识        (默认为否,1是0否)
                ,''                                                                AS MASTERNETAGREEID           --净额结算主协议代号
                ,''                                                                AS SFTTYPE                    --证券融资交易类型
                ,'0'                                                               AS SECUOWNERTRANSFLAG         --证券所有权是否转移        (默认为否,1是0否)
                ,'0'                                                               AS OTCFLAG                    --场外衍生工具标识          (默认为否,1是0否)
                ,'0'                                                               AS VALIDNETTINGFLAG           --有效净额结算协议标识      (默认为否,1是0否)
                ,''                                                                AS VALIDNETAGREEMENTID        --有效净额结算协议代号
                ,''                                                                AS OTCTYPE                    --场外衍生工具类型
                ,0                                                                 AS DEPOSITRISKPERIOD          --保证金风险期间
                ,0                                                                 AS MTM                        --重置成本
                ,''                                                                AS MTMCURRENCY                --重置成本币种
                ,''                                                                AS BUYERORSELLER              --买方卖方
                ,'0'                                                               AS QUALROFLAG                 --合格参照资产标识          (默认为否,1是0否)
                ,'0'                                                               AS ROISSUERPERFORMFLAG        --参照资产发行人是否能履约  (默认为否,1是0否)
                ,'0'                                                               AS BUYERINSOLVENCYFLAG        --信用保护买方是否破产      (默认为否,1是0否)
                ,''                                                                AS NONPAYMENTFEES             --尚未支付费用
                ,'0'                                                               AS RETAILEXPOFLAG             --零售暴露标识              (默认为否,1是0否)
                ,''                                                                AS RETAILCLAIMTYPE            --零售债权类型
                ,''                                                                AS MORTGAGETYPE               --住房抵押贷款类型
                ,1                                                                 AS ExpoNumber                 --风险暴露个数               默认 1
                ,0.8                                                               AS LTV                        --贷款价值比                  默认 0.8
                ,NULL                                                              AS Aging                      --账龄                       默认 NULL
                ,''                                                                AS NewDefaultDebtFlag         --新增违约债项标识           默认 NULL
                ,''                                                                AS PDPoolModelID              --PD分池模型ID               默认 NULL
                ,''                                                                AS LGDPoolModelID             --LGD分池模型ID              默认 NULL
                ,''                                                                AS CCFPoolModelID             --CCF分池模型ID              默认 NULL
                ,''                                                                AS PDPoolID                   --所属PD池ID                  默认 NULL
                ,''                                                                AS LGDPoolID                  --所属LGD池ID                 默认 NULL
                ,''                                                                AS CCFPoolID                  --所属CCF池ID                 默认 NULL
                ,'0'                                                               AS ABSUAFlag                  --资产证券化基础资产标识      默认 否(0)
                ,''                                                                AS ABSPoolID                  --证券化资产池ID             默认 NULL
                ,''                                                                AS GroupID                    --分组编号                   默认 NULL
                ,NULL                                                              AS DefaultDate                --违约时点
                ,NULL                                                              AS ABSPROPORTION              --资产证券化比重
                ,NULL                                                              AS DEBTORNUMBER               --借款人个数
    FROM RWA.RWA_WS_SUPPLY T
   WHERE T.DATADATE = V_DATADATE
     AND T.BALANCE<>0
   ;
    COMMIT;

    INSERT INTO RWA_DEV.RWA_EI_CLIENT(
                 DataDate                   --数据日期
                ,DataNo                     --数据流水号
                ,ClientID                   --参与主体ID
                ,SourceClientID             --源参与主体ID
                ,SSysID                     --源系统ID
                ,ClientName                 --参与主体名称
                ,SOrgID                     --源机构ID
                ,SOrgName                   --源机构名称
                ,OrgSortNo                  --所属机构排序号
                ,OrgID                      --所属机构ID
                ,OrgName                    --所属机构名称
                ,IndustryID                 --所属行业代码
                ,IndustryName               --所属行业名称
                ,ClientType                 --参与主体大类
                ,ClientSubType              --参与主体小类
                ,RegistState                --注册国家或地区
                ,RCERating                  --境外注册地外部评级
                ,RCERAgency                 --境外注册地外部评级机构
                ,OrganizationCode           --组织机构代码
                ,ConsolidatedSCFlag         --是否并表子公司
                ,SLClientFlag               --专业贷款客户标识
                ,SLClientType               --专业贷款客户类型
                ,ExpoCategoryIRB            --内评法暴露类别
                ,ModelID                    --模型ID
                ,ModelIRating               --模型内部评级
                ,ModelPD                    --模型违约概率
                ,IRating                    --内部评级
                ,PD                         --违约概率
                ,DefaultFlag                --违约标识
                ,NewDefaultFlag             --新增违约标识
                ,DefaultDate                --违约时点
                ,ClientERating              --参与主体外部评级
                ,CCPFlag                    --中央交易对手标识
                ,QualCCPFlag                --是否合格中央交易对手
                ,ClearMemberFlag            --清算会员标识
                ,CompanySize                --企业规模
                ,SSMBFlag                   --标准小微企业标识
                ,AnnualSale                 --公司客户年销售额
                ,CountryCode                --注册国家代码
                ,MSMBFlag                   --工信部微小企业标识
      )
      SELECT DISTINCT
                      REE.DATADATE                                 AS DATADATE,
                      REE.DATANO                                   AS DATANO,
                      REE.CLIENTID                                 AS CLIENTID,
                      ''                                           AS SOURCECLIENTID,
                      REE.SSYSID                                   AS SSYSID,
                      REE.CLIENTID || '-虚拟客户'                  AS CLIENTNAME,
                      '9998'                                   AS SORGID,
                      '重庆银行'                                   AS SORGNAME,
                      '1'                                AS ORGSORTNO,
                      '9998'                                   AS ORGID,
                      '重庆银行'                                   AS ORGNAME,
                      REE.INDUSTRYID                               AS INDUSTRYID,
                      REE.INDUSTRYNAME                             AS INDUSTRYNAME,
                      CASE WHEN REE.ACCSUBJECT1 LIKE '1003%' THEN '01'
                           WHEN REE.RETAILEXPOFLAG = '1' THEN '04'
                           WHEN REE.EXPOCLASSSTD = '0104' THEN '02'
                        ELSE  '03' END                             AS CLIENTTYPE,
                      CASE WHEN REE.ACCSUBJECT1 LIKE '1003%' THEN '0103'
                           WHEN REE.RETAILEXPOFLAG = '1' THEN '0401'
                           WHEN REE.EXPOSUBCLASSSTD = '010406' THEN '0202'
                        ELSE '0301' END                            AS CLIENTSUBTYPE,
                      '01'                                         AS REGISTSTATE,
                      '0124'                                       AS RCERATING,
                      ''                                           AS RCERAGENCY,
                      'XN' || REE.ACCSUBJECT1                      AS ORGANIZATIONCODE,
                      '0'                                          AS CONSOLIDATEDSCFLAG,
                      '0'                                          AS SLCLIENTFLAG,
                      NULL                                         AS SLCLIENTTYPE,
                      NULL                                         AS EXPOCATEGORYIRB,
                      NULL                                         AS MODELID,
                      NULL                                         AS MODELIRATING,
                      NULL                                         AS MODELPD,
                      NULL                                         AS IRATING,
                      NULL                                         AS PD,
                      '0'                                          AS DEFAULTFLAG,
                      '0'                                          AS NEWDEFAULTFLAG,
                      NULL                                         AS DEFAULTDATE,
                      NULL                                         AS CLIENTERATING,
                      '0'                                          AS CCPFLAG,
                      '0'                                          AS QUALCCPFLAG,
                      '0'                                          AS CLEARMEMBERFLAG,
                      NULL                                         AS COMPANYSIZE,
                      '0'                                          AS SSMBFLAG,
                      NULL                                         AS ANNUALSALE,
                      'CHN'                                        AS COUNTRYCODE,
                      ''                                           AS MSMBFLAG
        FROM RWA_DEV.RWA_EI_EXPOSURE REE
       WHERE REE.DATADATE = V_DATADATE
         AND REE.SSYSID = 'XN'
     ;
     COMMIT;

     INSERT INTO RWA_DEV.RWA_EI_CONTRACT (
                 DataDate                             --数据日期
                ,DataNo                               --数据流水号
                ,ContractID                           --合同ID
                ,SContractID                          --源合同ID
                ,SSysID                               --源系统ID
                ,ClientID                             --参与主体ID
                ,SOrgID                               --源机构ID
                ,SOrgName                             --源机构名称
                ,OrgSortNo                            --所属机构排序号
                ,OrgID                                --所属机构ID
                ,OrgName                              --所属机构名称
                ,IndustryID                           --所属行业代码
                ,IndustryName                         --所属行业名称
                ,BusinessLine                         --业务条线
                ,AssetType                            --资产大类
                ,AssetSubType                         --资产小类
                ,BusinessTypeID                       --业务品种代码
                ,BusinessTypeName                     --业务品种名称
                ,CreditRiskDataType                   --信用风险数据类型
                ,StartDate                            --起始日期
                ,DueDate                              --到期日期
                ,OriginalMaturity                     --原始期限
                ,ResidualM                            --剩余期限
                ,SettlementCurrency                   --结算币种
                ,ContractAmount                       --合同总金额
                ,NotExtractPart                       --合同未提取部分
                ,UncondCancelFlag                     --是否可随时无条件撤销
                ,ABSUAFlag                            --资产证券化基础资产标识
                ,ABSPoolID                            --证券化资产池ID
                ,GroupID                              --分组编号
                ,GUARANTEETYPE                        --主要担保方式
                ,ABSPROPORTION                        --资产证券化比重
                 )
        SELECT
                 REE.DATADATE                                                    AS DATADATE,
                 REE.DATANO                                                      AS DATANO,
                 REE.CONTRACTID                                                  AS CONTRACTID,
                 NULL                                                            AS SCONTRACTID,
                 REE.SSYSID                                                      AS SSYSID,
                 REE.CLIENTID                                                    AS CLIENTID,
                 REE.ORGID                                                       AS SORGID,
                 REE.ORGNAME                                                     AS SORGNAME,
                 REE.ORGSORTNO                                                   AS ORGSORTNO,
                 REE.ORGID                                                       AS ORGID,
                 REE.ORGNAME                                                     AS ORGNAME,
                 REE.INDUSTRYID                                                  AS INDUSTRYID,
                 REE.INDUSTRYNAME                                                AS INDUSTRYNAME,
                 REE.BUSINESSLINE                                                AS BUSINESSLINE,
                 REE.ASSETTYPE                                                   AS ASSETTYPE,
                 REE.ASSETSUBTYPE                                                AS ASSETSUBTYPE,
                 REE.BUSINESSTYPEID                                              AS BUSINESSTYPEID,
                 REE.BUSINESSTYPENAME                                            AS BUSINESSTYPENAME,
                 REE.CREDITRISKDATATYPE                                          AS CREDITRISKDATATYPE,
                 REE.STARTDATE                                                   AS STARTDATE,
                 REE.DUEDATE                                                     AS DUEDATE,
                 REE.ORIGINALMATURITY                                            AS ORIGINALMATURITY,
                 REE.RESIDUALM                                                   AS RESIDUALM,
                 REE.CURRENCY                                                    AS SETTLEMENTCURRENCY,
                 REE.NORMALPRINCIPAL                                             AS CONTRACTAMOUNT,
                 0                                                               AS NOTEXTRACTPART,
                 '0'                                                             AS UNCONDCANCELFLAG,
                 '0'                                                             AS ABSUAFLAG,
                 NULL                                                            AS ABSPOOLID,
                 NULL                                                            AS GROUPID,
                 NULL                                                            AS GUARANTEETYPE,
                 NULL                                                            AS ABSPROPORTION
           FROM RWA_DEV.RWA_EI_EXPOSURE REE
          WHERE REE.DATADATE = v_datadate
            AND REE.SSYSID = 'XN'
    ;
    COMMIT;

  ----------直销银行计算G4A-1A报表 按照ID和五级分类计算  BY WZB

    UPDATE RWA_EI_EXPOSURE SET SSYSID='ZX'  WHERE DATANO=p_data_dt_str AND ACCSUBJECT1='13070800';
    COMMIT;


    /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO V_COUNT
    FROM RWA_DEV.RWA_EI_EXPOSURE T1
    WHERE DATADATE = V_DATADATE
    AND SSYSID = 'XN' ;

    DBMS_OUTPUT.PUT_LINE('RWA_DEV.rwa_ei_exposure表插入总账虚拟的数据记录为:' || V_COUNT || '条');
    DBMS_OUTPUT.PUT_LINE('【执行 ' || V_PRO_NAME || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS'));

    P_PO_RTNCODE := '1';
    P_PO_RTNMSG  := '成功-'||V_COUNT;
    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
          --DBMS_OUTPUT.PUT_LINE('出错了,错误代码为:'||SQLCODE||';错误信息为:'||SQLERRM||';错误行数为:'||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
          ROLLBACK;
          P_PO_RTNCODE := SQLCODE;
          P_PO_RTNMSG  := '总账虚拟-信用风险暴露(PRO_RWA_XN_EXPOSURE)ETL转换失败！'|| SQLERRM||';错误行数为:'|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
         RETURN;

END PRO_RWA_XN_EXPOSURE;
/

