CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_GQ_EXPOSURE(
                                                P_DATA_DT_STR  IN   VARCHAR2,    --数据日期
                                                P_PO_RTNCODE   OUT  VARCHAR2,    --返回编号
                                                P_PO_RTNMSG    OUT  VARCHAR2     --返回描述
                                               )
  /*
    存储过程名称:RWA_DEV.PRO_RWA_GQ_EXPOSURE
    实现功能:股权投资-信用风险暴露
    数据口径:全量
    跑批频率:月末
    版  本  :V1.0.0
    编写人  :qpzhong
    编写时间:2016-08-23
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.rwa_ei_unconsfiinvest        |长期股权投资补录表
    源  表2 :RWA.ORG_INFO             |机构表
    目标表  :RWA_DEV.rwa_gq_exposure                  |股权投资-信用风险暴露表
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  V_PRO_NAME VARCHAR2(200) := 'RWA_DEV.PRO_RWA_GQ_EXPOSURE';
  V_COUNT INTEGER;
  --定义异常变量
  V_RAISE EXCEPTION;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --1.清除目标表中的原有记录
    /*如果是全量数据加载需清空目标表*/
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_GQ_EXPOSURE';

    --2.将满足条件的数据从源表插入到目标表中
    /*插入目标表*/
    INSERT INTO RWA_DEV.RWA_GQ_EXPOSURE (
                 DataDate                                                                           --数据日期
                ,DataNo                                                                             --数据流水号
                ,ExposureID                                                                         --风险暴露ID
                ,DueID                                                                              --债项ID
                ,SSysID                                                                             --源系统ID
                ,ContractID                                                                         --合同ID
                ,ClientID                                                                           --参与主体ID
                ,SOrgID                                                                             --源机构ID
                ,SOrgName                                                                           --源机构名称
                ,OrgSortNo                                                                          --所属机构排序号
                ,OrgID                                                                              --所属机构ID
                ,OrgName                                                                            --所属机构名称
                ,AccOrgID                                                                           --账务机构ID
                ,AccOrgName                                                                         --账务机构名称
                ,IndustryID                                                                         --所属行业代码
                ,IndustryName                                                                       --所属行业名称
                ,BusinessLine                                                                       --业务条线
                ,AssetType                                                                          --资产大类
                ,AssetSubType                                                                       --资产小类
                ,BusinessTypeID                                                                     --业务品种代码
                ,BusinessTypeName                                                                   --业务品种名称
                ,CreditRiskDataType                                                                 --信用风险数据类型
                ,AssetTypeOfHaircuts                                                                --折扣系数对应资产类别
                ,BusinessTypeSTD                                                                    --权重法业务类型
                ,ExpoClassSTD                                                                       --权重法暴露大类
                ,ExpoSubClassSTD                                                                    --权重法暴露小类
                ,ExpoClassIRB                                                                       --内评法暴露大类
                ,ExpoSubClassIRB                                                                    --内评法暴露小类
                ,ExpoBelong                                                                         --暴露所属标识
                ,BookType                                                                           --账户类别
                ,ReguTranType                                                                       --监管交易类型
                ,RepoTranFlag                                                                       --回购交易标识
                ,RevaFrequency                                                                      --重估频率
                ,Currency                                                                           --币种
                ,NormalPrincipal                                                                    --正常本金余额
                ,OverdueBalance                                                                     --逾期余额
                ,NonAccrualBalance                                                                  --非应计余额
                ,OnSheetBalance                                                                     --表内余额
                ,NormalInterest                                                                     --正常利息
                ,OnDebitInterest                                                                    --表内欠息
                ,OffDebitInterest                                                                   --表外欠息
                ,ExpenseReceivable                                                                  --应收费用
                ,AssetBalance                                                                       --资产余额
                ,AccSubject1                                                                        --科目一
                ,AccSubject2                                                                        --科目二
                ,AccSubject3                                                                        --科目三
                ,StartDate                                                                          --起始日期
                ,DueDate                                                                            --到期日期
                ,OriginalMaturity                                                                   --原始期限
                ,ResidualM                                                                          --剩余期限
                ,RiskClassify                                                                       --风险分类
                ,ExposureStatus                                                                     --风险暴露状态
                ,OverdueDays                                                                        --逾期天数
                ,SpecialProvision                                                                   --专项准备金
                ,GeneralProvision                                                                   --一般准备金
                ,EspecialProvision                                                                  --特别准备金
                ,WrittenOffAmount                                                                   --已核销金额
                ,OffExpoSource                                                                      --表外暴露来源
                ,OffBusinessType                                                                    --表外业务类型
                ,OffBusinessSdvsSTD                                                                 --权重法表外业务类型细分
                ,UncondCancelFlag                                                                   --是否可随时无条件撤销
                ,CCFLevel                                                                           --信用转换系数级别
                ,CCFAIRB                                                                            --高级法信用转换系数
                ,ClaimsLevel                                                                        --债权级别
                ,BondFlag                                                                           --是否为债券
                ,BondIssueIntent                                                                    --债券发行目的
                ,NSURealPropertyFlag                                                                --是否非自用不动产
                ,RepAssetTermType                                                                   --抵债资产期限类型
                ,DependOnFPOBFlag                                                                   --是否依赖于银行未来盈利
                ,IRating                                                                            --内部评级
                ,PD                                                                                 --违约概率
                ,LGDLevel                                                                           --违约损失率级别
                ,LGDAIRB                                                                            --高级法违约损失率
                ,MAIRB                                                                              --高级法有效期限
                ,EADAIRB                                                                            --高级法违约风险暴露
                ,DefaultFlag                                                                        --违约标识
                ,BEEL                                                                               --已违约暴露预期损失比率
                ,DefaultLGD                                                                         --已违约暴露违约损失率
                ,EquityExpoFlag                                                                     --股权暴露标识
                ,EquityInvestType                                                                   --股权投资对象类型
                ,EquityInvestCause                                                                  --股权投资形成原因
                ,SLFlag                                                                             --专业贷款标识
                ,SLType                                                                             --专业贷款类型
                ,PFPhase                                                                            --项目融资阶段
                ,ReguRating                                                                         --监管评级
                ,CBRCMPRatingFlag                                                                   --银监会认定评级是否更为审慎
                ,LargeFlucFlag                                                                      --是否波动性较大
                ,LiquExpoFlag                                                                       --是否清算过程中风险暴露
                ,PaymentDealFlag                                                                    --是否货款对付模式
                ,DelayTradingDays                                                                   --延迟交易天数
                ,SecuritiesFlag                                                                     --有价证券标识
                ,SecuIssuerID                                                                       --证券发行人ID
                ,RatingDurationType                                                                 --评级期限类型
                ,SecuIssueRating                                                                    --证券发行等级
                ,SecuResidualM                                                                      --证券剩余期限
                ,SecuRevaFrequency                                                                  --证券重估频率
                ,CCPTranFlag                                                                        --是否中央交易对手相关交易
                ,CCPID                                                                              --中央交易对手ID
                ,QualCCPFlag                                                                        --是否合格中央交易对手
                ,BankRole                                                                           --银行角色
                ,ClearingMethod                                                                     --清算方式
                ,BankAssetFlag                                                                      --是否银行提交资产
                ,MatchConditions                                                                    --符合条件情况
                ,SFTFlag                                                                            --证券融资交易标识
                ,MasterNetAgreeFlag                                                                 --净额结算主协议标识
                ,MasterNetAgreeID                                                                   --净额结算主协议ID
                ,SFTType                                                                            --证券融资交易类型
                ,SecuOwnerTransFlag                                                                 --证券所有权是否转移
                ,OTCFlag                                                                            --场外衍生工具标识
                ,ValidNettingFlag                                                                   --有效净额结算协议标识
                ,ValidNetAgreementID                                                                --有效净额结算协议ID
                ,OTCType                                                                            --场外衍生工具类型
                ,DepositRiskPeriod                                                                  --保证金风险期间
                ,MTM                                                                                --重置成本
                ,MTMCurrency                                                                        --重置成本币种
                ,BuyerOrSeller                                                                      --买方卖方
                ,QualROFlag                                                                         --合格参照资产标识
                ,ROIssuerPerformFlag                                                                --参照资产发行人是否能履约
                ,BuyerInsolvencyFlag                                                                --信用保护买方是否破产
                ,NonpaymentFees                                                                     --尚未支付费用
                ,RetailExpoFlag                                                                     --零售暴露标识
                ,RetailClaimType                                                                    --零售债权类型
                ,MortgageType                                                                       --住房抵押贷款类型
                ,ExpoNumber                                                                         --风险暴露个数
                ,LTV                                                                                --贷款价值比
                ,Aging                                                                              --账龄
                ,NewDefaultDebtFlag                                                                 --新增违约债项标识
                ,PDPoolModelID                                                                      --PD分池模型ID
                ,LGDPoolModelID                                                                     --LGD分池模型ID
                ,CCFPoolModelID                                                                     --CCF分池模型ID
                ,PDPoolID                                                                           --所属PD池ID
                ,LGDPoolID                                                                          --所属LGD池ID
                ,CCFPoolID                                                                          --所属CCF池ID
                ,ABSUAFlag                                                                          --资产证券化基础资产标识
                ,ABSPoolID                                                                          --证券化资产池ID
                ,GroupID                                                                            --分组编号
                ,DefaultDate                                                                        --违约时点
                ,ABSPROPORTION                                                                      --资产证券化比重
                ,DEBTORNUMBER                                                                       --借款人个数
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')                               AS datadate                    --数据日期
                ,p_data_dt_str                                                  AS datano                      --数据流水号
                ,'GQ'||t1.serialno                                              AS exposureid                  --风险暴露ID
                ,''                                                             AS dueid                       --债项ID
                ,'GQ'                                                           AS ssysid                      --源系统ID                  (默认为'GC')
                ,'GQ'||t1.serialno                                              AS contractid                  --合同ID
                ,t1.custid1                                                     AS clientid                    --参与主体ID
                ,t1.orgid                                                       AS sorgid                      --源机构ID
                ,t2.orgname                                                     AS sorgname                    --源机构名称
                ,T2.SORTNO                                                      AS ORGSORTNO                   --所属机构排序号
                ,t1.orgid                                                       AS orgid                       --所属机构ID
                ,t2.orgname                                                     AS orgname                     --所属机构名称
                ,t1.orgid                                                       AS accorgid                    --账务机构ID                (默认值0000-杭州银行,)
                ,t2.orgname                                                     AS accorgname                  --账务机构名称              (默认值杭州银行,)
                ,'999999'                                                       AS industryid                  --所属行业代码
                ,'未知'                                                         AS industryname                --所属行业名称
                ,t1.businessline                                                AS businessline                --条线                      (默认为'6其它',1公司,2零售,3小企业,4资金,5资管,6其它)
                ,'121'                                                          AS assettype                   --资产大类
                ,'12103'                                                        AS assetsubtype                --资产小类
                ,'109060'                                                       AS businesstypeid              --业务品种代码              (默认值'GQ')
                ,'股权投资'                                                     AS businesstypename            --业务品种名称              (默认值'股权投资')
                ,'01'                                                           AS creditriskdatatype          --信用风险数据类型          (默认为股权,01:一般非零售;02:一般零售;03交易对手)
                ,'02'                                                           AS assettypeofhaircuts         --折扣系数对应资产类别      (默认值：'02'具有现金价值的人寿保险单及类似理财产品)
                ,'05'                                                           AS businesstypestd             --权重法业务类型            (默认为05股权)
                ,'0110'                                                         AS expoclassstd                --权重法暴露大类            (默认为0110股权)
                ,CASE WHEN substr(t1.equityinvesttype,1,2)='02' THEN '011001'
                      WHEN t1.equityinvestcause = '01'  THEN '011002'           --股权资产形成原因(equityinvestcause) 01 被动持有 02 政策性原因 03 其他
                      WHEN t1.equityinvestcause = '02' THEN '011003'
                      ELSE '011004' END                                         AS exposubclassstd             --权重法暴露小类
                ,''                                                             AS expoclassirb                --内评法暴露大类
                ,''                                                             AS exposubclassirb             --内评法暴露小类            (对金融机构的股权  020501  对企业的股权  020502)
                ,'01'                                                           AS expobelong                  --暴露所属标识              (默认值为'表内',01表内;02一般表外;03交易对手)
                ,'01'                                                           AS booktype                    --账户类别                  (默认为银行账户：'01'银行账户)
                ,'02'                                                           AS regutrantype                --监管交易类型              (默认值为其他资本市场交易,'02'其他资本市场交易)
                ,'0'                                                            AS repotranflag                --回购交易标识              (默认值'否',1是0否)
                ,1                                                              AS revafrequency               --重估频率                  (默认为'1')
                ,t1.currency                                                    AS currency                    --币种                      (默认值'人民币')
                ,CASE WHEN substr(t1.equityinvesttype,1,2)='02' THEN t1.equityinvestamount
                      else ctocinvestamount end                                 AS normalprincipal             --正常本金余额             (股权投资金额)
                ,0                                                              AS overduebalance              --逾期余额                  (默认为空)
                ,0                                                              AS nonaccrualbalance           --非应计余额                (默认为空)
                ,CASE WHEN substr(t1.equityinvesttype,1,2)='02' THEN t1.equityinvestamount
                      ELSE ctocinvestamount END                                 AS onsheetbalance              --表内余额                  (正常本金余额 + 逾期余额 + 非应余额计)
                ,0                                                              AS normalinterest              --正常利息                  (默认为空)
                ,0                                                              AS ondebitinterest             --表内欠息                  (默认为0)
                ,0                                                              AS offdebitinterest            --表外欠息                  (默认为0)
                ,0                                                              AS expensereceivable           --应收费用                  (默认为0)
                ,CASE WHEN substr(t1.equityinvesttype,1,2)='02' THEN t1.equityinvestamount
                      ELSE ctocinvestamount END                                 AS assetbalance                --资产余额                  (资产余额=表内余额+正常利息+表内欠息+应收费用)
                ,t1.Subject                                                     AS accsubject1                 --科目一                    (会计科目)
                ,''                                                             AS accsubject2                 --科目二                    (默认为空)
                ,''                                                             AS accsubject3                 --科目三                    (默认为空)
                ,TO_CHAR(TO_DATE(p_data_dt_str,'YYYY-MM-DD'),'YYYY-MM-DD')      AS startdate                   --起始日期
                ,TO_CHAR(ADD_MONTHS(TO_DATE(p_data_dt_str,'YYYY-MM-DD'),1),'YYYY-MM-DD')
                                                                                AS duedate                     --到期日期
                ,(ADD_MONTHS(TO_DATE(p_data_dt_str,'YYYYMMDD'),1) - TO_DATE(p_data_dt_str,'YYYYMMDD'))/365
                                                                                AS originalmaturity            --原始期限
                ,(ADD_MONTHS(TO_DATE(p_data_dt_str,'YYYYMMDD'),1) - TO_DATE(p_data_dt_str,'YYYYMMDD'))/365
                                                                                AS residualm                   --剩余期限
                ,NVL(t1.riskclassify,'01')                                      AS riskclassify                --风险分类                  (默认为01-正常)
                ,'01'                                                           AS exposurestatus              --风险暴露状态              (默认为01-正常)
                ,0                                                              AS overduedays                 --逾期天数                  (默认为0)
                ,0                                                              AS specialprovision            --专项准备金                (默认为0)
                ,0                                                              AS generalprovision            --一般准备金                (默认为0)
                ,0                                                              AS especialprovision           --特别准备金                (默认为0)
                ,0                                                              AS writtenoffamount            --已核销金额                (默认为0)
                ,''                                                             AS offexposource               --表外暴露来源              (默认为空)
                ,''                                                             AS offbusinesstype             --表外业务类型              (默认为空)
                ,''                                                             AS offbusinesssdvsstd          --权重法表外业务类型细分    (默认为空)
                ,'0'                                                            AS uncondcancelflag            --是否可随时无条件撤销      (默认值'否',1是0否)
                ,''                                                             AS ccflevel                    --信用转换系数级别          (默认值空)
                ,NULL                                                           AS ccfairb                     --高级法信用转换系数        (默认为空)
                ,'01'                                                           AS claimslevel                 --债权级别                  (默认值为'高级债权',01高级债权;02低级债权)
                ,'0'                                                            AS bondflag                    --是否为债券                (默认值'否',1是0否)
                ,''                                                             AS bondissueintent             --债券发行目的
                ,'0'                                                            AS nsurealpropertyflag         --是否非自用不动产          (默认值'否',1是0否)
                ,''                                                             AS repassettermtype            --抵债资产期限类型          (默认为空)
                ,'0'                                                            AS securevafrequency           --是否依赖于银行未来盈利    (默认值'否',1是0否)
                ,''                                                             AS irating                     --内部评级                  (默认为空)
                ,NULL                                                           AS pd                          --违约概率
                ,''                                                             AS lgdlevel                    --违约损失率级别            (默认为空值)
                ,NULL                                                           AS lgdairb                     --高级法违约损失率
                ,NULL                                                           AS mairb                       --高级法有效期限            (默认为空)
                ,''                                                             AS eadairb                     --高级法违约风险暴露
                ,'0'                                                            AS defaultflag                 --违约标识                  (默认值'否',1是0否)
                ,0.45                                                           AS beel                        --已违约暴露预期损失比率    (默认为0.45)
                ,0.45                                                           AS defaultlgd                  --已违约暴露违约损失率      (默认为0.45)
                ,'1'                                                            AS equityexpoflag              --股权暴露标识              (默认值'是',1是0否)
                ,case when substr(t1.EQUITYINVESTTYPE,1,2)= '02' then '01' else '02' end                                                   AS equityinvesttype            --股权投资对象类型
                ,t1.equityinvestcause                                           AS equityinvestcause           --股权投资形成原因
                ,'0'                                                            AS slflag                      --专业贷款标识              (默认值'否',1是0否)
                ,''                                                             AS sltype                      --专业贷款类型
                ,''                                                             AS pfphase                     --项目融资阶段
                ,''                                                             AS regurating                  --监管评级
                ,''                                                             AS cbrcmpratingflag            --银监会认定评级是否更为审慎
                ,'0'                                                            AS largeflucflag               --是否波动性较大            (默认值'否',1是0否)
                ,'0'                                                            AS liquexpoflag                --是否清算过程中风险暴露    (默认值'否',1是0否)
                ,'0'                                                            AS paymentdealflag             --是否货款对付模式          (默认值'否',1是0否)
                ,0                                                              AS delaytradingdays            --延迟交易天数
                ,'0'                                                            AS securitiesflag              --有价证券标识              (默认值'否',1是0否)
                ,''                                                             AS secuissuerid                --证券发行人ID
                ,''                                                             AS ratingdurationtype          --评级期限类型
                ,''                                                             AS secuissuerating             --证券发行等级
                ,0                                                              AS securesidualm               --证券剩余期限
                ,1                                                              AS dependonfpobflag            --证券重估频率              (默认为1)
                ,'0'                                                            AS ccptranflag                 --是否中央交易对手相关交易  (默认值'否',1是0否)
                ,''                                                             AS ccpid                       --中央交易对手ID
                ,'0'                                                            AS qualccpflag                 --是否合格中央交易对手      (默认值'否',1是0否)
                ,''                                                             AS bankrole                    --银行角色
                ,''                                                             AS clearingmethod              --清算方式
                ,''                                                             AS bankassetflag               --是否银行提交资产
                ,''                                                             AS matchconditions             --符合条件情况
                ,'0'                                                            AS sftflag                     --证券融资交易标识          (默认值'否',1是0否)
                ,'0'                                                            AS masternetagreeflag          --净额结算主协议标识        (默认值'否',1是0否)
                ,''                                                             AS masternetagreeid            --净额结算主协议ID
                ,''                                                             AS sfttype                     --证券融资交易类型
                ,'0'                                                            AS secuownertransflag          --证券所有权是否转移        (默认值'否',1是0否)
                ,'0'                                                            AS otcflag                     --场外衍生工具标识          (默认值'否',1是0否)
                ,'0'                                                            AS validnettingflag            --有效净额结算协议标识      (默认值'否',1是0否)
                ,''                                                             AS validnetagreementid         --有效净额结算协议ID
                ,''                                                             AS otctype                     --场外衍生工具类型
                ,0                                                              AS depositriskperiod           --保证金风险期间
                ,0                                                              AS mtm                         --重置成本
                ,''                                                             AS mtmcurrency                 --重置成本币种
                ,''                                                             AS buyerorseller               --买方卖方
                ,'0'                                                            AS qualroflag                  --合格参照资产标识          (默认值'否',1是0否)
                ,'0'                                                            AS roissuerperformflag         --参照资产发行人是否能履约  (默认值'否',1是0否)
                ,''                                                             AS buyerinsolvencyflag         --信用保护买方是否破产
                ,0                                                              AS nonpaymentfees              --尚未支付费用
                ,'0'                                                            AS retailexpoflag              --零售暴露标识              (默认值'否',1是0否)
                ,''                                                             AS retailclaimtype             --零售债权类型              (默认为空)
                ,''                                                             AS mortgagetype                --住房抵押贷款类型          (默认为空)
                ,1                                                              AS exponumber                  --风险暴露个数              (默认为1)
                ,0.8                                                            AS LTV                         --贷款价值比                    默认 0.8
                ,NULL                                                           AS Aging                       --账龄                          默认 NULL
                ,''                                                             AS NewDefaultDebtFlag          --新增违约债项标识                 默认 NULL
                ,''                                                             AS PDPoolModelID               --PD分池模型ID                  默认 NULL
                ,''                                                             AS LGDPoolModelID              --LGD分池模型ID                 默认 NULL
                ,''                                                             AS CCFPoolModelID              --CCF分池模型ID                 默认 NULL
                ,''                                                             AS PDPoolID                    --所属PD池ID                    默认 NULL
                ,''                                                             AS LGDPoolID                   --所属LGD池ID                   默认 NULL
                ,''                                                             AS CCFPoolID                   --所属CCF池ID                   默认 NULL
                ,'0'                                                            AS ABSUAFlag                   --资产证券化基础资产标识        默认 否(0)
                ,''                                                             AS ABSPoolID                   --证券化资产池ID                默认 NULL
                ,''                                                             AS GroupID                     --分组编号                      默认 NULL
                ,NULL                                                           AS DefaultDate                 --违约时点
                ,NULL                                                           AS ABSPROPORTION               --资产证券化比重
                ,NULL                                                           AS DEBTORNUMBER                --借款人个数
    FROM        RWA_DEV.rwa_ei_unconsfiinvest t1                  --长期股权投资补录表
    LEFT JOIN   RWA.ORG_INFO T2
    ON          T2.ORGID = T1.ORGID
    WHERE       T1.DATADATE = TO_DATE(p_data_dt_str,'yyyy-mm-dd')
    AND         T1.CONSOLIDATEFLAG = '0'
    AND         t1.EQUITYINVESTTYPE LIKE '03%'
    ;
    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_GQ_EXPOSURE',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_GQ_EXPOSURE;
    Dbms_output.Put_line('RWA_DEV.rwa_gq_exposure表当前插入的数据记录为:' || v_count || '条');
    Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功-'||v_count;
    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
         Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '股权投资-信用风险暴露(RWA_DEV.pro_rwa_gq_exposure)ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
    RETURN;

END pro_rwa_gq_exposure;
/

