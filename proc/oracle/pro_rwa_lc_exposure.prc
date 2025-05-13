CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_LC_EXPOSURE(P_DATA_DT_STR IN VARCHAR2, --数据日期
                                                P_PO_RTNCODE  OUT VARCHAR2, --返回编号
                                                P_PO_RTNMSG   OUT VARCHAR2 --返回描述
                                                )
/*
  存储过程名称:RWA_DEV.PRO_RWA_LC_EXPOSURE
  实现功能:理财系统-理财投资-信用风险暴露(从数据源理财系统将业务相关信息全量导入RWA理财投资接口表风险暴露表中)
  数据口径:全量
  跑批频率:月初运行
  版  本  :V1.0.0
  编写人  :QHJIANG
  编写时间:2016-04-14
  单  位  :上海安硕信息技术股份有限公司
  源  表1 :RWA_DEV.ZGS_INVESTASSETDETAIL|资产详情表
  源  表2 :RWA_DEV.ZGS_FINANCING_INFO|产品信息表
  源  表3 :RWA_DEV.ZGS_ATBOND|债券信息表
  源  表4 :RWA_DEV.ZGS_ATINTRUST_PLAN|资产管理计划表
  源  表5 :RWA.CODE_LIBRARY|RWA代码表
  --源 表6 :RWA.RWA_WS_FCII_BOND|债券理财投资补录表 弃用
  --源 表7 :RWA.RWA_WS_FCII_PLAN|资管计划理财投资补录表 弃用
  --源  表8 :RWA.RWA_WP_DATASUPPLEMENT|补录任务明细信息表 弃用
  目标表1 :RWA_DEV.RWA_LC_EXPOSURE|RWA信用风险暴露信息表
  辅助表  :无
  变更记录(修改人|修改时间|修改内容):
  */

 AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  V_PRO_NAME VARCHAR2(200) := 'RWA_DEV.PRO_RWA_LC_EXPOSURE';
  --定义异常变量
  V_RAISE EXCEPTION;
  --定义当前插入的记录数
  V_COUNT1 INTEGER;
  --v_count2 INTEGER;

BEGIN

  --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

  --清除目标表中的原有记录
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_LC_EXPOSURE';

  --15030201 直接从总账取
  INSERT INTO RWA_DEV.RWA_LC_EXPOSURE
    (DATADATE, --数据日期
     DATANO, --数据流水号
     EXPOSUREID, --风险暴露ID
     DUEID, --债项ID
     SSYSID, --源系统ID
     CONTRACTID, --合同ID
     CLIENTID, --参与主体ID
     SORGID, --源机构ID
     SORGNAME, --源机构名称
     ORGSORTNO, --所属机构排序号
     ORGID, --所属机构ID
     ORGNAME, --所属机构名称
     ACCORGID, --账务机构ID
     ACCORGNAME, --账务机构名称
     INDUSTRYID, --所属行业代码
     INDUSTRYNAME, --所属行业名称
     BUSINESSLINE, --业务条线
     ASSETTYPE, --资产大类
     ASSETSUBTYPE, --资产小类
     BUSINESSTYPEID, --业务品种代码
     BUSINESSTYPENAME, --业务品种名称
     CREDITRISKDATATYPE, --信用风险数据类型
     ASSETTYPEOFHAIRCUTS, --折扣系数对应资产类别
     BUSINESSTYPESTD, --权重法业务类型
     EXPOCLASSSTD, --权重法暴露大类
     EXPOSUBCLASSSTD, --权重法暴露小类
     EXPOCLASSIRB, --内评法暴露大类
     EXPOSUBCLASSIRB, --内评法暴露小类
     EXPOBELONG, --暴露所属标识
     BOOKTYPE, --账户类别
     REGUTRANTYPE, --监管交易类型
     REPOTRANFLAG, --回购交易标识
     REVAFREQUENCY, --重估频率
     CURRENCY, --币种
     NORMALPRINCIPAL, --正常本金余额
     OVERDUEBALANCE, --逾期余额
     NONACCRUALBALANCE, --非应计余额
     ONSHEETBALANCE, --表内余额
     NORMALINTEREST, --正常利息
     ONDEBITINTEREST, --表内欠息
     OFFDEBITINTEREST, --表外欠息
     EXPENSERECEIVABLE, --应收费用
     ASSETBALANCE, --资产余额
     ACCSUBJECT1, --科目一
     ACCSUBJECT2, --科目二
     ACCSUBJECT3, --科目三
     STARTDATE, --起始日期
     DUEDATE, --到期日期
     ORIGINALMATURITY, --原始期限
     RESIDUALM, --剩余期限
     RISKCLASSIFY, --风险分类
     EXPOSURESTATUS, --风险暴露状态
     OVERDUEDAYS, --逾期天数
     SPECIALPROVISION, --专项准备金
     GENERALPROVISION, --一般准备金
     ESPECIALPROVISION, --特别准备金
     WRITTENOFFAMOUNT, --已核销金额
     OFFEXPOSOURCE, --表外暴露来源
     OFFBUSINESSTYPE, --表外业务类型
     OFFBUSINESSSDVSSTD, --权重法表外业务类型细分
     UNCONDCANCELFLAG, --是否可随时无条件撤销
     CCFLEVEL, --信用转换系数级别
     CCFAIRB, --高级法信用转换系数
     CLAIMSLEVEL, --债权级别
     BONDFLAG, --是否为债券
     BONDISSUEINTENT, --债券发行目的
     NSUREALPROPERTYFLAG, --是否非自用不动产
     REPASSETTERMTYPE, --抵债资产期限类型
     DEPENDONFPOBFLAG, --是否依赖于银行未来盈利
     IRATING, --内部评级
     PD, --违约概率
     LGDLEVEL, --违约损失率级别
     LGDAIRB, --高级法违约损失率
     MAIRB, --高级法有效期限
     EADAIRB, --高级法违约风险暴露
     DEFAULTFLAG, --违约标识
     BEEL, --已违约暴露预期损失比率
     DEFAULTLGD, --已违约暴露违约损失率
     EQUITYEXPOFLAG, --股权暴露标识
     EQUITYINVESTTYPE, --股权投资对象类型
     EQUITYINVESTCAUSE, --股权投资形成原因
     SLFLAG, --专业贷款标识
     SLTYPE, --专业贷款类型
     PFPHASE, --项目融资阶段
     REGURATING, --监管评级
     CBRCMPRATINGFLAG, --银监会认定评级是否更为审慎
     LARGEFLUCFLAG, --是否波动性较大
     LIQUEXPOFLAG, --是否清算过程中风险暴露
     PAYMENTDEALFLAG, --是否货款对付模式
     DELAYTRADINGDAYS, --延迟交易天数
     SECURITIESFLAG, --有价证券标识
     SECUISSUERID, --证券发行人ID
     RATINGDURATIONTYPE, --评级期限类型
     SECUISSUERATING, --证券发行等级
     SECURESIDUALM, --证券剩余期限
     SECUREVAFREQUENCY, --证券重估频率
     CCPTRANFLAG, --是否中央交易对手相关交易
     CCPID, --中央交易对手ID
     QUALCCPFLAG, --是否合格中央交易对手
     BANKROLE, --银行角色
     CLEARINGMETHOD, --清算方式
     BANKASSETFLAG, --是否银行提交资产
     MATCHCONDITIONS, --符合条件情况
     SFTFLAG, --证券融资交易标识
     MASTERNETAGREEFLAG, --净额结算主协议标识
     MASTERNETAGREEID, --净额结算主协议ID
     SFTTYPE, --证券融资交易类型
     SECUOWNERTRANSFLAG, --证券所有权是否转移
     OTCFLAG, --场外衍生工具标识
     VALIDNETTINGFLAG, --有效净额结算协议标识
     VALIDNETAGREEMENTID, --有效净额结算协议ID
     OTCTYPE, --场外衍生工具类型
     DEPOSITRISKPERIOD, --保证金风险期间
     MTM, --重置成本
     MTMCURRENCY, --重置成本币种
     BUYERORSELLER, --买方卖方
     QUALROFLAG, --合格参照资产标识
     ROISSUERPERFORMFLAG, --参照资产发行人是否能履约
     BUYERINSOLVENCYFLAG, --信用保护买方是否破产
     NONPAYMENTFEES, --尚未支付费用
     RETAILEXPOFLAG, --零售暴露标识
     RETAILCLAIMTYPE, --零售债权类型
     MORTGAGETYPE, --住房抵押贷款类型
     EXPONUMBER, --风险暴露个数
     LTV, --贷款价值比
     AGING, --账龄
     NEWDEFAULTDEBTFLAG, --新增违约债项标识
     PDPOOLMODELID, --PD分池模型ID
     LGDPOOLMODELID, --LGD分池模型ID
     CCFPOOLMODELID, --CCF分池模型ID
     PDPOOLID, --所属PD池ID
     LGDPOOLID, --所属LGD池ID
     CCFPOOLID, --所属CCF池ID
     ABSUAFLAG, --资产证券化基础资产标识
     ABSPOOLID, --证券化资产池ID
     GROUPID, --分组编号
     DEFAULTDATE, --违约时点
     ABSPROPORTION, --资产证券化比重
     DEBTORNUMBER, --借款人个数
     SBJT4,
     SBJT_VAL4)
  
    SELECT TO_DATE(P_DATA_DT_STR, 'YYYYMMDD') AS DATADATE, --数据日期
           P_DATA_DT_STR AS DATANO, --数据流水号
           'XN-15030201' AS EXPOSUREID, --风险暴露ID
           'XN-15030201' AS DUEID, --债项ID
           'LC' AS SSYSID, --源系统ID
           'XN-15030201' AS CONTRACTID, --合同ID
           'XN-ZGSYYH' AS CLIENTID, --参与主体ID
           '9998' AS SORGID, --源机构ID  默认 总行资产管理部(01160000)
           '重庆银行' AS SORGNAME, --源机构名称  默认 总行资产管理部
           '1' AS ORGSORTNO, --所属机构排序号
           '9998' AS ORGID, --所属机构ID  默认 总行资产管理部(01160000)
           '重庆银行' AS ORGNAME, --所属机构名称  默认 总行资产管理部
           '9998' AS ACCORGID, --账务机构ID  默认 总行资产管理部(01160000)
           '重庆银行' AS ACCORGNAME, --账务机构名称  默认 总行资产管理部
           'C41' AS INDUSTRYID, --所属行业代码
           '其他制造业' AS INDUSTRYNAME, --所属行业名称
           '0402' AS BUSINESSLINE, --业务条线  默认 同业(04)
           '132' AS ASSETTYPE, --资产大类  默认 NULL RWA规则处理
           '13205' AS ASSETSUBTYPE, --资产小类  默认 NULL RWA规则处理
           '109020' AS BUSINESSTYPEID, --业务品种代码  重编业务品种代码
           '理财-债券投资' AS BUSINESSTYPENAME, --业务品种名称  重编业务品种代码
           '01' AS CREDITRISKDATATYPE, --信用风险数据类型  默认 一般非零售(01)
           '01' AS ASSETTYPEOFHAIRCUTS, --折扣系数对应资产类别  默认 现金及现金等价物(01)
           '07' AS BUSINESSTYPESTD, --权重法业务类型  默认 一般资产(07)
           '0112' AS EXPOCLASSSTD, --权重法暴露大类  默认 NULL RWA规则处理
           '011216' AS EXPOSUBCLASSSTD, --权重法暴露小类  默认 NULL RWA规则处理
           '0203' AS EXPOCLASSIRB, --内评法暴露大类  默认 NULL RWA规则处理
           '020301' AS EXPOSUBCLASSIRB, --内评法暴露小类  默认 NULL RWA规则处理
           '01' AS EXPOBELONG, --暴露所属标识  默认 表内(01)
           '01' AS BOOKTYPE, --账户类别  资产类型为交易性金融资产(10)则为交易账户(02)，否则为银行账户(01)
           '03' AS REGUTRANTYPE, --监管交易类型  默认 抵押贷款(03)
           '0' AS REPOTRANFLAG, --回购交易标识  默认 否(0)
           '1' AS REVAFREQUENCY, --重估频率  默认 1
           T1.CURRENCY AS CURRENCY, --币种
           T1.CUR_BAL AS NORMALPRINCIPAL, --正常本金余额
           0 AS OVERDUEBALANCE, --逾期余额  默认 0
           0 AS NONACCRUALBALANCE, --非应计余额  默认 0
           T1.CUR_BAL AS ONSHEETBALANCE, --表内余额
           0 AS NORMALINTEREST, --正常利息  默认 0 利息统一从总账表虚拟
           0 AS ONDEBITINTEREST, --表内欠息  默认 0
           0 AS OFFDEBITINTEREST, --表外欠息  默认 0
           0 AS EXPENSERECEIVABLE, --应收费用  默认 0
           T1.CUR_BAL AS ASSETBALANCE, --资产余额  理财业务屏蔽校验规则
           T1.SUBJECT_NO AS ACCSUBJECT1, --科目一
           '' AS ACCSUBJECT2, --科目二
           '' AS ACCSUBJECT3, --科目三
           P_DATA_DT_STR AS STARTDATE, --起始日期
           P_DATA_DT_STR AS DUEDATE, --到期日期
           0 AS ORIGINALMATURITY, --原始期限
           0 AS RESIDUALM, --剩余期限
           '01' AS RISKCLASSIFY, --风险分类                        后续使用信贷的12级分类转换
           '01' AS EXPOSURESTATUS, --风险暴露状态                    默认 正常(01)
           0 AS OVERDUEDAYS, --逾期天数                        默认 0
           0 AS SPECIALPROVISION, --专项准备金                     默认 0  RWA计算
           0 AS GENERALPROVISION, --一般准备金                     默认 0  RWA计算
           0 AS ESPECIALPROVISION, --特别准备金                     默认 0  RWA计算
           0 AS WRITTENOFFAMOUNT, --已核销金额                     默认 0
           '' AS OFFEXPOSOURCE, --表外暴露来源                    默认 NULL
           '' AS OFFBUSINESSTYPE, --表外业务类型                    默认 NULL
           '' AS OFFBUSINESSSDVSSTD, --权重法表外业务类型细分         默认 NULL
           '0' AS UNCONDCANCELFLAG, --是否可随时无条件撤销            默认 NULL
           '' AS CCFLEVEL, --信用转换系数级别                默认 NULL
           NULL AS CCFAIRB, --高级法信用转换系数             默认 NULL
           '01' AS CLAIMSLEVEL, --债权级别
           '1' AS BONDFLAG, --是否为债券                     默认 是(1)
           '02' AS BONDISSUEINTENT, --债券发行目的
           '0' AS NSUREALPROPERTYFLAG, --是否非自用不动产                默认 否(0)
           '' AS REPASSETTERMTYPE, --抵债资产期限类型                默认 NULL
           '0' AS DEPENDONFPOBFLAG, --是否依赖于银行未来盈利         默认 否(0)
           NULL AS IRATING, --内部评级
           NULL AS PD, --违约概率
           '' AS LGDLEVEL, --违约损失率级别                 默认 NULL
           NULL AS LGDAIRB, --高级法违约损失率                默认 NULL
           NULL AS MAIRB, --高级法有效期限                 默认 NULL
           NULL AS EADAIRB, --高级法违约风险暴露             默认 NULL
           '0' AS DEFAULTFLAG, --违约标识
           0.45 AS BEEL, --已违约暴露预期损失比率          债权级别=次级债(02)，则为0.75，否则为0.45
           0.45 AS DEFAULTLGD, --已违约暴露违约损失率            默认 NULL
           '0' AS EQUITYEXPOFLAG, --股权暴露标识                    默认 否(0)
           '' AS EQUITYINVESTTYPE, --股权投资对象类型                默认 NULL
           '' AS EQUITYINVESTCAUSE, --股权投资形成原因                默认 NULL
           '0' AS SLFLAG, --专业贷款标识                    默认 否(0)
           '' AS SLTYPE, --专业贷款类型                    默认 NULL
           '' AS PFPHASE, --项目融资阶段                    默认 NULL
           '01' AS REGURATING, --监管评级                        默认 优(01)
           '' AS CBRCMPRATINGFLAG, --银监会认定评级是否更为审慎     默认 NULL
           '' AS LARGEFLUCFLAG, --是否波动性较大                 默认 NULL
           '0' AS LIQUEXPOFLAG, --是否清算过程中风险暴露         默认 否(0)
           '1' AS PAYMENTDEALFLAG, --是否货款对付模式                默认 是(1)
           '0' AS DELAYTRADINGDAYS, --延迟交易天数                    默认 NULL
           '1' AS SECURITIESFLAG, --有价证券标识                    默认 是(1)
           '' AS SECUISSUERID, --证券发行人ID
           '' AS RATINGDURATIONTYPE, --评级期限类型
           '' AS SECUISSUERATING, --证券发行等级
           NULL AS SECURESIDUALM, --证券剩余期限
           1 AS SECUREVAFREQUENCY, --证券重估频率                    默认 1
           '0' AS CCPTRANFLAG, --是否中央交易对手相关交易        默认 否(0)
           '' AS CCPID, --中央交易对手ID                  默认 NULL
           '' AS QUALCCPFLAG, --是否合格中央交易对手            默认 NULL
           '' AS BANKROLE, --银行角色                        默认 NULL
           '' AS CLEARINGMETHOD, --清算方式                        默认 NULL
           '' AS BANKASSETFLAG, --是否银行提交资产                默认 NULL
           '' AS MATCHCONDITIONS, --符合条件情况                    默认 NULL
           '0' AS SFTFLAG, --证券融资交易标识                默认 否(0)
           '0' AS MASTERNETAGREEFLAG, --净额结算主协议标识             默认 否(0)
           '' AS MASTERNETAGREEID, --净额结算主协议ID               默认 NULL
           '' AS SFTTYPE, --证券融资交易类型                默认 NULL
           '' AS SECUOWNERTRANSFLAG, --证券所有权是否转移             默认 NULL
           '0' AS OTCFLAG, --场外衍生工具标识                默认 否(0)
           '' AS VALIDNETTINGFLAG, --有效净额结算协议标识            默认 NULL
           '' AS VALIDNETAGREEMENTID, --有效净额结算协议ID              默认 NULL
           '' AS OTCTYPE, --场外衍生工具类型                默认 NULL
           '' AS DEPOSITRISKPERIOD, --保证金风险期间                 默认 NULL
           NULL AS MTM, --重置成本                        默认 NULL
           '' AS MTMCURRENCY, --重置成本币种                    默认 NULL
           '' AS BUYERORSELLER, --买方卖方                        默认 NULL
           '' AS QUALROFLAG, --合格参照资产标识                默认 NULL
           '' AS ROISSUERPERFORMFLAG, --参照资产发行人是否能履约        默认 NULL
           '' AS BUYERINSOLVENCYFLAG, --信用保护买方是否破产            默认 NULL
           NULL AS NONPAYMENTFEES, --尚未支付费用                    默认 NULL
           '0' AS RETAILEXPOFLAG, --零售暴露标识                    默认 否(0)
           '' AS RETAILCLAIMTYPE, --零售债权类型                    默认 NULL
           '' AS MORTGAGETYPE, --住房抵押贷款类型                默认 NULL
           1 AS EXPONUMBER, --风险暴露个数                    默认 1
           0.8 AS LTV, --贷款价值比                     默认 0.8
           NULL AS AGING, --账龄                            默认 NULL
           '' AS NEWDEFAULTDEBTFLAG, --新增违约债项标识                默认 NULL
           '' AS PDPOOLMODELID, --PD分池模型ID                    默认 NULL
           '' AS LGDPOOLMODELID, --LGD分池模型ID                   默认 NULL
           '' AS CCFPOOLMODELID, --CCF分池模型ID                   默认 NULL
           '' AS PDPOOLID, --所属PD池ID                     默认 NULL
           '' AS LGDPOOLID, --所属LGD池ID                    默认 NULL
           '' AS CCFPOOLID, --所属CCF池ID                    默认 NULL
           '0' AS ABSUAFLAG, --资产证券化基础资产标识         默认 否(0)
           '' AS ABSPOOLID, --证券化资产池ID                  默认 NULL
           '' AS GROUPID, --分组编号                        默认 NULL
           NULL AS DEFAULTDATE, --违约时点                        默认 NULL
           NULL AS ABSPROPORTION, --资产证券化比重
           NULL AS DEBTORNUMBER, --借款人个数
           NULL,
           NULL
      FROM (SELECT T1.DATANO,
                   '12220102' AS SUBJECT_NO,
                   'CNY' AS CURRENCY,
                   SUM(T1.BALANCE_D * T2.JZRAT / 100 -
                       T1.BALANCE_C * T2.JZRAT / 100) AS CUR_BAL -- 借方-贷方
              FROM FNS_GL_BALANCE T1
              LEFT JOIN NNS_JT_EXRATE T2 -- 折人
                ON T1.DATANO = T2.DATANO
               AND T1.CURRENCY_CODE = T2.CCY
             WHERE T1.CURRENCY_CODE <> 'RMB'
               AND T1.DATANO = P_DATA_DT_STR
               AND (T1.SUBJECT_NO LIKE '150302%' OR
                   T1.SUBJECT_NO IN ('12220102', '12220103', '12220104'))
             GROUP BY T1.DATANO) T1; -- 12220102=15030201+15030202+15030203+15030204+12220102+12220103+12220104

  COMMIT;

  DBMS_STATS.GATHER_TABLE_STATS(OWNNAME => 'RWA_DEV',
                                TABNAME => 'RWA_LC_EXPOSURE',
                                CASCADE => TRUE);

  --DBMS_OUTPUT.PUT_LINE('结束【步骤2】：导入【信用风险暴露-资管】' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

  /*目标表数据统计*/
  --统计插入的记录
  SELECT COUNT(1) INTO V_COUNT1 FROM RWA_DEV.RWA_LC_EXPOSURE;
  --DBMS_OUTPUT.PUT_LINE('RWA_DEV.RWA_LC_EXPOSURE表当前插入的理财系统-资管计划投资数据记录为：' || (v_count2-v_count1) || '条');

  --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
  P_PO_RTNCODE := '1';
  P_PO_RTNMSG  := '成功' || '-' || V_COUNT1;

  --定义异常
EXCEPTION
  WHEN OTHERS THEN
    --DBMS_OUTPUT.PUT_LINE('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
    ROLLBACK;
    P_PO_RTNCODE := SQLCODE;
    P_PO_RTNMSG  := '信用风险暴露(' || V_PRO_NAME || ')ETL转换失败！' || SQLERRM ||
                    ';错误行数为:' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
  
    RETURN;
END PRO_RWA_LC_EXPOSURE;
/

