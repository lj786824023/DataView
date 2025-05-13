CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_TZ_EXPOSURE_1224(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_TZ_EXPOSURE_1224
    实现功能:财务系统-投资-信用风险暴露(从数据源财务系统将业务相关信息全量导入RWA投资接口表风险暴露表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-04-12
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.FNS_BND_INFO_B|财务系统债券信息表
    源  表2 :RWA_DEV.FNS_BND_BOOK_B|财务系统账面活动表
    源  表5 :RWA.CODE_LIBRARY|RWA代码表
    源	表6 :RWA.ORG_INFO|机构信息表
    源  表8 :RWA_DEV.NCM_BUSINESS_DUEBILL|信贷借据表
    源  表9 :RWA_DEV.NCM_CUSTOMER_INFO|客户信息表
    源  表10:RWA_DEV.IRS_CR_CUSTOMER_RATE|非零售客户评级信息表
    源  表11:RWA_DEV.NCM_BREAKDEFINEDREMARK|信贷违约记录表
    源  表14:RWA.RWA_WSIB_ABS_ISSUE_EXPOSURE|发行机构资产证券化暴露铺底表
    源  表15:RWA.RWA_WSIB_ABS_INVEST_EXPOSURE|投资机构资产证券化暴露铺底表
    目标表  :RWA_DEV.RWA_TZ_EXPOSURE|财务系统投资类信用风险暴露表
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_TZ_EXPOSURE_1224';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;
  --v_count1 INTEGER;
  V_BALANCE NUMBER(24,6);
  S_BALANCE NUMBER(24,6);

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TZ_EXPOSURE';

    --2.将满足条件的数据从源表插入到目标表中
    --2.1 财务系统-外币债券投资 BY LJZ 21/1/11
    INSERT INTO RWA_DEV.RWA_TZ_EXPOSURE(
                 DataDate           																					--数据日期
								,DataNo                                 											--数据流水号
								,ExposureID                             											--风险暴露ID
								,DueID                                  											--债项ID
								,SSysID                                 											--源系统ID
								,ContractID                             											--合同ID
								,ClientID                               											--参与主体ID
								,SOrgID                                 											--源机构ID
								,SOrgName                               											--源机构名称
								,OrgSortNo                              											--所属机构排序号
								,OrgID                                  											--所属机构ID
								,OrgName                                											--所属机构名称
								,AccOrgID                               											--账务机构ID
								,AccOrgName                             											--账务机构名称
								,IndustryID                             											--所属行业代码
								,IndustryName                           											--所属行业名称
								,BusinessLine                           											--业务条线
								,AssetType                              											--资产大类
								,AssetSubType                           											--资产小类
								,BusinessTypeID                         											--业务品种代码
								,BusinessTypeName                       											--业务品种名称
								,CreditRiskDataType                     											--信用风险数据类型
								,AssetTypeOfHaircuts                    											--折扣系数对应资产类别
								,BusinessTypeSTD                        											--权重法业务类型
								,ExpoClassSTD                           											--权重法暴露大类
								,ExpoSubClassSTD                        											--权重法暴露小类
								,ExpoClassIRB                           											--内评法暴露大类
								,ExpoSubClassIRB                        											--内评法暴露小类
								,ExpoBelong                             											--暴露所属标识
								,BookType                               											--账户类别
								,ReguTranType                           											--监管交易类型
								,RepoTranFlag                           											--回购交易标识
								,RevaFrequency                          											--重估频率
								,Currency                               											--币种
								,NormalPrincipal                        											--正常本金余额
								,OverdueBalance                         											--逾期余额
								,NonAccrualBalance                      											--非应计余额
								,OnSheetBalance                         											--表内余额
								,NormalInterest                         											--正常利息
								,OnDebitInterest                        											--表内欠息
								,OffDebitInterest                       											--表外欠息
								,ExpenseReceivable                      											--应收费用
								,AssetBalance                           											--资产余额
								,AccSubject1                            											--科目一
								,AccSubject2                            											--科目二
								,AccSubject3                            											--科目三
								,StartDate                              											--起始日期
								,DueDate                                											--到期日期
								,OriginalMaturity                       											--原始期限
								,ResidualM                              											--剩余期限
								,RiskClassify                           											--风险分类
								,ExposureStatus                         											--风险暴露状态
								,OverdueDays                            											--逾期天数
								,SpecialProvision                       											--专项准备金
								,GeneralProvision                       											--一般准备金
								,EspecialProvision                      											--特别准备金
								,WrittenOffAmount                       											--已核销金额
								,OffExpoSource                          											--表外暴露来源
								,OffBusinessType                        											--表外业务类型
								,OffBusinessSdvsSTD                     											--权重法表外业务类型细分
								,UncondCancelFlag                       											--是否可随时无条件撤销
								,CCFLevel                               											--信用转换系数级别
								,CCFAIRB                                											--高级法信用转换系数
								,ClaimsLevel                            											--债权级别
								,BondFlag                               											--是否为债券
								,BondIssueIntent                        											--债券发行目的
								,NSURealPropertyFlag                    											--是否非自用不动产
								,RepAssetTermType                       											--抵债资产期限类型
								,DependOnFPOBFlag                       											--是否依赖于银行未来盈利
								,IRating                                											--内部评级
								,PD                                     											--违约概率
								,LGDLevel                               											--违约损失率级别
								,LGDAIRB                                											--高级法违约损失率
								,MAIRB                                  											--高级法有效期限
								,EADAIRB                                											--高级法违约风险暴露
								,DefaultFlag                            											--违约标识
								,BEEL                                   											--已违约暴露预期损失比率
								,DefaultLGD                             											--已违约暴露违约损失率
								,EquityExpoFlag                         											--股权暴露标识
								,EquityInvestType                       											--股权投资对象类型
								,EquityInvestCause          																	--股权投资形成原因
								,SLFlag                                 											--专业贷款标识
								,SLType                               												--专业贷款类型
								,PFPhase                                											--项目融资阶段
								,ReguRating                             											--监管评级
								,CBRCMPRatingFlag                       											--银监会认定评级是否更为审慎
								,LargeFlucFlag                          											--是否波动性较大
								,LiquExpoFlag                           											--是否清算过程中风险暴露
								,PaymentDealFlag                        											--是否货款对付模式
								,DelayTradingDays                       											--延迟交易天数
								,SecuritiesFlag                         											--有价证券标识
								,SecuIssuerID                           											--证券发行人ID
								,RatingDurationType                     											--评级期限类型
								,SecuIssueRating                        											--证券发行等级
								,SecuResidualM                          											--证券剩余期限
								,SecuRevaFrequency                      											--证券重估频率
								,CCPTranFlag                            											--是否中央交易对手相关交易
								,CCPID                                  											--中央交易对手ID
								,QualCCPFlag                         													--是否合格中央交易对手
								,BankRole                               											--银行角色
								,ClearingMethod                        												--清算方式
								,BankAssetFlag                          											--是否银行提交资产
								,MatchConditions    																					--符合条件情况
								,SFTFlag                                											--证券融资交易标识
								,MasterNetAgreeFlag                     											--净额结算主协议标识
								,MasterNetAgreeID                       											--净额结算主协议ID
								,SFTType                                											--证券融资交易类型
								,SecuOwnerTransFlag                     											--证券所有权是否转移
								,OTCFlag                                 											--场外衍生工具标识
								,ValidNettingFlag                       											--有效净额结算协议标识
								,ValidNetAgreementID                    											--有效净额结算协议ID
								,OTCType                                											--场外衍生工具类型
								,DepositRiskPeriod                      											--保证金风险期间
								,MTM                                    											--重置成本
								,MTMCurrency                            											--重置成本币种
								,BuyerOrSeller                          											--买方卖方
								,QualROFlag                             											--合格参照资产标识
								,ROIssuerPerformFlag                    											--参照资产发行人是否能履约
								,BuyerInsolvencyFlag                    											--信用保护买方是否破产
								,NonpaymentFees                         											--尚未支付费用
								,RetailExpoFlag                         											--零售暴露标识
								,RetailClaimType                        											--零售债权类型
								,MortgageType                           											--住房抵押贷款类型
								,ExpoNumber                             											--风险暴露个数
								,LTV                                    											--贷款价值比
								,Aging                                  											--账龄
								,NewDefaultDebtFlag                     											--新增违约债项标识
								,PDPoolModelID                          											--PD分池模型ID
								,LGDPoolModelID                         											--LGD分池模型ID
								,CCFPoolModelID                         											--CCF分池模型ID
								,PDPoolID           																					--所属PD池ID
								,LGDPoolID             																				--所属LGD池ID
								,CCFPoolID                                      							--所属CCF池ID
								,ABSUAFlag           																					--资产证券化基础资产标识
								,ABSPoolID                                                    --证券化资产池ID
								,GroupID                                                      --分组编号
								,DefaultDate                                                  --违约时点
								,ABSPROPORTION																								--资产证券化比重
								,DEBTORNUMBER																									--借款人个数
                ,SBJT2
                ,SBJT_VAL2
                ,SBJT3
                ,SBJT_VAL3
                ,SBJT4
                ,SBJT_VAL4
                ,SBJT5
                ,SBJT_VAL5
    )
    SELECT P_DATA_DT_STR, --数据日期
       P_DATA_DT_STR, --数据流水号
       T1.SECID, --风险暴露ID
       T1.SECID, --债项ID
       'tz', --源系统ID
       T1.SECID, --合同ID
       T3.CNO, --参与主体ID
       '6001', --源机构ID
       '重庆银行股份有限公司国际业务部', --源机构名称
       '1290', --所属机构排序号
       '6001', --所属机构ID
       '重庆银行股份有限公司国际业务部', --所属机构名称
       '6001', --账务机构ID
       '重庆银行股份有限公司国际业务部', --账务机构名称
       'J66', --所属行业代码
       '货币金融服务', --所属行业名称
       '04', --业务条线
       NULL, --资产大类
       NULL, --资产小类
       '1040202010', --业务品种代码
       '外币债券投资', --业务品种名称
       '01', --信用风险数据类型
       '01', --折扣系数对应资产类别
       '07', --权重法业务类型
       NULL, --权重法暴露大类
       NULL, --权重法暴露小类
       NULL, --内评法暴露大类
       NULL, --内评法暴露小类
       '01', --暴露所属标识
       NULL, --账户类别
       '02', --监管交易类型
       '0', --回购交易标识
       1, --重估频率
       T1.CCY, --币种
       -T1.PRINAMT, --正常本金余额
       0, --逾期余额
       0, --非应计余额
       -T1.PRINAMT, --表内余额
       0, --正常利息
       0, --表内欠息
       0, --表外欠息
       0, --应收费用
       -T1.PRINAMT, --资产余额
       T2.INTGLNO, --科目一
       NULL, --科目二
       NULL, --科目三
       T4.VDATE, --起始日期
       T4.MDATE, --到期日期
       T4.MDATE - T4.VDATE, --原始期限
       T4.MDATE - P_DATA_DT_STR, --剩余期限
       '01', --风险分类
       '01', --风险暴露状态
       0, --逾期天数
       0, --专项准备金
       0, --一般准备金
       0, --特别准备金
       0, --已核销金额
       NULL, --表外暴露来源
       NULL, --表外业务类型
       NULL, --权重法表外业务类型细分
       NULL, --是否可随时无条件撤销
       NULL, --信用转换系数级别
       NULL, --高级法信用转换系数
       NULL, --债权级别
       '1', --是否为债券
       '02', --债券发行目的
       '0', --是否非自用不动产
       NULL, --抵债资产期限类型
       '0', --是否依赖于银行未来盈利
       NULL, --内部评级
       NULL, --违约概率
       NULL, --违约损失率级别
       NULL, --高级法违约损失率
       NULL, --高级法有效期限
       NULL, --高级法违约风险暴露
       '0', --违约标识
       0.45, --已违约暴露预期损失比率
       0.45, --已违约暴露违约损失率
       '0', --股权暴露标识
       NULL, --股权投资对象类型
       NULL, --股权投资形成原因
       '0', --专业贷款标识
       NULL, --专业贷款类型
       NULL, --项目融资阶段
       '01', --监管评级
       NULL, --银监会认定评级是否更为审慎
       NULL, --是否波动性较大
       '0', --是否清算过程中风险暴露
       '1', --是否货款对付模式
       NULL, --延迟交易天数
       '1', --有价证券标识
       T6.BONDPUBLISHID, --证券发行人ID
       T6.TIMELIMIT, --评级期限类型
       T6.BONDRATING, --证券发行等级
       (T4.MDATE - P_DATA_DT_STR) / 12, --证券剩余期限
       1, --证券重估频率
       '0', --是否中央交易对手相关交易
       NULL, --中央交易对手ID
       NULL, --是否合格中央交易对手
       NULL, --银行角色
       NULL, --清算方式
       NULL, --是否银行提交资产
       NULL, --符合条件情况
       '0', --证券融资交易标识
       '0', --净额结算主协议标识
       NULL, --净额结算主协议ID
       NULL, --证券融资交易类型
       NULL, --证券所有权是否转移
       '0', --场外衍生工具标识
       NULL, --有效净额结算协议标识
       NULL, --有效净额结算协议ID
       NULL, --场外衍生工具类型
       NULL, --保证金风险期间
       NULL, --重置成本
       NULL, --重置成本币种
       NULL, --买方卖方
       NULL, --合格参照资产标识
       NULL, --参照资产发行人是否能履约
       NULL, --信用保护买方是否破产
       NULL, --尚未支付费用
       '0', --零售暴露标识
       NULL, --零售债权类型
       NULL, --住房抵押贷款类型
       1, --风险暴露个数
       0.8, --贷款价值比
       NULL, --账龄
       NULL, --新增违约债项标识
       NULL, --PD分池模型ID
       NULL, --LGD分池模型ID
       NULL, --CCF分池模型ID
       NULL, --所属PD池ID
       NULL, --所属LGD池ID
       NULL, --所属CCF池ID
       '0', --资产证券化基础资产标识
       NULL, --证券化资产池ID
       NULL, --分组编号
       NULL, --违约时点
       NULL, --资产证券化比重
       NULL, --借款人个数
       T2.INTGLNO, --利息调整科目
       T1.UNAMORTAMT, --利息调整金额
       T2.INTGLNO, --利息科目
       T1.TDYINTINCEXP, --利息金额
       T2.INTGLNO, --公允价值变动科目
       T1.TDYMTM, --公允价值变动金额
       NULL, --应收利息科目
       NULL --应收利息金额
  FROM OPI_TPOS T1
 INNER JOIN OPI_SL_ACUP T2
    ON T1.DATANO = T2.DATANO
   AND T1.SECID || '|' || T1.PORT || '|' || T1.COST || '|' || T1.INVTYPE =
       SUBSTR(T2.DESCR, 1, INSTR(T2.DESCR, '|', 1, 4) - 1)
 INNER JOIN OPI_SPSH T3
    ON T1.DATANO = T3.DATANO
   AND T1.SECID = T3.SECID
   AND T1.PORT = T3.PORT
   AND T1.INVTYPE = T3.INVTYPE
   AND T1.COST = T3.COST
 INNER JOIN OPI_SECM T4
    ON T1.DATANO = T4.DATANO
   AND T1.SECID = T4.SECID
  LEFT JOIN NCM_BUSINESS_DUEBILL T5
    ON T1.DATANO = T5.DATANO
   AND 'CW_IMPORTDATA' || T1.SECID = T5.THIRDPARTYACCOUNTS
   AND T1.DATANO = T5.DATANO
  LEFT JOIN NCM_BOND_INFO T6
    ON T5.DATANO = T6.DATANO
   AND T5.RELATIVESERIALNO2 = T6.OBJECTNO
   AND T6.OBJECTTYPE = 'BusinessContract'
 WHERE T1.DATANO = P_DATA_DT_STR;

    COMMIT;
    
    -- 空
    INSERT INTO RWA_DEV.RWA_TZ_EXPOSURE(
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
    WITH TEMP_BND_BOOK AS (
                        SELECT BOND_ID,
                               INITIAL_COST,
                               INT_ADJUST,
                               MKT_VALUE_CHANGE,
                               RECEIVABLE_INT,
                               ACCOUNTABLE_INT
                          FROM (SELECT BOND_ID,
                                       INITIAL_COST,
                                       INT_ADJUST,
                                       MKT_VALUE_CHANGE,
                                       RECEIVABLE_INT,
                                       ACCOUNTABLE_INT,
                                       ROW_NUMBER() OVER(PARTITION BY BOND_ID ORDER BY SORT_SEQ DESC) RM
                                  FROM FNS_BND_BOOK_B
                                 WHERE AS_OF_DATE <= p_data_dt_str
                                   AND DATANO = p_data_dt_str)
                         WHERE RM = 1
                           AND NVL(INITIAL_COST, 0) + NVL(MKT_VALUE_CHANGE, 0) + NVL(ACCOUNTABLE_INT, 0) <> 0 --NVL(INT_ADJUST, 0) + ，利息调整虚拟，因为会手工调账
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')                                       AS DataDate               --数据日期
                ,p_data_dt_str                                                          AS DataNo                 --数据流水号
                ,NVL(T3.SERIALNO,T1.BOND_ID)                                            AS ExposureID             --风险暴露ID
                ,T1.BOND_ID                                                             AS DueID                  --债项ID
                ,'TZ'                                                                   AS SSysID                 --源系统ID                       默认 投资(TZ)
                ,T1.BOND_ID                                                             AS ContractID             --合同ID
                ,CASE WHEN REPLACE(T3.CUSTOMERID,'NCM_','') IS NULL THEN 'XN-YBGS'
                      ELSE T3.CUSTOMERID
                 END                                                                    AS ClientID               --参与主体ID
                ,T1.DEPARTMENT                                                          AS SOrgID                 --源机构ID
                ,T4.ORGNAME                                                             AS SOrgName               --源机构名称
                ,T4.SORTNO                                                              AS OrgSortNo              --所属机构排序号
                ,T1.DEPARTMENT                                                          AS OrgID                  --所属机构ID
                ,T4.ORGNAME                                                             AS OrgName                --所属机构名称
                ,T1.DEPARTMENT                                                          AS AccOrgID               --账务机构ID
                ,T4.ORGNAME                                                             AS AccOrgName             --账务机构名称
                ,CASE WHEN NVL(T5.INDUSTRYTYPE,DECODE(SUBSTR(T5.RWACUSTOMERTYPE,1,2),'02','J6620','J66')) = '999999' THEN 'J66'
                      ELSE NVL(T5.INDUSTRYTYPE,DECODE(SUBSTR(T5.RWACUSTOMERTYPE,1,2),'02','J6620','J66'))
                 END                                                                    AS IndustryID              --所属行业代码
                ,CASE WHEN NVL(T6.ITEMNAME,DECODE(SUBSTR(T5.RWACUSTOMERTYPE,1,2),'02','货币银行服务','货币金融服务')) = '未知' THEN '货币金融服务'
                      ELSE NVL(T6.ITEMNAME,DECODE(SUBSTR(T5.RWACUSTOMERTYPE,1,2),'02','货币银行服务','货币金融服务'))
                 END                                                                    AS IndustryName            --所属行业名称
                ,'0401'                                                                 AS BusinessLine           --业务条线                        默认 同业(04)
                ,''                                                                     AS AssetType              --资产大类                        默认 NULL RWA规则计算
                ,''                                                                     AS AssetSubType           --资产小类                        默认 NULL RWA规则计算
                ,CASE WHEN T1.BOND_TYPE1 = '081' THEN '1040105061'
                      WHEN T1.BOND_TYPE1 = '100' THEN '1040105062'
                      ELSE '1040105060'
                 END                                                                    AS BusinessTypeID         --业务品种代码
                ,CASE WHEN T1.BOND_TYPE1 = '081' THEN '应收款项类投资_保本'
                      WHEN T1.BOND_TYPE1 = '100' THEN '应收款项类投资_同业存单'
                      ELSE '应收款项类投资'
                 END                                                                    AS BusinessTypeName       --业务品种名称
                ,'01'                                                                   AS CreditRiskDataType     --信用风险数据类型                默认 一般非零售(01)
                ,'01'                                                                   AS AssetTypeOfHaircuts    --折扣系数对应资产类别            默认 现金及现金等价物(01)
                ,'07'                                                                   AS BusinessTypeSTD        --权重法业务类型                 默认 一般资产(07)
                ,CASE WHEN REPLACE(T3.CUSTOMERID,'NCM_','') IS NULL THEN '0112'                                   --客户为空，默认 其他(0112)
                      ELSE ''
                 END                                                                    AS ExpoClassSTD           --权重法暴露大类                 默认 NULL RWA规则处理
                ,CASE WHEN REPLACE(T3.CUSTOMERID,'NCM_','') IS NULL THEN '011216'                                 --客户为空，默认 其他适用100%风险权重的资产(011216)
                      ELSE ''
                 END                                                                    AS ExpoSubClassSTD        --权重法暴露小类                 默认 NULL RWA规则处理
                ,SUBSTR(T13.DITEMNO,1,4)                                                AS ExpoClassIRB           --内评法暴露大类                 默认 NULL RWA规则处理
                ,T13.DITEMNO                                                            AS ExpoSubClassIRB        --内评法暴露小类                 默认 NULL RWA规则处理
                ,'01'                                                                   AS ExpoBelong             --暴露所属标识                    默认：表内(01)
                ,CASE WHEN T1.ASSET_CLASS = '10' THEN '02'
                      ELSE '01'
                 END                                                                    AS BookType               --账户类别                        资产分类 ＝ “交易性金融资产(10)” , 则为02-交易账户；资产分类 ≠ “交易性金融资产”  , 则为01-银行账户
                ,'02'                                                                   AS ReguTranType           --监管交易类型                    默认 其他资本市场交易(02)
                ,'0'                                                                    AS RepoTranFlag           --回购交易标识                    默认 否(0)
                ,1                                                                      AS RevaFrequency          --重估频率                        默认  1
                ,NVL(T1.CURRENCY_CODE,'CNY')                                            AS Currency               --币种
                ,NVL(T2.INITIAL_COST,0) + NVL(T2.MKT_VALUE_CHANGE,0) + NVL(T2.ACCOUNTABLE_INT,0)
                                                                                        AS NormalPrincipal        --正常本金余额                    正常本金余额＝成本＋利息调整(initial_cost)＋公允价值变动/公允价值变动损益(int_adjust)＋应计利息(mkt_value_change)
                ,0                                                                      AS OverdueBalance         --逾期余额                        默认 0
                ,0                                                                      AS NonAccrualBalance      --非应计余额                     默认 0
                ,NVL(T2.INITIAL_COST,0) + NVL(T2.MKT_VALUE_CHANGE,0) + NVL(T2.ACCOUNTABLE_INT,0)
                                                                                        AS OnSheetBalance         --表内余额                        表内余额=正常本金余额+逾期余额+非应计余额
                ,0                                                                      AS NormalInterest         --正常利息                        默认 0
                ,0                                                                      AS OnDebitInterest        --表内欠息                        默认 0
                ,0                                                                      AS OffDebitInterest       --表外欠息                        默认 0
                ,0                                                                      AS ExpenseReceivable      --应收费用                        默认 0
                ,NVL(T2.INITIAL_COST,0) + NVL(T2.MKT_VALUE_CHANGE,0) + NVL(T2.ACCOUNTABLE_INT,0)
                                                                                        AS AssetBalance           --资产余额                        表内资产总余额=表内余额+应收费用+正常利息+表内欠息
                ,CASE WHEN T1.ASSET_CLASS = '50' THEN '12220400'
                      WHEN T1.ASSET_CLASS = '60' THEN '12220701'
                 END                                                                    AS AccSubject1            --科目一                         根据原系统的资产分类对照会计科目表确认
                ,''                                                                     AS AccSubject2            --科目二                         默认 NULL
                ,''                                                                     AS AccSubject3            --科目三                         默认 NULL
                ,T1.EFFECT_DATE                                                         AS StartDate              --起始日期
                ,T1.MATURITY_DATE                                                       AS DueDate                --到期日期
                ,CASE WHEN (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(T1.EFFECT_DATE,'YYYYMMDD')) / 365<0
                      THEN 0
                      ELSE (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(T1.EFFECT_DATE,'YYYYMMDD')) / 365
                 END                                                                    AS OriginalMaturity       --原始期限                        单位 年
                ,CASE WHEN (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                      THEN 0
                      ELSE (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                                                    AS ResidualM              --剩余期限                        单位 年
                ,CASE WHEN T18.FINALLYRESULT IN ('A1','A2','A3','A4','A') THEN '01'                               --十二级分类转为五级分类
                      WHEN T18.FINALLYRESULT IN ('B1','B2','B3','B') THEN '02'
                      WHEN T18.FINALLYRESULT IN ('C1','C2','C') THEN '03'
                      WHEN T18.FINALLYRESULT IN ('D1','D2','D') THEN '04'
                      WHEN T18.FINALLYRESULT = 'E' THEN '05'
                      ELSE '01'
                 END                                                                    AS RiskClassify           --风险分类                        默认 正常(01)
                ,''                                                                     AS ExposureStatus         --风险暴露状态                    默认 NULL
                ,0                                                                      AS OverdueDays            --逾期天数                        默认 0
                ,0                                                                      AS SpecialProvision       --专项准备金                     默认 0 RWA计算 科目12220400，直接提1%的准备金
                ,0                                                                      AS GeneralProvision       --一般准备金                     默认 0 RWA计算
                ,0                                                                      AS EspecialProvision      --特别准备金                     默认 0 RWA计算
                ,0                                                                      AS WrittenOffAmount       --已核销金额                     默认 0
                ,''                                                                     AS OffExpoSource          --表外暴露来源                    默认 NULL
                ,''                                                                     AS OffBusinessType        --表外业务类型                    默认 NULL
                ,''                                                                     AS OffBusinessSdvsSTD     --权重法表外业务类型细分         默认 NULL
                ,''                                                                     AS UncondCancelFlag       --是否可随时无条件撤销            默认 NULL
                ,''                                                                     AS CCFLevel               --信用转换系数级别                默认 NULL
                ,NULL                                                                   AS CCFAIRB                --高级法信用转换系数             默认 NULL
                ,CASE WHEN T1.BOND_TYPE2 = '20' THEN '02'
                      ELSE '01'
                 END                                                                    AS ClaimsLevel            --债权级别                        债券种类2＝次级债券(20)，则债权级别＝次级债权(02)；否则为高级债权(01)
                ,'0'                                                                    AS BondFlag               --是否为债券                     默认 否(0)
                ,'02'                                                                   AS BondIssueIntent        --债券发行目的                    默认 其他(02)
                ,'0'                                                                    AS NSURealPropertyFlag    --是否非自用不动产                默认 否(0)
                ,''                                                                     AS RepAssetTermType       --抵债资产期限类型                默认 NULL
                ,'0'                                                                    AS DependOnFPOBFlag       --是否依赖于银行未来盈利         默认 否(0)
                ,T8.PDADJLEVEL                                                          AS IRating                --内部评级
                ,T8.PD                                                                  AS PD                     --违约概率
                ,''                                                                     AS LGDLevel               --违约损失率级别                 默认 NULL
                ,NULL                                                                   AS LGDAIRB                --高级法违约损失率                默认 NULL
                ,NULL                                                                   AS MAIRB                  --高级法有效期限                 默认 NULL
                ,NULL                                                                   AS EADAIRB                --高级法违约风险暴露             默认 NULL
                ,CASE WHEN T8.PDADJLEVEL = '0116' THEN '1'
                 ELSE '0'
                 END                                                                    AS DefaultFlag            --违约标识
                ,CASE WHEN T1.BOND_TYPE2 = '20' THEN 0.75
                 ELSE 0.45
                 END                                                                    AS BEEL                   --已违约暴露预期损失比率         债权级别＝‘高级债权’，BEEL ＝ 45%；债权级别＝‘次级债权’，BEEL ＝ 75%
                ,CASE WHEN T1.BOND_TYPE2 = '20' THEN 0.75
                 ELSE 0.45
                 END                                                                    AS DefaultLGD             --已违约暴露违约损失率            默认 NULL
                ,'0'                                                                    AS EquityExpoFlag         --股权暴露标识                    默认 否(0)
                ,''                                                                     AS EquityInvestType       --股权投资对象类型                默认 NULL
                ,''                                                                     AS EquityInvestCause      --股权投资形成原因                默认 NULL
                ,'0'                                                                    AS SLFlag                 --专业贷款标识                    默认 否(0)
                ,''                                                                     AS SLType                 --专业贷款类型                    默认 NULL
                ,''                                                                     AS PFPhase                --项目融资阶段                    默认 NULL
                ,'01'                                                                   AS ReguRating             --监管评级                        默认 优(01)
                ,''                                                                     AS CBRCMPRatingFlag       --银监会认定评级是否更为审慎       默认 NULL
                ,''                                                                     AS LargeFlucFlag          --是否波动性较大                 默认 NULL
                ,'0'                                                                    AS LiquExpoFlag           --是否清算过程中风险暴露         默认 否(0)
                ,'1'                                                                    AS PaymentDealFlag        --是否货款对付模式                默认 是(1)
                ,NULL                                                                   AS DelayTradingDays       --延迟交易天数                    默认 NULL
                ,'0'                                                                    AS SecuritiesFlag         --有价证券标识                    默认 否(0)
                ,''                                                                     AS SecuIssuerID           --证券发行人ID                   默认 NULL
                ,''                                                                     AS RatingDurationType     --评级期限类型                    默认 NULL
                ,''                                                                     AS SecuIssueRating        --证券发行等级                    默认 NULL
                ,NULL                                                                   AS SecuResidualM          --证券剩余期限                    默认 NULL
                ,1                                                                      AS SecuRevaFrequency      --证券重估频率                    默认 1
                ,'0'                                                                    AS CCPTranFlag            --是否中央交易对手相关交易        默认 否(0)
                ,''                                                                     AS CCPID                  --中央交易对手ID                  默认 NULL
                ,''                                                                     AS QualCCPFlag            --是否合格中央交易对手            默认 NULL
                ,''                                                                     AS BankRole               --银行角色                        默认 NULL
                ,''                                                                     AS ClearingMethod         --清算方式                        默认 NULL
                ,''                                                                     AS BankAssetFlag          --是否银行提交资产                默认 NULL
                ,''                                                                     AS MatchConditions        --符合条件情况                    默认 NULL
                ,'0'                                                                    AS SFTFlag                --证券融资交易标识                默认 否(0)
                ,'0'                                                                    AS MasterNetAgreeFlag     --净额结算主协议标识             默认 否(0)
                ,''                                                                     AS MasterNetAgreeID       --净额结算主协议ID               默认 NULL
                ,''                                                                     AS SFTType                --证券融资交易类型                默认 NULL
                ,''                                                                     AS SecuOwnerTransFlag     --证券所有权是否转移             默认 NULL
                ,'0'                                                                    AS OTCFlag                --场外衍生工具标识                默认 否(0)
                ,''                                                                     AS ValidNettingFlag       --有效净额结算协议标识            默认 NULL
                ,''                                                                     AS ValidNetAgreementID    --有效净额结算协议ID              默认 NULL
                ,''                                                                     AS OTCType                --场外衍生工具类型                默认 NULL
                ,''                                                                     AS DepositRiskPeriod      --保证金风险期间                 默认 NULL
                ,''                                                                     AS MTM                    --重置成本                        默认 NULL
                ,''                                                                     AS MTMCurrency            --重置成本币种                    默认 NULL
                ,''                                                                     AS BuyerOrSeller          --买方卖方                        默认 NULL
                ,''                                                                     AS QualROFlag             --合格参照资产标识                默认 NULL
                ,''                                                                     AS ROIssuerPerformFlag    --参照资产发行人是否能履约        默认 NULL
                ,''                                                                     AS BuyerInsolvencyFlag    --信用保护买方是否破产            默认 NULL
                ,''                                                                     AS NonpaymentFees         --尚未支付费用                    默认 NULL
                ,'0'                                                                    AS RetailExpoFlag         --零售暴露标识                    默认 否(0)
                ,''                                                                     AS RetailClaimType        --零售债权类型                    默认 NULL
                ,''                                                                     AS MortgageType           --住房抵押贷款类型                默认 NULL
                ,1                                                                      AS ExpoNumber             --风险暴露个数                    默认 1
                ,0.8                                                                    AS LTV                    --贷款价值比                     默认 0.8
                ,NULL                                                                   AS Aging                  --账龄                            默认 NULL
                ,''                                                                     AS NewDefaultDebtFlag     --新增违约债项标识                默认 NULL
                ,''                                                                     AS PDPoolModelID          --PD分池模型ID                    默认 NULL
                ,''                                                                     AS LGDPoolModelID         --LGD分池模型ID                   默认 NULL
                ,''                                                                     AS CCFPoolModelID         --CCF分池模型ID                   默认 NULL
                ,''                                                                     AS PDPoolID               --所属PD池ID                     默认 NULL
                ,''                                                                     AS LGDPoolID              --所属LGD池ID                    默认 NULL
                ,''                                                                     AS CCFPoolID              --所属CCF池ID                    默认 NULL
                ,'0'                                                                    AS ABSUAFlag              --资产证券化基础资产标识         默认 否(0)
                ,''                                                                     AS ABSPoolID              --证券化资产池ID                  默认 NULL
                ,''                                                                     AS GroupID                --分组编号                        默认 NULL
                ,CASE WHEN T8.PDADJLEVEL = '0116' THEN TO_DATE(T8.PDVAVLIDDATE,'YYYYMMDD')
                 ELSE NULL
                 END                                                                    AS DefaultDate            --违约时点
                ,NULL                                                                   AS ABSPROPORTION          --资产证券化比重
                ,NULL                                                                   AS DEBTORNUMBER           --借款人个数

    FROM        RWA_DEV.FNS_BND_INFO_B T1
    INNER JOIN  TEMP_BND_BOOK T2
    ON          T1.BOND_ID = T2.BOND_ID
    INNER JOIN  RWA_DEV.NCM_BUSINESS_DUEBILL T3                           --信贷借据表
    ON          'CW_IMPORTDATA' || T1.BOND_ID = T3.THIRDPARTYACCOUNTS
    AND         T3.DATANO = p_data_dt_str
    LEFT JOIN   RWA.ORG_INFO T4                                           --RWA机构表
    ON          T1.DEPARTMENT = T4.ORGID
    LEFT JOIN NCM_CUSTOMER_INFO T5 --统一客户信息表
    ON          T3.CUSTOMERID = T5.CUSTOMERID
    AND         T5.DATANO = p_data_dt_str
    LEFT JOIN   RWA.CODE_LIBRARY  T6                                      --RWA码表，获取行业
    ON          T5.INDUSTRYTYPE = T6.ITEMNO
    AND         T6.CODENO = 'IndustryType'
    LEFT JOIN   RWA.CODE_LIBRARY  T7                                      --RWA码表，获取业务品种
    ON          T3.BUSINESSTYPE = T7.ITEMNO
    AND         T7.CODENO = 'BusinessType'
    LEFT JOIN   RWA_DEV.RWA_TEMP_PDLEVEL T8                               --客户内部评级临时表
    ON          T3.CUSTOMERID = T8.CUSTID
    INNER JOIN  RWA_DEV.NCM_BUSINESS_CONTRACT T10
    ON          T3.RELATIVESERIALNO2 = T10.SERIALNO
    AND         T10.BUSINESSSUBTYPE LIKE '0010%'                          --基于银行
    AND         T10.DATANO = p_data_dt_str
    LEFT JOIN   RWA_DEV.NCM_RWA_RISK_EXPO_RST T12                 --内评发暴露分类结果表
    ON          T3.SERIALNO = T12.OBJECTNO
    AND         T12.OBJECTTYPE = 'BusinessDuebill'
    AND         T12.DATANO = p_data_dt_str
    LEFT JOIN   RWA_DEV.RWA_CD_CODE_MAPPING T13                           --代码映射表
    ON          T12.RISKEXPOSURERESULT = T13.SITEMNO
    AND         T13.SYSID = 'XD'
    AND         T13.SCODENO = 'RwaResultType'
    AND         T13.ISINUSE = '1'
    /*
    LEFT JOIN   (SELECT TR.BOND_ID
                       ,TO_NUMBER(REPLACE(TR.RESERVESUM,',','')) AS RESERVESUM
                   FROM RWA.RWA_WS_RESERVE TR
             INNER JOIN RWA.RWA_WP_DATASUPPLEMENT TD                      --数据补录表
                     ON TR.SUPPORGID = TD.ORGID
                    AND TD.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
                    AND TD.SUPPTMPLID = 'M-0210'
                    AND TD.SUBMITFLAG = '1'
                  WHERE TR.DATADATE = to_date(p_data_dt_str,'YYYYMMDD')
                ) T17                                                     --应收款投资准备金补录表
    ON          T1.BOND_ID = T17.BOND_ID
    */
    LEFT JOIN   RWA_DEV.NCM_CLASSIFY_RECORD T18                           --十二级分类信息表
    ON          T3.RELATIVESERIALNO2 = T18.OBJECTNO
    AND         T18.OBJECTTYPE = 'TwelveClassify'
    AND         T18.ISWORK = '1'
    AND         T18.DATANO = p_data_dt_str
    WHERE       T1.ASSET_CLASS IN ('50','60')                       --通过资产分类来确定债券还是应收款投资。
    AND         T1.DATANO = p_data_dt_str                           --债券信息表,获取有效的债券信息
    AND         T1.BOND_CODE IS NOT NULL                            --排除无效的债券数据
    ;

    COMMIT;

    --清空应收款投资实际融资人客户临时表
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TMP_TZCUST';

    --插入当期应收款投资实际融资人客户数据 空
    INSERT INTO RWA_DEV.RWA_TMP_TZCUST
    SELECT CONTRACTNO,
           CUSTOMERID,
           PDLEVEL,
           PD,
           PDVAVLIDDATE,
           RWACUSTOMERTYPE,
           INDUSTRYTYPE
      FROM (SELECT T1.SERIALNO AS CONTRACTNO,
                   NVL(T2.CUSTOMERID, T3.CUSTOMERID) AS CUSTOMERID,
                   T4.PDADJLEVEL AS PDLEVEL,
                   T4.PD AS PD,
                   T4.PDVAVLIDDATE AS PDVAVLIDDATE,
                   CASE
                     WHEN T2.CUSTOMERID IS NOT NULL THEN
                      T2.RWACUSTOMERTYPE
                     ELSE
                      T3.RWACUSTOMERTYPE
                   END AS RWACUSTOMERTYPE,
                   CASE
                     WHEN T2.CUSTOMERID IS NOT NULL THEN
                      T2.INDUSTRYTYPE
                     ELSE
                      T3.INDUSTRYTYPE
                   END AS INDUSTRYTYPE,
                   ROW_NUMBER() OVER(PARTITION BY T1.SERIALNO ORDER BY T4.PDADJLEVEL DESC,CASE
                     WHEN T2.CUSTOMERID IS NOT NULL THEN
                      T2.RWACUSTOMERTYPE
                     ELSE
                      T3.RWACUSTOMERTYPE
                   END DESC) AS RN
              FROM RWA_DEV.NCM_CONTRACT_RELATIVE T1
             INNER JOIN RWA_DEV.NCM_BUSINESS_DUEBILL T8
                ON T1.SERIALNO = T8.RELATIVESERIALNO2
               AND T8.BUSINESSTYPE = '1040105060'
               AND T8.DATANO = p_data_dt_str
              LEFT JOIN NCM_CUSTOMER_INFO T2 --统一客户信息表
                ON T1.OBJECTNO = T2.CUSTOMERID
               AND T2.DATANO = p_data_dt_str
              LEFT JOIN NCM_CUSTOMER_INFO T3 --统一客户信息表
                ON SUBSTR(T1.OBJECTNO, 5) = T3.customerid
               AND T3.DATANO = p_data_dt_str
              LEFT JOIN RWA_DEV.RWA_TEMP_PDLEVEL T4
                ON NVL(T2.CUSTOMERID, T3.CUSTOMERID) = T4.CUSTID
             WHERE T1.OBJECTTYPE = 'Financier'
               AND T1.DATANO = p_data_dt_str)
     WHERE RN = 1
     ;

    COMMIT;

    --分析应收款投资实际融资人客户临时表
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_TMP_TZCUST',cascade => true);


    --2.3 财务系统-应收款投资-非基于银行-实际融资人 空
    INSERT INTO RWA_DEV.RWA_TZ_EXPOSURE(
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
    WITH TEMP_BND_BOOK AS (
                        SELECT BOND_ID,
                               INITIAL_COST,
                               INT_ADJUST,
                               MKT_VALUE_CHANGE,
                               RECEIVABLE_INT,
                               ACCOUNTABLE_INT
                          FROM (SELECT BOND_ID,
                                       INITIAL_COST,
                                       INT_ADJUST,
                                       MKT_VALUE_CHANGE,
                                       RECEIVABLE_INT,
                                       ACCOUNTABLE_INT,
                                       ROW_NUMBER() OVER(PARTITION BY BOND_ID ORDER BY SORT_SEQ DESC) RM
                                  FROM FNS_BND_BOOK_B
                                 WHERE AS_OF_DATE <= p_data_dt_str
                                   AND DATANO = p_data_dt_str)
                         WHERE RM = 1
                           AND NVL(INITIAL_COST, 0) + NVL(MKT_VALUE_CHANGE, 0) + NVL(ACCOUNTABLE_INT, 0) <> 0 --NVL(INT_ADJUST, 0) + ，利息调整虚拟，因为会手工调账
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')                                       AS DataDate               --数据日期
                ,p_data_dt_str                                                          AS DataNo                 --数据流水号
                ,NVL(T3.SERIALNO,T1.BOND_ID)                                            AS ExposureID             --风险暴露ID
                ,T1.BOND_ID                                                             AS DueID                  --债项ID
                ,'TZ'                                                                   AS SSysID                 --源系统ID                       默认 投资(TZ)
                ,T1.BOND_ID                                                             AS ContractID             --合同ID
                ,CASE WHEN REPLACE(T5.CUSTOMERID,'NCM_','') IS NULL THEN 'XN-YBGS'
                      ELSE T5.CUSTOMERID
                 END                                                                    AS ClientID               --参与主体ID                      如果没有实际融资人就默认一个一般公司
                ,T1.DEPARTMENT                                                          AS SOrgID                 --源机构ID
                ,T4.ORGNAME                                                             AS SOrgName               --源机构名称
                ,T4.SORTNO                                                              AS OrgSortNo              --所属机构排序号
                ,T1.DEPARTMENT                                                          AS OrgID                  --所属机构ID
                ,T4.ORGNAME                                                             AS OrgName                --所属机构名称
                ,T1.DEPARTMENT                                                          AS AccOrgID               --账务机构ID
                ,T4.ORGNAME                                                             AS AccOrgName             --账务机构名称
                ,CASE WHEN NVL(T5.INDUSTRYTYPE,DECODE(SUBSTR(T5.RWACUSTOMERTYPE,1,2),'02','J6620','J66')) = '999999' THEN 'J66'
                      ELSE NVL(T5.INDUSTRYTYPE,DECODE(SUBSTR(T5.RWACUSTOMERTYPE,1,2),'02','J6620','J66'))
                 END                                                                    AS IndustryID              --所属行业代码
                ,CASE WHEN NVL(T6.ITEMNAME,DECODE(SUBSTR(T5.RWACUSTOMERTYPE,1,2),'02','货币银行服务','货币金融服务')) = '未知' THEN '货币金融服务'
                      ELSE NVL(T6.ITEMNAME,DECODE(SUBSTR(T5.RWACUSTOMERTYPE,1,2),'02','货币银行服务','货币金融服务'))
                 END                                                                    AS IndustryName           --所属行业名称
                ,'0401'                                                                 AS BusinessLine           --业务条线                        默认 同业(04)
                ,''                                                                     AS AssetType              --资产大类                        默认 NULL RWA规则计算
                ,''                                                                     AS AssetSubType           --资产小类                        默认 NULL RWA规则计算
                ,CASE WHEN T1.BOND_TYPE1 = '081' THEN '1040105061'
                      WHEN T1.BOND_TYPE1 = '100' THEN '1040105062'
                      ELSE '1040105060'
                 END                                                                    AS BusinessTypeID         --业务品种代码
                ,CASE WHEN T1.BOND_TYPE1 = '081' THEN '应收款项类投资_保本'
                      WHEN T1.BOND_TYPE1 = '100' THEN '应收款项类投资_同业存单'
                      ELSE '应收款项类投资'
                 END                                                                    AS BusinessTypeName       --业务品种名称
                ,'01'                                                                   AS CreditRiskDataType     --信用风险数据类型                默认 一般非零售(01)
                ,'01'                                                                   AS AssetTypeOfHaircuts    --折扣系数对应资产类别            默认 现金及现金等价物(01)
                ,'07'                                                                   AS BusinessTypeSTD        --权重法业务类型                 默认 一般资产(07)
                ,CASE WHEN REPLACE(T5.CUSTOMERID,'NCM_','') IS NULL THEN '0112'                                   --若没有实际融资人则默认为0112-其他
                 ELSE ''
                 END                                                                    AS ExpoClassSTD           --权重法暴露大类                 默认 NULL RWA规则计算
                ,CASE WHEN REPLACE(T5.CUSTOMERID,'NCM_','') IS NULL THEN '011216'                                 --若没有实际融资人则默认为011216-其他适用100%风险权重的资产
                 ELSE ''
                 END                                                                    AS ExpoSubClassSTD        --权重法暴露小类                 默认 NULL RWA规则计算
                ,SUBSTR(T13.DITEMNO,1,4)                                                AS ExpoClassIRB           --内评法暴露大类                 默认 NULL RWA规则处理
                ,T13.DITEMNO                                                            AS ExpoSubClassIRB        --内评法暴露小类                 默认 NULL RWA规则处理
                ,'01'                                                                   AS ExpoBelong             --暴露所属标识                    默认：表内(01)
                ,CASE WHEN T1.ASSET_CLASS = '10' THEN '02'
                      ELSE '01'
                 END                                                                    AS BookType               --账户类别                        资产分类 ＝ “交易性金融资产(10)” , 则为02-交易账户；资产分类 ≠ “交易性金融资产”  , 则为01-银行账户
                ,'02'                                                                   AS ReguTranType           --监管交易类型                    默认 其他资本市场交易(02)
                ,'0'                                                                    AS RepoTranFlag           --回购交易标识                    默认 否(0)
                ,1                                                                      AS RevaFrequency          --重估频率                        默认  1
                ,NVL(T1.CURRENCY_CODE,'CNY')                                            AS Currency               --币种
                ,NVL(T2.INITIAL_COST,0) + NVL(T2.MKT_VALUE_CHANGE,0) + NVL(T2.ACCOUNTABLE_INT,0)
                                                                                        AS NormalPrincipal        --正常本金余额                    正常本金余额＝成本＋利息调整(initial_cost)＋公允价值变动/公允价值变动损益(int_adjust)＋应计利息(mkt_value_change)
                ,0                                                                      AS OverdueBalance         --逾期余额                        默认 0
                ,0                                                                      AS NonAccrualBalance      --非应计余额                     默认 0
                ,NVL(T2.INITIAL_COST,0) + NVL(T2.MKT_VALUE_CHANGE,0) + NVL(T2.ACCOUNTABLE_INT,0)
                                                                                        AS OnSheetBalance         --表内余额                        表内余额=正常本金余额+逾期余额+非应计余额
                ,0                                                                      AS NormalInterest         --正常利息                        默认 0
                ,0                                                                      AS OnDebitInterest        --表内欠息                        默认 0
                ,0                                                                      AS OffDebitInterest       --表外欠息                        默认 0
                ,0                                                                      AS ExpenseReceivable      --应收费用                        默认 0
                ,NVL(T2.INITIAL_COST,0) + NVL(T2.MKT_VALUE_CHANGE,0) + NVL(T2.ACCOUNTABLE_INT,0)
                                                                                        AS AssetBalance           --资产余额                        表内资产总余额=表内余额+应收费用+正常利息+表内欠息
                ,CASE WHEN T1.ASSET_CLASS = '50' THEN '12220400'
                      WHEN T1.ASSET_CLASS = '60' THEN '12220701'
                 END                                                                    AS AccSubject1            --科目一                         根据原系统的资产分类对照会计科目表确认
                ,''                                                                     AS AccSubject2            --科目二                         默认 NULL
                ,''                                                                     AS AccSubject3            --科目三                         默认 NULL
                ,T1.EFFECT_DATE                                                         AS StartDate              --起始日期
                ,T1.MATURITY_DATE                                                       AS DueDate                --到期日期
                ,CASE WHEN (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(T1.EFFECT_DATE,'YYYYMMDD')) / 365<0
                      THEN 0
                      ELSE (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(T1.EFFECT_DATE,'YYYYMMDD')) / 365
                 END                                                                    AS OriginalMaturity       --原始期限                        单位 年
                ,CASE WHEN (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                      THEN 0
                      ELSE (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                                                    AS ResidualM              --剩余期限                        单位 年
                ,CASE WHEN T18.FINALLYRESULT IN ('A1','A2','A3','A4','A') THEN '01'                               --十二级分类转为五级分类
                      WHEN T18.FINALLYRESULT IN ('B1','B2','B3','B') THEN '02'
                      WHEN T18.FINALLYRESULT IN ('C1','C2','C') THEN '03'
                      WHEN T18.FINALLYRESULT IN ('D1','D2','D') THEN '04'
                      WHEN T18.FINALLYRESULT = 'E' THEN '05'
                      ELSE '01'
                 END                                                                    AS RiskClassify           --风险分类                        默认 正常(01)
                ,''                                                                     AS ExposureStatus         --风险暴露状态                    默认 NULL
                ,0                                                                      AS OverdueDays            --逾期天数                        默认 0
                ,0                                                                      AS SpecialProvision       --专项准备金                     默认 0 RWA计算 科目12220400，直接提1%的准备金
                ,0                                                                      AS GeneralProvision       --一般准备金                     默认 0 RWA计算
                ,0                                                                      AS EspecialProvision      --特别准备金                     默认 0 RWA计算
                ,0                                                                      AS WrittenOffAmount       --已核销金额                     默认 0
                ,''                                                                     AS OffExpoSource          --表外暴露来源                    默认 NULL
                ,''                                                                     AS OffBusinessType        --表外业务类型                    默认 NULL
                ,''                                                                     AS OffBusinessSdvsSTD     --权重法表外业务类型细分         默认 NULL
                ,''                                                                     AS UncondCancelFlag       --是否可随时无条件撤销            默认 NULL
                ,''                                                                     AS CCFLevel               --信用转换系数级别                默认 NULL
                ,NULL                                                                   AS CCFAIRB                --高级法信用转换系数             默认 NULL
                ,CASE WHEN T1.BOND_TYPE2 = '20' THEN '02'
                      ELSE '01'
                 END                                                                    AS ClaimsLevel            --债权级别                        债券种类2＝次级债券(20)，则债权级别＝次级债权(02)；否则为高级债权(01)
                ,'0'                                                                    AS BondFlag               --是否为债券                     默认 否(0)
                ,'02'                                                                   AS BondIssueIntent        --债券发行目的                    默认 其他(02)
                ,'0'                                                                    AS NSURealPropertyFlag    --是否非自用不动产                默认 否(0)
                ,''                                                                     AS RepAssetTermType       --抵债资产期限类型                默认 NULL
                ,'0'                                                                    AS DependOnFPOBFlag       --是否依赖于银行未来盈利         默认 否(0)
                ,T5.PDLEVEL                                                             AS IRating                --内部评级
                ,T5.PD                                                                  AS PD                     --违约概率
                ,''                                                                     AS LGDLevel               --违约损失率级别                 默认 NULL
                ,NULL                                                                   AS LGDAIRB                --高级法违约损失率                默认 NULL
                ,NULL                                                                   AS MAIRB                  --高级法有效期限                 默认 NULL
                ,NULL                                                                   AS EADAIRB                --高级法违约风险暴露             默认 NULL
                ,CASE WHEN T5.PDLEVEL = '0116' THEN '1'
                 ELSE '0'
                 END                                                                    AS DefaultFlag            --违约标识
                ,CASE WHEN T1.BOND_TYPE2 = '20' THEN 0.75
                 ELSE 0.45
                 END                                                                    AS BEEL                   --已违约暴露预期损失比率         债权级别＝‘高级债权’，BEEL ＝ 45%；债权级别＝‘次级债权’，BEEL ＝ 75%
                ,CASE WHEN T1.BOND_TYPE2 = '20' THEN 0.75
                 ELSE 0.45
                 END                                                                    AS DefaultLGD             --已违约暴露违约损失率            默认 NULL
                ,'0'                                                                    AS EquityExpoFlag         --股权暴露标识                    默认 否(0)
                ,''                                                                     AS EquityInvestType       --股权投资对象类型                默认 NULL
                ,''                                                                     AS EquityInvestCause      --股权投资形成原因                默认 NULL
                ,'0'                                                                    AS SLFlag                 --专业贷款标识                    默认 否(0)
                ,''                                                                     AS SLType                 --专业贷款类型                    默认 NULL
                ,''                                                                     AS PFPhase                --项目融资阶段                    默认 NULL
                ,'01'                                                                   AS ReguRating             --监管评级                        默认 优(01)
                ,''                                                                     AS CBRCMPRatingFlag       --银监会认定评级是否更为审慎       默认 NULL
                ,''                                                                     AS LargeFlucFlag          --是否波动性较大                 默认 NULL
                ,'0'                                                                    AS LiquExpoFlag           --是否清算过程中风险暴露         默认 否(0)
                ,'1'                                                                    AS PaymentDealFlag        --是否货款对付模式                默认 是(1)
                ,NULL                                                                   AS DelayTradingDays       --延迟交易天数                    默认 NULL
                ,'0'                                                                    AS SecuritiesFlag         --有价证券标识                    默认 否(0)
                ,''                                                                     AS SecuIssuerID           --证券发行人ID                   默认 NULL
                ,''                                                                     AS RatingDurationType     --评级期限类型                    默认 NULL
                ,''                                                                     AS SecuIssueRating        --证券发行等级                    默认 NULL
                ,NULL                                                                   AS SecuResidualM          --证券剩余期限                    默认 NULL
                ,1                                                                      AS SecuRevaFrequency      --证券重估频率                    默认 1
                ,'0'                                                                    AS CCPTranFlag            --是否中央交易对手相关交易        默认 否(0)
                ,''                                                                     AS CCPID                  --中央交易对手ID                  默认 NULL
                ,''                                                                     AS QualCCPFlag            --是否合格中央交易对手            默认 NULL
                ,''                                                                     AS BankRole               --银行角色                        默认 NULL
                ,''                                                                     AS ClearingMethod         --清算方式                        默认 NULL
                ,''                                                                     AS BankAssetFlag          --是否银行提交资产                默认 NULL
                ,''                                                                     AS MatchConditions        --符合条件情况                    默认 NULL
                ,'0'                                                                    AS SFTFlag                --证券融资交易标识                默认 否(0)
                ,'0'                                                                    AS MasterNetAgreeFlag     --净额结算主协议标识             默认 否(0)
                ,''                                                                     AS MasterNetAgreeID       --净额结算主协议ID               默认 NULL
                ,''                                                                     AS SFTType                --证券融资交易类型                默认 NULL
                ,''                                                                     AS SecuOwnerTransFlag     --证券所有权是否转移             默认 NULL
                ,'0'                                                                    AS OTCFlag                --场外衍生工具标识                默认 否(0)
                ,''                                                                     AS ValidNettingFlag       --有效净额结算协议标识            默认 NULL
                ,''                                                                     AS ValidNetAgreementID    --有效净额结算协议ID              默认 NULL
                ,''                                                                     AS OTCType                --场外衍生工具类型                默认 NULL
                ,''                                                                     AS DepositRiskPeriod      --保证金风险期间                 默认 NULL
                ,''                                                                     AS MTM                    --重置成本                        默认 NULL
                ,''                                                                     AS MTMCurrency            --重置成本币种                    默认 NULL
                ,''                                                                     AS BuyerOrSeller          --买方卖方                        默认 NULL
                ,''                                                                     AS QualROFlag             --合格参照资产标识                默认 NULL
                ,''                                                                     AS ROIssuerPerformFlag    --参照资产发行人是否能履约        默认 NULL
                ,''                                                                     AS BuyerInsolvencyFlag    --信用保护买方是否破产            默认 NULL
                ,''                                                                     AS NonpaymentFees         --尚未支付费用                    默认 NULL
                ,'0'                                                                    AS RetailExpoFlag         --零售暴露标识                    默认 否(0)
                ,''                                                                     AS RetailClaimType        --零售债权类型                    默认 NULL
                ,''                                                                     AS MortgageType           --住房抵押贷款类型                默认 NULL
                ,1                                                                      AS ExpoNumber             --风险暴露个数                    默认 1
                ,0.8                                                                    AS LTV                    --贷款价值比                     默认 0.8
                ,NULL                                                                   AS Aging                  --账龄                            默认 NULL
                ,''                                                                     AS NewDefaultDebtFlag     --新增违约债项标识                默认 NULL
                ,''                                                                     AS PDPoolModelID          --PD分池模型ID                    默认 NULL
                ,''                                                                     AS LGDPoolModelID         --LGD分池模型ID                   默认 NULL
                ,''                                                                     AS CCFPoolModelID         --CCF分池模型ID                   默认 NULL
                ,''                                                                     AS PDPoolID               --所属PD池ID                     默认 NULL
                ,''                                                                     AS LGDPoolID              --所属LGD池ID                    默认 NULL
                ,''                                                                     AS CCFPoolID              --所属CCF池ID                    默认 NULL
                ,'0'                                                                    AS ABSUAFlag              --资产证券化基础资产标识         默认 否(0)
                ,''                                                                     AS ABSPoolID              --证券化资产池ID                  默认 NULL
                ,''                                                                     AS GroupID                --分组编号                        默认 NULL
                ,CASE WHEN T5.PDLEVEL = '0116' THEN TO_DATE(T5.PDVAVLIDDATE,'YYYYMMDD')
                 ELSE NULL
                 END                                                                    AS DefaultDate            --违约时点
                ,NULL                                                                   AS ABSPROPORTION          --资产证券化比重
                ,NULL                                                                   AS DEBTORNUMBER           --借款人个数

    FROM        RWA_DEV.FNS_BND_INFO_B T1
    INNER JOIN  TEMP_BND_BOOK T2
    ON          T1.BOND_ID = T2.BOND_ID
    INNER JOIN  RWA_DEV.NCM_BUSINESS_DUEBILL T3                           --信贷借据表
    ON          'CW_IMPORTDATA' || T1.BOND_ID = T3.THIRDPARTYACCOUNTS
    AND         T3.DATANO = p_data_dt_str
    LEFT JOIN   RWA.ORG_INFO T4                                           --RWA机构表
    ON          T1.DEPARTMENT = T4.ORGID
    LEFT JOIN   RWA_DEV.RWA_TMP_TZCUST T5                                 --实际融资人客户信息临时表
    ON          T3.RELATIVESERIALNO2 = T5.CONTRACTNO
    LEFT JOIN   RWA.CODE_LIBRARY  T6                                      --RWA码表，获取行业
    ON          T5.INDUSTRYTYPE = T6.ITEMNO
    AND         T6.CODENO = 'IndustryType'
    LEFT JOIN   RWA.CODE_LIBRARY  T7                                      --RWA码表，获取业务品种
    ON          T3.BUSINESSTYPE = T7.ITEMNO
    AND         T7.CODENO = 'BusinessType'
    INNER JOIN  RWA_DEV.NCM_BUSINESS_CONTRACT T10
    ON          T3.RELATIVESERIALNO2 = T10.SERIALNO
    AND         (T10.BUSINESSSUBTYPE NOT LIKE '0010%' OR T10.BUSINESSSUBTYPE IS NULL)             --非基于银行
    AND         T10.DATANO = p_data_dt_str
    LEFT JOIN   RWA_DEV.NCM_RWA_RISK_EXPO_RST T12                 --内评发暴露分类结果表
    ON          T5.CUSTOMERID = T12.OBJECTNO
    AND         T12.OBJECTTYPE = 'BusinessDuebillCust'                    --非银行对应实际融资人
    AND         T12.DATANO = p_data_dt_str
    LEFT JOIN   RWA_DEV.RWA_CD_CODE_MAPPING T13                           --代码映射表
    ON          T12.RISKEXPOSURERESULT = T13.SITEMNO
    AND         T13.SYSID = 'XD'
    AND         T13.SCODENO = 'RwaResultType'
    AND         T13.ISINUSE = '1'
    /*
    LEFT JOIN   (SELECT TR.BOND_ID
                       ,TO_NUMBER(REPLACE(TR.RESERVESUM,',','')) AS RESERVESUM
                   FROM RWA.RWA_WS_RESERVE TR
             INNER JOIN RWA.RWA_WP_DATASUPPLEMENT TD                      --数据补录表
                     ON TR.SUPPORGID = TD.ORGID
                    AND TD.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
                    AND TD.SUPPTMPLID = 'M-0210'
                    AND TD.SUBMITFLAG = '1'
                  WHERE TR.DATADATE = to_date(p_data_dt_str,'YYYYMMDD')
                ) T16                                                     --应收款投资准备金补录表
    ON          T1.BOND_ID = T16.BOND_ID
    */
    LEFT JOIN   RWA_DEV.NCM_CLASSIFY_RECORD T18                           --十二级分类信息表
    ON          T3.RELATIVESERIALNO2 = T18.OBJECTNO
    AND         T18.OBJECTTYPE = 'TwelveClassify'
    AND         T18.ISWORK = '1'
    AND         T18.DATANO = p_data_dt_str
    WHERE       T1.ASSET_CLASS IN ('50','60')                       --通过资产分类来确定债券还是应收款投资。
    AND         T1.DATANO = p_data_dt_str                           --债券信息表,获取有效的债券信息
    AND         T1.BOND_CODE IS NOT NULL                            --排除无效的债券数据
    ;

    COMMIT;
    
    --2.4.4 财务系统-应收款投资-货币基金-11010301 BY LJZ 21/1/11
    INSERT INTO RWA_DEV.RWA_TZ_EXPOSURE(
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
                ,SBJT2
                ,SBJT_VAL2
                ,SBJT3
                ,SBJT_VAL3
                ,SBJT4
                ,SBJT_VAL4
                ,SBJT5
                ,SBJT_VAL5
    )
    SELECT P_DATA_DT_STR, --数据日期
       P_DATA_DT_STR, --数据流水号
       T1.FUNDMARKETCODE, --风险暴露ID
       T1.FUNDMARKETCODE, --债项ID
       'TZ', --源系统ID
       T1.FUNDMARKETCODE, --合同ID
       NULL, --参与主体ID
       NVL(T2.ORGID, '6001'), --源机构ID
       NVL(T3.ORGNAME, '重庆银行股份有限公司国际业务部'), --源机构名称
       NVL(T3.SORTNO, '1290'), --所属机构排序号
       NVL(T2.ORGID, '6001'), --所属机构ID
       NVL(T3.ORGNAME, '重庆银行股份有限公司国际业务部'), --所属机构名称
       NVL(T2.ORGID, '6001'), --账务机构ID
       NVL(T3.ORGNAME, '重庆银行股份有限公司国际业务部'), --账务机构名称
       'J66', --所属行业代码
       '货币金融服务', --所属行业名称
       '0401', --业务条线
       NULL, --资产大类
       NULL, --资产小类
       '1040105060', --业务品种代码
       '应收款项类投资_货币基金', --业务品种名称
       '01', --信用风险数据类型
       '01', --折扣系数对应资产类别
       '07', --权重法业务类型
       NULL, --权重法暴露大类
       NULL, --权重法暴露小类
       NULL, --内评法暴露大类
       NULL, --内评法暴露小类
       '01', --暴露所属标识
       NULL, --账户类别
       '02', --监管交易类型
       '0', --回购交易标识
       '1', --重估频率
       T1.CURRENCY, --币种
       T4.REMAMT, --正常本金余额
       0, --逾期余额
       0, --非应计余额
       T4.REMAMT, --表内余额
       0, --正常利息
       0, --表内欠息
       0, --表外欠息
       0, --应收费用
       T4.REMAMT, --资产余额
       T2.SUBJECTFLOW, --科目一
       NULL, --科目二
       NULL, --科目三
       T1.DEFDATE, --起始日期
       NULL, --到期日期
       NULL, --原始期限
       NULL, --剩余期限
       '01', --风险分类
       '01', --风险暴露状态
       0, --逾期天数
       0, --专项准备金
       0, --一般准备金
       0, --特别准备金
       0, --已核销金额
       NULL, --表外暴露来源
       NULL, --表外业务类型
       NULL, --权重法表外业务类型细分
       NULL, --是否可随时无条件撤销
       NULL, --信用转换系数级别
       NULL, --高级法信用转换系数
       NULL, --债权级别
       '1', --是否为债券
       '02', --债券发行目的
       '0', --是否非自用不动产
       NULL, --抵债资产期限类型
       '0', --是否依赖于银行未来盈利
       NULL, --内部评级
       NULL, --违约概率
       NULL, --违约损失率级别
       NULL, --高级法违约损失率
       NULL, --高级法有效期限
       NULL, --高级法违约风险暴露
       '0', --违约标识
       0.45, --已违约暴露预期损失比率
       0.45, --已违约暴露违约损失率
       '0', --股权暴露标识
       NULL, --股权投资对象类型
       NULL, --股权投资形成原因
       '0', --专业贷款标识
       NULL, --专业贷款类型
       NULL, --项目融资阶段
       '01', --监管评级
       NULL, --银监会认定评级是否更为审慎
       NULL, --是否波动性较大
       '0', --是否清算过程中风险暴露
       '1', --是否货款对付模式
       NULL, --延迟交易天数
       '1', --有价证券标识
       NULL, --证券发行人ID
       NULL, --评级期限类型
       NULL, --证券发行等级
       NULL, --证券剩余期限
       1, --证券重估频率
       '0', --是否中央交易对手相关交易
       NULL, --中央交易对手ID
       NULL, --是否合格中央交易对手
       NULL, --银行角色
       NULL, --清算方式
       NULL, --是否银行提交资产
       NULL, --符合条件情况
       '0', --证券融资交易标识
       '0', --净额结算主协议标识
       NULL, --净额结算主协议ID
       NULL, --证券融资交易类型
       NULL, --证券所有权是否转移
       '0', --场外衍生工具标识
       NULL, --有效净额结算协议标识
       NULL, --有效净额结算协议ID
       NULL, --场外衍生工具类型
       NULL, --保证金风险期间
       NULL, --重置成本
       NULL, --重置成本币种
       NULL, --买方卖方
       NULL, --合格参照资产标识
       NULL, --参照资产发行人是否能履约
       NULL, --信用保护买方是否破产
       NULL, --尚未支付费用
       '0', --零售暴露标识
       NULL, --零售债权类型
       NULL, --住房抵押贷款类型
       1, --风险暴露个数
       0.8, --贷款价值比
       NULL, --账龄
       NULL, --新增违约债项标识
       NULL, --PD分池模型ID
       NULL, --LGD分池模型ID
       NULL, --CCF分池模型ID
       NULL, --所属PD池ID
       NULL, --所属LGD池ID
       NULL, --所属CCF池ID
       '0', --资产证券化基础资产标识
       NULL, --证券化资产池ID
       NULL, --分组编号
       NULL, --违约时点
       NULL, --资产证券化比重
       NULL, --借款人个数
       NULL, --利息调整科目
       NULL, --利息调整金额
       NULL, --利息科目
       NULL, --利息金额
       NULL, --公允价值变动科目
       NULL, --公允价值变动金额
       NULL, --应收利息科目
       NULL --应收利息金额
  FROM FDS_FUND_INFO T1
  LEFT JOIN FDS_SYS_BUSI_ACCOUNT T2
    ON T1.DATANO = T2.DATANO
   AND T1.FUNDCODE = T2.BUSINESSID
  LEFT JOIN RWA.ORG_INFO T3
    ON T2.ORGID = T3.ORGID
  LEFT JOIN FDS_SYS_ACCOUNT_HISDALIY T4
    ON T2.DATANO = T4.DATANO
   AND T2.INNERACCOUNT = T4.INNERACCOUNT
   AND T2.ACCOUNT = T4.ACCOUNT
 WHERE T1.DATANO = P_DATA_DT_STR
  AND   T1.effectflag = 'E'  and t4.remamt >0;

    COMMIT;

    --2.5 新增资金系统-获取本币的债券投资-11010101,15010101,15030101
    INSERT INTO RWA_DEV.RWA_TZ_EXPOSURE(
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
                ,SBJT2
                ,SBJT_VAL2
                ,SBJT3
                ,SBJT_VAL3
                ,SBJT4
                ,SBJT_VAL4
                ,SBJT5
                ,SBJT_VAL5
    )
  select TO_DATE(p_data_dt_str, 'YYYYMMDD'),
       p_data_dt_str,
       --T2.ACCT_NO||T2.SECURITY_REFERENCE,
       T2.ACCT_NO,
       --T2.ACCT_NO||T2.SECURITY_REFERENCE,
       T2.ACCT_NO,
       'TZ',
       --T1.BOND_ID,
       T2.ACCT_NO||T2.SECURITY_REFERENCE,
       CASE WHEN T1.BOND_TYPE in('XYB','IBD') THEN 'XN-ZGSYYH' ----XYB商业银行债
       ELSE NVL(T1.ISSUER_CODE,'XN_JRJG') END            AS CLIENTID,
       T2.ORG_CD,
       T3.ORGNAME,
       --T3.SORTNO,
       NVL(T3.SORTNO,'1010'),
       --T2.ORG_CD,
       DECODE(SUBSTR(T2.ORG_CD,1,1),'@','01000000',T2.ORG_CD),
       --T3.ORGNAME,
       NVL(T3.ORGNAME,'总行'),
       T2.ORG_CD,
       T3.ORGNAME,
       NVL(T4.INDUSTRYTYPE,'J6620'),
       T5.ITEMNAME,
       '0401',
       '',
       '',
       '1040102040',
       '人民币债券投资',
       '01',
       '01',
       '07',
       CASE WHEN T1.BOND_TYPE='TB' THEN '0102'--国债
            WHEN T1.BOND_TYPE='TBB' THEN '0102'--央行债券
            WHEN T1.BOND_TYPE='SSS' THEN '0103'--地方政府债
            WHEN T1.BOND_TYPE='TDZ' THEN '0103'--铁道债
            WHEN T1.BOND_TYPE='PBB' THEN '0104'--政策性银行债
            WHEN T1.BOND_TYPE in('XYB','IBD') THEN ''--商也银行债/同业存单  日期划分
            WHEN T1.BOND_TYPE ='ZQBB'THEN '0106'--中期票据    
            WHEN T1.BOND_TYPE ='CEB'AND T1.BOND_ID='1280175' THEN '0103' --铁道债 公共部门实体 
            WHEN T1.BOND_TYPE ='CEB'AND T1.BOND_ID<>'1280175'THEN '0106' --企业债
            WHEN T1.BOND_TYPE ='DOBB'THEN '0106' --短期融资券
            WHEN T1.BOND_TYPE ='CDQRZQ'THEN '0106' --超短期融资
            WHEN T1.BOND_TYPE ='PPN' THEN '0106'--非公开定向债务融资工具
            WHEN T1.BOND_TYPE ='SEB' THEN '0106'--集合票据
            WHEN T1.BOND_TYPE ='OTHB' THEN '0106'--其他债券
            WHEN T1.BOND_TYPE ='CPB' THEN '0106' --公司债
            WHEN T1.BOND_TYPE ='OCB' THEN '0106' --企业债券 
            WHEN T1.BOND_TYPE ='KFLZ' THEN '0106' --可分离债
            WHEN T1.BOND_TYPE ='IPEB' THEN '0106' --银行间私募债券 
            WHEN T1.BOND_TYPE ='M' THEN '0104'  ---金融机构债
            WHEN T1.BOND_TYPE ='AMCFB' THEN '0104'  --资产管理公司金融债
            WHEN T1.BOND_TYPE in ('TTC','OBB','XYBS') THEN '0104' ---次级债
            WHEN T1.BOND_TYPE ='ABS' THEN ''--ABS 在ABS借口计量
            END ,----暴露大类
       CASE WHEN T1.BOND_TYPE='TB' THEN '010201'--国债
            WHEN T1.BOND_TYPE='TBB' THEN '010202'--央行债券
            WHEN T1.BOND_TYPE='SSS' THEN '010303'--地方政府债
            WHEN T1.BOND_TYPE='TDZ' THEN '010302'--铁道债
            WHEN T1.BOND_TYPE='PBB' THEN '010401'--政策性银行债
            WHEN T1.BOND_TYPE in('XYB','IBD') THEN ''--商也银行债/同业存单  日期划分
            WHEN T1.BOND_TYPE ='ZQBB'THEN '010601'--中期票据    
            WHEN T1.BOND_TYPE ='CEB'AND T1.BOND_ID='1280175' THEN '010302' --铁道债 公共部门实体 
            WHEN T1.BOND_TYPE ='CEB'AND T1.BOND_ID<>'1280175'THEN '010601' --企业债
            WHEN T1.BOND_TYPE ='DOBB'THEN '010601' --短期融资券
            WHEN T1.BOND_TYPE ='CDQRZQ'THEN '010601' --超短期融资
            WHEN T1.BOND_TYPE ='PPN' THEN '010601'--非公开定向债务融资工具
            WHEN T1.BOND_TYPE ='SEB' THEN '010601'--集合票据
            WHEN T1.BOND_TYPE ='OTHB' THEN '010601'--其他债券
            WHEN T1.BOND_TYPE ='CPB' THEN '010601' --公司债
            WHEN T1.BOND_TYPE ='OCB' THEN '010601' --企业债券 
            WHEN T1.BOND_TYPE ='KFLZ' THEN '010601' --可分离债
            WHEN T1.BOND_TYPE ='IPEB' THEN '010601' --银行间私募债券 
            WHEN T1.BOND_TYPE ='M' THEN '010408'  ---金融机构债
            WHEN T1.BOND_TYPE ='AMCFB' THEN '010408'  --资产管理公司金融债
            WHEN T1.BOND_TYPE in ('TTC','OBB','XYBS') THEN '010407' ---次级债
            WHEN T1.BOND_TYPE ='ABS' THEN''--ABS 在ABS借口计量
            END ,----暴露小类
       '',
       '',
       '01',
       '01',
       '02',
       '0',
       1,
       'CNY',
       NVL(T2.POSITION_INITIAL_VALUE,0),
       0,
       0,
       NVL(T2.POSITION_INITIAL_VALUE,0),
       0,
       0,
       0,
       0,
       NVL(T2.POSITION_INITIAL_VALUE,0),
       T2.SBJT_CD,
       '',
       '',
       NVL(T1.ISSUE_DATE,p_data_dt_str),
       T1.MATU_DT,
      CASE
         WHEN (TO_DATE(T1.MATU_DT, 'YYYY-MM-DD') -
              TO_DATE(NVL(T1.ISSUE_DATE,p_data_dt_str), 'YYYY-MM-DD')) / 365 < 0 THEN
          0
         ELSE
          (TO_DATE(T1.MATU_DT, 'YYYY-MM-DD') -
          TO_DATE(NVL(T1.ISSUE_DATE,p_data_dt_str), 'YYYY-MM-DD')) / 365
       END,
       CASE
         WHEN (TO_DATE(T1.MATU_DT, 'YYYY-MM-DD') -
              TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365 < 0 THEN
          0
         ELSE
          (TO_DATE(T1.MATU_DT, 'YYYY-MM-DD') -
          TO_DATE(p_data_dt_str, 'YYYYMMDD')) / 365
       END,
       '01',
       '01',
       0,
       0,
       0,
       0,
       0,
       '',
       '',
       '',
       '',
       '',
       '',
       CASE WHEN T1.BOND_TYPE in ('TTC','OBB','XYBS') THEN '02'
         ELSE '01' END ,    ------债权级别
       '0',
       NVL(T6.BONDPUBLISHPURPOSE， '02'),
       '0',
       '',
       '0',
       T7.PDADJLEVEL,
       T7.PD,
       NULL,
       NULL,
       NULL,
       NULL,
       CASE
         WHEN T8.BREAKDATE IS NOT NULL THEN
          '1'
         ELSE
          '0'
       END,
       '',
       '',
       '0',
       '',
       '',
       '0',
       '',
       '',
       '01',
       '',
       '',
       '0',
       '0',
       NULL,
       '0',
       T6.BONDPUBLISHID,
       T6.TIMELIMIT,
       T6.BONDRATING,
       NULL,
       1,
       '0',
       '',
       '',
       '',
       '',
       '0',
       '',
       '0',
       '0',
       '',
       '',
       '0',
       '0',
       '0',
       '',
       '',
       '',
       '',
       '',
       '',
       '0',
       '',
       '',
       '',
       '0',
       '',
       '',
       1,
       0.8,
       NULL,
       '',
       '',
       '',
       '',
       '',
       '',
       '',
       '0',
       '',
       '',
       CASE
         WHEN T7.PDADJLEVEL = '0116' THEN
          TO_DATE(T7.PDVAVLIDDATE, 'YYYYMMDD')
         ELSE
          NULL
       END,
       '',
       '',
       T2.INT_ADJ_ITEM,--利息调整
       T2.INT_ADJ_VAL,--利息调整
       DECODE(SUBSTR(T2.ACCRUAL_GLNO, 1, 4), '1132', NULL,'1101',NULL, T2.ACCRUAL_GLNO),--应计利息
       DECODE(SUBSTR(T2.ACCRUAL_GLNO, 1, 4), '1132', NULL,'1101',NULL, T2.ACCRUAL),--应计利息
       T2.FAIR_EXCH_ITEM,--公允价值变动 
       T2.FAIR_EXCH_VAL,--公允价值变动
       DECODE(T2.SBJT_CD,'11010101','11010104',NULL),--应计利息 11010101才有
       DECODE(T2.SBJT_CD,'11010101',T2.INS_RECEIVABLE,NULL)--应计利息
  FROM RWA_DEV.BRD_BOND T1
 INNER JOIN (
       SELECT T.ACCT_NO,
       T.SECURITY_REFERENCE,
       T.ORG_CD,
       T.SBJT_CD,
       T.BELONG_GROUP,
       NVL(T.POSITION_INITIAL_VALUE, 0) AS POSITION_INITIAL_VALUE,
       T.INT_ADJ_ITEM,
       NVL(T.INT_ADJ_VAL, 0) AS INT_ADJ_VAL,
       T.ACCRUAL_GLNO,
       NVL(T.ACCRUAL, 0) AS ACCRUAL,
       T.FAIR_EXCH_ITEM,
       NVL(T.FAIR_EXCH_VAL, 0) AS FAIR_EXCH_VAL,
       NVL(T.INS_RECEIVABLE, 0) AS INS_RECEIVABLE
  FROM BRD_SECURITY_POSI T
 WHERE T.BELONG_GROUP = '4'
      --AND SUBSTR(t.sbjt_cd,1,6) = SUBSTR(t.int_adj_item,1,6)
   AND T.DATANO = p_data_dt_str
   AND T.ACCT_NO NOT IN ('SQ0202201905090001',
                         'SQ0202201905090002',
                         'SQ0202201905090003',
                         'SQ0202201905090004')
UNION
SELECT T.ACCT_NO,
       T.SECURITY_REFERENCE,
       T.ORG_CD,
       T.SBJT_CD,
       T.BELONG_GROUP,
       NVL(T.POSITION_INITIAL_VALUE, 0) AS POSITION_INITIAL_VALUE,
       T.INT_ADJ_ITEM,
       NVL(T.INT_ADJ_VAL, 0) AS INT_ADJ_VAL,
       T.ACCRUAL_GLNO,
       NVL(T.ACCRUAL, 0) AS ACCRUAL,
       T.FAIR_EXCH_ITEM,
       NVL(T.FAIR_EXCH_VAL, 0) AS FAIR_EXCH_VAL,
       NVL(T.INS_RECEIVABLE, 0) AS INS_RECEIVABLE
  FROM BRD_SECURITY_POSI T
 WHERE T.BELONG_GROUP = '4'
   AND T.SBJT_CD = '15010101'
   AND T.INT_ADJ_ITEM = '15010102'
   AND T.ACCRUAL_GLNO = '11320701'
   AND T.DATANO = P_DATA_DT_STR
   AND T.ACCT_NO IN ('SQ0202201905090001',
                     'SQ0202201905090002',
                     'SQ0202201905090003',
                     'SQ0202201905090004')
         ) T2
    ON T1.BOND_ID = T2.SECURITY_REFERENCE
   AND T2.BELONG_GROUP = '4' --债券本币投资              
  LEFT JOIN RWA.ORG_INFO T3
    ON T2.ORG_CD = T3.ORGID
   AND T3.STATUS = '1'
  LEFT JOIN NCM_CUSTOMER_INFO T4 --统一客户信息表
    ON T1.ISSUER_CODE = T4.CUSTOMERID
    AND T1.DATANO=T4.DATANO
   AND T4.CUSTOMERTYPE NOT LIKE '03%' --对公客户             
  LEFT JOIN RWA.CODE_LIBRARY T5
    ON T4.INDUSTRYTYPE = T5.ITEMNO
   AND T5.CODENO = 'IndustryType'
  LEFT JOIN (SELECT *
  FROM (select ROW_NUMBER() OVER(PARTITION BY T.BONDNO ORDER BY T.BONDNO) AS ROW_ID,
               T.*
          from NCM_BOND_INFO t
          WHERE T.DATANO=p_data_dt_str)
 WHERE ROW_ID = 1) T6
    ON T1.BOND_ID = T6.BONDNO
   LEFT JOIN RWA_TEMP_PDLEVEL T7
    ON T1.ISSUER_CODE = T7.CUSTID
  LEFT JOIN (select *
  from (SELECT ROW_NUMBER() OVER(PARTITION BY CUSTOMERID ORDER BY BREAKDATE DESC) as row_id,
               T.*
          FROM NCM_BREAKDEFINEDREMARK T
         where t.breakdate <= p_data_dt_str)
 where row_id = 1) T8
    ON T1.ISSUER_CODE = T8.CUSTOMERID
 WHERE T1.BELONG_GROUP = '4' --债券本币投资
    AND T1.DATANO=p_data_dt_str ;
    commit;
    
    -- 资管、信托、融资计划 11020101、12220101 BY LJZ 21/1/11
    INSERT INTO RWA_DEV.RWA_TZ_EXPOSURE(
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
                ,SBJT2
                ,SBJT_VAL2
                ,SBJT3
                ,SBJT_VAL3
                ,SBJT4
                ,SBJT_VAL4
                ,SBJT5
                ,SBJT_VAL5
    )
    SELECT P_DATA_DT_STR, --数据日期
       P_DATA_DT_STR, --数据流水号
       T1.TRADENUM, --风险暴露ID
       T1.TRADENUM, --债项ID
       'TZ', --源系统ID
       T1.TRADENUM, --合同ID
       T4.CORPCODE, --参与主体ID
       T8.CODE, --源机构ID
       NULL, --源机构名称
       NULL, --所属机构排序号
       T8.CODE, --所属机构ID
       NULL, --所属机构名称
       T8.CODE, --账务机构ID
       NULL, --账务机构名称
       'J6620', --所属行业代码
       NULL, --所属行业名称
       '0401', --业务条线
       NULL, --资产大类
       NULL, --资产小类
       '1040105060', --业务品种代码
       CASE
         WHEN T1.ASSETNAME LIKE '%保本%' THEN
          '应收款项类投资_保本理财'
         WHEN T1.ASSETNAME NOT LIKE '%保本%' AND T1.ASSETNAME LIKE '%理财%' THEN
          '应收款项类投资_非保本理财'
         WHEN T1.ASSETNAME LIKE '%信%托%' THEN
          '应收款项类投资_信托计划'
         WHEN T1.ASSETNAME LIKE '%融资%' THEN
          '应收款项类投资_融资计划'
         WHEN T1.ASSETNAME LIKE '%资%管%' THEN
          '应收款项类投资_资管计划'
       --  WHEN T1.BOND_TYPE1 ='2008' THEN'毛主席像章'
         ELSE
          '应收款项类投资_资管信托'
       END AS BUSINESSTYPENAME, --业务品种名称
       '01', --信用风险数据类型
       '01', --折扣系数对应资产类别
       '07', --权重法业务类型
       NULL, --权重法暴露大类
       NULL, --权重法暴露小类
       NULL, --内评法暴露大类
       NULL, --内评法暴露小类
       '01', --暴露所属标识
       DECODE(T1.ZCFL, '10', '02', '01'), --账户类别
       '02', --监管交易类型
       '0', --回购交易标识
       1, --重估频率
       T1.PK_CURRTYPE, --币种
       T2.AMT, --正常本金余额
       0, --逾期余额
       0, --非应计余额
       T2.AMT, --表内余额
       0, --正常利息
       0, --表内欠息
       0, --表外欠息
       0, --应收费用
       T2.AMT, --资产余额
       T2.SUBJCODE, --科目一
       NULL, --科目二
       NULL, --科目三
       T1.STARTDATE, --起始日期
       T1.ENDDATE, --到期日期
       (T1.ENDDATE - T1.STARTDATE) / 12, --原始期限
       (T1.ENDDATE - P_DATA_DT_STR) / 12, --剩余期限
       '01', --风险分类
       NULL, --风险暴露状态
       0, --逾期天数
       0, --专项准备金
       0, --一般准备金
       0, --特别准备金
       0, --已核销金额
       NULL, --表外暴露来源
       NULL, --表外业务类型
       NULL, --权重法表外业务类型细分
       NULL, --是否可随时无条件撤销
       NULL, --信用转换系数级别
       NULL, --高级法信用转换系数
       T1.PK_ASSETDOC, --债权级别
       '0', --是否为债券
       '02', --债券发行目的
       '0', --是否非自用不动产
       NULL, --抵债资产期限类型
       '0', --是否依赖于银行未来盈利
       NULL, --内部评级
       NULL, --违约概率
       NULL, --违约损失率级别
       NULL, --高级法违约损失率
       NULL, --高级法有效期限
       NULL, --高级法违约风险暴露
       NULL, --违约标识
       NULL, --已违约暴露预期损失比率
       NULL, --已违约暴露违约损失率
       '0', --股权暴露标识
       NULL, --股权投资对象类型
       NULL, --股权投资形成原因
       '0', --专业贷款标识
       NULL, --专业贷款类型
       NULL, --项目融资阶段
       '01', --监管评级
       NULL, --银监会认定评级是否更为审慎
       NULL, --是否波动性较大
       '0', --是否清算过程中风险暴露
       '1', --是否货款对付模式
       NULL, --延迟交易天数
       '0', --有价证券标识
       NULL, --证券发行人ID
       NULL, --评级期限类型
       NULL, --证券发行等级
       NULL, --证券剩余期限
       1, --证券重估频率
       '0', --是否中央交易对手相关交易
       NULL, --中央交易对手ID
       NULL, --是否合格中央交易对手
       NULL, --银行角色
       NULL, --清算方式
       NULL, --是否银行提交资产
       NULL, --符合条件情况
       '0', --证券融资交易标识
       '0', --净额结算主协议标识
       NULL, --净额结算主协议ID
       NULL, --证券融资交易类型
       NULL, --证券所有权是否转移
       '0', --场外衍生工具标识
       NULL, --有效净额结算协议标识
       NULL, --有效净额结算协议ID
       NULL, --场外衍生工具类型
       NULL, --保证金风险期间
       NULL, --重置成本
       NULL, --重置成本币种
       NULL, --买方卖方
       NULL, --合格参照资产标识
       NULL, --参照资产发行人是否能履约
       NULL, --信用保护买方是否破产
       NULL, --尚未支付费用
       '0', --零售暴露标识
       NULL, --零售债权类型
       NULL, --住房抵押贷款类型
       1, --风险暴露个数
       0.8, --贷款价值比
       NULL, --账龄
       NULL, --新增违约债项标识
       NULL, --PD分池模型ID
       NULL, --LGD分池模型ID
       NULL, --CCF分池模型ID
       NULL, --所属PD池ID
       NULL, --所属LGD池ID
       NULL, --所属CCF池ID
       '0', --资产证券化基础资产标识
       NULL, --证券化资产池ID
       NULL, --分组编号
       NULL, --违约时点
       NULL, --资产证券化比重
       NULL, --借款人个数
       NULL,
       NULL,
       NULL,
       NULL,
       NULL,
       NULL,
       NULL,
       NULL
  FROM NFIN_NFT_TRADE_XQMR T1 --债券买入交易确认
  LEFT JOIN NFIN_FAE_CW_ACCT T2 --科目对照表
    ON T1.DATANO = T2.DATANO
   AND T1.TRADENUM = T2.ACCT_NO
   AND T2.AMT <> 0
  LEFT JOIN NFIN_V_FAE_PRODUCT T3 --产品档案表
    ON T1.DATANO = T3.DATANO
   AND T1.PRODUCT = T3.PK_PRODUCT
  LEFT JOIN NFIN_V_NFT_TRADECORP T4 --交易对手档案表
    ON T1.DATANO = T4.DATANO
   AND T1.PK_TRADECORP = T4.PK_TRADECORP
  LEFT JOIN NFIN_V_FAE_CHANNEL T5 --渠道档案表
    ON T1.DATANO = T5.DATANO
   AND T1.TRENCH = T5.PK_CHANNEL
  LEFT JOIN NFIN_V_FAE_CUSTMANGER T6 --客户经理档案表
    ON T1.DATANO = T6.DATANO
   AND T1.PK_PSNDOC = T6.PK_DEFDOC
  LEFT JOIN NFIN_V_FAE_BUSINESSLINE T7 --条线档案表
    ON T1.DATANO = T7.DATANO
   AND T1.LINES = T7.PK_DEFDOC
  LEFT JOIN NFIN_ORG_ORGS T8 --机构档案表
    ON T1.DATANO = T8.DATANO
   AND T1.PK_ORG = T8.PK_ORG
 WHERE T1.DATANO = P_DATA_DT_STR;

    COMMIT;
    
    -- 福费廷 15010101
    INSERT INTO RWA_DEV.RWA_TZ_EXPOSURE(
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
                ,SBJT2
                ,SBJT_VAL2
                ,SBJT3
                ,SBJT_VAL3
                ,SBJT4
                ,SBJT_VAL4
                ,SBJT5
                ,SBJT_VAL5
    )
     SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD') AS DataDate,  --数据日期
                p_data_dt_str AS DataNo,--数据流水号
                T2.Serialno  AS ExposureID,             --风险暴露ID
                T2.Serialno  AS DueID,                  --债项ID
                'TZ' AS SSysID,     --源系统ID             默认 投资(TZ)
                T2.Serialno  AS ContractID,             --合同ID
                t2.customerid  AS ClientID,    --参与主体ID                      默认一个一般公司
                t2.mforgid  AS SOrgID, --源机构ID
                T3.ORGNAME  AS SOrgName,   --源机构名称
                NVL(T3.SORTNO,'1010'),
                decode(substr(T2.mforgid,1,1),'@','01000000',T2.mforgid),
                NVL(T3.Orgname,'总行'),
                T2.mforgid   AS AccOrgID,               --账务机构ID
                T3.ORGNAME    AS AccOrgName,             --账务机构名称
                T5.Industrytype  AS IndustryID,             --所属行业代码
                '' AS IndustryName,           --所属行业名称
                '0401' AS BusinessLine,           --业务条线                        默认 同业(04)
                '' AS AssetType,              --资产大类                        默认 NULL RWA规则计算
                ''  AS AssetSubType,           --资产小类                        默认 NULL RWA规则计算
                t2.businesstype  AS BusinessTypeID         --业务品种代码
                ,DECODE(t2.businesstype,'10201080','福费廷（国际）','10202091','福费廷（国内）','其他')  AS BusinessTypeName       --业务品种名称
                ,'01'  AS CreditRiskDataType     --信用风险数据类型                默认 一般非零售(01)
                ,'01' AS AssetTypeOfHaircuts    --折扣系数对应资产类别            默认 现金及现金等价物(01)
                ,'07' AS BusinessTypeSTD        --权重法业务类型                 默认 一般资产(07)
                ,'' AS ExpoClassSTD           --权重法暴露大类                 默认 011216-其他适用100%风险权重的资产
                ,''  AS ExpoSubClassSTD        --权重法暴露小类                 默认 011216-其他适用100%风险权重的资产
                ,''  AS ExpoClassIRB           --内评法暴露大类                 默认 NULL RWA规则处理
                ,''   AS ExpoSubClassIRB        --内评法暴露小类                 默认 NULL RWA规则处理
                ,'01' AS ExpoBelong             --暴露所属标识                    默认：表内(01)
                ,'01' --账户类别 资产分类 ＝ “交易性金融资产(10)” , 则为02-交易账户；资产分类 ≠ “交易性金融资产”  , 则为01-银行账户
                ,'02' AS ReguTranType           --监管交易类型                    默认 其他资本市场交易(02)
                ,'0' AS RepoTranFlag           --回购交易标识                    默认 否(0)
                ,1 AS RevaFrequency          --重估频率                        默认  1
                ,T2.Businesscurrency AS Currency               --币种
                ,NVL(T2.balance,0)  AS NormalPrincipal        --正常本金余额                    正常本金余额＝成本＋利息调整(initial_cost)＋公允价值变动/公允价值变动损益(int_adjust)＋应计利息(mkt_value_change)
                ,0   AS OverdueBalance         --逾期余额                        默认 0
                ,0  AS NonAccrualBalance      --非应计余额                     默认 0
                ,NVL(T2.balance,0) AS OnSheetBalance         --表内余额                        表内余额=正常本金余额+逾期余额+非应计余额
                ,0  AS NormalInterest         --正常利息                        默认 0
                ,0  AS OnDebitInterest        --表内欠息                        默认 0
                ,0  AS OffDebitInterest       --表外欠息                        默认 0
                ,0  AS ExpenseReceivable      --应收费用                        默认 0
                ,NVL(T2.balance,0) AS AssetBalance           --资产余额                        表内资产总余额=表内余额+应收费用+正常利息+表内欠息
                ,'15010101'  AS AccSubject1            --科目一                         根据原系统的资产分类对照会计科目表确认
                ,'' AS AccSubject2            --科目二                         默认 NULL
                ,'' AS AccSubject3            --科目三                         默认 NULL
                ,T2.Putoutdate  AS StartDate  --起始日期
                ,T4.MATURITY  AS DueDate   --到期日期
                ,CASE WHEN (TO_DATE(T4.MATURITY,'YYYYMMDD') - TO_DATE(T2.Putoutdate,'YYYYMMDD')) / 365<0
                      THEN 0
                      ELSE (TO_DATE(T4.MATURITY,'YYYYMMDD') - TO_DATE(T2.Putoutdate,'YYYYMMDD')) / 365
                 END AS OriginalMaturity       --原始期限                        单位 年
                ,CASE WHEN (TO_DATE(T4.MATURITY,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                      THEN 0
                      ELSE (TO_DATE(T4.MATURITY,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END AS ResidualM      --剩余期限                        单位 年
                ,'01'  AS RiskClassify        --风险分类                        默认 正常(01)
                ,''    AS ExposureStatus         --风险暴露状态                    默认 NULL
                ,0 AS OverdueDays            --逾期天数                        默认 0
                ,0    AS SpecialProvision       --专项准备金                     默认 0 RWA计算 科目12220400，直接提1%的准备金
                ,0  AS GeneralProvision       --一般准备金                     默认 0 RWA计算
                ,0  AS EspecialProvision      --特别准备金                     默认 0 RWA计算
                ,0   AS WrittenOffAmount       --已核销金额                     默认 0
                ,'' AS OffExpoSource          --表外暴露来源                    默认 NULL
                ,'' AS OffBusinessType        --表外业务类型                    默认 NULL
                ,''   AS OffBusinessSdvsSTD     --权重法表外业务类型细分         默认 NULL
                ,''   AS UncondCancelFlag       --是否可随时无条件撤销            默认 NULL
                ,''   AS CCFLevel               --信用转换系数级别                默认 NULL
                ,NULL  AS CCFAIRB                --高级法信用转换系数             默认 NULL
                ,'01'            --债权级别                        债券种类2＝次级债券(20)，则债权级别＝次级债权(02)；否则为高级债权(01)
                ,'0'  AS BondFlag               --是否为债券                     默认 否(0)
                ,'02'   AS BondIssueIntent        --债券发行目的                    默认 其他(02)
                ,'0'  AS NSURealPropertyFlag    --是否非自用不动产                默认 否(0)
                ,'' AS RepAssetTermType       --抵债资产期限类型                默认 NULL
                ,'0' AS DependOnFPOBFlag       --是否依赖于银行未来盈利         默认 否(0)
                ,'' AS IRating                --内部评级
                ,NULL  AS PD                     --违约概率
                ,''  AS LGDLevel               --违约损失率级别                 默认 NULL
                ,NULL  AS LGDAIRB                --高级法违约损失率                默认 NULL
                ,NULL  AS MAIRB                  --高级法有效期限                 默认 NULL
                ,NULL  AS EADAIRB                --高级法违约风险暴露             默认 NULL
                ,'0' AS DefaultFlag            --违约标识
                ,0.45  AS BEEL                   --已违约暴露预期损失比率         债权级别＝‘高级债权’，BEEL ＝ 45%；债权级别＝‘次级债权’，BEEL ＝ 75%
                ,0.45 AS DefaultLGD             --已违约暴露违约损失率            默认 NULL
                ,'0'  AS EquityExpoFlag         --股权暴露标识                    默认 否(0)
                ,''  AS EquityInvestType       --股权投资对象类型                默认 NULL
                ,''   AS EquityInvestCause      --股权投资形成原因                默认 NULL
                ,'0' AS SLFlag                 --专业贷款标识                    默认 否(0)
                ,''  AS SLType                 --专业贷款类型                    默认 NULL
                ,''  AS PFPhase                --项目融资阶段                    默认 NULL
                ,'01'  AS ReguRating             --监管评级                        默认 优(01)
                ,'' AS CBRCMPRatingFlag       --银监会认定评级是否更为审慎       默认 NULL
                ,'' AS LargeFlucFlag          --是否波动性较大                 默认 NULL
                ,'0' AS LiquExpoFlag           --是否清算过程中风险暴露         默认 否(0)
                ,'1'  AS PaymentDealFlag        --是否货款对付模式                默认 是(1)
                ,NULL AS DelayTradingDays       --延迟交易天数                    默认 NULL
                ,'0' AS SecuritiesFlag         --有价证券标识                    默认 否(0)
                ,'' AS SecuIssuerID           --证券发行人ID                   默认 NULL
                ,'' AS RatingDurationType     --评级期限类型                    默认 NULL
                ,''  AS SecuIssueRating        --证券发行等级                    默认 NULL
                ,NULL AS SecuResidualM          --证券剩余期限                    默认 NULL
                ,1 AS SecuRevaFrequency      --证券重估频率                    默认 1
                ,'0' AS CCPTranFlag            --是否中央交易对手相关交易        默认 否(0)
                ,'' AS CCPID                  --中央交易对手ID                  默认 NULL
                ,'' AS QualCCPFlag            --是否合格中央交易对手            默认 NULL
                ,'' AS BankRole               --银行角色                        默认 NULL
                ,'' AS ClearingMethod         --清算方式                        默认 NULL
                ,'' AS BankAssetFlag          --是否银行提交资产                默认 NULL
                ,'' AS MatchConditions        --符合条件情况                    默认 NULL
                ,'0' AS SFTFlag                --证券融资交易标识                默认 否(0)
                ,'0' AS MasterNetAgreeFlag     --净额结算主协议标识             默认 否(0)
                ,'' AS MasterNetAgreeID       --净额结算主协议ID               默认 NULL
                ,'' AS SFTType                --证券融资交易类型                默认 NULL
                ,'' AS SecuOwnerTransFlag     --证券所有权是否转移             默认 NULL
                ,'0' AS OTCFlag                --场外衍生工具标识                默认 否(0)
                ,'' AS ValidNettingFlag       --有效净额结算协议标识            默认 NULL
                ,'' AS ValidNetAgreementID    --有效净额结算协议ID              默认 NULL
                ,'' AS OTCType                --场外衍生工具类型                默认 NULL
                ,'' AS DepositRiskPeriod      --保证金风险期间                 默认 NULL
                ,'' AS MTM                    --重置成本                        默认 NULL
                ,'' AS MTMCurrency            --重置成本币种                    默认 NULL
                ,'' AS BuyerOrSeller          --买方卖方                        默认 NULL
                ,'' AS QualROFlag             --合格参照资产标识                默认 NULL
                ,'' AS ROIssuerPerformFlag    --参照资产发行人是否能履约        默认 NULL
                ,'' AS BuyerInsolvencyFlag    --信用保护买方是否破产            默认 NULL
                ,'' AS NonpaymentFees         --尚未支付费用                    默认 NULL
                ,'0' AS RetailExpoFlag         --零售暴露标识                    默认 否(0)
                ,'' AS RetailClaimType        --零售债权类型                    默认 NULL
                ,'' AS MortgageType           --住房抵押贷款类型                默认 NULL
                ,1 AS ExpoNumber             --风险暴露个数                    默认 1
                ,0.8 AS LTV                    --贷款价值比                     默认 0.8
                ,NULL AS Aging                  --账龄                            默认 NULL
                ,'' AS NewDefaultDebtFlag     --新增违约债项标识                默认 NULL
                ,'' AS PDPoolModelID          --PD分池模型ID                    默认 NULL
                ,'' AS LGDPoolModelID         --LGD分池模型ID                   默认 NULL
                ,'' AS CCFPoolModelID         --CCF分池模型ID                   默认 NULL
                ,'' AS PDPoolID               --所属PD池ID                     默认 NULL
                ,'' AS LGDPoolID              --所属LGD池ID                    默认 NULL
                ,'' AS CCFPoolID              --所属CCF池ID                    默认 NULL
                ,'0' AS ABSUAFlag              --资产证券化基础资产标识         默认 否(0)
                ,'' AS ABSPoolID              --证券化资产池ID                  默认 NULL
                ,'' AS GroupID                --分组编号                        默认 NULL
                ,NULL AS DefaultDate            --违约时点
                ,NULL AS ABSPROPORTION          --资产证券化比重
                ,NULL AS DEBTORNUMBER           --借款人个数
                ,NULL AS sbjt2
                ,NULL AS sbjt_val2
                ,NULL AS sbjt3
                ,NULL AS sbjt_val3
                ,NULL AS sbjt4
                ,NULL AS sbjt_val4
                ,NULL
                ,NULL
         FROM RWA_DEV.NCM_BUSINESS_HISTORY T1
   INNER JOIN RWA_DEV.NCM_BUSINESS_DUEBILL T2
           ON T1.SERIALNO = T2.SERIALNO
          AND T1.DATANO = T2.DATANO
    LEFT JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T4
           ON T2.RELATIVESERIALNO2 = T4.SERIALNO
          AND T2.DATANO=T4.DATANO
    LEFT JOIN RWA.ORG_INFO T3                                           --RWA机构表
           ON T2.MFORGID = T3.ORGID
    LEFT JOIN RWA_DEV.NCM_CUSTOMER_INFO T5
           ON T2.CUSTOMERID = T5.CUSTOMERID
          AND T2.DATANO = T5.DATANO
    LEFT JOIN RWA_TEMP_PDLEVEL T6 --非零售内评客户评级临时表
           ON T6.CUSTID = T2.CUSTOMERID
        WHERE T1.BUSINESSTYPE IN ('10201080', '10202091')
          AND NVL(T2.SUBJECTNO,'1') <> '12220101'
          AND   T1.DATANO = P_DATA_DT_STR
    ;
    COMMIT;
    
   
   /*
   应收款项投资公允价值变动和应收利息分摊
   by chengang 
   */
   --取应收款项投资-公允价值变动和应收利息的总金额
  SELECT /*FGB.SUBJECT_NO,
          FGB.CURRENCY_CODE AS CURRENCY,*/
         CASE  WHEN CL.ATTRIBUTE8 = 'C-D' THEN
             NVL(SUM(FGB.BALANCE_C * NVL(NPQ.MIDDLEPRICE / 100, 1) -
                 FGB.BALANCE_D * NVL(NPQ.MIDDLEPRICE / 100, 1)),0)
            ELSE
             NVL(SUM(FGB.BALANCE_D * NVL(NPQ.MIDDLEPRICE / 100, 1) -
                 FGB.BALANCE_C * NVL(NPQ.MIDDLEPRICE / 100, 1)),0)
          END AS ACCOUNT_BALANCE
          INTO V_BALANCE --余额
     FROM RWA_DEV.FNS_GL_BALANCE FGB
     LEFT JOIN RWA_DEV.TMP_CURRENCY_CHANGE NPQ
       ON NPQ.DATANO = FGB.DATANO
      AND NPQ.CURRENCYCODE = FGB.CURRENCY_CODE
    /* LEFT JOIN RWA_DEV.RWA_ARTICULATION_PARAM RAP
       ON RAP.THIRD_SUBJECT_NO = FGB.SUBJECT_NO*/
     LEFT JOIN RWA.CODE_LIBRARY CL
       ON CL.CODENO = 'NewSubject'
      and cl.itemno = fgb.subject_no
    WHERE FGB.DATANO = P_DATA_DT_STR
      AND FGB.CURRENCY_CODE <> 'RMB'
      AND FGB.SUBJECT_NO IN ('11020201', '11020202') --取应收款项投资-公允价值变动和应收利息的金额
    GROUP BY/* FGB.SUBJECT_NO, CURRENCY_CODE ,*/ CL.ATTRIBUTE8;   
    
    
    --应收款投资余额汇总
    select nvl(sum(t.normalprincipal),1) into S_BALANCE from rwa_tz_exposure t 
    where t.accsubject1='11020101'
    and t.datano=P_DATA_DT_STR;
    
    ---分摊
    update rwa_tz_exposure t set t.normalprincipal=t.normalprincipal+V_BALANCE*(T.normalprincipal/S_BALANCE),
    t.assetbalance=t.assetbalance+V_BALANCE*(T.normalprincipal/S_BALANCE)
    where t.accsubject1='11020101'
    and t.datano=P_DATA_DT_STR;
    commit;
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_TZ_EXPOSURE',cascade => true);


    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_TZ_EXPOSURE;
    --Dbms_output.Put_line('RWA_DEV.RWA_TZ_EXPOSURE表当前插入的财务系统-应收款投资数据记录为: ' || (v_count1 - v_count) || ' 条');



    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '信用风险暴露('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_TZ_EXPOSURE_1224;
/

