CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_TZ_EXPOSURE(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_TZ_EXPOSURE
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
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_TZ_EXPOSURE';
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
    --2.1 财务系统-外币债券投资
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
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            														AS DataDate               --数据日期
                ,p_data_dt_str													     														AS DataNo                 --数据流水号
                --,NVL(T3.SERIALNO,T1.BOND_ID)     				 		 														AS ExposureID             --风险暴露ID
                ,T1.BOND_ID
                ,T1.BOND_ID                             		 														AS DueID              		--债项ID
                ,'TZ'                                        														AS SSysID                 --源系统ID
                ,T1.BOND_ID					                  			 														AS ContractID             --合同ID
                ,CASE WHEN T1.BOND_TYPE1 in('2004','2000','2020')
                THEN 'XN-ZGSYYH'
                 ELSE NVL(T5.BONDPUBLISHID,T14.PARTICIPANT_CODE)
                 END								                         														AS ClientID               --参与主体ID                债券发行人
                ,T1.DEPARTMENT 		                   				 														AS SOrgID                	--源机构ID
                ,T4.ORGNAME                                                            	AS SOrgName               --源机构名称
		            --,T4.SORTNO	                                                           	AS OrgSortNo              --所属机构排序号
                ,NVL(T4.SORTNO,'1010')
                --,T1.DEPARTMENT                           		 														AS OrgID                  --所属机构ID
                ,decode(substr(T1.DEPARTMENT,1,1),'@','01000000',T1.DEPARTMENT)
                ,nvl(T4.ORGNAME,'总行')			                        		 														AS OrgName                --所属机构名称
                ,T1.DEPARTMENT                           		 														AS AccOrgID               --账务机构ID
                ,T4.ORGNAME					 												 														AS AccOrgName             --账务机构名称
                ,CASE WHEN T1.BOND_ID = 'B200801010095' THEN 'J66'																								--毛主席像章默认行业
                		 	WHEN T5.ISCOUNTTR = '1' THEN 'S91'																													--国债时默认发行人行业为S91-国家机构
                 ELSE CASE WHEN NVL(T6.INDUSTRYTYPE,DECODE(SUBSTR(T6.RWACUSTOMERTYPE,1,2),'02','J6620','J66')) = '999999' THEN 'J66'
                 			ELSE NVL(T6.INDUSTRYTYPE,DECODE(SUBSTR(T6.RWACUSTOMERTYPE,1,2),'02','J6620','J66'))
                 			END
                 END						                                                       	AS IndustryID             --所属行业代码
		            ,CASE WHEN T1.BOND_ID = 'B200801010095' THEN '货币金融服务'																				--毛主席像章默认行业
		            			WHEN T5.ISCOUNTTR = '1' THEN '国家机构'																											--国债时默认发行人行业为S91-国家机构
                 ELSE CASE WHEN NVL(T7.ITEMNAME,DECODE(SUBSTR(T6.RWACUSTOMERTYPE,1,2),'02','货币银行服务','货币金融服务')) = '未知' THEN '货币金融服务'
                 			ELSE NVL(T7.ITEMNAME,DECODE(SUBSTR(T6.RWACUSTOMERTYPE,1,2),'02','货币银行服务','货币金融服务'))
                 			END
                 END				                                                           	AS IndustryName           --所属行业名称
                ,'0201'                                      														AS BusinessLine           --业务条线                  			默认 同业(04)
                ,''                                          														AS AssetType              --资产大类                  			默认 NULL RWA规则处理
                ,''                             	 																			AS AssetSubType           --资产小类                  			默认 NULL RWA规则处理
                ,'1040202010'                        	 													AS BusinessTypeID         --业务品种代码
                ,'外币债券投资'      														AS BusinessTypeName       --业务品种名称
                ,'01'                                        														AS CreditRiskDataType     --信用风险数据类型          			默认 一般非零售(01)
                ,'01'                                   		 														AS AssetTypeOfHaircuts    --折扣系数对应资产类别      			默认 现金及现金等价物(01)
                ,'07'                                        														AS BusinessTypeSTD    		--权重法业务类型            			默认 一般资产(07)
                ,CASE WHEN T1.BOND_TYPE1 in('2004','2000','2020')
                      THEN '' --商业银行
                      WHEN T1.BOND_TYPE1='2010'
                      THEN '0103'--地方政府债券
                      WHEN T1.BOND_TYPE1 in('2005','2007','3003','2017')
                      THEN '0106'--企业债
                      WHEN T1.BOND_TYPE1='3002'
                      THEN '0104'--金融机构债
                      WHEN T1.BOND_TYPE1='2001'
                      THEN '0102'--国债
                      WHEN T1.BOND_TYPE1='2003'
                      THEN '0104'--政策性金融债
                      WHEN T1.BOND_TYPE1='2002'
                      THEN '0102'--央行债
                      WHEN T1.BOND_TYPE1 IN('2008','2009','2013','3001')
                      THEN '0102'--股票暂时放其他
                       END ---暴露小类                                       														AS ExpoClassSTD		    		--权重法暴露大类            			默认 NULL RWA规则处理
                ,CASE WHEN T1.BOND_TYPE1 in('2004','2000','2020')
                      THEN '' --商业银行
                      WHEN T1.BOND_TYPE1='2010'
                      THEN '010303'--地方政府债券
                      WHEN T1.BOND_TYPE1 in('2005','2007','3003','2017')
                      THEN '010601'--企业债
                      WHEN T1.BOND_TYPE1='3002'
                      THEN '010408'--金融机构债
                      WHEN T1.BOND_TYPE1='2001'
                      THEN '010201'--国债
                      WHEN T1.BOND_TYPE1='2003'
                      THEN '010401'--政策性金融债
                      WHEN T1.BOND_TYPE1='2002'
                      THEN '010202'--央行债
                      WHEN T1.BOND_TYPE1 IN('2008','2009','2013','3001')
                      THEN '010202'--股票暂时放其他
                       END ---暴露小类
                ,CASE WHEN T1.BOND_ID = 'B200801010095' THEN '0202'																								--金融机构风险暴露
                 ELSE SUBSTR(T13.DITEMNO,1,4)
                 END															                                    	AS ExpoClassIRB           --内评法暴露大类            			默认 NULL RWA规则处理
		            ,CASE WHEN T1.BOND_ID = 'B200801010095' THEN '020202'																							--非银行类金融机构
		             ELSE T13.DITEMNO
		             END											                                             	AS ExpoSubClassIRB        --内评法暴露小类            			默认 NULL RWA规则处理
                ,'01'                                        														AS ExpoBelong          		--暴露所属标识              			默认 表内(01)
                ,CASE WHEN T1.ASSET_CLASS = '10' THEN '02'
                 ELSE '01'
                 END                                        														AS BookType               --账户类别                  			资产类型为交易性金融资产(10)则为交易账户(02)，否则为银行账户(01)
                ,'02'                                        														AS ReguTranType         	--监管交易类型              			默认 其他资本市场(02)
                ,'0'                                         														AS RepoTranFlag           --回购交易标识              			默认 否(0)
                ,1													                 														AS RevaFrequency          --重估频率                  			默认 1
                ,NVL(T1.CURRENCY_CODE,'CNY')								 														AS Currency               --币种
                ,NVL(T2.POSITION_INITIAL_VALUE,0)	                                      AS NormalPrincipal        --正常本金余额
                ,0                                           														AS OverdueBalance         --逾期余额                  			默认 0
                ,0								                           														AS NonAccrualBalance      --非应计余额                			默认 0
                ,NVL(T2.POSITION_INITIAL_VALUE,0)										                  	AS OnSheetBalance         --表内余额
                ,0                                         	 														AS NormalInterest         --正常利息                  			默认 0 利息统一从总账表虚拟
                ,0                                         	 														AS OnDebitInterest        --表内欠息                  			默认 0
                ,0                                         	 														AS OffDebitInterest       --表外欠息                  			默认 0
                ,0								                           														AS ExpenseReceivable      --应收费用                  			默认 0
                ,NVL(T2.POSITION_INITIAL_VALUE,0)																			 	AS AssetBalance       		--资产余额
                ,T2.SBJT_CD															 		 														AS AccSubject1        		--科目一
                ,''																			 		 														AS AccSubject2        		--科目二
                ,''					 																 														AS AccSubject3        		--科目三
                ,T1.origination_date 					 										 														AS StartDate          	 	--起始日期
                ,T1.MATURITY_DATE														 														AS DueDate            		--到期日期
								,CASE WHEN (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(T1.origination_date,'YYYYMMDD')) / 365 < 0 THEN 0
                      ELSE (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(T1.origination_date,'YYYYMMDD')) / 365
                 END																				 														AS OriginalMaturity   		--原始期限
								,CASE WHEN (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0 THEN 0
								      ELSE (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
								 END																				 														AS ResidualM          		--剩余期限
								,CASE WHEN T10.FINALLYRESULT IN ('A1','A2','A3','A4','A') THEN '01'       												--十二级分类转为五级分类
                			WHEN T10.FINALLYRESULT IN ('B1','B2','B3','B') THEN '02'
                      WHEN T10.FINALLYRESULT IN ('C1','C2','C') THEN '03'
                      WHEN T10.FINALLYRESULT IN ('D1','D2','D') THEN '04'
                      WHEN T10.FINALLYRESULT = 'E' THEN '05'
                      ELSE '01'
                 END																                                   AS RiskClassify            --风险分类												默认 正常(01)
								,''                                          														AS ExposureStatus         --风险暴露状态              			默认 NULL
								,0                                           														AS OverdueDays            --逾期天数                  			默认 0
								,0                                           														AS SpecialProvision       --专项准备金                			默认 0	RWA计算
								,0                                           														AS GeneralProvision       --一般准备金                			默认 0	RWA计算
								,0                                           														AS EspecialProvision      --特别准备金                			默认 0	RWA计算
								,0                                          														AS WrittenOffAmount       --已核销金额                			默认 0
								,''                                          														AS OffExpoSource          --表外暴露来源              			默认 NULL
								,''                                          														AS OffBusinessType        --表外业务类型              			默认 NULL
								,''                                          														AS OffBusinessSdvsSTD     --权重法表外业务类型细分    			默认 NULL
								,''                                          														AS UncondCancelFlag       --是否可随时无条件撤销      			默认 NULL
								,''	                                        														AS CCFLevel               --信用转换系数级别          			默认 NULL
								,NULL                                        														AS CCFAIRB                --高级法信用转换系数        			默认 NULL
								,CASE WHEN T1.BOND_TYPE2 = '20' THEN '02'
								 ELSE '01'
								 END                                         														AS ClaimsLevel            --债权级别
								,'1'                                        														AS BondFlag               --是否为债券                			默认 是(1)
								,SUBSTR(NVL(T5.BONDPUBLISHPURPOSE,'0020'),2,2)													AS BondIssueIntent        --债券发行目的              			默认 其他(02)
								,'0'                                         														AS NSURealPropertyFlag    --是否非自用不动产          			默认 否(0)
								,''                                         														AS RepAssetTermType       --抵债资产期限类型          			默认 NULL
								,'0'                                         														AS DependOnFPOBFlag       --是否依赖于银行未来盈利    			默认 否(0)
								,T9.PDADJLEVEL										           														AS IRating                --内部评级
								,T9.PD								                      														AS PD                     --违约概率
								,''	                                        														AS LGDLevel               --违约损失率级别            			默认 NULL
								,NULL                                        														AS LGDAIRB                --高级法违约损失率          			默认 NULL
								,NULL                                        														AS MAIRB                  --高级法有效期限            			默认 NULL
								,NULL                                        														AS EADAIRB                --高级法违约风险暴露        			默认 NULL
								,CASE WHEN T9.PDADJLEVEL = '0116' THEN '1'
								 ELSE '0'
								 END																																		AS DefaultFlag            --违约标识
								,CASE WHEN T1.BOND_TYPE2 = '20' THEN 0.75
								 ELSE 0.45
								 END                                        														AS BEEL                   --已违约暴露预期损失比率
								,CASE WHEN T1.BOND_TYPE2 = '20' THEN 0.75
								 ELSE 0.45
								 END                                        														AS DefaultLGD             --已违约暴露违约损失率      			默认 NULL
								,'0'                                         														AS EquityExpoFlag         --股权暴露标识              			默认 否(0)
								,''                                          														AS EquityInvestType       --股权投资对象类型          			默认 NULL
								,''                                         														AS EquityInvestCause      --股权投资形成原因          			默认 NULL
								,'0'                                         														AS SLFlag                 --专业贷款标识              			默认 否(0)
								,''                                          														AS SLType             		--专业贷款类型              			默认 NULL
								,''	                                        														AS PFPhase                --项目融资阶段              			默认 NULL
								,'01'                                        														AS ReguRating             --监管评级                  			默认 优(01)
								,''                                          														AS CBRCMPRatingFlag       --银监会认定评级是否更为审慎			默认 NULL
								,''                                         														AS LargeFlucFlag          --是否波动性较大            			默认 NULL
								,'0'                                         														AS LiquExpoFlag           --是否清算过程中风险暴露    			默认 否(0)
								,'1'                                        														AS PaymentDealFlag        --是否货款对付模式          			默认 是(1)
								,NULL                                        														AS DelayTradingDays       --延迟交易天数              			默认 NULL
								,'1'				                                 														AS SecuritiesFlag         --有价证券标识              			默认 是(1)
								,CASE WHEN T1.BOND_ID = 'B200801010095' THEN 'MZXXZ'																														--毛主席像章默认参与主体
                			WHEN T3.BUSINESSTYPE = '1040102040' AND (T5.ISCOUNTTR = '1' OR T5.BONDNAME LIKE '%国债%') THEN 'ZGZYZF'														--人民币债券投资国债时默认发行人为中国中央政府
                			WHEN T3.BUSINESSTYPE = '1040202010' AND T5.BONDFLAG04 = '1' AND T5.MARKETSCATEGORY = '01' THEN T5.BONDPUBLISHCOUNTRY || 'ZYZF'		--外币债券投资境外中央政府
                			WHEN T3.BUSINESSTYPE = '1040202010' AND T5.BONDFLAG04 = '1' AND T5.MARKETSCATEGORY = '02' THEN T5.BONDPUBLISHCOUNTRY || 'ZYYH'		--外币债券投资境外中央银行
                			WHEN T3.BUSINESSTYPE = '1040202010' AND T5.BONDFLAG04 = '1' AND T5.MARKETSCATEGORY = '03' THEN T5.BONDPUBLISHCOUNTRY || 'BMST'		--外币债券投资境外国家或地区注册的公共部门实体
                			WHEN REPLACE(T5.BONDPUBLISHID,'NCM_','') IS NULL THEN 'XN-YBGS'
                 ELSE T5.BONDPUBLISHID
                 END							                           														AS SecuIssuerID           --证券发行人ID
								,T5.TIMELIMIT														   															AS RatingDurationType     --评级期限类型
								,CASE WHEN T5.BONDRATING IS NULL THEN ''
                 ELSE RWA_DEV.GETSTANDARDRATING1(T5.BONDRATING)
                 END					                            	 														AS SecuIssueRating        --证券发行等级
								,CASE WHEN (TO_DATE(T5.MATURITYDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0 THEN 0
								      ELSE (TO_DATE(T5.MATURITYDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
								 END                                       	 														AS SecuResidualM          --证券剩余期限
								,1	                                         														AS SecuRevaFrequency      --证券重估频率              			默认 1
								,'0'                                         														AS CCPTranFlag            --是否中央交易对手相关交易  			默认 否(0)
								,''                                          														AS CCPID                  --中央交易对手ID            			默认 NULL
								,''                                          														AS QualCCPFlag            --是否合格中央交易对手      			默认 NULL
								,''                                          														AS BankRole               --银行角色                  			默认 NULL
								,''                                          														AS ClearingMethod         --清算方式                  			默认 NULL
								,''                                          														AS BankAssetFlag          --是否银行提交资产          			默认 NULL
								,''                                         														AS MatchConditions        --符合条件情况              			默认 NULL
								,'0'                                         														AS SFTFlag                --证券融资交易标识          			默认 否(0)
								,'0'                                         														AS MasterNetAgreeFlag     --净额结算主协议标识        			默认 否(0)
								,''                                          														AS MasterNetAgreeID       --净额结算主协议ID          			默认 NULL
								,''                                          														AS SFTType                --证券融资交易类型          			默认 NULL
								,''                                         														AS SecuOwnerTransFlag     --证券所有权是否转移        			默认 NULL
								,'0'                                         														AS OTCFlag                --场外衍生工具标识          			默认 否(0)
								,''                                          														AS ValidNettingFlag       --有效净额结算协议标识      			默认 NULL
								,''                                          														AS ValidNetAgreementID    --有效净额结算协议ID        			默认 NULL
								,''                                          														AS OTCType                --场外衍生工具类型          			默认 NULL
								,''                                          														AS DepositRiskPeriod      --保证金风险期间            			默认 NULL
								,NULL                                        														AS MTM                    --重置成本                  			默认 NULL
								,''                                          														AS MTMCurrency            --重置成本币种              			默认 NULL
								,''                                          														AS BuyerOrSeller          --买方卖方                  			默认 NULL
								,''                                          														AS QualROFlag             --合格参照资产标识          			默认 NULL
								,''                                          														AS ROIssuerPerformFlag    --参照资产发行人是否能履约  			默认 NULL
								,''                                          														AS BuyerInsolvencyFlag    --信用保护买方是否破产      			默认 NULL
								,NULL                                        														AS NonpaymentFees         --尚未支付费用              			默认 NULL
								,'0'                                         														AS RetailExpoFlag         --零售暴露标识              			默认 否(0)
								,''                                          														AS RetailClaimType        --零售债权类型              			默认 NULL
								,''                                          														AS MortgageType           --住房抵押贷款类型          			默认 NULL
								,1                                           														AS ExpoNumber             --风险暴露个数              			默认 1
								,0.8                                         														AS LTV                    --贷款价值比                			默认 0.8
								,NULL                                        														AS Aging                  --账龄                      			默认 NULL
								,''                                          														AS NewDefaultDebtFlag     --新增违约债项标识          			默认 NULL
								,''                                          														AS PDPoolModelID          --PD分池模型ID              			默认 NULL
								,''                                          														AS LGDPoolModelID         --LGD分池模型ID             			默认 NULL
								,''                                          														AS CCFPoolModelID         --CCF分池模型ID             			默认 NULL
								,''                                         														AS PDPoolID               --所属PD池ID                			默认 NULL
								,''                                          														AS LGDPoolID              --所属LGD池ID               			默认 NULL
								,''                                          														AS CCFPoolID              --所属CCF池ID               			默认 NULL
								,'0'                                         														AS ABSUAFlag              --资产证券化基础资产标识    			默认 否(0)
								,''				                                   														AS ABSPoolID              --证券化资产池ID            			默认 NULL
								,''                                          														AS GroupID            		--分组编号                  			默认 NULL
								,CASE WHEN T9.PDADJLEVEL = '0116' THEN TO_DATE(T9.PDVAVLIDDATE,'YYYYMMDD')
								 ELSE NULL
								 END          																													AS DefaultDate            --违约时点                  			默认 NULL
								,NULL																																	  AS ABSPROPORTION					--资产证券化比重
								,NULL																																	  AS DEBTORNUMBER						--借款人个数
                ,T2.INT_ADJ_ITEM
                ,T2.INT_ADJ_VAL
                ,DECODE(SUBSTR(T2.ACCRUAL_GLNO, 1, 4), '1132', NULL,'1101',NULL, T2.ACCRUAL_GLNO)
                ,DECODE(SUBSTR(T2.ACCRUAL_GLNO, 1, 4), '1132', NULL,'1101',NULL, T2.ACCRUAL)
                ,T2.FAIR_EXCH_ITEM
                ,T2.FAIR_EXCH_VAL
                ,NULL
                ,NULL
    FROM				RWA_DEV.FNS_BND_INFO_B T1
    INNER JOIN (
     SELECT T.ACCT_NO,
            T.SECURITY_REFERENCE,
            T.ORG_CD,
            T.SBJT_CD,
            T.BELONG_GROUP,
            SUM(NVL(T.POSITION_INITIAL_VALUE,0)) AS POSITION_INITIAL_VALUE,
            T.INT_ADJ_ITEM,
            SUM(NVL(T.INT_ADJ_VAL,0)) AS INT_ADJ_VAL,
            T.ACCRUAL_GLNO,
            SUM(NVL(T.ACCRUAL,0)) AS ACCRUAL,
            T.FAIR_EXCH_ITEM,
            SUM(NVL(T.FAIR_EXCH_VAL,0)) AS FAIR_EXCH_VAL
       FROM BRD_SECURITY_POSI T
      WHERE T.BELONG_GROUP IN ('1', '2') --取财务系统FNS
      AND T.DATANO=P_DATA_DT_STR
      GROUP BY T.ACCT_NO,
               T.SECURITY_REFERENCE,
               T.ORG_CD,
               T.SBJT_CD,
               T.BELONG_GROUP,
               T.INT_ADJ_ITEM,
               T.ACCRUAL_GLNO,
               T.FAIR_EXCH_ITEM
    ) t2
    on t1.bond_id=t2.SECURITY_REFERENCE
		LEFT JOIN		RWA_DEV.NCM_BUSINESS_DUEBILL T3														--信贷借据表
		ON					'CW_IMPORTDATA' || T1.BOND_ID = T3.THIRDPARTYACCOUNTS
		--AND					T3.BUSINESSTYPE IN ('1040102040','1040202011')						--1040102040-人民币债券投资;1040202011-外币债券投资
		AND					T3.DATANO = p_data_dt_str
    AND         T1.DATANO=T3.DATANO
		LEFT JOIN		RWA.ORG_INFO T4																            --RWA机构表
		ON					T1.DEPARTMENT = T4.ORGID
		LEFT JOIN		RWA_DEV.NCM_BOND_INFO T5																	--信贷债券信息表
	 	ON					T3.RELATIVESERIALNO2 = T5.OBJECTNO
	  AND					T5.OBJECTTYPE = 'BusinessContract'
		AND					T5.DATANO = p_data_dt_str
	  LEFT JOIN NCM_CUSTOMER_INFO T6 --统一客户信息表
    ON					NVL(T5.BONDPUBLISHID,T3.CUSTOMERID) = T6.CUSTOMERID
	  AND					T6.DATANO = p_data_dt_str
    LEFT JOIN		RWA.CODE_LIBRARY	T7																			--RWA码表，获取行业
	  ON					T6.INDUSTRYTYPE = T7.ITEMNO
	  AND					T7.CODENO = 'IndustryType'
	  LEFT JOIN		RWA.CODE_LIBRARY	T8																			--RWA码表，获取业务品种
	  ON					T3.BUSINESSTYPE = T8.ITEMNO
	  AND					T8.CODENO = 'BusinessType'
	  LEFT JOIN		RWA_DEV.RWA_TEMP_PDLEVEL T9																--客户内部评级临时表
	  ON					NVL(T5.BONDPUBLISHID,T3.CUSTOMERID) = T9.CUSTID
	  LEFT JOIN		RWA_DEV.NCM_CLASSIFY_RECORD T10														--十二级分类信息表
	  ON 					T3.RELATIVESERIALNO2 = T10.OBJECTNO
    AND 				T10.OBJECTTYPE = 'TwelveClassify'
    AND 				T10.ISWORK = '1'
    AND 				T10.DATANO = p_data_dt_str
    LEFT JOIN		RWA_DEV.NCM_RWA_RISK_EXPO_RST T11									--内评法暴露分类结果表
	  ON					T3.SERIALNO = T11.OBJECTNO
	  AND					T11.OBJECTTYPE = 'BusinessDuebill'
	  AND					T11.DATANO = p_data_dt_str
	  LEFT JOIN		RWA_DEV.RWA_CD_CODE_MAPPING T13														--代码映射表
	  ON					T11.RISKEXPOSURERESULT = T13.SITEMNO
	  AND					T13.SYSID = 'XD'
	  AND					T13.SCODENO = 'RwaResultType'
	  AND					T13.ISINUSE = '1'
    LEFT JOIN (
    SELECT *
  FROM (SELECT T.DATANO,
               T.BOND_ID,
               T.PARTICIPANT_CODE,
               B.PARTICIPANT_NAME,
               ROW_NUMBER() OVER(PARTITION BY T.DATANO, T.BOND_ID, T.PARTICIPANT_CODE ORDER BY T.SORT_SEQ DESC) AS ROW_ID
          FROM FNS_BND_TRANSACTION_B T
          LEFT JOIN FNS_BND_PARTICIPANT_B B
            ON T.DATANO = B.DATANO
            AND T.DATANO=p_data_dt_str
           AND T.PARTICIPANT_CODE = B.PARTICIPANT_CODE
         WHERE T.PARTICIPANT_CODE IS NOT NULL)
 WHERE ROW_ID = 1) T14
     ON T1.DATANO = T14.DATANO
    AND T1.BOND_ID = T14.BOND_ID
	  WHERE 			T1.BOND_TYPE1 <> '060'
		AND					T1.BOND_ID NOT IN
								(SELECT ZQNM FROM RWA.RWA_WSIB_ABS_ISSUE_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
								 UNION ALL
								 SELECT ZQNM FROM RWA.RWA_WSIB_ABS_INVEST_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
								)
																																		--从配置表排除资产证券化的债券内码
		AND 				T1.DATANO = p_data_dt_str														--债券信息表,获取有效的债券信息
		AND					T1.BOND_CODE IS NOT NULL														--排除无效的债券数据
	  ;

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
    
    --2.4.4 财务系统-应收款投资-货币基金-11010301
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
                ,T1.BOND_ID                                                             AS ExposureID             --风险暴露ID
                ,T1.BOND_ID                                                             AS DueID                  --债项ID
                ,'TZ'                                                                   AS SSysID                 --源系统ID                       默认 投资(TZ)
                ,T1.BOND_ID                                                             AS ContractID             --合同ID
                ,T3.PARTICIPANT_CODE                                                             AS ClientID               --参与主体ID                      默认一个一般公司
                ,T1.DEPARTMENT                                                          AS SOrgID                 --源机构ID
                ,T4.ORGNAME                                                             AS SOrgName               --源机构名称
                --,T4.SORTNO                                                              AS OrgSortNo              --所属机构排序号
                ,NVL(T4.SORTNO,'1010')
                --,T1.DEPARTMENT                                                          AS OrgID                  --所属机构ID
                ,decode(substr(T1.DEPARTMENT,1,1),'@','01000000',T1.DEPARTMENT)
                --,T4.ORGNAME                                                             AS OrgName                --所属机构名称
                ,NVL(T4.ORGNAME,'总行')
                ,T1.DEPARTMENT                                                          AS AccOrgID               --账务机构ID
                ,T4.ORGNAME                                                             AS AccOrgName             --账务机构名称
                ,'J66'                                                                  AS IndustryID             --所属行业代码
                ,'货币金融服务'                                                         AS IndustryName           --所属行业名称
                ,'0401'                                                                 AS BusinessLine           --业务条线                        默认 同业(04)
                ,''                                                                     AS AssetType              --资产大类                        默认 NULL RWA规则计算
                ,''                                                                     AS AssetSubType           --资产小类                        默认 NULL RWA规则计算
                ,CASE WHEN T1.BOND_TYPE1 = '081' THEN '1040105061'
                      WHEN T1.BOND_TYPE1 = '100' THEN '1040105062'
                      ELSE '1040105060'
                 END                                                                    AS BusinessTypeID         --业务品种代码
                ,CASE WHEN T1.BOND_TYPE1 = '081' THEN '应收款项类投资_保本'
                      WHEN T1.BOND_TYPE1 = '100' THEN '应收款项类投资_同业存单'
                      ELSE '应收款项类投资_货币基金'
                 END                                                                    AS BusinessTypeName       --业务品种名称
                ,'01'                                                                   AS CreditRiskDataType     --信用风险数据类型                默认 一般非零售(01)
                ,'01'                                                                   AS AssetTypeOfHaircuts    --折扣系数对应资产类别            默认 现金及现金等价物(01)
                ,'07'                                                                   AS BusinessTypeSTD        --权重法业务类型                 默认 一般资产(07)
                ,'0104'                                                                 AS ExpoClassSTD           --权重法暴露大类                 默认 011216-其他适用100%风险权重的资产
                ,'010408'                                                               AS ExpoSubClassSTD        --权重法暴露小类                 默认 011216-其他适用100%风险权重的资产
                ,''                                                                     AS ExpoClassIRB           --内评法暴露大类                 默认 NULL RWA规则处理
                ,''                                                                     AS ExpoSubClassIRB        --内评法暴露小类                 默认 NULL RWA规则处理
                ,'01'                                                                   AS ExpoBelong             --暴露所属标识                    默认：表内(01)
                ,CASE WHEN T1.ASSET_CLASS = '10' THEN '02'
                      ELSE '01'
                 END                                                                    AS BookType               --账户类别                        资产分类 ＝ “交易性金融资产(10)” , 则为02-交易账户；资产分类 ≠ “交易性金融资产”  , 则为01-银行账户
                ,'02'                                                                   AS ReguTranType           --监管交易类型                    默认 其他资本市场交易(02)
                ,'0'                                                                    AS RepoTranFlag           --回购交易标识                    默认 否(0)
                ,1                                                                      AS RevaFrequency          --重估频率                        默认  1
                ,NVL(T1.CURRENCY_CODE,'CNY')                                            AS Currency               --币种
                ,NVL(T2.INITIAL_COST,0)                                                 AS NormalPrincipal        --正常本金余额                    正常本金余额＝成本＋利息调整(initial_cost)＋公允价值变动/公允价值变动损益(int_adjust)＋应计利息(mkt_value_change)
                ,0                                                                      AS OverdueBalance         --逾期余额                        默认 0
                ,0                                                                      AS NonAccrualBalance      --非应计余额                     默认 0
                ,NVL(T2.INITIAL_COST,0)                                                 AS OnSheetBalance         --表内余额                        表内余额=正常本金余额+逾期余额+非应计余额
                ,0                                                                      AS NormalInterest         --正常利息                        默认 0
                ,0                                                                      AS OnDebitInterest        --表内欠息                        默认 0
                ,0                                                                      AS OffDebitInterest       --表外欠息                        默认 0
                ,0                                                                      AS ExpenseReceivable      --应收费用                        默认 0
                ,NVL(T2.INITIAL_COST,0)                                                 AS AssetBalance           --资产余额                        表内资产总余额=表内余额+应收费用+正常利息+表内欠息
                ,CASE WHEN T1.ASSET_CLASS = '50' AND T1.BOND_TYPE2 = '50' THEN '11010301'
                 END                                                                    AS AccSubject1            --科目一                         根据原系统的资产分类对照会计科目表确认
                ,''                                                                     AS AccSubject2            --科目二                         默认 NULL
                ,''                                                                     AS AccSubject3            --科目三                         默认 NULL
                ,T1.origination_date                                                         AS StartDate              --起始日期
                ,T1.MATURITY_DATE                                                       AS DueDate                --到期日期
                ,CASE WHEN (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(T1.origination_date,'YYYYMMDD')) / 365<0
                      THEN 0
                      ELSE (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(T1.origination_date,'YYYYMMDD')) / 365
                 END                                                                    AS OriginalMaturity       --原始期限                        单位 年
                ,CASE WHEN (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                      THEN 0
                      ELSE (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                                                    AS ResidualM              --剩余期限                        单位 年
                ,'01'                                                                   AS RiskClassify           --风险分类                        默认 正常(01)
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
                ,''                                                                     AS IRating                --内部评级
                ,NULL                                                                   AS PD                     --违约概率
                ,''                                                                     AS LGDLevel               --违约损失率级别                 默认 NULL
                ,NULL                                                                   AS LGDAIRB                --高级法违约损失率                默认 NULL
                ,NULL                                                                   AS MAIRB                  --高级法有效期限                 默认 NULL
                ,NULL                                                                   AS EADAIRB                --高级法违约风险暴露             默认 NULL
                ,'0'                                                                    AS DefaultFlag            --违约标识
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
                ,NULL                                                                   AS DefaultDate            --违约时点
                ,NULL                                                                   AS ABSPROPORTION          --资产证券化比重
                ,NULL                                                                   AS DEBTORNUMBER           --借款人个数
                ,NULL AS sbjt2
                ,NULL AS sbjt_val2
                ,'11010302' AS sbjt3
                ,t2.ACCOUNTABLE_INT AS sbjt_val3
                ,'11010303' AS sbjt4
                ,t2.MKT_VALUE_CHANGE AS sbjt_val4
                ,NULL
                ,NULL
    FROM        RWA_DEV.FNS_BND_INFO_B T1
    INNER JOIN  TEMP_BND_BOOK T2
    ON          T1.BOND_ID = T2.BOND_ID
    LEFT JOIN (
    SELECT *
  FROM (SELECT T.DATANO,
               T.BOND_ID,
               T.PARTICIPANT_CODE,
               B.PARTICIPANT_NAME,
               ROW_NUMBER() OVER(PARTITION BY T.DATANO, T.BOND_ID, T.PARTICIPANT_CODE ORDER BY T.SORT_SEQ DESC) AS ROW_ID
          FROM FNS_BND_TRANSACTION_B T
          LEFT JOIN FNS_BND_PARTICIPANT_B B
            ON T.DATANO = B.DATANO
           AND T.PARTICIPANT_CODE = B.PARTICIPANT_CODE
         WHERE T.PARTICIPANT_CODE IS NOT NULL)
 WHERE ROW_ID = 1) T3
    ON T1.DATANO=T3.DATANO
    AND T1.BOND_ID = T3.BOND_ID
    LEFT JOIN   RWA.ORG_INFO T4                                           --RWA机构表
    ON          T1.DEPARTMENT = T4.ORGID
    WHERE       T1.Bond_Type1 = '3002' AND T1.BOND_TYPE2 = '50'  --通过资产分类来确定债券还是应收款投资。
    AND         T1.DATANO = p_data_dt_str                           --债券信息表,获取有效的债券信息
    AND         T1.BOND_CODE IS NOT NULL                            --排除无效的债券数据
    --AND         T1.MATURITY_DATE >= p_data_dt_str                   --排除到期的债券数据
    --AND NOT EXISTS (SELECT 1 FROM RWA_DEV.NCM_BUSINESS_DUEBILL CBD INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT CBC ON CBD.RELATIVESERIALNO2 = CBC.SERIALNO AND CBD.DATANO = CBC.DATANO WHERE 'CW_IMPORTDATA' || T1.BOND_ID = CBD.THIRDPARTYACCOUNTS AND CBD.DATANO = p_data_dt_str)
    ;

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
    
    -- 资管、信托、融资计划 11020101、12220101
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
                ,T1.BOND_ID                                                             AS ExposureID             --风险暴露ID
                ,T1.BOND_ID                                                             AS DueID                  --债项ID
                ,'TZ'                                                                   AS SSysID                 --源系统ID                       默认 投资(TZ)
                ,T1.BOND_ID                                                             AS ContractID             --合同ID
                , case WHEN T1.BOND_NAME LIKE '%保本%' AND T1.BOND_NAME NOT LIKE'%国开%' 
                  THEN 'XN-ZGSYYH'
                  ELSE NVL(nvl(T3.OBJECTNO,T5.PARTICIPANT_CODE),'XN-YBGS') END          AS ClientID               --参与主体ID                      默认一个一般公司
                ,T1.DEPARTMENT                                                          AS SOrgID                 --源机构ID
                ,T4.ORGNAME                                                             AS SOrgName               --源机构名称
                ,NVL(T4.SORTNO,'1010')
                ,decode(substr(T1.DEPARTMENT,1,1),'@','01000000',T1.DEPARTMENT)
                ,NVL(T4.ORGNAME,'总行')
                ,T1.DEPARTMENT AS AccOrgID --账务机构ID
                ,T4.ORGNAME AS AccOrgName --账务机构名称
                ,NVL(T3.INDUSTRYTYPE,'J6620') AS IndustryID --所属行业代码
                ,'' AS IndustryName           --所属行业名称
                ,'0401' AS BusinessLine           --业务条线                        默认 同业(04)
                ,'' AS AssetType              --资产大类                        默认 NULL RWA规则计算
                ,'' AS AssetSubType           --资产小类                        默认 NULL RWA规则计算
                ,'1040105060' AS BusinessTypeID         --业务品种代码
                ,CASE WHEN T1.BOND_NAME LIKE '%保本%'  THEN '应收款项类投资_保本理财'
                      WHEN T1.BOND_NAME NOT LIKE '%保本%' AND T1.BOND_NAME LIKE '%理财%' THEN '应收款项类投资_非保本理财'
                      WHEN T1.BOND_NAME LIKE '%融资%' THEN '应收款项类投资_融资计划'
                      WHEN T1.BOND_NAME LIKE '%资%管%' THEN '应收款项类投资_资管计划'
                      WHEN T1.BOND_NAME LIKE '%信%托%' THEN '应收款项类投资_信托计划'
                      WHEN T1.BOND_TYPE1 ='2008' THEN'毛主席像章'
                      ELSE '应收款项类投资_资管信托'
                 END AS BusinessTypeName       --业务品种名称
                ,'01' AS CreditRiskDataType     --信用风险数据类型                默认 一般非零售(01)
                ,'01' AS AssetTypeOfHaircuts    --折扣系数对应资产类别            默认 现金及现金等价物(01)
                ,'07' AS BusinessTypeSTD        --权重法业务类型                 默认 一般资产(07)
                ,CASE WHEN T1.BOND_NAME LIKE '%保本%' AND T1.BOND_NAME LIKE'%国开%' THEN '0104' ---政策性银行 保本理财
                      WHEN T1.BOND_NAME LIKE '%保本%' AND T1.BOND_NAME NOT LIKE'%国开%' THEN '0104' ---商业银行 保本理财
                      WHEN T1.BOND_NAME NOT LIKE '%保本%' AND T1.BOND_NAME LIKE '%理财%' THEN '0112' -- 非保本理财
                      WHEN T1.BOND_NAME LIKE '%融资%' THEN '0106'
                      WHEN T1.BOND_NAME LIKE '%资%管%' THEN '0104'
                      WHEN T1.BOND_NAME LIKE '%信%托%' THEN '0104'
                      WHEN T1.BOND_TYPE1='2008'  THEN'0112'
                      ELSE '0104'
                        END  AS ExpoClassSTD           --权重法暴露大类                 默认 011216-其他适用100%风险权重的资产
                ,CASE WHEN T1.BOND_NAME LIKE '%保本%' AND T1.BOND_NAME LIKE'%国开%' THEN '010401' ---政策性银行 保本理财
                      WHEN T1.BOND_NAME LIKE '%保本%' AND T1.BOND_NAME NOT LIKE'%国开%' THEN '010406' ---商业银行 保本理财
                      WHEN T1.BOND_NAME NOT LIKE '%保本%' AND T1.BOND_NAME LIKE '%理财%' THEN '011216' -- 非保本理财
                      WHEN T1.BOND_NAME LIKE '%融资%' THEN '010601'
                      WHEN T1.BOND_NAME LIKE '%资%管%' THEN '010408'
                      WHEN T1.BOND_NAME LIKE '%信%托%' THEN '010408'
                      WHEN T1.BOND_TYPE1='2008'  THEN'011216'
                      ELSE '010408'
                        END    AS ExpoSubClassSTD        --权重法暴露小类                 默认 011216-其他适用100%风险权重的资产
                ,'' AS ExpoClassIRB           --内评法暴露大类                 默认 NULL RWA规则处理
                ,'' AS ExpoSubClassIRB        --内评法暴露小类                 默认 NULL RWA规则处理
                ,'01' AS ExpoBelong             --暴露所属标识                    默认：表内(01)
                ,CASE WHEN T1.ASSET_CLASS = '10' THEN '02'
                      ELSE '01'
                 END AS BookType               --账户类别                        资产分类 ＝ “交易性金融资产(10)” , 则为02-交易账户；资产分类 ≠ “交易性金融资产”  , 则为01-银行账户
                ,'02' AS ReguTranType           --监管交易类型                    默认 其他资本市场交易(02)
                ,'0' AS RepoTranFlag           --回购交易标识                    默认 否(0)
                ,1 AS RevaFrequency          --重估频率                        默认  1
                ,NVL(T1.CURRENCY_CODE,'CNY') AS Currency               --币种
                ,NVL(T2.INITIAL_COST,0) AS NormalPrincipal        --正常本金余额                    正常本金余额＝成本＋利息调整(initial_cost)＋公允价值变动/公允价值变动损益(int_adjust)＋应计利息(mkt_value_change)
                ,0 AS OverdueBalance         --逾期余额                        默认 0
                ,0 AS NonAccrualBalance      --非应计余额                     默认 0
                ,NVL(T2.INITIAL_COST,0)                                                 AS OnSheetBalance         --表内余额                        表内余额=正常本金余额+逾期余额+非应计余额
                ,0                                                                      AS NormalInterest         --正常利息                        默认 0
                ,0                                                                      AS OnDebitInterest        --表内欠息                        默认 0
                ,0                                                                      AS OffDebitInterest       --表外欠息                        默认 0
                ,0                                                                      AS ExpenseReceivable      --应收费用                        默认 0
                ,NVL(T2.INITIAL_COST,0)                                                 AS AssetBalance           --资产余额                        表内资产总余额=表内余额+应收费用+正常利息+表内欠息
                ,NVL(B.Sbjt_Cd,'12220101')                                                                    AS AccSubject1            --科目一                         根据原系统的资产分类对照会计科目表确认
                ,''                                                                     AS AccSubject2            --科目二                         默认 NULL
                ,''                                                                     AS AccSubject3            --科目三                         默认 NULL
                ,T1.origination_date                                                         AS StartDate              --起始日期
                ,T1.MATURITY_DATE                                                       AS DueDate                --到期日期
                ,CASE WHEN (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(T1.origination_date,'YYYYMMDD')) / 365<0
                      THEN 0
                      ELSE (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(T1.origination_date,'YYYYMMDD')) / 365
                 END                                                                    AS OriginalMaturity       --原始期限                        单位 年
                ,CASE WHEN (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                      THEN 0
                      ELSE (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                                                    AS ResidualM              --剩余期限                        单位 年
                ,'01'                                                                   AS RiskClassify           --风险分类                        默认 正常(01)
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
                ,T6.Pdadjlevel                                                          AS IRating                --内部评级
                ,T6.Pd                                                                  AS PD                     --违约概率
                ,''                                                                     AS LGDLevel               --违约损失率级别                 默认 NULL
                ,NULL                                                                   AS LGDAIRB                --高级法违约损失率                默认 NULL
                ,NULL                                                                   AS MAIRB                  --高级法有效期限                 默认 NULL
                ,NULL                                                                   AS EADAIRB                --高级法违约风险暴露             默认 NULL
                ,'0'                                                                    AS DefaultFlag            --违约标识
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
                ,''                                                                     AS GROUPID                --分组编号                        默认 NULL
                ,NULL                                                                   AS DEFAULTDATE            --违约时点
                ,NULL                                                                   AS ABSPROPORTION          --资产证券化比重
                ,NULL                                                                   AS DEBTORNUMBER           --借款人个数
                ,NULL AS SBJT2
                ,T2.INT_ADJUST AS SBJT_VAL2
                ,NULL AS SBJT3
                ,T2.ACCOUNTABLE_INT AS SBJT_VAL3
                ,NULL AS SBJT4
                ,T2.MKT_VALUE_CHANGE AS SBJT_VAL4
                ,NULL
                ,NULL
    FROM        RWA_DEV.FNS_BND_INFO_B T1
    INNER JOIN  TEMP_BND_BOOK T2
    ON          T1.BOND_ID = T2.BOND_ID
    LEFT JOIN BRD_UN_BOND B
    ON T1.BOND_ID=B.ACCT_NO
    AND T1.DATANO=B.DATANO
    AND B.BELONG_GROUP = '1'
    LEFT JOIN (
         SELECT *
                FROM (SELECT T1.THIRDPARTYACCOUNTS,
                             T2.OBJECTNO,--客户号
                             T3.CUSTOMERNAME,
                             T3.INDUSTRYTYPE,
                             ROW_NUMBER() OVER(PARTITION BY T1.THIRDPARTYACCOUNTS ORDER BY NVL(LENGTH(T3.CUSTOMERNAME), 0) DESC) AS ROW_ID
                        FROM NCM_BUSINESS_DUEBILL T1 --借据表
                  INNER JOIN NCM_CONTRACT_RELATIVE T2 --合同关联表
                          ON T1.RELATIVESERIALNO2 = T2.SERIALNO
                         AND T1.DATANO = T2.DATANO
                  INNER JOIN NCM_CUSTOMER_INFO T3 --客户信息表
                          ON T2.OBJECTNO = T3.CUSTOMERID
                         AND T2.DATANO = T3.DATANO
                       WHERE T1.THIRDPARTYACCOUNTS IS NOT NULL
                         AND T1.DATANO = p_data_dt_str) T
                       WHERE ROW_ID = 1) t3
           ON 'CW_IMPORTDATA'||t1.bond_id=t3.THIRDPARTYACCOUNTS
    LEFT JOIN   RWA.ORG_INFO T4                                           --RWA机构表
           ON   T1.DEPARTMENT = T4.ORGID
    LEFT JOIN (
    SELECT *
  FROM (SELECT T.DATANO,
               T.BOND_ID,
               T.PARTICIPANT_CODE,
               B.PARTICIPANT_NAME,
               ROW_NUMBER() OVER(PARTITION BY T.DATANO, T.BOND_ID, T.PARTICIPANT_CODE ORDER BY T.SORT_SEQ DESC) AS ROW_ID
          FROM FNS_BND_TRANSACTION_B T
          LEFT JOIN FNS_BND_PARTICIPANT_B B
            ON T.DATANO = B.DATANO
           AND T.PARTICIPANT_CODE = B.PARTICIPANT_CODE
         WHERE T.PARTICIPANT_CODE IS NOT NULL)
 WHERE ROW_ID = 1) T5
       ON T1.DATANO = T5.DATANO
       AND T1.BOND_ID = T5. BOND_ID
    LEFT JOIN RWA_TEMP_PDLEVEL T6 --非零售内评客户评级临时表
           ON T6.CUSTID = NVL(T3.OBJECTNO,T5.PARTICIPANT_CODE)
        WHERE   T1.BOND_TYPE1 IN ('2018','2019','2008')  --筛选融资计划  by wzb  2008 境内上市股票
          AND   t1.par_value <> 0
          AND   T1.DATANO = p_data_dt_str                           --债券信息表,获取有效的债券信息
          AND   T1.CLOSED <> '1' --排除已关闭的
         -- AND   T1.BOND_ID NOT in('B201803296435','B201712285095')
    ;     -- 排除ABS产品 BY WZB

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
END PRO_RWA_TZ_EXPOSURE;
/

