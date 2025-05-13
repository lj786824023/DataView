CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_CD_DECREASE_TYPE(
														p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
                                                       )
	/*
    存储过程名称:RWA_DEV.PRO_RWA_CD_DECREASE_TYPE
    实现功能:实现(UPDATE)各相关业务的减值准备计提和分摊
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :YUSJ
    编写时间:2016-11-29
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.RWA_EI_EXPOSURE|信用风险暴露表
    源  表2 :RWA_DEV.RWA_CD_DECREASE_TYPE|准备金计提参数配置表
    源	 表3 :RWA_DEV.RWA_TEMP_EXPOSURE_ZBJ|信用风险暴露明细与总账准备金科目对照表
    源  表4 :RWA_DEV.RWA_TEMP_ZBJ|总账准备金科目差额分摊对照表
    目标表1 :RWA_DEV.RWA_EI_EXPOSURE|信用风险暴露表
    变更记录(修改人|修改时间|修改内容):
    备   注 :准备金计提方案
    				 2.计提条件根据五级分类来计提
      				 五级分类为：1 正常、2 关注、3 次级 、4 可疑、5 损失
    				 3.五级分类的判别条件
      				 3.1.档级别为1时，将所有的应计提的科目作为条件，统计所有记录总余额，再分别将相关记录的余额平方除以计算出来的总余额（注：此项计为“一般准备金”）。
      				 3.2.如果级别2、3、4、5则直接通过正常余额分别乘以对应的相关系数2%、25%、50%、100%（注：此项计为“专项准备金”）
    */
	AS
	--创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

	/*变量定义*/
  --定义当前更新的数据数
  v_count1 				INTEGER;
  --定义更新的sql语句
  v_update_sql 		VARCHAR2(2000);
  --资产分类
  ASSET_TYPE   		VARCHAR2(500);
  --风险分类
  RISK_TYPE    		VARCHAR2(100);
  --科目号
  SUBJECT_NO   		VARCHAR2(100);
  --业务品种
  BUSINESS_TYPE 	VARCHAR2(100);
  --行业类型
  INDUSTRY_TYPE 	VARCHAR2(100);
  --计提比例
  DECREASE_COUNT 	VARCHAR2(100);
  --定义异常变量
  v_raise EXCEPTION;

  BEGIN
  	--1 准备金抽取
  	--1.1 更新信贷、转贴现部分的准备金，仅更新准备金大于0的部分
  	MERGE INTO (SELECT EXPOSUREID, DUEID, ASSETBALANCE, GENERALPROVISION
		              FROM RWA_DEV.RWA_EI_EXPOSURE
		             WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		               AND SSYSID IN ('XD', 'PJ')) T
		USING (SELECT A1.ACCT_NO     AS ACCT_NO,
		              A1.JEBALANCE   AS JEBALANCE,
		              B.ASSETBALANCE AS ASSETBALANCE
		         FROM (SELECT CASE
		                        WHEN A.ACCT_NO LIKE '%T%' THEN
		                         SUBSTR(A.ACCT_NO, 1, INSTR(A.ACCT_NO, 'T') - 1)
		                        ELSE
		                         A.ACCT_NO
		                      END AS ACCT_NO, --部分账号在借据号的基础上追加了“T2601”类似的字符串
		                      CASE
		                        WHEN SUM(NVL(A.ACCOUNT_CR, 0) - NVL(A.ACCOUNT_DR, 0)) < 0 THEN
		                         0
		                        ELSE
		                         SUM(NVL(A.ACCOUNT_CR, 0) - NVL(A.ACCOUNT_DR, 0))
		                      END AS JEBALANCE
		                 FROM RWA_DEV.FNS_XXT_LOAN_JE_B A
		                WHERE A.DATANO = '20170630'
		                  AND A.ACCOUNT_ACCT IN ('13040101', '13040102')
		                  AND A.ACCT_NO <> 'DTB13070800001' --直销银行
		                  AND A.ACCT_NO NOT LIKE 'CQCBCEDACCT1%' --信用卡
		                  AND A.ACCT_NO NOT LIKE 'KID%' --快I贷
		                  AND A.DATA_DATE <= '20170630'
		                GROUP BY CASE
		                           WHEN A.ACCT_NO LIKE '%T%' THEN
		                            SUBSTR(A.ACCT_NO, 1, INSTR(A.ACCT_NO, 'T') - 1)
		                           ELSE
		                            A.ACCT_NO
		                         END
		               HAVING SUM(NVL(A.ACCOUNT_CR, 0) - NVL(A.ACCOUNT_DR, 0)) > 0) A1
		        INNER JOIN (SELECT DUEID, SUM(ASSETBALANCE) AS ASSETBALANCE							--为防止从核心供过来的逾期数据冲突，暴露号=YQ+借据号，债项ID=借据号。顾按照债项ID汇总资产余额按资产余额占比分配正常和逾期暴露的准备金
		                     FROM RWA_DEV.RWA_EI_EXPOSURE
		                    where DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		                      AND SSYSID IN ('XD', 'PJ')
		                    group by DUEID) B
		           ON B.DUEID = A1.ACCT_NO) T1
		ON (T.DUEID = T1.ACCT_NO)
		WHEN MATCHED THEN
		  UPDATE
		     SET T.GENERALPROVISION = T1.JEBALANCE * T.ASSETBALANCE / T1.ASSETBALANCE
		;

		COMMIT;

		--1.2 更新信贷表外业务使用合同号作为暴露ID的数据的准备金(保函、信用证、垫款)，直接覆盖
		/*
		MERGE INTO (SELECT EXPOSUREID
											,GENERALPROVISION
									FROM RWA_DEV.RWA_EI_EXPOSURE
								 WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		     					 AND SSYSID = 'XD'
		     					 AND BUSINESSTYPEID IN ('105010', '105150', '105120', '102020', '102050', '106')) T
		USING (SELECT D.RELATIVESERIALNO2 AS ACCT_NO,
		              SUM(NVL(A.ACCOUNT_CR, 0) - NVL(A.ACCOUNT_DR, 0)) AS JEBALANCE
		         FROM RWA_DEV.FNS_XXT_LOAN_JE_B A
		        INNER JOIN RWA_DEV.CMS_BUSINESS_DUEBILL D
		           ON A.ACCT_NO = D.SERIALNO
		          AND D.DATANO = p_data_dt_str
		        WHERE A.DATANO = p_data_dt_str
		          AND A.ACCOUNT_ACCT IN ('13040101', '13040102')
		          AND A.DATA_DATE <= p_data_dt_str
		        GROUP BY D.RELATIVESERIALNO2
		       HAVING SUM(NVL(A.ACCOUNT_CR, 0) - NVL(A.ACCOUNT_DR, 0)) > 0) T1
		ON (T.EXPOSUREID = T1.ACCT_NO)
		WHEN MATCHED THEN
		  UPDATE
		     SET T.GENERALPROVISION = T1.JEBALANCE
		;

		COMMIT;
		*/

		--1.3 更新直销银行垫款准备金，仅更新准备金大于0的部分
		UPDATE RWA_DEV.RWA_EI_EXPOSURE T
		   SET T.GENERALPROVISION =
		       (SELECT CASE WHEN SUM(NVL(A.ACCOUNT_CR, 0) - NVL(A.ACCOUNT_DR, 0)) < 0 THEN 0
		       				 ELSE SUM(NVL(A.ACCOUNT_CR, 0) - NVL(A.ACCOUNT_DR, 0))
		       				 END
		          FROM RWA_DEV.FNS_XXT_LOAN_JE_B A
		         WHERE A.DATANO = p_data_dt_str
		           AND A.ACCOUNT_ACCT IN ('13040101', '13040102')
		           AND A.DATA_DATE <= p_data_dt_str
		           AND A.ACCT_NO = 'DTB13070800001')
		 WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		   AND T.SSYSID = 'ZX'
		;

		COMMIT;

		--1.4 更新信用卡准备金(不包含未使用额度)，仅更新准备金大于0的部分
		MERGE INTO (SELECT RISKCLASSIFY
											,NORMALPRINCIPAL
											,GENERALPROVISION
									FROM RWA_DEV.RWA_EI_EXPOSURE
								 WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		     					 AND SSYSID = 'XYK'
		     					 AND BUSINESSTYPEID = '11106010') T
		USING (SELECT T2.RISKCLASSIFY    AS RISKCLASSIFY,
		              T2.NORMALPRINCIPAL AS NORMALPRINCIPAL,
		              CASE WHEN T1.JEBALANCE < 0 THEN 0
		              ELSE T1.JEBALANCE
		              END       				 AS JEBALANCE
		         FROM (SELECT A.ACCT_NO AS ACCT_NO,
		                      SUM(NVL(A.ACCOUNT_CR, 0) - NVL(A.ACCOUNT_DR, 0)) AS JEBALANCE
		                 FROM RWA_DEV.FNS_XXT_LOAN_JE_B A
		                WHERE A.DATANO = p_data_dt_str
		                  AND A.ACCOUNT_ACCT IN ('13040101', '13040102')
		                  AND A.DATA_DATE <= p_data_dt_str
		                  AND A.ACCT_NO LIKE 'CQCBCEDACCT1%'
		                GROUP BY A.ACCT_NO) T1
		        INNER JOIN (SELECT 'CQCBCEDACCT1' || RISKCLASSIFY AS EXPOSUREID,
		                          RISKCLASSIFY,
		                          SUM(NORMALPRINCIPAL) AS NORMALPRINCIPAL
		                     FROM RWA_DEV.RWA_EI_EXPOSURE
		                    WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		                      AND SSYSID = 'XYK'
		                      AND BUSINESSTYPEID = '11106010'
		                    GROUP BY RISKCLASSIFY) T2
		           ON T1.ACCT_NO = T2.EXPOSUREID) T3
		ON (T.RISKCLASSIFY = T3.RISKCLASSIFY)
		WHEN MATCHED THEN
		  UPDATE
		     SET T.GENERALPROVISION = T3.JEBALANCE * T.NORMALPRINCIPAL / T3.NORMALPRINCIPAL
		;

		COMMIT;

		--1.5 信贷快I贷准备金，仅更新准备金大于0的部分
		UPDATE RWA_DEV.RWA_EI_EXPOSURE T
		   SET T.GENERALPROVISION = T.NORMALPRINCIPAL *
		                            (SELECT CASE WHEN T1.JEBALANCE < 0 THEN 0 ELSE T1.JEBALANCE END / T2.NORMALPRINCIPAL
		                               FROM (SELECT SUM(NVL(A.ACCOUNT_CR, 0) -
		                                                NVL(A.ACCOUNT_DR, 0)) AS JEBALANCE
		                                       FROM RWA_DEV.FNS_XXT_LOAN_JE_B A
		                                      WHERE A.DATANO = p_data_dt_str
		                                        AND A.ACCOUNT_ACCT IN
		                                            ('13040101', '13040102')
		                                        AND A.DATA_DATE <= p_data_dt_str
		                                        AND A.ACCT_NO LIKE 'KID%') T1,
		                                    (SELECT SUM(NORMALPRINCIPAL) AS NORMALPRINCIPAL
		                                       FROM RWA_DEV.RWA_EI_EXPOSURE
		                                      WHERE DATADATE =
		                                            TO_DATE(p_data_dt_str, 'YYYYMMDD')
		                                        AND SSYSID = 'XD'
		                                        AND BUSINESSTYPEID = '11103036') T2)
		 WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		   AND T.SSYSID = 'XD'
		   AND T.BUSINESSTYPEID = '11103036'
		;

		COMMIT;

		--更新信贷类资产表内部分准备金大于资产余额的记录，其准备金等于资产余额
		UPDATE RWA_DEV.RWA_EI_EXPOSURE T
		   SET T.GENERALPROVISION = T.ASSETBALANCE
		 WHERE T.GENERALPROVISION > T.ASSETBALANCE
		 	 AND T.SSYSID IN ('XD', 'XYK', 'PJ', 'ZX')
			 AND T.EXPOBELONG = '01'
		;

		COMMIT;

	  --2.虚拟准备金暴露
	  --删除目标表当期数据
    DELETE FROM RWA_DEV.RWA_EI_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD') AND SSYSID IN ('ZBJ');
    DELETE FROM RWA_DEV.RWA_EI_CLIENT WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD') AND SSYSID IN ('ZBJ');
    DELETE FROM RWA_DEV.RWA_EI_CONTRACT WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD') AND SSYSID IN ('ZBJ');
    COMMIT;

	  --2.1 虚拟暴露应收款投资对应的准备金差值，默认12312000科目
    INSERT INTO RWA_DEV.RWA_EI_EXPOSURE(
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
    )
    WITH TMP_FNS_JE AS (
    						SELECT ABS(SUM(T1.BALANCE_D - T1.BALANCE_C)) AS FNS_BALANCE
								  FROM FNS_GL_BALANCE T1
								 WHERE T1.DATANO = p_data_dt_str
								   AND T1.CURRENCY_CODE = 'RMB'
								   AND T1.SUBJECT_NO LIKE '1231%'
    )
    , TMP_EI_JE AS (
    					SELECT SUM(T.GENERALPROVISION) AS EI_BALANCE
							  FROM RWA_EI_EXPOSURE T
							 WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
							   AND T.SSYSID IN ('TZ')
    )
    , TMP_JE AS (
    					SELECT A.FNS_BALANCE - B.EI_BALANCE AS ZBJ_BALANCE
    						FROM TMP_FNS_JE A, TMP_EI_JE B
    )
    SELECT
                 TO_DATE(p_data_dt_str,'YYYYMMDD')                                     AS DataDate                --数据日期
		            ,p_data_dt_str                                                         AS DataNo                  --数据流水号
		            ,'XN-ZBJ-12312000-10000000-CNY'                                        AS ExposureID              --风险暴露ID
		            ,'XN-ZBJ-12312000-10000000-CNY'                                        AS DueID                   --债项ID
		            ,'ZBJ'                                                                 AS SSysID                  --源系统ID
		            ,'XN-ZBJ-12312000-10000000-CNY'                                        AS ContractID              --合同ID
		            ,'XN-ZBJ-10000000-CNY'				                                         AS ClientID                --参与主体ID
		            ,'9998'	                                                           AS SOrgID                  --源机构ID
		            ,'重庆银行股份有限公司'                                                            AS SOrgName                --源机构名称
		            ,'1010'				                                                           AS OrgSortNo               --所属机构排序号
		            ,'9998'	                                                           AS OrgID                   --所属机构ID
		            ,'重庆银行股份有限公司'	                                                           AS OrgName                 --所属机构名称
		            ,'9998'	                                                           AS AccOrgID                --账务机构ID
		            ,'重庆银行股份有限公司'			                                                       AS AccOrgName              --账务机构名称
		            ,'J6620'										                                           AS IndustryID              --所属行业代码              			默认 货币银行服务(J6620)
		            ,'货币银行服务'									                                       AS IndustryName            --所属行业名称              			默认 货币银行服务
		            ,'0501'	                                                               AS BusinessLine            --业务条线                  			默认 总行(0501)
		            ,'132'	                                                               AS AssetType               --资产大类                  			默认 其他资产(132)
		            ,'13205'                                                               AS AssetSubType            --资产小类                  			默认 其他表内资产(13205)
		            ,'9010101010'		                                                       AS BusinessTypeID          --业务品种代码              			默认 虚拟业务品种(9010101010)
		            ,'虚拟业务品种'                                                        AS BusinessTypeName        --业务品种名称              			默认 虚拟业务品种(9010101010)
		            ,'01'                                                                  AS CreditRiskDataType      --信用风险数据类型          			默认 一般非零售(01)
		            ,'01'                                                                  AS AssetTypeOfHaircuts     --折扣系数对应资产类别      			默认 现金及现金等价物(01)
		            ,'99'	                                                                 AS BusinessTypeSTD         --权重法业务类型            			默认 其他资产(99)
		            ,'0112'                                                                AS ExpoClassSTD            --权重法暴露大类            			默认 其他(0112)
		            ,'011216'                                                              AS ExpoSubClassSTD         --权重法暴露小类            			默认 其他适用100%风险权重的资产(011216)
		            ,'0203'																                                 AS ExpoClassIRB            --内评法暴露大类            			默认 公司风险暴露(0203)
		            ,'020301'												                                       AS ExpoSubClassIRB         --内评法暴露小类            			默认 一般公司(020301)
		            ,'01'                                                                  AS ExpoBelong              --暴露所属标识              			默认 表内(01)
		            ,'01'                                                                  AS BookType                --账户类别                  			默认 银行账户(01)
                ,'02'                                                                  AS ReguTranType            --监管交易类型              			默认 其他资本市场(02)
                ,'0'                                                                   AS RepoTranFlag            --回购交易标识              			默认 否(0)
                ,1								                                                     AS RevaFrequency           --重估频率                  			默认 1
                ,'CNY'										                                             AS Currency                --币种
                ,0									                                                   AS NormalPrincipal         --正常本金余额              			默认 0
                ,0                                                                     AS OverdueBalance          --逾期余额                  			默认 0
                ,0		            								                                     AS NonAccrualBalance       --非应计余额                			默认 0
                ,0									                                                   AS OnSheetBalance          --表内余额                  			默认 0
                ,0					                                                           AS NormalInterest          --正常利息                  			默认 0	利息统一从总账表虚拟
                ,0                                                                     AS OnDebitInterest         --表内欠息                  			默认 0
                ,0                                                                     AS OffDebitInterest        --表外欠息                  			默认 0
                ,0																					                           AS ExpenseReceivable       --应收费用                  			默认 0
                ,0									                                                   AS AssetBalance            --资产余额                  			默认 0
                ,'12312000'	                                                           AS AccSubject1             --科目一
                ,NULL                                                                  AS AccSubject2             --科目二
                ,NULL		                                                               AS AccSubject3             --科目三
                ,TO_CHAR(TO_DATE(p_data_dt_str,'YYYYMMDD'),'YYYY-MM-DD')               AS StartDate               --起始日期
                ,TO_CHAR(ADD_MONTHS(TO_DATE(p_data_dt_str,'YYYYMMDD'),1),'YYYY-MM-DD')
                																                                       AS DueDate             		--到期日期
                ,(ADD_MONTHS(TO_DATE(p_data_dt_str,'YYYYMMDD'),1) - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                																													             AS OriginalMaturity    		--原始期限
                ,(ADD_MONTHS(TO_DATE(p_data_dt_str,'YYYYMMDD'),1) - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                		                                                                   AS ResidualM               --剩余期限
                ,'01'																                                   AS RiskClassify            --风险分类												默认 正常(01)
                ,'01'                                                                  AS ExposureStatus          --风险暴露状态              			默认 正常(01)
                ,0                                                                     AS OverdueDays             --逾期天数                  			默认 0
                ,0						                                                         AS SpecialProvision        --专项准备金                			默认 0	RWA计算
                ,T1.ZBJ_BALANCE                                                        AS GeneralProvision        --一般准备金                			默认 0	RWA计算
                ,0                                                                     AS EspecialProvision       --特别准备金                			默认 0	RWA计算
                ,0	                                                                   AS WrittenOffAmount        --已核销金额                			默认 0
                ,''                                                                    AS OffExpoSource           --表外暴露来源              			默认 NULL
                ,''                                                                    AS OffBusinessType         --表外业务类型              			默认 NULL
                ,''	                                                                   AS OffBusinessSdvsSTD      --权重法表外业务类型细分    			默认 NULL
                ,'0'                                                                   AS UncondCancelFlag        --是否可随时无条件撤销      			默认 否(0)
                ,''                                                                    AS CCFLevel                --信用转换系数级别          			默认 NULL
                ,''		                                                                 AS CCFAIRB                 --高级法信用转换系数        			默认 NULL
                ,'01'                                                                  AS ClaimsLevel             --债权级别                  			默认 高级债权(01)
                ,'0'	                                                                 AS BondFlag                --是否为债券                			默认 否(0)
                ,'02'                                                                  AS BondIssueIntent         --债券发行目的              			默认 其他(02)
                ,'0'	                                                                 AS NSURealPropertyFlag     --是否非自用不动产          			默认 否(0)
                ,'01'                                                                  AS RepAssetTermType        --抵债资产期限类型          			默认 法律规定处分期限内(01)
                ,'0'                                                                   AS DependOnFPOBFlag        --是否依赖于银行未来盈利    			默认 否(0)
                ,''															                                       AS IRating                 --内部评级                  			默认 NULL
                ,NULL									                                                 AS PD                      --违约概率                  			默认 NULL
                ,NULL                                                                  AS LGDLevel                --违约损失率级别            			默认 NULL
                ,NULL                                                                  AS LGDAIRB                 --高级法违约损失率          			默认 NULL
                ,NULL                                                                  AS MAIRB                   --高级法有效期限            			默认 NULL
                ,NULL                                                                  AS EADAIRB                 --高级法违约风险暴露        			默认 NULL
                ,'0'																								                   AS DefaultFlag             --违约标识                  			默认 否(0)
                ,0.45                                                                  AS BEEL                    --已违约暴露预期损失比率    			默认 0.45
                ,0.45                                                                  AS DefaultLGD              --已违约暴露违约损失率      			默认 0.45
                ,'0'                                                                   AS EquityExpoFlag          --股权暴露标识              			默认 否(0)
                ,''                                                                    AS EquityInvestType        --股权投资对象类型          			默认 NULL
                ,''	                                                                   AS EquityInvestCause       --股权投资形成原因          			默认 NULL
                ,'0'                                                                   AS SLFlag                  --专业贷款标识              			默认 否(0)
                ,''                                                                    AS SLType                  --专业贷款类型              			默认 NULL
                ,''                                                                    AS PFPhase                 --项目融资阶段              			默认 NULL
                ,''	                                                                   AS ReguRating              --监管评级                  			默认 NULL
                ,'0'                                                                   AS CBRCMPRatingFlag        --银监会认定评级是否更为审慎			默认 否(0)
                ,'0'                                                                   AS LargeFlucFlag           --是否波动性较大            			默认 否(0)
                ,'0'                                                                   AS LiquExpoFlag            --是否清算过程中风险暴露    			默认 否(0)
                ,'0'                                                                   AS PaymentDealFlag         --是否货款对付模式          			默认 否(0)
                ,0	                                                                   AS DelayTradingDays        --延迟交易天数              			默认 0
                ,'0'                                                                   AS SecuritiesFlag          --有价证券标识              			默认 否(0)
                ,''                                                                    AS SecuIssuerID            --证券发行人ID              			默认 NULL
                ,''                                                                    AS RatingDurationType      --评级期限类型              			默认 NULL
                ,''	                                                                   AS SecuIssueRating         --证券发行等级              			默认 NULL
                ,''                                                                    AS SecuResidualM           --证券剩余期限              			默认 NULL
                ,NULL                                                                  AS SecuRevaFrequency       --证券重估频率              			默认 NULL
                ,'0'                                                                   AS CCPTranFlag             --是否中央交易对手相关交易  			默认 否(0)
                ,''	                                                                   AS CCPID                   --中央交易对手ID            			默认 NULL
                ,'0'                                                                   AS QualCCPFlag             --是否合格中央交易对手      			默认 否(0)
                ,''                                                                    AS BankRole                --银行角色                  			默认 NULL
                ,''	                                                                   AS ClearingMethod          --清算方式                  			默认 NULL
                ,'0'                                                                   AS BankAssetFlag           --是否银行提交资产          			默认 否(0)
                ,''	                                                                   AS MatchConditions         --符合条件情况              			默认 NULL
                ,'0'                                                                   AS SFTFlag                 --证券融资交易标识          			默认 否(0)
                ,''		                                                                 AS MasterNetAgreeFlag      --净额结算主协议标识        			默认 NULL
                ,''                                                                    AS MasterNetAgreeID        --净额结算主协议ID          			默认 NULL
                ,''	                                                                   AS SFTType                 --证券融资交易类型          			默认 NULL
                ,'0'                                                                   AS SecuOwnerTransFlag      --证券所有权是否转移        			默认 否(0)
                ,'0'                                                                   AS OTCFlag                 --场外衍生工具标识          			默认 否(0)
                ,'0'                                                                   AS ValidNettingFlag        --有效净额结算协议标识      			默认 否(0)
                ,''                                                                    AS ValidNetAgreementID     --有效净额结算协议ID        			默认 NULL
                ,''                                                                    AS OTCType                 --场外衍生工具类型          			默认 NULL
                ,NULL                                                                  AS DepositRiskPeriod       --保证金风险期间            			默认 NULL
                ,0	                                                                   AS MTM                     --重置成本                  			默认 0
                ,''                                                                    AS MTMCurrency             --重置成本币种              			默认 NULL
                ,''	                                                                   AS BuyerOrSeller           --买方卖方                  			默认 NULL
                ,'0'                                                                   AS QualROFlag              --合格参照资产标识          			默认 否(0)
                ,'0'                                                                   AS ROIssuerPerformFlag     --参照资产发行人是否能履约  			默认 否(0)
                ,'0'                                                                   AS BuyerInsolvencyFlag     --信用保护买方是否破产      			默认 否(0)
                ,0	                                                                   AS NonpaymentFees          --尚未支付费用              			默认 0
                ,'0'                                                                   AS RetailExpoFlag          --零售暴露标识              			默认 否(0)
                ,''                                                                    AS RetailClaimType         --零售债权类型              			默认 NULL
                ,''                                                                    AS MortgageType            --住房抵押贷款类型          			默认 NULL
                ,1                                                                     AS ExpoNumber              --风险暴露个数              			默认 1
                ,0.8                                                                   AS LTV                     --贷款价值比                			默认 0.8
                ,NULL                                                                  AS Aging                   --账龄                      			默认 NULL
                ,''								                                                     AS NewDefaultDebtFlag      --新增违约债项标识     						默认 NULL
                ,''                                                                    AS PDPoolModelID           --PD分池模型ID              			默认 NULL
                ,''                                                                    AS LGDPoolModelID          --LGD分池模型ID             			默认 NULL
                ,''                                                                    AS CCFPoolModelID          --CCF分池模型ID             			默认 NULL
                ,''	                                                                   AS PDPoolID                --所属PD池ID                			默认 NULL
                ,''                                                                    AS LGDPoolID               --所属LGD池ID               			默认 NULL
                ,''                                                                    AS CCFPoolID               --所属CCF池ID               			默认 NULL
                ,'0'                                                                   AS ABSUAFlag               --资产证券化基础资产标识    			默认 否(0)
                ,''				                                                             AS ABSPoolID               --证券化资产池ID            			默认 NULL
                ,''                                                                    AS GroupID                 --分组编号                  			默认 NULL
                ,NULL															                                     AS DefaultDate             --违约时点
                ,NULL																																	 AS ABSPROPORTION						--资产证券化比重
								,NULL																																	 AS DEBTORNUMBER						--借款人个数


    FROM				TMP_JE T1
    WHERE				T1.ZBJ_BALANCE > 0
    ;

    COMMIT;

    --4.2 虚拟暴露信贷类表内资产对应的准备金差值，默认13040101科目
    INSERT INTO RWA_DEV.RWA_EI_EXPOSURE(
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
    )
    WITH TMP_FNS_JE AS (
    						SELECT ABS(SUM(T1.BALANCE_D - T1.BALANCE_C)) AS FNS_BALANCE
								  FROM FNS_GL_BALANCE T1
								 WHERE T1.DATANO = p_data_dt_str
								   AND T1.CURRENCY_CODE = 'RMB'
								   AND T1.SUBJECT_NO IN ('13040101',
								                         '13040102')
    )
    , TMP_EI_BAL AS (
    					SELECT T.EXPOCLASSSTD						AS EXPOCLASSSTD
    								,T.EXPOSUBCLASSSTD				AS EXPOSUBCLASSSTD
    								,SUM(T.NORMALPRINCIPAL) 	AS BALANCE
    								,SUM(T.GENERALPROVISION) 	AS EI_BALANCE
							  FROM RWA_EI_EXPOSURE T
							 WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
							   AND T.SSYSID IN ('XD', 'XYK', 'PJ', 'ZX')
							   AND T.EXPOBELONG = '01'
						GROUP BY T.EXPOCLASSSTD,T.EXPOSUBCLASSSTD
							HAVING SUM(T.NORMALPRINCIPAL) > 0
    )
    , TMP_EI_JE AS (
    					SELECT SUM(T.NORMALPRINCIPAL) 	AS BALANCE
    								,SUM(T.GENERALPROVISION) 	AS EI_BALANCE
							  FROM RWA_EI_EXPOSURE T
							 WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
							   AND T.SSYSID IN ('XD', 'XYK', 'PJ', 'ZX')
							   AND T.EXPOBELONG = '01'
    )
    , TMP_JE AS (
    					SELECT C.EXPOCLASSSTD											AS EXPOCLASSSTD
    								,C.EXPOSUBCLASSSTD									AS EXPOSUBCLASSSTD
    								,C.BALANCE													AS BALANCE
    								,B.BALANCE			 										AS TOTAL_BALANCE
    								,A.FNS_BALANCE - B.EI_BALANCE			 	AS ZBJ_BALANCE
    						FROM TMP_FNS_JE A, TMP_EI_JE B, TMP_EI_BAL C
    )
    SELECT
                 TO_DATE(p_data_dt_str,'YYYYMMDD')                                     AS DataDate                --数据日期
		            ,p_data_dt_str                                                         AS DataNo                  --数据流水号
		            ,'XN-ZBJ-' || T1.EXPOSUBCLASSSTD || '-10000000-CNY'                    AS ExposureID              --风险暴露ID
		            ,'XN-ZBJ-13040101-10000000-CNY'                                        AS DueID                   --债项ID
		            ,'ZBJ'                                                                 AS SSysID                  --源系统ID
		            ,'XN-ZBJ-' || T1.EXPOSUBCLASSSTD || '-10000000-CNY'                    AS ContractID              --合同ID
		            ,'XN-ZBJ-10000000-CNY'			                                           AS ClientID                --参与主体ID
		            ,'9998'	                                                           AS SOrgID                  --源机构ID
		            ,'重庆银行股份有限公司'                                                            AS SOrgName                --源机构名称
		            ,'1010'				                                                           AS OrgSortNo               --所属机构排序号
		            ,'9998'	                                                           AS OrgID                   --所属机构ID
		            ,'重庆银行股份有限公司'	                                                           AS OrgName                 --所属机构名称
		            ,'9998'	                                                           AS AccOrgID                --账务机构ID
		            ,'重庆银行股份有限公司'			                                                       AS AccOrgName              --账务机构名称
		            ,'J6620'										                                           AS IndustryID              --所属行业代码              			默认 货币银行服务(J6620)
		            ,'货币银行服务'									                                       AS IndustryName            --所属行业名称              			默认 货币银行服务
		            ,'0501'	                                                               AS BusinessLine            --业务条线                  			默认 总行(0501)
		            ,'132'	                                                               AS AssetType               --资产大类                  			默认 其他资产(132)
		            ,'13205'                                                               AS AssetSubType            --资产小类                  			默认 其他表内资产(13205)
		            ,'9010101010'		                                                       AS BusinessTypeID          --业务品种代码              			默认 虚拟业务品种(9010101010)
		            ,'虚拟业务品种'                                                        AS BusinessTypeName        --业务品种名称              			默认 虚拟业务品种(9010101010)
		            ,'01'                                                                  AS CreditRiskDataType      --信用风险数据类型          			默认 一般非零售(01)
		            ,'01'                                                                  AS AssetTypeOfHaircuts     --折扣系数对应资产类别      			默认 现金及现金等价物(01)
		            ,'99'	                                                                 AS BusinessTypeSTD         --权重法业务类型            			默认 其他资产(99)
		            ,T1.EXPOCLASSSTD                                                       AS ExpoClassSTD            --权重法暴露大类            			默认 其他(0112)
		            ,T1.EXPOSUBCLASSSTD                                                    AS ExpoSubClassSTD         --权重法暴露小类            			默认 其他适用100%风险权重的资产(011216)
		            ,'0203'																                                 AS ExpoClassIRB            --内评法暴露大类            			默认 公司风险暴露(0203)
		            ,'020301'												                                       AS ExpoSubClassIRB         --内评法暴露小类            			默认 一般公司(020301)
		            ,'01'                                                                  AS ExpoBelong              --暴露所属标识              			默认 表内(01)
		            ,'01'                                                                  AS BookType                --账户类别                  			默认 银行账户(01)
                ,'02'                                                                  AS ReguTranType            --监管交易类型              			默认 其他资本市场(02)
                ,'0'                                                                   AS RepoTranFlag            --回购交易标识              			默认 否(0)
                ,1								                                                     AS RevaFrequency           --重估频率                  			默认 1
                ,'CNY'										                                             AS Currency                --币种
                ,0									                                                   AS NormalPrincipal         --正常本金余额              			默认 0
                ,0                                                                     AS OverdueBalance          --逾期余额                  			默认 0
                ,0		            								                                     AS NonAccrualBalance       --非应计余额                			默认 0
                ,0									                                                   AS OnSheetBalance          --表内余额                  			默认 0
                ,0					                                                           AS NormalInterest          --正常利息                  			默认 0	利息统一从总账表虚拟
                ,0                                                                     AS OnDebitInterest         --表内欠息                  			默认 0
                ,0                                                                     AS OffDebitInterest        --表外欠息                  			默认 0
                ,0																					                           AS ExpenseReceivable       --应收费用                  			默认 0
                ,0									                                                   AS AssetBalance            --资产余额                  			默认 0
                ,'13040101'	                                                           AS AccSubject1             --科目一
                ,NULL                                                                  AS AccSubject2             --科目二
                ,NULL		                                                               AS AccSubject3             --科目三
                ,TO_CHAR(TO_DATE(p_data_dt_str,'YYYYMMDD'),'YYYY-MM-DD')               AS StartDate               --起始日期
                ,TO_CHAR(ADD_MONTHS(TO_DATE(p_data_dt_str,'YYYYMMDD'),1),'YYYY-MM-DD')
                																                                       AS DueDate             		--到期日期
                ,(ADD_MONTHS(TO_DATE(p_data_dt_str,'YYYYMMDD'),1) - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                																													             AS OriginalMaturity    		--原始期限
                ,(ADD_MONTHS(TO_DATE(p_data_dt_str,'YYYYMMDD'),1) - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                		                                                                   AS ResidualM               --剩余期限
                ,'01'																                                   AS RiskClassify            --风险分类												默认 正常(01)
                ,'01'                                                                  AS ExposureStatus          --风险暴露状态              			默认 正常(01)
                ,0                                                                     AS OverdueDays             --逾期天数                  			默认 0
                ,0																						                         AS SpecialProvision        --专项准备金                			默认 0	RWA计算
                ,T1.ZBJ_BALANCE * T1.BALANCE / T1.TOTAL_BALANCE	                       AS GeneralProvision        --一般准备金                			默认 0	RWA计算
                ,0                                                                     AS EspecialProvision       --特别准备金                			默认 0	RWA计算
                ,0	                                                                   AS WrittenOffAmount        --已核销金额                			默认 0
                ,''                                                                    AS OffExpoSource           --表外暴露来源              			默认 NULL
                ,''                                                                    AS OffBusinessType         --表外业务类型              			默认 NULL
                ,''	                                                                   AS OffBusinessSdvsSTD      --权重法表外业务类型细分    			默认 NULL
                ,'0'                                                                   AS UncondCancelFlag        --是否可随时无条件撤销      			默认 否(0)
                ,''                                                                    AS CCFLevel                --信用转换系数级别          			默认 NULL
                ,''		                                                                 AS CCFAIRB                 --高级法信用转换系数        			默认 NULL
                ,'01'                                                                  AS ClaimsLevel             --债权级别                  			默认 高级债权(01)
                ,'0'	                                                                 AS BondFlag                --是否为债券                			默认 否(0)
                ,'02'                                                                  AS BondIssueIntent         --债券发行目的              			默认 其他(02)
                ,'0'	                                                                 AS NSURealPropertyFlag     --是否非自用不动产          			默认 否(0)
                ,'01'                                                                  AS RepAssetTermType        --抵债资产期限类型          			默认 法律规定处分期限内(01)
                ,'0'                                                                   AS DependOnFPOBFlag        --是否依赖于银行未来盈利    			默认 否(0)
                ,''															                                       AS IRating                 --内部评级                  			默认 NULL
                ,NULL									                                                 AS PD                      --违约概率                  			默认 NULL
                ,NULL                                                                  AS LGDLevel                --违约损失率级别            			默认 NULL
                ,NULL                                                                  AS LGDAIRB                 --高级法违约损失率          			默认 NULL
                ,NULL                                                                  AS MAIRB                   --高级法有效期限            			默认 NULL
                ,NULL                                                                  AS EADAIRB                 --高级法违约风险暴露        			默认 NULL
                ,'0'																								                   AS DefaultFlag             --违约标识                  			默认 否(0)
                ,0.45                                                                  AS BEEL                    --已违约暴露预期损失比率    			默认 0.45
                ,0.45                                                                  AS DefaultLGD              --已违约暴露违约损失率      			默认 0.45
                ,'0'                                                                   AS EquityExpoFlag          --股权暴露标识              			默认 否(0)
                ,''                                                                    AS EquityInvestType        --股权投资对象类型          			默认 NULL
                ,''	                                                                   AS EquityInvestCause       --股权投资形成原因          			默认 NULL
                ,'0'                                                                   AS SLFlag                  --专业贷款标识              			默认 否(0)
                ,''                                                                    AS SLType                  --专业贷款类型              			默认 NULL
                ,''                                                                    AS PFPhase                 --项目融资阶段              			默认 NULL
                ,''	                                                                   AS ReguRating              --监管评级                  			默认 NULL
                ,'0'                                                                   AS CBRCMPRatingFlag        --银监会认定评级是否更为审慎			默认 否(0)
                ,'0'                                                                   AS LargeFlucFlag           --是否波动性较大            			默认 否(0)
                ,'0'                                                                   AS LiquExpoFlag            --是否清算过程中风险暴露    			默认 否(0)
                ,'0'                                                                   AS PaymentDealFlag         --是否货款对付模式          			默认 否(0)
                ,0	                                                                   AS DelayTradingDays        --延迟交易天数              			默认 0
                ,'0'                                                                   AS SecuritiesFlag          --有价证券标识              			默认 否(0)
                ,''                                                                    AS SecuIssuerID            --证券发行人ID              			默认 NULL
                ,''                                                                    AS RatingDurationType      --评级期限类型              			默认 NULL
                ,''	                                                                   AS SecuIssueRating         --证券发行等级              			默认 NULL
                ,''                                                                    AS SecuResidualM           --证券剩余期限              			默认 NULL
                ,NULL                                                                  AS SecuRevaFrequency       --证券重估频率              			默认 NULL
                ,'0'                                                                   AS CCPTranFlag             --是否中央交易对手相关交易  			默认 否(0)
                ,''	                                                                   AS CCPID                   --中央交易对手ID            			默认 NULL
                ,'0'                                                                   AS QualCCPFlag             --是否合格中央交易对手      			默认 否(0)
                ,''                                                                    AS BankRole                --银行角色                  			默认 NULL
                ,''	                                                                   AS ClearingMethod          --清算方式                  			默认 NULL
                ,'0'                                                                   AS BankAssetFlag           --是否银行提交资产          			默认 否(0)
                ,''	                                                                   AS MatchConditions         --符合条件情况              			默认 NULL
                ,'0'                                                                   AS SFTFlag                 --证券融资交易标识          			默认 否(0)
                ,''		                                                                 AS MasterNetAgreeFlag      --净额结算主协议标识        			默认 NULL
                ,''                                                                    AS MasterNetAgreeID        --净额结算主协议ID          			默认 NULL
                ,''	                                                                   AS SFTType                 --证券融资交易类型          			默认 NULL
                ,'0'                                                                   AS SecuOwnerTransFlag      --证券所有权是否转移        			默认 否(0)
                ,'0'                                                                   AS OTCFlag                 --场外衍生工具标识          			默认 否(0)
                ,'0'                                                                   AS ValidNettingFlag        --有效净额结算协议标识      			默认 否(0)
                ,''                                                                    AS ValidNetAgreementID     --有效净额结算协议ID        			默认 NULL
                ,''                                                                    AS OTCType                 --场外衍生工具类型          			默认 NULL
                ,NULL                                                                  AS DepositRiskPeriod       --保证金风险期间            			默认 NULL
                ,0	                                                                   AS MTM                     --重置成本                  			默认 0
                ,''                                                                    AS MTMCurrency             --重置成本币种              			默认 NULL
                ,''	                                                                   AS BuyerOrSeller           --买方卖方                  			默认 NULL
                ,'0'                                                                   AS QualROFlag              --合格参照资产标识          			默认 否(0)
                ,'0'                                                                   AS ROIssuerPerformFlag     --参照资产发行人是否能履约  			默认 否(0)
                ,'0'                                                                   AS BuyerInsolvencyFlag     --信用保护买方是否破产      			默认 否(0)
                ,0	                                                                   AS NonpaymentFees          --尚未支付费用              			默认 0
                ,'0'                                                                   AS RetailExpoFlag          --零售暴露标识              			默认 否(0)
                ,''                                                                    AS RetailClaimType         --零售债权类型              			默认 NULL
                ,''                                                                    AS MortgageType            --住房抵押贷款类型          			默认 NULL
                ,1                                                                     AS ExpoNumber              --风险暴露个数              			默认 1
                ,0.8                                                                   AS LTV                     --贷款价值比                			默认 0.8
                ,NULL                                                                  AS Aging                   --账龄                      			默认 NULL
                ,''								                                                     AS NewDefaultDebtFlag      --新增违约债项标识     						默认 NULL
                ,''                                                                    AS PDPoolModelID           --PD分池模型ID              			默认 NULL
                ,''                                                                    AS LGDPoolModelID          --LGD分池模型ID             			默认 NULL
                ,''                                                                    AS CCFPoolModelID          --CCF分池模型ID             			默认 NULL
                ,''	                                                                   AS PDPoolID                --所属PD池ID                			默认 NULL
                ,''                                                                    AS LGDPoolID               --所属LGD池ID               			默认 NULL
                ,''                                                                    AS CCFPoolID               --所属CCF池ID               			默认 NULL
                ,'0'                                                                   AS ABSUAFlag               --资产证券化基础资产标识    			默认 否(0)
                ,''				                                                             AS ABSPoolID               --证券化资产池ID            			默认 NULL
                ,''                                                                    AS GroupID                 --分组编号                  			默认 NULL
                ,NULL															                                     AS DefaultDate             --违约时点
                ,NULL																																	 AS ABSPROPORTION						--资产证券化比重
								,NULL																																	 AS DEBTORNUMBER						--借款人个数


    FROM				TMP_JE T1
    WHERE				T1.ZBJ_BALANCE > 0
    ;

    COMMIT;

    --4.3 虚拟合同
    INSERT INTO RWA_DEV.RWA_EI_CONTRACT(
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
							,UncondCancelFlag  									  --是否可随时无条件撤销
							,ABSUAFlag         									  --资产证券化基础资产标识
							,ABSPoolID         									  --证券化资产池ID
							,GroupID           									  --分组编号
							,GUARANTEETYPE     									  --主要担保方式
							,ABSPROPORTION												--资产证券化比重
    )
    SELECT			--DISTINCT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DataDate                             --数据日期
                ,p_data_dt_str													     AS DataNo                               --数据流水号
                ,T1.CONTRACTID                   				 		 AS ContractID                           --合同ID
                ,T1.CONTRACTID                           		 AS SContractID                          --源合同ID
                ,T1.SSYSID                                   AS SSysID                               --源系统ID
                ,T1.CLIENTID                         			 	 AS ClientID                             --参与主体ID
                ,T1.SORGID                         					 AS SOrgID                               --源机构ID
                ,T1.SORGNAME                              	 AS SOrgName                             --源机构名称
                ,T1.ORGSORTNO                                AS OrgSortNo                            --所属机构排序号
                ,T1.ORGID                              			 AS OrgID                                --所属机构ID
                ,T1.ORGNAME                                  AS OrgName                              --所属机构名称
                ,T1.INDUSTRYID                               AS IndustryID                           --所属行业代码
                ,T1.INDUSTRYNAME                             AS IndustryName                         --所属行业名称
                ,T1.BUSINESSLINE                             AS BusinessLine                         --业务条线              				默认 同业(04)
                ,T1.ASSETTYPE                              	 AS AssetType                            --资产大类
                ,T1.ASSETSUBTYPE              							 AS AssetSubType                         --资产小类
                ,T1.BUSINESSTYPEID                           AS BusinessTypeID                       --业务品种代码
                ,T1.BUSINESSTYPENAME                         AS BusinessTypeName                     --业务品种名称
                ,T1.CREDITRISKDATATYPE                       AS CreditRiskDataType                   --信用风险数据类型
                ,T1.STARTDATE                                AS StartDate                            --起始日期
                ,T1.DUEDATE                                  AS DueDate                              --到期日期
                ,T1.ORIGINALMATURITY                         AS OriginalMaturity                     --原始期限
                ,T1.RESIDUALM                                AS ResidualM                            --剩余期限
                ,T1.CURRENCY                                 AS SettlementCurrency                   --结算币种
                ,T1.NORMALPRINCIPAL                          AS ContractAmount                       --合同总金额
                ,0                                           AS NotExtractPart                       --合同未提取部分        				默认 0
                ,'0'                                         AS UncondCancelFlag  									 --是否可随时无条件撤销  				默认 否(0)
                ,'0'                                         AS ABSUAFlag         									 --资产证券化基础资产标识				默认 否(0)
                ,''                                        	 AS ABSPoolID         									 --证券化资产池ID        				默认 空
                ,''                                          AS GroupID           									 --分组编号              				默认 空
                ,''																					 AS GUARANTEETYPE     									 --主要担保方式
                ,NULL																				 AS ABSPROPORTION												 --资产证券化比重

    FROM				RWA_DEV.RWA_EI_EXPOSURE T1
    WHERE				T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND					T1.SSYSID = 'ZBJ'
    ;

    COMMIT;

    --4.4 虚拟参与主体
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
				        ,MSMBFlag										--工信部微小企业标识
    )
		SELECT 			--DISTINCT
        				 TO_DATE(p_data_dt_str,'yyyyMMdd')     																	AS DataDate            		--数据日期
        				,p_data_dt_str                         																	AS DataNo              		--数据流水号
        				,'XN-ZBJ-10000000-CNY'																									AS ClientID            		--参与主体ID            默认 'XN-ZBJ-10000000-CNY'
        				,'XN-ZBJ-10000000-CNY'																									AS SourceClientID      		--源参与主体ID
        				,'ZBJ'		                             																	AS SSysID              		--源系统ID
        				,'准备金-虚拟客户'																											AS ClientName          		--参与主体名称          默认 准备金-虚拟客户
        				,'9998'	                                                            AS SOrgID                 --源机构ID
		            ,'重庆银行股份有限公司'                                                             AS SOrgName               --源机构名称
		            ,'1010'				                                                            AS OrgSortNo              --所属机构排序号
		            ,'9998'	                                                            AS OrgID                  --所属机构ID
		            ,'重庆银行股份有限公司'	                                                            AS OrgName                --所属机构名称
        				,'J6620'										                                            AS IndustryID             --所属行业代码          默认 货币银行服务(J6620)
		            ,'货币银行服务'									                                        AS IndustryName           --所属行业名称          默认 货币银行服务
        				,'03'																																		AS ClientType          		--参与主体大类          默认 03-公司
        				,'0301'																																	AS ClientSubType       		--参与主体小类          默认 0301-一般公司
        				,'01'	                                 																	AS RegistState         		--注册国家或地区        默认 01-境内
        				,''							                       																	AS RCERating           		--境外注册地外部评级
        				,'01'                                  																	AS RCERAgency          		--境外注册地外部评级机构
        				,'202869177'											     																	AS OrganizationCode    		--组织机构代码
        				,'0'	                                																	AS ConsolidatedSCFlag  		--是否并表子公司
        				,'0'                                  																	AS SLClientFlag        		--专业贷款客户标识
        				,''	                                  																	AS SLClientType        		--专业贷款客户类型
        				,'020301'						                   																	AS ExpoCategoryIRB     		--内评法暴露类别        默认 020301-一般公司
        				,''				                            																	AS ModelID             		--模型ID
        				,''				                            																	AS ModelIRating        		--模型内部评级
        				,NULL                                 																	AS ModelPD             		--模型违约概率
        				,''								                    																	AS IRating             		--内部评级
        				,NULL	                                																	AS PD                  		--违约概率
        				,'0'																																		AS DefaultFlag         		--违约标识
        				,'0'											            																	AS NewDefaultFlag      		--新增违约标识
        				,''																     																	AS DefaultDate         		--违约时点
        				,''																     																	AS ClientERating       		--参与主体外部评级
        				,'0'                                  																	AS CCPFlag             		--中央交易对手标识
        				,'0'                                   																	AS QualCCPFlag         		--是否合格中央交易对手
        				,'0'                                   																	AS ClearMemberFlag     		--清算会员标识
        				,'00'									                																	AS CompanySize         		--企业规模
        				,'0'											            																	AS SSMBFlag            		--标准小微企业标识
        				,NULL							                     																	AS AnnualSale          		--公司客户年销售额
        				,'CHN'																																	AS CountryCode            --注册国家代码
        				,''																																			AS MSMBFlag								--工信部微小企业标识

    FROM				DUAL
    ;

    COMMIT;

    --5.更新一般准备金，按五级分类计提
		UPDATE RWA_DEV.RWA_EI_EXPOSURE SET SPECIALPROVISION = NORMALPRINCIPAL * CASE WHEN RISKCLASSIFY = '01' THEN 0 WHEN RISKCLASSIFY = '02' THEN 0.02 WHEN RISKCLASSIFY = '03' THEN 0.25 WHEN RISKCLASSIFY = '04' THEN 0.5 WHEN RISKCLASSIFY = '05' THEN 1 ELSE 0 END WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

		COMMIT;

		--6.更新暴露表BEEL字段值，等于分摊后的准备金/资产余额
		UPDATE RWA_DEV.RWA_EI_EXPOSURE SET BEEL = CASE WHEN NVL(GENERALPROVISION,0) / ASSETBALANCE > 1 THEN 1 ELSE  NVL(GENERALPROVISION,0) / ASSETBALANCE END WHERE ASSETBALANCE <> 0 AND DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

		COMMIT;

		--7.更新暴露表BEEL字段值，使用零售评级得到的beel值更新
		UPDATE RWA_DEV.RWA_EI_EXPOSURE T SET T.BEEL = (SELECT RH.BEELVALUE FROM RWA_DEV.RWA_TEMP_LGDLEVEL RH WHERE RH.BUSINESSID = T.CONTRACTID)
		WHERE  EXISTS (SELECT 1 FROM RWA_DEV.RWA_TEMP_LGDLEVEL RH WHERE RH.BUSINESSID = T.CONTRACTID AND RH.BEELVALUE IS NOT NULL)
		AND		 T.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

		COMMIT;

  	--8.统计暴露表的条数
  	SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_EI_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD') AND GENERALPROVISION <> 0;

		p_po_rtncode := '1';
		p_po_rtnmsg  := '成功'||'-'||v_count1;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '准备金计提和分摊更新失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace ;
         RETURN;

END PRO_RWA_CD_DECREASE_TYPE;
/

