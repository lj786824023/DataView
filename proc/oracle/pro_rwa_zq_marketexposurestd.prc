CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ZQ_MARKETEXPOSURESTD(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_ZQ_MARKETEXPOSURESTD
    实现功能:财务系统-债券-市场风险-标准法暴露表(从数据源交易债券头寸关联债券信息表全量导入RWA市场风险债券接口表债券标准法暴露表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-04-12
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.RWA_ZQ_TRADBONDPOSITION|交易债券头寸表
    源  表2 :RWA_DEV.RWA_ZQ_BONDINFO|债券信息表
    源  表3 :RWA.ORG_INFO|RWA机构表
    源  表4 :RWA_DEV.FNS_BND_INFO_B|财务系统债券信息表
    源  表5 :RWA.RWA_WP_COUNTRYRATING|国家评级表
    源  表6 :RWA.RWA_WS_BONDTRADE|债券投资补录信息表
    源  表7 :RWA.RWA_WP_DATASUPPLEMENT|补录任务明细信息表
    目标表  :RWA_DEV.RWA_ZQ_MARKETEXPOSURESTD|财务系统债券标准法暴露表
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ZQ_MARKETEXPOSURESTD';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_ZQ_MARKETEXPOSURESTD';

    --2.将满足条件的数据从源表插入到目标表中
    --2.1 财务系统-债券
    INSERT INTO RWA_DEV.RWA_ZQ_MARKETEXPOSURESTD(
                 DataDate                              --数据日期
                ,DataNo                                --数据流水号
                ,ExposureID                            --风险暴露ID
                ,BookType                              --账户类别
                ,InstrumentsID                         --金融工具ID
                ,InstrumentsType                       --金融工具类型
                ,OrgSortNo                             --所属机构排序号
                ,OrgID                                 --所属机构ID
                ,OrgName                               --所属机构名称
                ,OrgType                               --所属机构类型
                ,MarketRiskType                        --市场风险类型
                ,InteRateRiskType                      --利率风险类型
                ,EquityRiskType                        --股票风险类型
                ,ExchangeRiskType                      --外汇风险类型
                ,CommodityName                         --商品种类名称
                ,OptionRiskType                        --期权风险类型
                ,IssuerID                              --发行人ID
                ,IssuerName                            --发行人名称
                ,IssuerType                            --发行人大类
                ,IssuerSubType                         --发行人小类
                ,IssuerRegistState                     --发行人注册国家
                ,IssuerRCERating                       --发行人境外注册地外部评级
                ,SMBFlag                               --小微企业标识
                ,UnderBondFlag                         --是否承销债券
                ,PaymentDate                           --缴款日
                ,SecuritiesType      								   --证券类别
                ,BondIssueIntent                       --债券发行目的
                ,ClaimsLevel                           --债权级别
                ,ReABSFlag                             --再资产证券化标识
                ,OriginatorFlag                        --是否发起机构
                ,SecuritiesERating                     --证券外部评级
                ,StockCode                             --股票/股指代码
                ,StockMarket                           --交易市场
                ,ExchangeArea                          --交易地区
                ,StructuralExpoFlag                    --是否结构性敞口
                ,OptionUnderlyingFlag                  --是否期权基础工具
                ,OptionUnderlyingName                  --期权基础工具名称
                ,OptionID                              --期权工具ID
                ,Volatility                            --波动率
                ,StartDate                             --起始日期
                ,DueDate                               --到期日期
                ,OriginalMaturity                      --原始期限
                ,ResidualM                             --剩余期限
                ,NextRepriceDate                       --下次重定价日
                ,NextRepriceM                          --下次重定价期限
                ,RateType                              --利率类型
                ,CouponRate                            --票面利率
                ,ModifiedDuration                      --修正久期
                ,PositionType                          --头寸属性
                ,Position                              --头寸
                ,Currency            								   --币种
                ,OptionUnderlyingType									 --期权基础工具类型
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DataDate                	--数据日期
                ,p_data_dt_str												     	 AS DataNo                  	--数据流水号
                ,T1.POSITIONID                    				 	 AS ExposureID              	--风险暴露ID
                ,'02'					                           		 AS BookType                	--账户类别                					 默认：交易账户(02)
                ,T1.BONDID         	                         AS InstrumentsID           	--金融工具ID
                ,T1.INSTRUMENTSTYPE                    			 AS InstrumentsType         	--金融工具类型
                ,T3.SORTNO														 			 AS OrgSortNo        			 		--所属机构排序号
                ,T1.TRANORGID			 						               AS OrgID                   	--所属机构ID
                ,T3.ORGNAME										               AS OrgName                 	--所属机构名称
                ,'01'						                             AS OrgType                 	--所属机构类型            					 默认：境内机构(01)
                ,'01'			                              		 AS MarketRiskType          	--市场风险类型            					 默认：利率风险(01)
                ,'01'								                         AS InteRateRiskType        	--利率风险类型            					 默认：债券(01)
                ,''												                   AS EquityRiskType          	--股票风险类型            					 默认：空
                ,''							                             AS ExchangeRiskType        	--外汇风险类型            					 默认：空
                ,''																					 AS CommodityName      		 		--商品种类名称            					 默认：空
                ,''                                          AS OptionRiskType          	--期权风险类型            					 默认：空
                ,T2.ISSUERID                            		 AS IssuerID                	--发行人ID
                ,T2.ISSUERNAME	                             AS IssuerName              	--发行人名称
                ,T2.ISSUERTYPE	                             AS IssuerType              	--发行人大类
                ,T2.ISSUERSUBTYPE                            AS IssuerSubType           	--发行人小类
                ,T2.ISSUERREGISTSTATE                        AS IssuerRegistState       	--发行人注册国家
                ,NVL(T5.RATINGRESULT,'0124')                 AS IssuerRCERating         	--发行人境外注册地外部评级
                ,T2.ISSUERSMBFLAG	                           AS SMBFlag                 	--小微企业标识
                ,'0'                                         AS UnderBondFlag           	--是否承销债券            					 默认：否(0)
                ,''	                                         AS PaymentDate             	--缴款日                  					 默认：空
                ,T2.BONDTYPE                            		 AS SecuritiesType          	--证券类别
                ,T2.BONDISSUEINTENT													 AS BondIssueIntent    		 		--债券发行目的
                ,CASE WHEN T6.BOND_TYPE2 = '20' THEN '02'
                ELSE '01'
                END	                                         AS ClaimsLevel               --债权级别                					 债券种类2＝次级债券(20)，则债权级别＝次级债权(02)；否则为高级债权(01)
                ,T2.REABSFLAG                                AS ReABSFlag                 --再资产证券化标识
                ,T2.ORIGINATORFLAG                           AS OriginatorFlag            --是否发起机构
                ,T2.ERATING                                  AS SecuritiesERating         --证券外部评级
                ,''                                          AS StockCode                 --股票/股指代码           					 默认：空
                ,''                                          AS StockMarket               --交易市场                					 默认：空
                ,''                                          AS ExchangeArea              --交易地区                					 默认：空
                ,''                                          AS StructuralExpoFlag        --是否结构性敞口          					 默认：空
                ,'0'                                         AS OptionUnderlyingFlag      --是否期权基础工具        					 默认：否(0)
                ,''                                          AS OptionUnderlyingName      --期权基础工具名称        					 默认：空
                ,''                                          AS OptionID                  --期权工具ID              					 默认：空
                ,NULL                                        AS Volatility                --波动率                  					 默认：空
                ,T2.STARTDATE                                AS StartDate                 --起始日期
                ,T2.DUEDATE                                  AS DueDate                   --到期日期
                ,T2.ORIGINALMATURITY                         AS OriginalMaturity          --原始期限
                ,T2.RESIDUALM                                AS ResidualM                 --剩余期限
                ,T2.NEXTREPRICEDATE                          AS NextRepriceDate           --下次重定价日
                ,T2.NEXTREPRICEM                             AS NextRepriceM              --下次重定价期限
                ,T2.RATETYPE                                 AS RateType                  --利率类型
                ,T2.EXECUTIONRATE                            AS CouponRate                --票面利率
                ,T2.MODIFIEDDURATION                         AS ModifiedDuration          --修正久期
                ,'01'                                        AS PositionType              --头寸属性                					 默认：多头(01)
                ,T1.BOOKBALANCE                              AS Position                  --头寸
                ,T1.CURRENCY                                 AS Currency                  --币种
                ,''																					 AS OptionUnderlyingType		 --期权基础工具类型

    FROM				RWA_DEV.RWA_ZQ_TRADBONDPOSITION T1	             		 					--交易债券头寸信息表
	  INNER JOIN 	RWA_DEV.RWA_ZQ_BONDINFO T2											 							--债券信息表
	  ON 					T1.BONDID = T2.BONDID
	  AND					T2.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
	  LEFT  JOIN	RWA.ORG_INFO T3																								--RWA机构信息表
	  ON					T1.TRANORGID = T3.ORGID
	  LEFT JOIN		RWA_DEV.NCM_CUSTOMER_INFO T4																	--统一客户信息表
		ON					T2.ISSUERID = T4.CUSTOMERID
		AND					T4.DATANO = p_data_dt_str
	  LEFT  JOIN	RWA.RWA_WP_COUNTRYRATING T5																		--国家评级表
	  ON					T4.COUNTRYCODE = T5.COUNTRYCODE
	  AND					T5.ISINUSE = '1'
	  LEFT	JOIN	RWA_DEV.FNS_BND_INFO_B T6																			--财务系统债券信息表
	  ON					T1.POSITIONID = T6.BOND_ID
	  AND					T6.DATANO = p_data_dt_str
	  WHERE				T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
	  ;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_ZQ_MARKETEXPOSURESTD',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_ZQ_MARKETEXPOSURESTD;
    --Dbms_output.Put_line('RWA_DEV.RWA_ZQ_MARKETEXPOSURESTD表当前插入的财务系统-债券(市场风险)-标准法暴露记录为: ' || v_count || ' 条');



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
END PRO_RWA_ZQ_MARKETEXPOSURESTD;
/

