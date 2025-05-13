CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_LC_MARKETEXPOSURESTD(
														p_data_dt_str IN  VARCHAR2, --数据日期
                            p_po_rtncode  OUT VARCHAR2, --返回编号
                            p_po_rtnmsg   OUT VARCHAR2  --返回描述
)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_LC_MARKETEXPOSURESTD
    实现功能:理财系统-债券理财投资-市场风险-标准法暴露表(从数据源交易债券头寸关联债券信息表全量导入RWA市场风险理财接口表债券标准法暴露表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :QHJIANG
    编写时间:2016-04-14
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.RWA_LC_TRADBONDPOSITION|交易债券头寸表
    源  表2 :RWA_DEV.RWA_LC_BONDINFO|债券信息表
    源  表3 :RWA.RWA_WS_FCII_BOND|债券理财投资补录表
    源	表4 :RWA.ORG_INFO|RWA机构信息表
    源  表5 :RWA.RWA_WP_COUNTRYRATING|国家评级表
    源  表6 :RWA.RWA_WP_DATASUPPLEMENT|补录任务明细信息表
    目标表1 :RWA_DEV.RWA_LC_MARKETEXPOSURESTD|市场风险标准法风险暴露表
    辅助表  :无
    变更记录(修改人|修改时间|修改内容):
    */

  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_LC_MARKETEXPOSURESTD';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN

    --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --清除目标表中的原有记录
    --DELETE FROM RWA_EI_MARKETEXPOSURESTD WHERE DATADATE=TO_DATE(p_data_dt_str,'yyyyMMdd');
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_LC_MARKETEXPOSURESTD';


    --DBMS_OUTPUT.PUT_LINE('开始：导入【市场风险标准法风险暴露表】' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    INSERT INTO RWA_DEV.RWA_LC_MARKETEXPOSURESTD(
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
        				TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,p_data_dt_str											 				 AS DATANO                   --数据流水号
        				,T1.POSITIONID                   						 AS EXPOSUREID               --风险暴露ID(头寸ID:直接映射)
        				,'02'                            						 AS BOOKTYPE                 --账户类别(默认：交易账户 BookType 账户类别：02 交易账户)
        				,T1.BONDID                       						 AS INSTRUMENTSID            --金融工具ID(债券ID:直接映射)
        				,T1.INSTRUMENTSTYPE              						 AS INSTRUMENTSTYPE          --金融工具类型(金融工具类型:直接映射)
        				,T4.SORTNO														 			 AS ORGSORTNO								 --机构排序号
        				,T1.TRANORGID                    						 AS ORGID                    --所属机构ID(交易机构ID:直接映射)
        				,T4.ORGNAME	                    						 AS ORGNAME                  --所属机构名称(交易机构ID:转换映射)
        				,'01'                            						 AS ORGTYPE                  --所属机构类型(默认：01 境内机构"01 境内机构02 境外机构")
        				,'01'                              					 AS MARKETRISKTYPE           --市场风险类型(无需映射)，默认：利率风险(01)
        				,'01'                            						 AS INTERATERISKTYPE         --利率风险类型(默认：债券 InteRateRiskType 利率风险类型:01 债券)
        				,''                              						 AS EQUITYRISKTYPE           --股票风险类型(默认：NULL)
        				,''                              						 AS EXCHANGERISKTYPE         --外汇风险类型(默认：NULL)
        				,''                              						 AS COMMODITYNAME            --商品种类名称(默认：NULL)
        				,''                              						 AS OPTIONRISKTYPE           --期权风险类型(默认：NULL)
        				,T2.ISSUERID                     						 AS ISSUERID                 --发行人ID(发行人ID:直接映射
        				,T2.ISSUERNAME                   						 AS ISSUERNAME               --发行人名称(发行人名称:直接映射)
        				,T2.ISSUERTYPE                   						 AS ISSUERTYPE               --发行人大类(发行人大类:直接映射)
        				,T2.ISSUERSUBTYPE                						 AS ISSUERSUBTYPE            --发行人小类(发行人小类:直接映射)
        				,T2.ISSUERREGISTSTATE            						 AS ISSUERREGISTSTATE        --发行人注册国家(发行人注册国家:直接映射)
        				,T5.RATINGRESULT		            						 AS ISSUERRCERATING          --发行人境外注册地外部评级(发行人注册国家:根据发行人注册国家，取国家评级表中对应的评级)
        				,T2.ISSUERSMBFLAG                						 AS SMBFLAG                  --小微企业标识(发行人小微企业标识:直接映射)
        				,'0'                             						 AS UNDERBONDFLAG            --是否承销债券(默认：否 1 是 0 否)
        				,''                              						 AS PAYMENTDATE              --缴款日(默认：NULL)
        				,T2.BONDTYPE                     						 AS SECURITIESTYPE           --证券类别(债券类型:转换映射)
        				,T2.BONDISSUEINTENT              						 AS BONDISSUEINTENT          --债券发行目的(债券发行目的:直接映射)
        				,CASE WHEN T3.C_BONDDETAIL_TYPE IN ('02','03','04') THEN '02'
        				 ELSE '01'
        				 END											         			 		 AS CLAIMSLEVEL              --债权级别(债权级别:直接映射)
        				,T2.REABSFLAG                    						 AS REABSFLAG                --再资产证券化标识(再资产证券化标识:直接映射)
        				,T2.ORIGINATORFLAG               						 AS ORIGINATORFLAG           --是否发起机构(是否发起机构:直接映射)
        				,T2.ERATING                      						 AS SECURITIESERATING        --证券外部评级(外部评级:直接映射)
        				,''                              						 AS STOCKCODE                --股票/股指代码(默认：NULL)
        				,''                              						 AS STOCKMARKET              --交易市场(默认：NULL)
        				,''                              						 AS EXCHANGEAREA             --交易地区(默认：NULL)
        				,''                              						 AS STRUCTURALEXPOFLAG       --是否结构性敞口(默认：NULL)
        				,'0'                             						 AS OPTIONUNDERLYINGFLAG     --是否期权基础工具(默认：否 1 是 0 否)
        				,''																		 			 AS OPTIONUNDERLYINGNAME		 --期权基础工具名称
        				,''                              						 AS OPTIONID                 --期权工具ID(默认：NULL)
        				,''                              						 AS VOLATILITY               --波动率(默认：NULL)
        				,T2.STARTDATE                    						 AS STARTDATE                --起始日期(起始日期:直接映射)
        				,T2.DUEDATE                      						 AS DUEDATE                  --到期日期(到期日期:直接映射)
        				,T2.ORIGINALMATURITY             						 AS ORIGINALMATURITY         --原始期限(原始期限:直接映射)
        				,T2.RESIDUALM                    						 AS RESIDUALM                --剩余期限(剩余期限:直接映射)
        				,T2.NEXTREPRICEDATE              						 AS NEXTREPRICEDATE          --下次重定价日(下次重定价日:直接映射)
        				,T2.NEXTREPRICEM                 						 AS NEXTREPRICEM             --下次重定价期限(下次重定价期限:直接映射)
        				,T2.RATETYPE                     						 AS RATETYPE                 --利率类型(利率类型:直接映射)
        				,T2.EXECUTIONRATE                						 AS COUPONRATE               --票面利率(执行利率:直接映射)
        				,T2.MODIFIEDDURATION             						 AS MODIFIEDDURATION         --修正久期(修正久期:直接映射)
        				,'01'                            						 AS POSITIONTYPE             --头寸属性(默认：多头 PositionType 头寸属性：01 多头)
        				,T1.BOOKBALANCE                  						 AS POSITION                 --头寸(账面余额:直接映射)
        				,T1.CURRENCY                     						 AS CURRENCY                 --币种(币种:直接映射)
        				,''																					 AS OptionUnderlyingType		 --期权基础工具类型

    FROM				RWA_DEV.RWA_LC_TRADBONDPOSITION T1	             		 					--交易债券头寸信息表
	  INNER JOIN 	RWA_DEV.RWA_LC_BONDINFO T2											 							--债券信息表
	  ON 					T1.BONDID = T2.BONDID
	  LEFT JOIN		RWA_DEV.ZGS_ATBOND T3
	  ON					T1.BONDID = T3.C_BOND_CODE
	  AND					T3.DATANO = p_data_dt_str
	  LEFT  JOIN	RWA.ORG_INFO T4																								--RWA机构信息表
	  ON					T1.TRANORGID = T4.ORGID
	  LEFT  JOIN	RWA.RWA_WP_COUNTRYRATING T5																		--国家评级表
	  ON					T3.C_ISSUER_REGCOUNTRY_CODE = T5.COUNTRYCODE
	  AND					T5.ISINUSE = '1'
	  ;

	  COMMIT;

	  dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_LC_MARKETEXPOSURESTD',cascade => true);

    --DBMS_OUTPUT.PUT_LINE('结束：导入【市场风险标准法风险暴露表】' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_LC_MARKETEXPOSURESTD;
    --Dbms_output.Put_line('RWA_DEV.RWA_LC_MARKETEXPOSURESTD表当前插入的理财系统-债券理财投资(市场风险)-标准法暴露记录为: ' || v_count || ' 条');



    --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功' || '-' || v_count;

    --定义异常
    EXCEPTION WHEN OTHERS THEN
        --DBMS_OUTPUT.PUT_LINE('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '市场风险标准法暴露信息('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;

    RETURN;

END PRO_RWA_LC_MARKETEXPOSURESTD;
/

