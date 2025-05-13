CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_XD_EXPOSURE(
                             p_data_dt_str  IN  VARCHAR2,    --数据日期
                             p_po_rtncode   OUT  VARCHAR2,    --返回编号
                             p_po_rtnmsg    OUT VARCHAR2    --返回描述
)
  /*
    存储过程名称:PRO_RWA_XD_EXPOSURE
    实现功能:信贷系统，将据相关信息全量导入RWA接口表信用风险暴露表中
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :YUSJ
    编写时间:2016-04-15
    单  位  :上海安硕信息技术股份有限公司
    源  表1  :NCM_BUSINESS_CONTRACT|授信业务合同表
    源  表2  :NCM_BUSINESS_DUEBILL|授信业务借据信息表
    源  表3  :NCM_ORG_INFO|机构信息表
    源  表4  :NCM_BUSINESS_TYPE|业务品种信息表
    源  表5  :NCM_CUSTOMER_INFO|客户基本信息记录
    源  表6  :NCM_CODE_LIBRARY|代码库
    源  表7  :NCM_ORG_CONTRAST|核心机构对照关系表
    目标表  :RWA_XD_EXPOSURE|信息管理系统-借据-对公
    辅助表  :无
    变更记录(修改人|修改时间|修改内容):
         pxl 2019/04/11 调整13100001 逾期贷款-微粒贷逾期贷款 取数逻辑弃用老核心逻辑
    
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  --v_pro_name VARCHAR2(200) := 'PRO_RWA_XD_EXPOSURE';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count1 INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*初始化对公客户评级信息表*/
    --临时表只保存一期数据，插入之前删除表数据
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_TEMP_PDLEVEL';

    --插入对公客户当期评级信息
    INSERT INTO RWA_DEV.RWA_TEMP_PDLEVEL(
                 CUSTID                           --01客户ID
                ,CUSTNAME                         --02客户名称
                ,ORGCERTCODE                      --03组织机构代码
                ,MODELID                          --04模型ID
                ,PDCODE                           --05PD等级
                ,PDLEVEL                          --06PD等级代码
                ,PDADJCODE                        --07PD调整等级
                ,PDADJLEVEL                       --08PD调整等级代码
                ,PD                               --09违约概率
                ,PDVAVLIDDATE                     --10评级更新时间(违约时间)
    )
    SELECT T.CUSTID,                              --01客户ID
           T.CUSTNAME,                            --02客户名称
           T.ORGCERTCODE,                         --03组织机构代码
           T.MODELCODE     AS MODELID,            --04模型ID
           T.PDLEVEL      AS PDCODE,              --05PD等级
           TC.ITEMNO      AS PDLEVEL,             --06PD等级代码
           T.PDADJLEVEL   AS PDADJCODE,           --07PD调整等级
           TL.ITEMNO      AS PDADJLEVEL,          --08PD调整等级代码
           T.PD,                                  --09违约概率
           T.PDVAVLIDDATE                         --10评级更新时间(违约时间)
		  FROM (SELECT T2.CUSTID,
		               T2.CUSTNAME,
		               T2.ORGCERTCODE,
		               T1.MODELCODE,
		               T1.PDLEVEL,
		               T1.PDADJLEVEL,
		               T3.PDVALUE   AS PD,
		               SUBSTR(REPLACE(T1.PDVAVLIDDATE,'/',''),1,8) AS PDVAVLIDDATE,
                   --一个客户在一段时间内有多次评级，取PDVAVLIDDATE最大的一次（最后一次评级）
		               ROW_NUMBER() OVER(PARTITION BY T2.CUSTID ORDER BY T1.PDVAVLIDDATE DESC) RM
		          FROM RWA_DEV.IRS_CR_CUSTOMER_RATE T1 --客户评级表(对公)
		         INNER JOIN RWA_DEV.IRS_IT_CUSTOMER T2 --客户信息表（信贷系统）(对公)
		                 ON T1.T_IT_CUSTOMER_ID = T2.Custid
		                AND T2.DATANO = p_data_dt_str
		          LEFT JOIN RWA_DEV.IRS_MD_SCALE_PD T3 --标尺违约概率
		                 ON T1.PDADJLEVEL = T3.PDLEVEL
		                AND T3.DATANO = p_data_dt_str
		              WHERE T1.STATUS = 'CO'
		              AND T1.PDADJLEVEL <> 'N'
		              AND T1.PDADJLEVEL IS NOT NULL
		              AND SUBSTR(REPLACE(T1.PDVAVLIDDATE,'/',''),1,8) <= p_data_dt_str
		              AND T1.DATANO = p_data_dt_str) T
		  LEFT JOIN RWA.CODE_LIBRARY TC
		       ON T.PDLEVEL = TC.ITEMNAME
		       AND TC.CODENO = 'IRating'
		       AND TC.ITEMNO LIKE '01%'
		  LEFT JOIN RWA.CODE_LIBRARY TL
		       ON T.PDADJLEVEL = TL.ITEMNAME
		       AND TL.CODENO = 'IRating'
		       AND TL.ITEMNO LIKE '01%'
		 WHERE T.RM = 1
      ;
      COMMIT;

    --插入同业手工补录客户的评级信息
    INSERT INTO RWA_DEV.RWA_TEMP_PDLEVEL(
                 CUSTID                       --01客户ID
                ,CUSTNAME                     --02客户名称
                ,ORGCERTCODE                  --03组织机构代码
                ,MODELID                      --04模型ID
                ,PDCODE                       --05PD等级
                ,PDLEVEL                      --06PD等级代码
                ,PDADJCODE                    --07PD调整等级
                ,PDADJLEVEL                   --08PD调整等级代码
                ,PD                           --09违约概率
                ,PDVAVLIDDATE                 --10评级更新时间(违约时间)
    )
    SELECT      T1.CUSTOMERID                 --01客户ID
               ,T1.CUSTOMERNAME               --02客户名称
               ,T1.CERTID                     --03组织机构代码
               ,'CQM01'                       --04模型ID
               ,T2.ITEMNAME                   --05PD等级
               ,T1.RATINGCODE                 --06PD等级代码
               ,T2.ITEMNAME                   --07PD调整等级
               ,T1.RATINGCODE                 --08PD调整等级代码
               ,T3.PDVALUE                    --09违约概率
               ,NULL                          --10评级更新时间(违约时间)
      FROM RWA.RWA_CD_BANKRATING T1   --银行评级客户清单
 LEFT JOIN		RWA.CODE_LIBRARY T2				--获取内部平代码对应的字符代码
	    ON					T1.RATINGCODE = T2.ITEMNO
	    AND					T2.CODENO = 'IRating'
 LEFT JOIN		RWA_DEV.IRS_MD_SCALE_PD	T3	--标尺违约概率
	    ON					T2.ITEMNAME = T3.PDLEVEL
	    AND					T3.DATANO = P_DATA_DT_STR
	  WHERE T1.STATUS = '1'
	    AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_TEMP_PDLEVEL T4 WHERE T1.CUSTOMERID=T4.CUSTID)
	  ;
	  COMMIT;

    --整理表信息
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_TEMP_PDLEVEL',cascade => true);

    --临时表只保存一期数据，插入之前删除表数据
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TEMP_LGDLEVEL';

      --插入零售债项当期评级信息
		--个贷
    INSERT INTO RWA_DEV.RWA_TEMP_LGDLEVEL
		  (BUSINESSTYPE,            --01业务品种
		   BUSINESSID,              --02业务ID
		   CUSTNAME,                --03客户名称
		   CARDTYPE,                --04客户证件类型
		   CARDNO,                  --05客户证件号码
		   PDCODE,                  --06PD分池编号
		   PDVALUE,                 --07PD值
		   PDMODELNAME,             --08PD模型名称
		   PDMODELCODE,             --09PD模型代码
		   LGDCODE,                 --10LGD分池编号
		   LGDVALUE,                --11LGD值
		   LGDMODELNAME,            --12LGD模型名称
		   LGDMODELCODE,            --13LGD模型代码
		   BEELVALUE,               --14BEEL值
		   CCFCODE,                 --15CCF分池编号
		   CCFVALUE,                --16CCF值
		   CCFMODELNAME,            --17CCF模型名称
		   CCFMODELCODE,            --18CCF模型代码
		   RISK_EXPOSURE,           --19风险暴露类型
		   DEFAULTFLAG,             --20是否违约
		   MOB,                     --21账龄
		   UPDATETIME)              --22评级更新时间(违约时间)
		  SELECT BUSINESSTYPE,
		         BUSINESSID,
		         CUSTNAME,
		         CARDTYPE,
		         CARDNO,
		         PDCODE,
		         PDVALUE,
		         PDMODELNAME,
		         PDMODELCODE,
		         LGDCODE,
		         LGDVALUE,
		         LGDMODELNAME,
		         LGDMODELCODE,
		         BEELVALUE,
		         CCFCODE,
		         CCFVALUE,
		         CCFMODELNAME,
		         CCFMODELCODE,
		         RISK_EXPOSURE,
		         DEFAULTFLAG,
		         MOB,
		         UPDATETIME
		    FROM (SELECT TR.BUSSCODE AS BUSINESSTYPE,                                            --01业务品种
                     TR.CONTRACTID AS BUSINESSID,                                            --02业务ID
                     TR.CUSTNAME,                                                            --03客户名称
                     TR.CARDTYPE,                                                            --04客户证件类型
                     TR.CARDNO,                                                              --05客户证件号码
                     TR.PDCODE,                                                              --06PD分池编号
                     CASE WHEN TR.PDVALUE=0 THEN NULL ELSE TR.PDVALUE END AS PDVALUE,        --07PD值
                     TR.PDMODELNAME,                                                         --08PD模型名称
                     TR.PDMODELCODE,                                                         --09PD模型代码
                     TR.LGDCODE,                                                             --10LGD分池编号
                     CASE WHEN TR.LGDVALUE=0 THEN NULL ELSE TR.LGDVALUE END AS LGDVALUE,     --11LGD值
                     TR.LGDMODELNAME,                                                        --12LGD模型名称
                     TR.LGDMODELCODE,                                                        --13LGD模型代码
                     CASE WHEN TR.BEELVALUE=0 THEN NULL ELSE TR.BEELVALUE END AS BEELVALUE,  --14BEEL值
                     TR.CCFCODE,                                                             --15CCF分池编号
                     CASE WHEN TR.CCFVALUE=0 THEN NULL ELSE TR.CCFVALUE END AS CCFVALUE,     --16CCF值
                     TR.CCFMODELNAME,                                                        --17CCF模型名称
                     TR.CCFMODELCODE,                                                        --18CCF模型代码
                     TR.RISK_EXPOSURE,                                                       --19风险暴露类型
		                 CASE
		                   WHEN TR.PDDEFAULTFLAG = '1' OR TR.LGDDEFAULTFLAG = '1' THEN
		                    '1'
		                   ELSE
		                    '0'
		                 END AS DEFAULTFLAG,                                                     --20是否违约
		                 TG.MOB,                                                                 --21账龄
		                 REPLACE(SUBSTR(TR.UPDATETIME,1,10),'-','') AS UPDATETIME,               --22评级更新时间(违约时间)
		                 --取最后一次的更新时间
                     ROW_NUMBER() OVER(PARTITION BY TR.CONTRACTID ORDER BY TR.UPDATETIME DESC) AS RECORDNUM
		            FROM RWA_DEV.RRS_MC_FC_RESULT_HIS TR --零售分池模型结果历史表
		       LEFT JOIN RWA_DEV.RRS_PER_LOAN_GRADE_H TG --个人贷款评级信息历史表
		              ON TR.BUSSINESS_SEQ = TG.ID
		             AND TG.DATANO = p_data_dt_str
		           WHERE --TO_DATE(TR.UPDATETIME, 'YYYY-MM-DD HH24:MI:SS') <= TO_DATE(p_data_dt_str, 'YYYYMMDD')
		                 TR.BUSSCODE <> 'CREDITCARD'
		             AND TR.DATANO = p_data_dt_str)
		   WHERE RECORDNUM = 1 AND PDVALUE>0
		;
		COMMIT;

		--信用卡
		INSERT INTO RWA_DEV.RWA_TEMP_LGDLEVEL
		  (BUSINESSTYPE,
		   BUSINESSID,
		   CUSTNAME,
		   CARDTYPE,
		   CARDNO,
		   PDCODE,
		   PDVALUE,
		   PDMODELNAME,
		   PDMODELCODE,
		   LGDCODE,
		   LGDVALUE,
		   LGDMODELNAME,
		   LGDMODELCODE,
		   BEELVALUE,
		   CCFCODE,
		   CCFVALUE,
		   CCFMODELNAME,
		   CCFMODELCODE,
		   RISK_EXPOSURE,
		   DEFAULTFLAG,
		   MOB,
		   UPDATETIME)
		  SELECT BUSINESSTYPE,
		         BUSINESSID,
		         CUSTNAME,
		         CARDTYPE,
		         CARDNO,
		         PDCODE,
		         PDVALUE,
		         PDMODELNAME,
		         PDMODELCODE,
		         LGDCODE,
		         LGDVALUE,
		         LGDMODELNAME,
		         LGDMODELCODE,
		         BEELVALUE,
		         CCFCODE,
		         CCFVALUE,
		         CCFMODELNAME,
		         CCFMODELCODE,
		         RISK_EXPOSURE,
		         DEFAULTFLAG,
		         MOB,
		         UPDATETIME
		    FROM (SELECT TR.BUSSCODE AS BUSINESSTYPE,
		                 TR.CREDITCARDNO AS BUSINESSID,
		                 TR.CUSTNAME,
		                 TR.CARDTYPE,
		                 TR.CARDNO,
		                 TR.PDCODE,
		                 CASE WHEN TR.PDVALUE=0 THEN NULL ELSE TR.PDVALUE END AS PDVALUE,
		                 TR.PDMODELNAME,
		                 TR.PDMODELCODE,
		                 TR.LGDCODE,
		                 CASE WHEN TR.LGDVALUE=0 THEN NULL ELSE TR.LGDVALUE END AS LGDVALUE,
		                 TR.LGDMODELNAME,
		                 TR.LGDMODELCODE,
		                 CASE WHEN TR.BEELVALUE=0 THEN NULL ELSE TR.BEELVALUE END AS BEELVALUE,
		                 TR.CCFCODE,
		                 CASE WHEN TR.CCFVALUE=0 THEN NULL ELSE TR.CCFVALUE END AS CCFVALUE,
		                 TR.CCFMODELNAME,
		                 TR.CCFMODELCODE,
		                 TR.RISK_EXPOSURE,
		                 CASE
		                   WHEN TR.PDDEFAULTFLAG = '1' OR TR.LGDDEFAULTFLAG = '1' THEN
		                    '1'
		                   ELSE
		                    '0'
		                 END AS DEFAULTFLAG,
		                 TG.MOB,
		                 REPLACE(SUBSTR(TR.UPDATETIME,1,10),'-','') AS UPDATETIME,
		                 ROW_NUMBER() OVER(PARTITION BY TR.CREDITCARDNO ORDER BY TR.UPDATETIME DESC) AS RECORDNUM
		            FROM RWA_DEV.RRS_MC_FC_RESULT_HIS TR --零售分池模型结果历史表
		            LEFT JOIN RWA_DEV.RRS_PER_CARD_GRADE_H TG --个人信用卡评级信息历史表
		              ON TR.BUSSINESS_SEQ = TG.ID
		             AND TG.DATANO = P_DATA_DT_STR
		           WHERE --TO_DATE(TR.UPDATETIME, 'YYYY-MM-DD HH24:MI:SS') <= TO_DATE(p_data_dt_str, 'YYYYMMDD')
		                 TR.BUSSCODE = 'CREDITCARD'
		             AND TR.DATANO = p_data_dt_str)
		   WHERE RECORDNUM = 1 AND PDVALUE>0
	  ;

	  COMMIT;

	  --整理表信息
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_TEMP_LGDLEVEL',cascade => true);

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_XD_EXPOSURE';    

    --2.将满足条件的数据从源表插入到目标表中
    /*1 插入 信贷系统对公借据 到目标表*/ 
    INSERT INTO RWA_DEV.RWA_XD_EXPOSURE(
               DATADATE         --01 数据日期
              ,DATANO           --02 数据流水号
              ,EXPOSUREID       --03 风险暴露ID
              ,DUEID            --04 债项ID
              ,SSYSID           --05 源系统ID
              ,CONTRACTID       --06 合同ID
              ,CLIENTID         --07 参与主体ID
              ,SORGID           --08 源机构ID
              ,SORGNAME         --09 源机构名称
              ,ORGSORTNO        --10 所属机构排序号
              ,ORGID            --11 所属机构ID
              ,ORGNAME          --12 所属机构名称
              ,ACCORGID         --13 账务机构ID
              ,ACCORGNAME       --14 账务机构名称
              ,INDUSTRYID       --15 所属行业代码
              ,INDUSTRYNAME     --16 所属行业名称
              ,BUSINESSLINE     --17 业务条线
              ,ASSETTYPE        --18 资产大类
              ,ASSETSUBTYPE     --19 资产小类
              ,BUSINESSTYPEID   --20 业务品种代码
              ,BUSINESSTYPENAME --21 业务品种名称
              ,CREDITRISKDATATYPE    --22 信用风险数据类型
              ,ASSETTYPEOFHAIRCUTS   --23 折扣系数对应资产类别
              ,BUSINESSTYPESTD       --24 权重法业务类型
              ,EXPOCLASSSTD          --25 权重法暴露大类
              ,EXPOSUBCLASSSTD       --26 权重法暴露小类
              ,EXPOCLASSIRB          --27 内评法暴露大类
              ,EXPOSUBCLASSIRB       --28 内评法暴露小类
              ,EXPOBELONG            --29 暴露所属标识
              ,BOOKTYPE              --30 账户类别
              ,REGUTRANTYPE          --31 监管交易类型
              ,REPOTRANFLAG          --32 回购交易标识
              ,REVAFREQUENCY         --33 重估频率
              ,CURRENCY              --34 币种
              ,NORMALPRINCIPAL       --35 正常本金余额
              ,OVERDUEBALANCE        --36 逾期余额
              ,NONACCRUALBALANCE     --37 非应计余额
              ,ONSHEETBALANCE        --38 表内余额
              ,NORMALINTEREST        --39 正常利息
              ,ONDEBITINTEREST       --40 表内欠息
              ,OFFDEBITINTEREST      --41 表外欠息
              ,EXPENSERECEIVABLE     --42 应收费用
              ,ASSETBALANCE          --43 资产余额
              ,ACCSUBJECT1           --44 科目一
              ,ACCSUBJECT2           --45 科目二
              ,ACCSUBJECT3           --46 科目三
              ,STARTDATE             --47 起始日期
              ,DUEDATE               --48 到期日期
              ,ORIGINALMATURITY      --49 原始期限
              ,RESIDUALM             --50 剩余期限
              ,RISKCLASSIFY          --51 风险分类
              ,EXPOSURESTATUS        --52 风险暴露状态
              ,OVERDUEDAYS           --53 逾期天数
              ,SPECIALPROVISION      --54 专项准备金
              ,GENERALPROVISION      --55 一般准备金
              ,ESPECIALPROVISION     --56 特别准备金
              ,WRITTENOFFAMOUNT      --57 已核销金额
              ,OFFEXPOSOURCE         --58 表外暴露来源
              ,OFFBUSINESSTYPE       --59 表外业务类型
              ,OFFBUSINESSSDVSSTD    --60 权重法表外业务类型细分
              ,UNCONDCANCELFLAG      --61 是否可随时无条件撤销
              ,CCFLEVEL              --62 信用转换系数级别
              ,CCFAIRB               --63 高级法信用转换系数
              ,CLAIMSLEVEL           --64 债权级别
              ,BONDFLAG              --65 是否为债券
              ,BONDISSUEINTENT       --66 债券发行目的
              ,NSUREALPROPERTYFLAG   --67 是否非自用不动产
              ,REPASSETTERMTYPE      --68 抵债资产期限类型
              ,DEPENDONFPOBFLAG      --69 是否依赖于银行未来盈利
              ,IRATING               --70 内部评级
              ,PD                    --71 违约概率
              ,LGDLEVEL              --72 违约损失率级别
              ,LGDAIRB               --73 高级法违约损失率
              ,MAIRB                 --74 高级法有效期限
              ,EADAIRB               --75 高级法违约风险暴露
              ,DEFAULTFLAG           --76 违约标识
              ,BEEL                  --77 已违约暴露预期损失比率
              ,DEFAULTLGD            --78 已违约暴露违约损失率
              ,EQUITYEXPOFLAG        --79 股权暴露标识
              ,EQUITYINVESTTYPE      --80 股权投资对象类型
              ,EQUITYINVESTCAUSE     --81 股权投资形成原因
              ,SLFLAG                --82 专业贷款标识
              ,SLTYPE                --83 专业贷款类型
              ,PFPHASE               --84 项目融资阶段
              ,REGURATING            --85 监管评级
              ,CBRCMPRATINGFLAG      --86 银监会认定评级是否更为审慎
              ,LARGEFLUCFLAG         --87 是否波动性较大
              ,LIQUEXPOFLAG          --88 是否清算过程中风险暴露
              ,PAYMENTDEALFLAG       --89 是否货款对付模式
              ,DELAYTRADINGDAYS      --90 延迟交易天数
              ,SECURITIESFLAG        --91 有价证券标识
              ,SECUISSUERID          --92 证券发行人ID
              ,RATINGDURATIONTYPE    --93 评级期限类型
              ,SECUISSUERATING       --94 证券发行等级
              ,SECURESIDUALM         --95 证券剩余期限
              ,SECUREVAFREQUENCY     --96 证券重估频率
              ,CCPTRANFLAG           --97 是否中央交易对手相关交易
              ,CCPID                 --98 中央交易对手ID
              ,QUALCCPFLAG           --99 是否合格中央交易对手
              ,BANKROLE              --100 银行角色
              ,CLEARINGMETHOD        --101 清算方式
              ,BANKASSETFLAG         --102 是否银行提交资产
              ,MATCHCONDITIONS       --103 符合条件情况
              ,SFTFLAG               --104 证券融资交易标识
              ,MASTERNETAGREEFLAG    --105 净额结算主协议标识
              ,MASTERNETAGREEID      --106 净额结算主协议ID
              ,SFTTYPE               --107 证券融资交易类型
              ,SECUOWNERTRANSFLAG    --108 证券所有权是否转移
              ,OTCFLAG               --109 场外衍生工具标识
              ,VALIDNETTINGFLAG      --110 有效净额结算协议标识
              ,VALIDNETAGREEMENTID   --111 有效净额结算协议ID
              ,OTCTYPE               --112 场外衍生工具类型
              ,DEPOSITRISKPERIOD     --113 保证金风险期间
              ,MTM                   --114 重置成本
              ,MTMCURRENCY           --115 重置成本币种
              ,BUYERORSELLER         --116 买方卖方
              ,QUALROFLAG            --117 合格参照资产标识
              ,ROISSUERPERFORMFLAG   --118 参照资产发行人是否能履约
              ,BUYERINSOLVENCYFLAG   --119 信用保护买方是否破产
              ,NONPAYMENTFEES        --120 尚未支付费用
              ,RETAILEXPOFLAG        --121 零售暴露标识
              ,RETAILCLAIMTYPE       --122 零售债权类型
              ,MORTGAGETYPE          --123 住房抵押贷款类型
              ,EXPONUMBER            --124 风险暴露个数
              ,LTV                   --125 贷款价值比
              ,AGING                 --126 账龄
              ,NEWDEFAULTDEBTFLAG    --127 新增违约债项标识
              ,PDPOOLMODELID         --128 PD分池模型ID
              ,LGDPOOLMODELID        --129 LGD分池模型ID
              ,CCFPOOLMODELID        --130 CCF分池模型ID
              ,PDPOOLID              --131 所属PD池ID
              ,LGDPOOLID             --132 所属LGD池ID
              ,CCFPOOLID             --133 所属CCF池ID
              ,ABSUAFLAG             --134 资产证券化基础资产标识
              ,ABSPOOLID             --135 证券化资产池ID
              ,GROUPID               --136 分组编号
              ,DefaultDate           --137 违约时点
              ,ABSPROPORTION         --138 资产证券化比重
              ,DEBTORNUMBER          --139 借款人个数
              ,flag
    
    )
    SELECT
                TO_DATE(T1.DATANO,'YYYYMMDD')                                           AS DATADATE                -- 数据日期
                ,T1.DATANO                                                              AS DATANO                  -- 数据流水号
                ,T1.SERIALNO                                                            AS EXPOSUREID              -- 风险暴露ID
                ,T1.SERIALNO                                                            AS DUEID                   -- 债项ID
                ,'XD'                                                                   AS SSYSID                  -- 源系统ID
                ,T1.RELATIVESERIALNO2                                                   AS CONTRACTID              -- 合同ID
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN DECODE(T3.LINETYPE,'0030','XN-GRKH','XN-YBGS') 								--若客户ID为空，条线为个人就虚拟为个人客户，否则为一般公司
                			ELSE T1.CUSTOMERID
                 END					                                                          AS CLIENTID                -- 参与主体ID
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID) 为了数据校验报告通过，把所有@开头的机构都改为总行
                -- 后续数仓加工完成后再改回来
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end                                                                     AS SORGID                  -- 源机构ID
                --,T4.ORGNAME                                                            AS SORGNAME                -- 源机构名称
                ,nvl(T4.ORGNAME,'总行')                                                  
                --,T4.SORTNO                                                              AS ORGSORTNO               -- 所属机构排序号
                ,nvl(T4.SORTNO,'1010')
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS ORGID                   -- 所属机构ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                --,T4.ORGNAME                                                             AS ORGNAME                 -- 所属机构名称
                ,nvl(T4.ORGNAME,'总行')
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS ACCORGID                -- 账务机构ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                --,T4.ORGNAME                                                             AS ACCORGNAME              -- 账务机构名称
                ,nvl(T4.ORGNAME,'总行')
                ,NVL(T3.DIRECTION,CPS.DIRECTION)                                        AS INDUSTRYID              -- 所属行业代码
                ,NVL(T5.ITEMNAME,CL.ITEMNAME)                                           AS INDUSTRYNAME            -- 所属行业名称
                ,CASE WHEN T1.BUSINESSCURRENCY<>'CNY' THEN '0102'     --外币的表内业务   大中-贸金部
                      WHEN T1.BUSINESSTYPE IN('10352010','10352020') THEN '0101'--银行承兑汇票承兑（低风险）、银行承兑汇票承兑（非低风险）	大中-公司 by chengang
                      WHEN T1.BUSINESSTYPE IN('10302010','10302015','10302020') THEN '0401'  --贴现业务        同业金融市场部
                      WHEN T3.LINETYPE='0010' THEN '0101'
                	    WHEN T3.LINETYPE='0020' THEN '0201'
                	    WHEN T3.LINETYPE='0030' THEN '0301'
                	    WHEN T3.LINETYPE='0040' THEN '0401'
                	    ELSE '0101'
                 END                                                                    AS BUSINESSLINE            -- 条线
                ,''                                                                     AS ASSETTYPE               -- 资产大类
                ,''                                                                     AS ASSETSUBTYPE            -- 资产小类
                ,T1.BUSINESSTYPE                                                        AS BUSINESSTYPEID          -- 业务品种代码
                ,T2.TYPENAME                                                            AS BUSINESSTYPENAME        -- 业务品种名称
                /*,CASE WHEN T1.SERIALNO='20170125c0000373' THEN '02'
                	    ELSE '01'
                 END                                                                    AS CREDITRISKDATATYPE      -- 信用风险数据类型          01-一般非零售
                */
                ,CASE WHEN T16.CUSTOMERTYPE = '0310' OR T16.CERTTYPE LIKE 'Ind%'
                      THEN '02' --零售
                      ELSE '01' --非零售
                  END                                                              			AS CREDITRISKDATATYPE  		 --信用风险数据类型
                ,'01'                                                                   AS ASSETTYPEOFHAIRCUTS     -- 折扣系数对应资产类别     01-现金及现金等价物
                ,''                                                                     AS BUSINESSTYPESTD         -- 权重法业务类型
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN '0112' 																	 --若客户ID为空，默认 其他(0112)
                			ELSE ''
                 END                                                                    AS EXPOCLASSSTD            -- 权重法暴露大类
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN DECODE(T3.LINETYPE,'0030','011215','011216') 												 --若客户ID为空，条线为个人就默认 其他适用75%风险权重的资产(011215)，否则默认 其他适用100%风险权重的资产(011216)
                			ELSE ''
                 END                                                                    AS EXPOSUBCLASSSTD         -- 权重法暴露小类
                ,SUBSTR(T10.DITEMNO,1,4)                                                AS EXPOCLASSIRB            -- 内评法暴露大类
                ,T10.DITEMNO                                                            AS EXPOSUBCLASSIRB         -- 内评法暴露小类
                ,CASE WHEN T2.OFFSHEETFLAG='EntOff' THEN '02'   --  02-表外
                      ELSE '01'                                 --  01-表内
                 END                                                                   AS EXPOBELONG              -- 暴露所属标识
                ,'01'                                                                  AS BOOKTYPE                -- 账户类别           01-银行账户
                ,'03'                                                                  AS REGUTRANTYPE            -- 监管交易类型      03-抵押贷款
                ,'0'                                                                   AS REPOTRANFLAG            -- 回购交易标识       0-否
                ,1                                                                     AS REVAFREQUENCY           -- 重估频率
                ,NVL(T1.BUSINESSCURRENCY,'CNY')                                        AS CURRENCY                -- 币种
                ,T31.Balance                                                     AS NORMALPRINCIPAL         -- 正常本金余额
                ,0                                                                     AS OVERDUEBALANCE          -- 逾期余额
                ,0                                                                     AS NONACCRUALBALANCE       -- 非应计余额
                ,T31.Balance                                                     AS ONSHEETBALANCE          -- 表内余额
                ,0                                                                     AS NORMALINTEREST          -- 正常利息
                ,0                                                                     AS ONDEBITINTEREST         -- 表内欠息
                ,0                                                                     AS OFFDEBITINTEREST        -- 表外欠息
                ,0                                                                     AS EXPENSERECEIVABLE       -- 应收费用
                ,T31.Balance                                                     AS ASSETBALANCE            -- 资产余额
                ,CASE 
                   WHEN T1.SUBJECTNO = '@01010502' THEN '13050100' --进口押汇科目特殊处理
                   WHEN T1.SUBJECTNO = '@01010501' THEN '13050200' --出口押汇科目特殊处理
                   WHEN T1.SUBJECTNO = '@01010521' THEN '13050502' --出口押汇科目特殊处理
                   when substr(t1.businesstype,1,6) = '103520' then '70020000' --承兑汇票科目特殊处理
                 ELSE T1.SUBJECTNO END                                                 AS ACCSUBJECT1             -- 科目一
                ,''                                                                    AS ACCSUBJECT2             -- 科目二
                ,''                                                                    AS ACCSUBJECT3             -- 科目三
                ,NVL(T1.PUTOUTDATE,T3.PUTOUTDATE)                                      AS STARTDATE               -- 起始日期
                ,NVL(T1.ACTUALMATURITY,T3.MATURITY)                                    AS DUEDATE                 -- 到期日期
                ,CASE WHEN (TO_DATE(NVL(T1.ACTUALMATURITY,T3.MATURITY),'YYYYMMDD')-TO_DATE(NVL(T1.PUTOUTDATE,T3.PUTOUTDATE),'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(NVL(T1.ACTUALMATURITY,T3.MATURITY),'YYYYMMDD')-TO_DATE(NVL(T1.PUTOUTDATE,T3.PUTOUTDATE),'YYYYMMDD'))/365
                END                                                                    AS ORIGINALMATURITY        -- 原始期限
                ,CASE WHEN (TO_DATE(NVL(T1.ACTUALMATURITY,T3.MATURITY),'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(NVL(T1.ACTUALMATURITY,T3.MATURITY),'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365
                END                                                                    AS RESIDUALM               -- 剩余期限
                ,CASE WHEN NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))='A' THEN '01'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))='B' THEN '02'       --十二级分类转为五级分类
                      WHEN NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))='C' THEN '03'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))='D' THEN '04'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))='E' THEN '05'
                      ELSE NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))
                 END                                                                   AS RISKCLASSIFY            -- 风险分类
                ,'01'                                                                  AS EXPOSURESTATUS          -- 风险暴露状态  01-正常
                ,T1.OVERDUEDAYS                                                        AS OVERDUEDAYS             -- 逾期天数
                ,0                                                                     AS SPECIALPROVISION        -- 专项准备金-规则处理
                ,0                                                                     AS GENERALPROVISION        -- 一般准备金
                ,0                                                                     AS ESPECIALPROVISION       -- 特别准备金
                ,T1.CANCELSUM                                                          AS WRITTENOFFAMOUNT        -- 已核销金额
                ,CASE WHEN T2.OFFSHEETFLAG='EntOff'  THEN '03'                         --  03-实际表外业务
                      ELSE ''
                 END                                                                   AS OffExpoSource           -- 表外暴露来源
                ,''                                                                    AS OffBusinessType         -- 表外业务类型
                ,''                                                                    AS OffBusinessSdvsSTD      -- 权重法表外业务类型细分
                ,'1'                                                                   AS UncondCancelFlag        -- 是否可随时无条件撤销
                ,''                                                                    AS CCFLevel                -- 信用转换系数级别
                ,NULL                                                                  AS CCFAIRB                 -- 高级法信用转换系数
                ,'01'                                                                  AS CLAIMSLEVEL             -- 债权级别
                ,'0'                                                                   AS BONDFLAG                -- 是否为债券
                ,'02'                                                                  AS BONDISSUEINTENT         -- 债券发行目的
                ,'0'                                                                   AS NSUREALPROPERTYFLAG     -- 是否非自用不动产
                ,''                                                                    AS REPASSETTERMTYPE        -- 抵债资产期限类型
                ,'0'                                                                   AS DEPENDONFPOBFLAG        -- 是否依赖于银行未来盈利
                ,T6.PDADJLEVEL                                                         AS IRATING                 -- 内部评级
                ,T6.PD                                                                 AS PD                      -- 违约概率
                ,''                                                                    AS LGDLEVEL                -- 违约损失率级别
                ,0                                                                     AS LGDAIRB                 -- 高级法违约损失率
                ,0                                                                     AS MAIRB                   -- 高级法有效期限
                ,T31.Balance                                                     AS EADAIRB                 -- 高级法违约风险暴露
                ,CASE WHEN T6.PDADJCODE='D' THEN '1'
                      ELSE '0'
                 END                                                                   AS DEFAULTFLAG             -- 违约标识
                ,0                                                                     AS BEEL                    -- 已违约暴露预期损失比率
                ,0                                                                     AS DEFAULTLGD              -- 已违约暴露违约损失率
                ,'0'                                                                   AS EQUITYEXPOFLAG          -- 股权暴露标识
                ,''                                                                    AS EQUITYINVESTTYPE        -- 股权投资对象类型
                ,''                                                                    AS EQUITYINVESTCAUSE       -- 股权投资形成原因
                ,'0'                                                                   AS SLFLAG                  -- 专业贷款标识       专业贷款相关字段一期先赋空
                ,''                                                                    AS SLTYPE                  -- 专业贷款类型
                ,''                                                                    AS PFPHASE                 -- 项目融资阶段
                ,'01'                                                                  AS REGURATING              -- 监管评级
                ,''                                                                    AS CBRCMPRATINGFLAG        -- 银监会认定评级是否更为审慎
                ,''                                                                    AS LARGEFLUCFLAG           -- 是否波动性较大
                ,'0'                                                                   AS LIQUEXPOFLAG            -- 是否清算过程中风险暴露
                ,''                                                                    AS PAYMENTDEALFLAG         -- 是否货款对付模式
                ,0                                                                     AS DELAYTRADINGDAYS        -- 延迟交易天数
                ,'0'                                                                   AS SECURITIESFLAG          -- 有价证券标识
                ,''                                                                    AS SECUISSUERID            -- 证券发行人ID
                ,''                                                                    AS RATINGDURATIONTYPE      -- 评级期限类型
                ,''                                                                    AS SECUISSUERATING         -- 证券发行等级
                ,0                                                                     AS SECURESIDUALM           -- 证券剩余期限
                ,1                                                                     AS SECUREVAFREQUENCY       -- 证券重估频率
                ,'0'                                                                   AS CCPTRANFLAG             -- 是否中央交易对手相关交易
                ,''                                                                    AS CCPID                   -- 中央交易对手ID
                ,'0'                                                                   AS QUALCCPFLAG             -- 是否合格中央交易对手
                ,''                                                                    AS BANKROLE                -- 银行角色
                ,''                                                                    AS CLEARINGMETHOD          -- 清算方式
                ,'0'                                                                   AS BANKASSETFLAG           -- 是否银行提交资产
                ,''                                                                    AS MATCHCONDITIONS         -- 符合条件情况
                ,'0'                                                                   AS SFTFLAG                 -- 证券融资交易标识
                ,''                                                                    AS MASTERNETAGREEFLAG      -- 净额结算主协议标识
                ,''                                                                    AS MASTERNETAGREEID        -- 净额结算主协议ID
                ,''                                                                    AS SFTTYPE                 -- 证券融资交易类型
                ,''                                                                    AS SECUOWNERTRANSFLAG      -- 证券所有权是否转移
                ,'0'                                                                   AS OTCFLAG                 -- 场外衍生工具标识
                ,''                                                                    AS VALIDNETTINGFLAG        -- 有效净额结算协议标识
                ,''                                                                    AS VALIDNETAGREEMENTID     -- 有效净额结算协议ID
                ,''                                                                    AS OTCTYPE                 -- 场外衍生工具类型
                ,''                                                                    AS DEPOSITRISKPERIOD       -- 保证金风险期间
                ,0                                                                     AS MTM                     -- 重置成本
                ,''                                                                    AS MTMCURRENCY             -- 重置成本币种
                ,''                                                                    AS BUYERORSELLER           -- 买方卖方
                ,''                                                                    AS QUALROFLAG              -- 合格参照资产标识
                ,''                                                                    AS ROISSUERPERFORMFLAG     -- 参照资产发行人是否能履约
                ,''                                                                    AS BUYERINSOLVENCYFLAG     -- 信用保护买方是否破产
                ,0                                                                     AS NONPAYMENTFEES          -- 尚未支付费用
                ,'0'                                                                   AS RETAILEXPOFLAG          -- 零售暴露标识
                ,''                                                                    AS RETAILCLAIMTYPE         -- 零售债权类型
                ,''                                                                    AS MORTGAGETYPE            -- 住房抵押贷款类型
                ,1                                                                     AS EXPONUMBER              -- 风险暴露个数
                ,0.8                                                                   AS LTV                     --贷款价值比
                ,0                                                                     AS AGING                   --账龄
                ,''                                                                    AS NEWDEFAULTDEBTFLAG      --新增违约债项标识
                ,''                                                                    AS PDPOOLMODELID           -- PD分池模型ID
                ,''                                                                    AS LGDPOOLMODELID          -- LGD分池模型ID
                ,''                                                                    AS CCFPOOLMODELID          -- CCF分池模型ID
                ,''                                                                    AS PDPOOLID                -- 所属PD池ID
                ,''                                                                    AS LGDPOOLID               -- 所属LGD池ID
                ,''                                                                    AS CCFPOOLID               -- 所属CCF池ID
                ,CASE WHEN T9.PROJECTNO IS NULL THEN '0'
                      ELSE '1'
                 END                                                                   AS ABSUAFLAG           --资产证券化基础资产标识
                ,CASE WHEN T9.PROJECTNO IS NULL THEN ''
                      ELSE T9.PROJECTNO
                 END                                                                   AS ABSPOOLID           --证券化资产池ID
                ,''                                                                    AS GROUPID                 -- 分组编号
                ,CASE WHEN T6.PDADJCODE='D' THEN TO_DATE(T6.PDVAVLIDDATE,'YYYYMMDD')
                      ELSE NULL
                 END                                                                   AS DefaultDate             -- 违约时点
                ,0                                                                     AS ABSPROPORTION           --资产证券化比重
                ,0                                                                     AS DEBTORNUMBER            --借款人个数
                ,'DG'
    FROM 				RWA_DEV.NCM_BUSINESS_DUEBILL T1
    INNER JOIN 	RWA_DEV.NCM_BUSINESS_TYPE T2
    ON 					T1.BUSINESSTYPE = T2.TYPENO
    AND 				T1.DATANO = T2.DATANO
    AND 				T2.ATTRIBUTE1 <> '2'                    --只取对公业务
    AND 				T2.TYPENO NOT LIKE '30%'               	--排除额度类业务
    INNER JOIN 	RWA_DEV.NCM_BUSINESS_CONTRACT T3
    ON 					T1.RELATIVESERIALNO2 = T3.SERIALNO      --对账以借据为准，所以关联合同时，不应该加合同的有效条件
    AND 				T1.DATANO = T3.DATANO
    INNER JOIN 	RWA_DEV.NCM_BUSINESS_HISTORY T31
    ON 					T1.SERIALNO = T31.SERIALNO
    AND 				T31.Balance > 0                   --只取正常本金余额
    AND 				T31.DATANO = P_DATA_DT_STR
               /* rwa_dev.BRD_LOAN_NOR T31                  --支付集市正常贷款
    ON          T1.SERIALNO = T31.CRDT_ACCT_NO
    AND         t31.cur_bal > 0
    AND         t31.datano = P_DATA_DT_STR*/ 
    LEFT JOIN 	RWA.ORG_INFO T4
    ON 				 decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID) = T4.ORGID
    LEFT JOIN 	RWA.CODE_LIBRARY T5
    ON 					T3.DIRECTION = T5.ITEMNO
    AND 				T5.CODENO = 'IndustryType'
    LEFT JOIN 	RWA_DEV.RWA_TEMP_PDLEVEL T6
    ON 					T1.CUSTOMERID = T6.CUSTID
    LEFT JOIN 	(SELECT DISTINCT AA.PROJECTNO,AA.CONTRACTNO   --判断是否资产证券化
               		 FROM RWA_DEV.NCM_ABS_PROJECT_ASSET AA --项目内资产
             INNER JOIN RWA_DEV.NCM_ABS_PROJECT_INFO BB  --项目基本信息
               			 ON AA.PROJECTNO = BB.PROJECTNO
               			AND BB.DATANO = P_DATA_DT_STR
               			AND BB.PROJECTSTATUS = '0401'            	--交割成功
               		WHERE AA.DATANO = P_DATA_DT_STR
    						) T9
    ON 					T3.SERIALNO = T9.CONTRACTNO
    LEFT JOIN 	RWA_DEV.ncm_rwa_risk_expo_rst T3 --风险暴露结果信息表
    ON 					T1.SERIALNO = T3.OBJECTNO
    AND 				T3.OBJECTTYPE = 'BusinessDuebill'
    AND 				T3.DATANO = P_DATA_DT_STR
    LEFT JOIN 	RWA_DEV.RWA_CD_CODE_MAPPING T10 --代码映射转换表
    ON  				T3.RISKEXPOSURERESULT = T10.SITEMNO
    AND 				T10.SCODENO = 'RwaResultType'
    LEFT JOIN 	RWA_DEV.NCM_CLASSIFY_RECORD T11 --五级分类信息表
    ON 					T1.RELATIVESERIALNO2 = T11.OBJECTNO
    AND 				T11.OBJECTTYPE = 'TwelveClassify'
    AND 				T11.ISWORK = '1'
    AND 				T11.DATANO = P_DATA_DT_STR
    LEFT JOIN		RWA_DEV.NCM_CUSTOMER_INFO T16
    ON					T1.CUSTOMERID = T16.CUSTOMERID
    AND					T1.DATANO = T16.DATANO
    LEFT JOIN		(
    						select OBJECTNO, DIRECTION
								  from (select T.OBJECTNO,
								               T.DIRECTION,
								               ROW_NUMBER() OVER(PARTITION BY T.OBJECTNO order by T.SERIALNO DESC) AS RM
								          from RWA_DEV.NCM_PUTOUT_SCHEME T  --额度提用表
								         where T.DATANO = P_DATA_DT_STR
								           and T.OBJECTTYPE = 'BusinessContract'
								           and T.DIRECTION IS NOT NULL)
								 where RM = 1
								) CPS									--额度类业务的行业投向需从提用表取
    ON					T3.SERIALNO = CPS.OBJECTNO
    LEFT JOIN 	RWA.CODE_LIBRARY CL
    ON 					CPS.DIRECTION = CL.ITEMNO
    AND 				CL.CODENO = 'IndustryType'
    WHERE  			T1.DATANO = P_DATA_DT_STR
    --AND T1.BALANCE>0
    --AND T1.SERIALNO NOT LIKE 'BD%'
    AND 				T1.BUSINESSTYPE NOT IN ('1040101010','1040101020','1040101030','1040102010','1040102020','1040102040','1040105060','1040201010','1040201020','1040202010','105010','10303010'
                                ,'10201010','10202010','1035101010','1035101020','1035102010','1035102020','103550','108010','1040101040',
                                '10201080','10202091' --不取福费廷 20190729添加
                                ,'10302020','10302030' --不取银行承兑汇票贴现 已在票据系统取 20190730添加
                                )  --排除同业，回购，投资，委托贷款,转帖现业务以及不是从核心返回的借据
    and T1.SERIALNO not in ('60012004001001','60012004001002','60012004001003','42019999163401') --排除这几笔出口押汇 已到期且余额不为0的数据 20190814
    ;
    COMMIT; 

    /*2 插入 信贷系统零售借据 到目标表*/ 
    INSERT INTO RWA_DEV.RWA_XD_EXPOSURE(
                DATADATE                        --数据日期
               ,DATANO                          --数据流水号
               ,EXPOSUREID                      --风险暴露ID
               ,DUEID                           --债项ID
               ,SSYSID                          --源系统ID
               ,CONTRACTID                      --合同ID
               ,CLIENTID                        --参与主体ID
               ,SORGID                          --源机构ID
               ,SORGNAME                        --源机构名称
               ,ORGSORTNO                       --所属机构排序号
               ,ORGID                           --所属机构ID
               ,ORGNAME                         --所属机构名称
               ,ACCORGID                        --账务机构ID
               ,ACCORGNAME                      --账务机构名称
               ,INDUSTRYID                      --所属行业代码
               ,INDUSTRYNAME                    --所属行业名称
               ,BUSINESSLINE                    --业务条线
               ,ASSETTYPE                       --资产大类
               ,ASSETSUBTYPE                    --资产小类
               ,BUSINESSTYPEID                  --业务品种代码
               ,BUSINESSTYPENAME                --业务品种名称
               ,CREDITRISKDATATYPE              --信用风险数据类型
               ,ASSETTYPEOFHAIRCUTS             --折扣系数对应资产类别
               ,BUSINESSTYPESTD                 --权重法业务类型
               ,EXPOCLASSSTD                    --权重法暴露大类
               ,EXPOSUBCLASSSTD                 --权重法暴露小类
               ,EXPOCLASSIRB                    --内评法暴露大类
               ,EXPOSUBCLASSIRB                 --内评法暴露小类
               ,EXPOBELONG                      --暴露所属标识
               ,BOOKTYPE                        --账户类别
               ,REGUTRANTYPE                    --监管交易类型
               ,REPOTRANFLAG                    --回购交易标识
               ,REVAFREQUENCY                   --重估频率
               ,CURRENCY                        --币种
               ,NORMALPRINCIPAL                 --正常本金余额
               ,OVERDUEBALANCE                  --逾期余额
               ,NONACCRUALBALANCE               --非应计余额
               ,ONSHEETBALANCE                  --表内余额
               ,NORMALINTEREST                  --正常利息
               ,ONDEBITINTEREST                 --表内欠息
               ,OFFDEBITINTEREST                --表外欠息
               ,EXPENSERECEIVABLE               --应收费用
               ,ASSETBALANCE                    --资产余额
               ,ACCSUBJECT1                     --科目一
               ,ACCSUBJECT2                     --科目二
               ,ACCSUBJECT3                     --科目三
               ,STARTDATE                       --起始日期
               ,DUEDATE                         --到期日期
               ,ORIGINALMATURITY                --原始期限
               ,RESIDUALM                       --剩余期限
               ,RISKCLASSIFY                    --风险分类
               ,EXPOSURESTATUS                  --风险暴露状态
               ,OVERDUEDAYS                     --逾期天数
               ,SPECIALPROVISION                --专项准备金
               ,GENERALPROVISION                --一般准备金
               ,ESPECIALPROVISION               --特别准备金
               ,WRITTENOFFAMOUNT                --已核销金额
               ,OFFEXPOSOURCE                   --表外暴露来源
               ,OFFBUSINESSTYPE                 --表外业务类型
               ,OFFBUSINESSSDVSSTD              --权重法表外业务类型细分
               ,UNCONDCANCELFLAG                --是否可随时无条件撤销
               ,CCFLEVEL                        --信用转换系数级别
               ,CCFAIRB                         --高级法信用转换系数
               ,CLAIMSLEVEL                     --债权级别
               ,BONDFLAG                        --是否为债券
               ,BONDISSUEINTENT                 --债券发行目的
               ,NSUREALPROPERTYFLAG             --是否非自用不动产
               ,REPASSETTERMTYPE                --抵债资产期限类型
               ,DEPENDONFPOBFLAG                --是否依赖于银行未来盈利
               ,IRATING                         --内部评级
               ,PD                              --违约概率
               ,LGDLEVEL                        --违约损失率级别
               ,LGDAIRB                         --高级法违约损失率
               ,MAIRB                           --高级法有效期限
               ,EADAIRB                         --高级法违约风险暴露
               ,DEFAULTFLAG                     --违约标识
               ,BEEL                            --已违约暴露预期损失比率
               ,DEFAULTLGD                      --已违约暴露违约损失率
               ,EQUITYEXPOFLAG                  --股权暴露标识
               ,EQUITYINVESTTYPE                --股权投资对象类型
               ,EQUITYINVESTCAUSE               --股权投资形成原因
               ,SLFLAG                          --专业贷款标识
               ,SLTYPE                          --专业贷款类型
               ,PFPHASE                         --项目融资阶段
               ,REGURATING                      --监管评级
               ,CBRCMPRATINGFLAG                --银监会认定评级是否更为审慎
               ,LARGEFLUCFLAG                   --是否波动性较大
               ,LIQUEXPOFLAG                    --是否清算过程中风险暴露
               ,PAYMENTDEALFLAG                 --是否货款对付模式
               ,DELAYTRADINGDAYS                --延迟交易天数
               ,SECURITIESFLAG                  --有价证券标识
               ,SECUISSUERID                    --证券发行人ID
               ,RATINGDURATIONTYPE              --评级期限类型
               ,SECUISSUERATING                 --证券发行等级
               ,SECURESIDUALM                   --证券剩余期限
               ,SECUREVAFREQUENCY               --证券重估频率
               ,CCPTRANFLAG                     --是否中央交易对手相关交易
               ,CCPID                           --中央交易对手ID
               ,QUALCCPFLAG                     --是否合格中央交易对手
               ,BANKROLE                        --银行角色
               ,CLEARINGMETHOD                  --清算方式
               ,BANKASSETFLAG                   --是否银行提交资产
               ,MATCHCONDITIONS                 --符合条件情况
               ,SFTFLAG                         --证券融资交易标识
               ,MASTERNETAGREEFLAG              --净额结算主协议标识
               ,MASTERNETAGREEID                --净额结算主协议ID
               ,SFTTYPE                         --证券融资交易类型
               ,SECUOWNERTRANSFLAG              --证券所有权是否转移
               ,OTCFLAG                         --场外衍生工具标识
               ,VALIDNETTINGFLAG                --有效净额结算协议标识
               ,VALIDNETAGREEMENTID             --有效净额结算协议ID
               ,OTCTYPE                         --场外衍生工具类型
               ,DEPOSITRISKPERIOD               --保证金风险期间
               ,MTM                             --重置成本
               ,MTMCURRENCY                     --重置成本币种
               ,BUYERORSELLER                   --买方卖方
               ,QUALROFLAG                      --合格参照资产标识
               ,ROISSUERPERFORMFLAG             --参照资产发行人是否能履约
               ,BUYERINSOLVENCYFLAG             --信用保护买方是否破产
               ,NONPAYMENTFEES                  --尚未支付费用
               ,RETAILEXPOFLAG                  --零售暴露标识
               ,RETAILCLAIMTYPE                 --零售债权类型
               ,MORTGAGETYPE                    --住房抵押贷款类型
               ,EXPONUMBER                      --风险暴露个数
               ,LTV                             --贷款价值比
               ,AGING                           --账龄
               ,NEWDEFAULTDEBTFLAG              --新增违约债项标识
               ,PDPOOLMODELID                   --PD分池模型ID
               ,LGDPOOLMODELID                  --LGD分池模型ID
               ,CCFPOOLMODELID                  --CCF分池模型ID
               ,PDPOOLID                        --所属PD池ID
               ,LGDPOOLID                       --所属LGD池ID
               ,CCFPOOLID                       --所属CCF池ID
               ,ABSUAFLAG                       --资产证券化基础资产标识
               ,ABSPOOLID                       --证券化资产池ID
               ,GROUPID                         --分组编号
               ,DefaultDate                     --违约时点
               ,ABSPROPORTION                   --资产证券化比重
               ,DEBTORNUMBER                    --借款人个数
               ,flag
    )
    SELECT
                TO_DATE(T1.DATANO,'YYYYMMDD')                                           AS DATADATE                -- 数据日期
                ,T1.DATANO                                                              AS DATANO                  -- 数据流水号
                ,T1.SERIALNO                                                            AS EXPOSUREID              -- 风险暴露ID
                ,T1.SERIALNO                                                            AS DUEID                   -- 债项ID
                ,'XD'                                                                   AS SSYSID                  -- 源系统ID
                ,T1.RELATIVESERIALNO2                                                   AS CONTRACTID              -- 合同ID
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN DECODE(T3.LINETYPE,'0030','XN-GRKH','XN-YBGS')                 --若客户ID为空，条线为个人就虚拟为个人客户，否则为一般公司
                      ELSE T1.CUSTOMERID
                 END                                                                    AS CLIENTID                -- 参与主体ID
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS SORGID                  -- 源机构ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T4.ORGNAME,'总行')                                                             AS SORGNAME                -- 源机构名称
                ,nvl(T4.SORTNO,'1010')                                                              AS ORGSORTNO               -- 所属机构排序号
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS ORGID                   -- 所属机构ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T4.ORGNAME,'总行')                                                             AS ORGNAME                 -- 所属机构名称
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS ACCORGID                -- 账务机构ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T4.ORGNAME,'总行')                                                             AS ACCORGNAME              -- 账务机构名称
                ,NVL(T3.DIRECTION,CPS.DIRECTION)                                        AS INDUSTRYID              -- 所属行业代码
                ,NVL(T5.ITEMNAME,CL.ITEMNAME)                                           AS INDUSTRYNAME            -- 所属行业名称
                ,CASE WHEN T1.BUSINESSCURRENCY<>'CNY' THEN '0102'                            --外币的表内业务   大中-贸金部
                      WHEN T1.BUSINESSTYPE IN('10302010','10302015','10302020') THEN '0401'  --贴现业务        同业金融市场部
                      WHEN T3.LINETYPE='0010' THEN '0101'
                      WHEN T3.LINETYPE='0020' THEN '0201'
                      WHEN T3.LINETYPE='0030' THEN '0301'
                      WHEN T3.LINETYPE='0040' THEN '0401'
                      ELSE '0101'
                 END                                                                    AS BUSINESSLINE            -- 条线
                ,''                                                                     AS ASSETTYPE               -- 资产大类
                ,''                                                                     AS ASSETSUBTYPE            -- 资产小类
                ,CASE WHEN T1.BUSINESSTYPE='11103019' AND T3.PURPOSE='010'                  --区分出个人住房抵押贷款
                      THEN '11103040'
                      ELSE T1.BUSINESSTYPE
                 END                                                                    AS BUSINESSTYPEID          --业务品种代码
                ,CASE WHEN T1.BUSINESSTYPE='11103019' AND T3.PURPOSE='010'
                      THEN '个人综合消费贷款(个人住房贷款)'
                      ELSE T2.TYPENAME
                 END                                                                    AS BUSINESSTYPENAME        -- 业务品种名称
                --,'02'                                                                   AS CREDITRISKDATATYPE      -- 信用风险数据类型          01-一般非零售
                ,CASE WHEN T16.CUSTOMERTYPE = '0310' OR T16.CERTTYPE LIKE 'Ind%'
                      THEN '02' --零售
                      ELSE '01' --非零售
                  END                                                                   AS CREDITRISKDATATYPE      --信用风险数据类型
                ,'01'                                                                   AS ASSETTYPEOFHAIRCUTS     -- 折扣系数对应资产类别     01-现金及现金等价物
                ,''                                                                     AS BUSINESSTYPESTD         -- 权重法业务类型
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN '0112'                                    --若客户ID为空，默认 其他(0112)
                      ELSE ''
                 END                                                                    AS EXPOCLASSSTD            -- 权重法暴露大类
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN DECODE(T3.LINETYPE,'0030','011215','011216')                          --若客户ID为空，条线为个人就默认 其他适用75%风险权重的资产(011215)，否则默认 其他适用100%风险权重的资产(011216)
                      ELSE ''
                 END                                                                    AS EXPOSUBCLASSSTD         -- 权重法暴露小类
                ,SUBSTR(T12.DITEMNO,1,4)                                                AS EXPOCLASSIRB            -- 内评法暴露大类
                ,T12.DITEMNO                                                            AS EXPOSUBCLASSIRB         -- 内评法暴露小类
                ,'01'                                                                   AS EXPOBELONG              -- 暴露所属标识
                ,'01'                                                                   AS BOOKTYPE                -- 账户类别           01-银行账户
                ,'03'                                                                   AS REGUTRANTYPE            -- 监管交易类型      03-抵押贷款
                ,'0'                                                                    AS REPOTRANFLAG            -- 回购交易标识       0-否
                ,1                                                                      AS REVAFREQUENCY           -- 重估频率
                ,NVL(T1.BUSINESSCURRENCY,'CNY')                                         AS CURRENCY                -- 币种
                ,T31.Balance                                                            AS NORMALPRINCIPAL         -- 正常本金余额
                ,0                                                                      AS OVERDUEBALANCE          -- 逾期余额
                ,0                                                                      AS NONACCRUALBALANCE       -- 非应计余额
                ,T31.Balance                                                            AS ONSHEETBALANCE          -- 表内余额
                ,0                                                                      AS NORMALINTEREST          -- 正常利息
                ,0                                                                      AS ONDEBITINTEREST         -- 表内欠息
                ,0                                                                      AS OFFDEBITINTEREST        -- 表外欠息
                ,0                                                                      AS EXPENSERECEIVABLE       -- 应收费用
                ,T31.Balance                                                            AS ASSETBALANCE            -- 资产余额
                ,T1.SUBJECTNO                                                           AS ACCSUBJECT1             -- 科目一
                ,''                                                                     AS ACCSUBJECT2             -- 科目二
                ,''                                                                     AS ACCSUBJECT3             -- 科目三
                ,T1.PUTOUTDATE                                                          AS STARTDATE               -- 起始日期
                ,T1.ACTUALMATURITY                                                      AS DUEDATE                 -- 到期日期
                ,CASE WHEN (TO_DATE(T1.ACTUALMATURITY,'YYYYMMDD')-TO_DATE(T1.PUTOUTDATE,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T1.ACTUALMATURITY,'YYYYMMDD')-TO_DATE(T1.PUTOUTDATE,'YYYYMMDD'))/365
                END                                                                     AS ORIGINALMATURITY        -- 原始期限
                ,CASE WHEN (TO_DATE(T1.ACTUALMATURITY,'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T1.ACTUALMATURITY,'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365
                END                                                                     AS RESIDUALM               -- 剩余期限
                ,CASE WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='A' THEN '01'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='B' THEN '02'       --十二级分类转为五级分类
                      WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='C' THEN '03'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='D' THEN '04'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='E' THEN '05'
                      ELSE NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))
                 END                                                                    AS RISKCLASSIFY            -- 风险分类
                ,'01'                                                                   AS EXPOSURESTATUS          -- 风险暴露状态  01-正常
                ,T1.OVERDUEDAYS                                                         AS OVERDUEDAYS             -- 逾期天数
                ,0                                                                      AS SPECIALPROVISION        -- 专项准备金-规则处理
                ,0                                                                      AS GENERALPROVISION        -- 一般准备金
                ,0                                                                      AS ESPECIALPROVISION       -- 特别准备金
                ,T1.CANCELSUM                                                           AS WRITTENOFFAMOUNT        -- 已核销金额
                ,''                                                                     AS OffExpoSource           -- 表外暴露来源
                ,''                                                                     AS OffBusinessType         -- 表外业务类型
                ,''                                                                     AS OffBusinessSdvsSTD      -- 权重法表外业务类型细分
                ,'1'                                                                    AS UncondCancelFlag        -- 是否可随时无条件撤销
                ,''                                                                     AS CCFLevel                -- 信用转换系数级别
                ,T6.CCFVALUE                                                            AS CCFAIRB                 -- 高级法信用转换系数
                ,'01'                                                                   AS CLAIMSLEVEL             -- 债权级别
                ,'0'                                                                    AS BONDFLAG                -- 是否为债券
                ,'02'                                                                   AS BONDISSUEINTENT         -- 债券发行目的
                ,'0'                                                                    AS NSUREALPROPERTYFLAG     -- 是否非自用不动产
                ,''                                                                     AS REPASSETTERMTYPE        -- 抵债资产期限类型
                ,'0'                                                                    AS DEPENDONFPOBFLAG        -- 是否依赖于银行未来盈利
                ,''                                                                     AS IRATING                 -- 内部评级
                ,T6.PDVALUE                                                             AS PD                      -- 违约概率
                ,''                                                                     AS LGDLEVEL                -- 违约损失率级别
                ,T6.LGDVALUE                                                            AS LGDAIRB                 -- 高级法违约损失率
                ,0                                                                      AS MAIRB                   -- 高级法有效期限
                ,T31.Balance                                                            AS EADAIRB                 -- 高级法违约风险暴露
                ,CASE WHEN T6.DEFAULTFLAG='1' THEN '1'
                      ELSE '0'
                 END                                                                    AS DEFAULTFLAG             -- 违约标识
                ,T6.BEELVALUE                                                           AS BEEL                    -- 已违约暴露预期损失比率
                ,T6.LGDVALUE                                                            AS DEFAULTLGD              -- 已违约暴露违约损失率
                ,'0'                                                                    AS EQUITYEXPOFLAG          -- 股权暴露标识
                ,''                                                                     AS EQUITYINVESTTYPE        -- 股权投资对象类型
                ,''                                                                     AS EQUITYINVESTCAUSE       -- 股权投资形成原因
                ,'0'                                                                    AS SLFLAG                  -- 专业贷款标识       专业贷款相关字段一期先赋空
                ,''                                                                     AS SLTYPE                  -- 专业贷款类型
                ,''                                                                     AS PFPHASE                 -- 项目融资阶段
                ,'01'                                                                   AS REGURATING              -- 监管评级
                ,''                                                                     AS CBRCMPRATINGFLAG        -- 银监会认定评级是否更为审慎
                ,''                                                                     AS LARGEFLUCFLAG           -- 是否波动性较大
                ,'0'                                                                    AS LIQUEXPOFLAG            -- 是否清算过程中风险暴露
                ,''                                                                     AS PAYMENTDEALFLAG         -- 是否货款对付模式
                ,0                                                                      AS DELAYTRADINGDAYS        -- 延迟交易天数
                ,'0'                                                                    AS SECURITIESFLAG          -- 有价证券标识
                ,''                                                                     AS SECUISSUERID            -- 证券发行人ID
                ,''                                                                     AS RATINGDURATIONTYPE      -- 评级期限类型
                ,''                                                                     AS SECUISSUERATING         -- 证券发行等级
                ,0                                                                      AS SECURESIDUALM           -- 证券剩余期限
                ,1                                                                      AS SECUREVAFREQUENCY       -- 证券重估频率
                ,'0'                                                                    AS CCPTRANFLAG             -- 是否中央交易对手相关交易
                ,''                                                                     AS CCPID                   -- 中央交易对手ID
                ,'0'                                                                    AS QUALCCPFLAG             -- 是否合格中央交易对手
                ,''                                                                     AS BANKROLE                -- 银行角色
                ,''                                                                     AS CLEARINGMETHOD          -- 清算方式
                ,'0'                                                                    AS BANKASSETFLAG           -- 是否银行提交资产
                ,''                                                                     AS MATCHCONDITIONS         -- 符合条件情况
                ,'0'                                                                    AS SFTFLAG                 -- 证券融资交易标识
                ,''                                                                     AS MASTERNETAGREEFLAG      -- 净额结算主协议标识
                ,''                                                                     AS MASTERNETAGREEID        -- 净额结算主协议ID
                ,''                                                                     AS SFTTYPE                 -- 证券融资交易类型
                ,''                                                                     AS SECUOWNERTRANSFLAG      -- 证券所有权是否转移
                ,'0'                                                                    AS OTCFLAG                 -- 场外衍生工具标识
                ,''                                                                     AS VALIDNETTINGFLAG        -- 有效净额结算协议标识
                ,''                                                                     AS VALIDNETAGREEMENTID     -- 有效净额结算协议ID
                ,''                                                                     AS OTCTYPE                 -- 场外衍生工具类型
                ,''                                                                     AS DEPOSITRISKPERIOD       -- 保证金风险期间
                ,0                                                                      AS MTM                     -- 重置成本
                ,''                                                                     AS MTMCURRENCY             -- 重置成本币种
                ,''                                                                     AS BUYERORSELLER           -- 买方卖方
                ,''                                                                     AS QUALROFLAG              -- 合格参照资产标识
                ,''                                                                     AS ROISSUERPERFORMFLAG     -- 参照资产发行人是否能履约
                ,''                                                                     AS BUYERINSOLVENCYFLAG     -- 信用保护买方是否破产
                ,0                                                                      AS NONPAYMENTFEES          -- 尚未支付费用
                ,'1'                                                                    AS RETAILEXPOFLAG          -- 零售暴露标识
                ,CASE WHEN T6.RISK_EXPOSURE='01' THEN '020401'
                      WHEN T6.RISK_EXPOSURE='02' THEN '020403'
                      ELSE '020402'
                 END                                                                    AS RETAILCLAIMTYPE         -- 零售债权类型
                ,CASE WHEN T6.RISK_EXPOSURE='01' THEN '01'
                      ELSE '02'
                 END                                                                    AS MORTGAGETYPE            -- 住房抵押贷款类型
                ,1                                                                      AS EXPONUMBER              -- 风险暴露个数
                ,0.8                                                                    AS LTV                     --贷款价值比  统一更新
                ,T6.MOB                                                                 AS AGING                   --账龄
                ,CASE WHEN T1.NEWDEFAULTFLAG='0' THEN '1'
                      ELSE '0'
                 END                                                                    AS NEWDEFAULTDEBTFLAG      --新增违约债项标识
                ,T6.PDMODELCODE                                                         AS PDPOOLMODELID           -- PD分池模型ID
                ,T6.LGDMODELCODE                                                        AS LGDPOOLMODELID          -- LGD分池模型ID
                ,T6.CCFMODELCODE                                                        AS CCFPOOLMODELID          -- CCF分池模型ID
                ,T6.PDCODE                                                              AS PDPOOLID                -- 所属PD池ID
                ,T6.LGDCODE                                                             AS LGDPOOLID               -- 所属LGD池ID
                ,T6.CCFCODE                                                             AS CCFPOOLID               -- 所属CCF池ID
                ,CASE WHEN T10.PROJECTNO IS NULL THEN '0'
                      ELSE '1'
                 END                                                                    AS ABSUAFLAG           --资产证券化基础资产标识
                ,CASE WHEN T10.PROJECTNO IS NULL THEN ''
                      ELSE T10.PROJECTNO
                 END                                                                    AS ABSPOOLID           --证券化资产池ID
                ,''                                                                     AS GROUPID                 -- 分组编号
                ,CASE WHEN T6.DEFAULTFLAG='1' THEN TO_DATE(T6.UPDATETIME,'YYYYMMDD')
                      ELSE NULL
                 END                                                                    AS DefaultDate             -- 违约时点
                ,0                                                                      AS ABSPROPORTION           --资产证券化比重
                ,0                                                                      AS DEBTORNUMBER            --借款人个数
                ,'LS'
    FROM        RWA_DEV.NCM_BUSINESS_DUEBILL T1
    INNER JOIN  RWA_DEV.NCM_BUSINESS_TYPE T2
    ON          T1.BUSINESSTYPE = T2.TYPENO
    AND         T1.DATANO = T2.DATANO
    AND         T2.ATTRIBUTE1 = '2'                       --只取零售业务
    INNER JOIN  RWA_DEV.NCM_BUSINESS_CONTRACT T3
    ON          T1.RELATIVESERIALNO2 = T3.SERIALNO        --对账以借据为准，所以关联合同时，不应该加合同的有效条件
    AND         T1.DATANO = T3.DATANO
    INNER JOIN  RWA_DEV.NCM_BUSINESS_HISTORY T31
    ON          T1.SERIALNO = T31.SERIALNO
    AND         T31.Balance > 0                     --只取正常本金余额
    AND         T31.DATANO = P_DATA_DT_STR
              /*  rwa_dev.brd_loan_nor t31
    ON          t1.serialno=t31.crdt_acct_no
    AND         t31.cur_bal > 0 
    AND         t31.DATANO = P_DATA_DT_STR*/
    LEFT JOIN   RWA.ORG_INFO T4
    ON          decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID) = T4.ORGID
    LEFT JOIN   RWA.CODE_LIBRARY T5
    ON          T3.DIRECTION = T5.ITEMNO
    AND         T5.CODENO = 'IndustryType'
    LEFT JOIN   RWA_DEV.RWA_TEMP_LGDLEVEL T6              --零售债项评级
    ON          T1.RELATIVESERIALNO2 = T6.BUSINESSID
    AND         T6.BUSINESSTYPE <> 'CREDITCARD'               -- 不取信用卡
    LEFT JOIN   (SELECT DISTINCT AA.PROJECTNO,AA.CONTRACTNO   --判断是否资产证券化
                   FROM RWA_DEV.NCM_ABS_PROJECT_ASSET AA
             INNER JOIN RWA_DEV.NCM_ABS_PROJECT_INFO BB
                     ON AA.PROJECTNO = BB.PROJECTNO
                    AND BB.DATANO = P_DATA_DT_STR
                    AND BB.PROJECTSTATUS = '0401'             --交割成功
                  WHERE AA.DATANO = P_DATA_DT_STR
                ) T10
    ON          T3.SERIALNO = T10.CONTRACTNO
    LEFT JOIN   RWA_DEV.ncm_rwa_risk_expo_rst T11
    ON          T1.SERIALNO = T11.OBJECTNO
    AND         T11.OBJECTTYPE = 'BusinessDuebill'
    AND         T11.DATANO = P_DATA_DT_STR
    LEFT JOIN   RWA_DEV.RWA_CD_CODE_MAPPING T12
    ON          T11.RISKEXPOSURERESULT=T12.SITEMNO
    AND         T12.SCODENO = 'RwaResultType'
    LEFT JOIN   RWA_DEV.NCM_CLASSIFY_RECORD T13
    ON          T1.RELATIVESERIALNO2=T13.OBJECTNO
    AND         T13.OBJECTTYPE = 'TwelveClassify'
    AND         T13.ISWORK = '1'
    AND         T13.DATANO = P_DATA_DT_STR
    LEFT JOIN   RWA_DEV.NCM_CUSTOMER_INFO T16
    ON					T1.CUSTOMERID = T16.CUSTOMERID
    AND					T1.DATANO = T16.DATANO
    LEFT JOIN   (
                select OBJECTNO, DIRECTION
                  from (select T.OBJECTNO,
                               T.DIRECTION,
                               ROW_NUMBER() OVER(PARTITION BY T.OBJECTNO order by T.SERIALNO DESC) AS RM
                          from RWA_DEV.NCM_PUTOUT_SCHEME T
                         where T.DATANO = P_DATA_DT_STR
                           and T.OBJECTTYPE = 'BusinessContract'
                           and T.DIRECTION IS NOT NULL)
                 where RM = 1
                ) CPS                 --额度类业务的行业投向需从提用表取
    ON          T3.SERIALNO = CPS.OBJECTNO
    LEFT JOIN   RWA.CODE_LIBRARY CL
    ON          CPS.DIRECTION = CL.ITEMNO
    AND         CL.CODENO = 'IndustryType'
    WHERE       T1.DATANO=P_DATA_DT_STR
    --AND T1.BALANCE>0
    --AND T1.SERIALNO NOT LIKE 'BD%'
    AND         T1.BUSINESSTYPE NOT IN ('11105010','11105020','11103030')  --排除个人委托贷款业务 排除微粒贷
    ;
    COMMIT;
    
    /*部分借据表的科目没有成功转码@,该部分科目关联brd_loan_nor对应的科目为13100000,71090101;
      13100000逾期在下面逾期部分已经插入，71090101已核销无需取数*/
       DELETE FROM RWA_XD_EXPOSURE T
        WHERE T.ACCSUBJECT1 LIKE '@%'
        AND T.DATANO=p_data_dt_str
          AND EXISTS (SELECT 1
                 FROM BRD_LOAN_NOR B
                WHERE T.DUEID = B.CRDT_ACCT_NO
                  AND T.CONTRACTID = B.CONTRACT_NO
                  AND T.DATANO = B.DATANO
                  AND B.SBJT_CD IN ('13100000', '71090101'));
         COMMIT;
      
    
    /*3 信贷系统零售借据-微粒贷*/
    INSERT INTO RWA_DEV.RWA_XD_EXPOSURE(
                DATADATE                        --数据日期
               ,DATANO                          --数据流水号
               ,EXPOSUREID                      --风险暴露ID
               ,DUEID                           --债项ID
               ,SSYSID                          --源系统ID
               ,CONTRACTID                      --合同ID
               ,CLIENTID                        --参与主体ID
               ,SORGID                          --源机构ID
               ,SORGNAME                        --源机构名称
               ,ORGSORTNO                       --所属机构排序号
               ,ORGID                           --所属机构ID
               ,ORGNAME                         --所属机构名称
               ,ACCORGID                        --账务机构ID
               ,ACCORGNAME                      --账务机构名称
               ,INDUSTRYID                      --所属行业代码
               ,INDUSTRYNAME                    --所属行业名称
               ,BUSINESSLINE                    --业务条线
               ,ASSETTYPE                       --资产大类
               ,ASSETSUBTYPE                    --资产小类
               ,BUSINESSTYPEID                  --业务品种代码
               ,BUSINESSTYPENAME                --业务品种名称
               ,CREDITRISKDATATYPE              --信用风险数据类型
               ,ASSETTYPEOFHAIRCUTS             --折扣系数对应资产类别
               ,BUSINESSTYPESTD                 --权重法业务类型
               ,EXPOCLASSSTD                    --权重法暴露大类
               ,EXPOSUBCLASSSTD                 --权重法暴露小类
               ,EXPOCLASSIRB                    --内评法暴露大类
               ,EXPOSUBCLASSIRB                 --内评法暴露小类
               ,EXPOBELONG                      --暴露所属标识
               ,BOOKTYPE                        --账户类别
               ,REGUTRANTYPE                    --监管交易类型
               ,REPOTRANFLAG                    --回购交易标识
               ,REVAFREQUENCY                   --重估频率
               ,CURRENCY                        --币种
               ,NORMALPRINCIPAL                 --正常本金余额
               ,OVERDUEBALANCE                  --逾期余额
               ,NONACCRUALBALANCE               --非应计余额
               ,ONSHEETBALANCE                  --表内余额
               ,NORMALINTEREST                  --正常利息
               ,ONDEBITINTEREST                 --表内欠息
               ,OFFDEBITINTEREST                --表外欠息
               ,EXPENSERECEIVABLE               --应收费用
               ,ASSETBALANCE                    --资产余额
               ,ACCSUBJECT1                     --科目一
               ,ACCSUBJECT2                     --科目二
               ,ACCSUBJECT3                     --科目三
               ,STARTDATE                       --起始日期
               ,DUEDATE                         --到期日期
               ,ORIGINALMATURITY                --原始期限
               ,RESIDUALM                       --剩余期限
               ,RISKCLASSIFY                    --风险分类
               ,EXPOSURESTATUS                  --风险暴露状态
               ,OVERDUEDAYS                     --逾期天数
               ,SPECIALPROVISION                --专项准备金
               ,GENERALPROVISION                --一般准备金
               ,ESPECIALPROVISION               --特别准备金
               ,WRITTENOFFAMOUNT                --已核销金额
               ,OFFEXPOSOURCE                   --表外暴露来源
               ,OFFBUSINESSTYPE                 --表外业务类型
               ,OFFBUSINESSSDVSSTD              --权重法表外业务类型细分
               ,UNCONDCANCELFLAG                --是否可随时无条件撤销
               ,CCFLEVEL                        --信用转换系数级别
               ,CCFAIRB                         --高级法信用转换系数
               ,CLAIMSLEVEL                     --债权级别
               ,BONDFLAG                        --是否为债券
               ,BONDISSUEINTENT                 --债券发行目的
               ,NSUREALPROPERTYFLAG             --是否非自用不动产
               ,REPASSETTERMTYPE                --抵债资产期限类型
               ,DEPENDONFPOBFLAG                --是否依赖于银行未来盈利
               ,IRATING                         --内部评级
               ,PD                              --违约概率
               ,LGDLEVEL                        --违约损失率级别
               ,LGDAIRB                         --高级法违约损失率
               ,MAIRB                           --高级法有效期限
               ,EADAIRB                         --高级法违约风险暴露
               ,DEFAULTFLAG                     --违约标识
               ,BEEL                            --已违约暴露预期损失比率
               ,DEFAULTLGD                      --已违约暴露违约损失率
               ,EQUITYEXPOFLAG                  --股权暴露标识
               ,EQUITYINVESTTYPE                --股权投资对象类型
               ,EQUITYINVESTCAUSE               --股权投资形成原因
               ,SLFLAG                          --专业贷款标识
               ,SLTYPE                          --专业贷款类型
               ,PFPHASE                         --项目融资阶段
               ,REGURATING                      --监管评级
               ,CBRCMPRATINGFLAG                --银监会认定评级是否更为审慎
               ,LARGEFLUCFLAG                   --是否波动性较大
               ,LIQUEXPOFLAG                    --是否清算过程中风险暴露
               ,PAYMENTDEALFLAG                 --是否货款对付模式
               ,DELAYTRADINGDAYS                --延迟交易天数
               ,SECURITIESFLAG                  --有价证券标识
               ,SECUISSUERID                    --证券发行人ID
               ,RATINGDURATIONTYPE              --评级期限类型
               ,SECUISSUERATING                 --证券发行等级
               ,SECURESIDUALM                   --证券剩余期限
               ,SECUREVAFREQUENCY               --证券重估频率
               ,CCPTRANFLAG                     --是否中央交易对手相关交易
               ,CCPID                           --中央交易对手ID
               ,QUALCCPFLAG                     --是否合格中央交易对手
               ,BANKROLE                        --银行角色
               ,CLEARINGMETHOD                  --清算方式
               ,BANKASSETFLAG                   --是否银行提交资产
               ,MATCHCONDITIONS                 --符合条件情况
               ,SFTFLAG                         --证券融资交易标识
               ,MASTERNETAGREEFLAG              --净额结算主协议标识
               ,MASTERNETAGREEID                --净额结算主协议ID
               ,SFTTYPE                         --证券融资交易类型
               ,SECUOWNERTRANSFLAG              --证券所有权是否转移
               ,OTCFLAG                         --场外衍生工具标识
               ,VALIDNETTINGFLAG                --有效净额结算协议标识
               ,VALIDNETAGREEMENTID             --有效净额结算协议ID
               ,OTCTYPE                         --场外衍生工具类型
               ,DEPOSITRISKPERIOD               --保证金风险期间
               ,MTM                             --重置成本
               ,MTMCURRENCY                     --重置成本币种
               ,BUYERORSELLER                   --买方卖方
               ,QUALROFLAG                      --合格参照资产标识
               ,ROISSUERPERFORMFLAG             --参照资产发行人是否能履约
               ,BUYERINSOLVENCYFLAG             --信用保护买方是否破产
               ,NONPAYMENTFEES                  --尚未支付费用
               ,RETAILEXPOFLAG                  --零售暴露标识
               ,RETAILCLAIMTYPE                 --零售债权类型
               ,MORTGAGETYPE                    --住房抵押贷款类型
               ,EXPONUMBER                      --风险暴露个数
               ,LTV                             --贷款价值比
               ,AGING                           --账龄
               ,NEWDEFAULTDEBTFLAG              --新增违约债项标识
               ,PDPOOLMODELID                   --PD分池模型ID
               ,LGDPOOLMODELID                  --LGD分池模型ID
               ,CCFPOOLMODELID                  --CCF分池模型ID
               ,PDPOOLID                        --所属PD池ID
               ,LGDPOOLID                       --所属LGD池ID
               ,CCFPOOLID                       --所属CCF池ID
               ,ABSUAFLAG                       --资产证券化基础资产标识
               ,ABSPOOLID                       --证券化资产池ID
               ,GROUPID                         --分组编号
               ,DefaultDate                     --违约时点
               ,ABSPROPORTION                   --资产证券化比重
               ,DEBTORNUMBER                    --借款人个数
               ,flag
    )
    SELECT
                TO_DATE(T1.DATANO,'YYYYMMDD')                                           AS DATADATE                -- 数据日期
                ,T1.DATANO                                                              AS DATANO                  -- 数据流水号
                ,T1.SERIALNO                                                            AS EXPOSUREID              -- 风险暴露ID
                ,T1.SERIALNO                                                            AS DUEID                   -- 债项ID
                ,'XD'                                                                   AS SSYSID                  -- 源系统ID
                ,T1.RELATIVESERIALNO2                                                   AS CONTRACTID              -- 合同ID
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN DECODE(T3.LINETYPE,'0030','XN-GRKH','XN-YBGS')                 --若客户ID为空，条线为个人就虚拟为个人客户，否则为一般公司
                      ELSE T1.CUSTOMERID
                 END                                                                    AS CLIENTID                -- 参与主体ID
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS SORGID                  -- 源机构ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T4.ORGNAME,'总行')                                                             AS SORGNAME                -- 源机构名称
                ,nvl(T4.SORTNO,'1010')                                                              AS ORGSORTNO               -- 所属机构排序号
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS ORGID                   -- 所属机构ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T4.ORGNAME,'总行')                                                              AS ORGNAME                 -- 所属机构名称
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS ACCORGID                -- 账务机构ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T4.ORGNAME,'总行')                                                              AS ACCORGNAME              -- 账务机构名称
                ,NVL(T3.DIRECTION,CPS.DIRECTION)                                        AS INDUSTRYID              -- 所属行业代码
                ,NVL(T5.ITEMNAME,CL.ITEMNAME)                                           AS INDUSTRYNAME            -- 所属行业名称
                ,CASE WHEN T1.BUSINESSCURRENCY<>'CNY' THEN '0102'                            --外币的表内业务   大中-贸金部
                      WHEN T1.BUSINESSTYPE IN('10302010','10302015','10302020') THEN '0401'  --贴现业务        同业金融市场部
                      WHEN T3.LINETYPE='0010' THEN '0101'
                      WHEN T3.LINETYPE='0020' THEN '0201'
                      WHEN T3.LINETYPE='0030' THEN '0301'
                      WHEN T3.LINETYPE='0040' THEN '0401'
                      ELSE '0101'
                 END                                                                    AS BUSINESSLINE            -- 条线
                ,''                                                                     AS ASSETTYPE               -- 资产大类
                ,''                                                                     AS ASSETSUBTYPE            -- 资产小类
                ,'11103030'                                                             AS BUSINESSTYPEID          --业务品种代码
                ,'微粒贷'   AS BUSINESSTYPENAME        -- 业务品种名称
                --,'02'                                                                   AS CREDITRISKDATATYPE      -- 信用风险数据类型          01-一般非零售
                ,CASE WHEN T16.CUSTOMERTYPE = '0310' OR T16.CERTTYPE LIKE 'Ind%'
                      THEN '02' --零售
                      ELSE '01' --非零售
                  END                                                                   AS CREDITRISKDATATYPE      --信用风险数据类型
                ,'01'                                                                   AS ASSETTYPEOFHAIRCUTS     -- 折扣系数对应资产类别     01-现金及现金等价物
                ,''                                                                     AS BUSINESSTYPESTD         -- 权重法业务类型
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN '0112'                                    --若客户ID为空，默认 其他(0112)
                      ELSE ''
                 END                                                                    AS EXPOCLASSSTD            -- 权重法暴露大类
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN DECODE(T3.LINETYPE,'0030','011215','011216')                          --若客户ID为空，条线为个人就默认 其他适用75%风险权重的资产(011215)，否则默认 其他适用100%风险权重的资产(011216)
                      ELSE ''
                 END                                                                    AS EXPOSUBCLASSSTD         -- 权重法暴露小类
                ,SUBSTR(T12.DITEMNO,1,4)                                                AS EXPOCLASSIRB            -- 内评法暴露大类
                ,T12.DITEMNO                                                            AS EXPOSUBCLASSIRB         -- 内评法暴露小类
                ,'01'                                                                   AS EXPOBELONG              -- 暴露所属标识
                ,'01'                                                                   AS BOOKTYPE                -- 账户类别           01-银行账户
                ,'03'                                                                   AS REGUTRANTYPE            -- 监管交易类型      03-抵押贷款
                ,'0'                                                                    AS REPOTRANFLAG            -- 回购交易标识       0-否
                ,1                                                                      AS REVAFREQUENCY           -- 重估频率
                ,case when t31.ccy_cd ='01' or t31.ccy_cd is null
                      then 'CNY'  end                                                   AS CURRENCY                -- 币种
                ,T1.BALANCE                                                            AS NORMALPRINCIPAL         -- 正常本金余额
                ,0                                                                      AS OVERDUEBALANCE          -- 逾期余额
                ,0                                                                      AS NONACCRUALBALANCE       -- 非应计余额
                ,T1.BALANCE                                                            AS ONSHEETBALANCE          -- 表内余额
                ,0                                                                      AS NORMALINTEREST          -- 正常利息
                ,0                                                                      AS ONDEBITINTEREST         -- 表内欠息
                ,0                                                                      AS OFFDEBITINTEREST        -- 表外欠息
                ,0                                                                      AS EXPENSERECEIVABLE       -- 应收费用
                ,T1.BALANCE                                                            AS ASSETBALANCE            -- 资产余额
                ,T31.sbjt_cd                                                           AS ACCSUBJECT1             -- 科目一
                ,''                                                                     AS ACCSUBJECT2             -- 科目二
                ,''                                                                     AS ACCSUBJECT3             -- 科目三
                ,T1.PUTOUTDATE                                                          AS STARTDATE               -- 起始日期
                ,T1.ACTUALMATURITY                                                      AS DUEDATE                 -- 到期日期
                ,CASE WHEN (TO_DATE(T1.ACTUALMATURITY,'YYYYMMDD')-TO_DATE(T1.PUTOUTDATE,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T1.ACTUALMATURITY,'YYYYMMDD')-TO_DATE(T1.PUTOUTDATE,'YYYYMMDD'))/365
                END                                                                     AS ORIGINALMATURITY        -- 原始期限
                ,CASE WHEN (TO_DATE(T1.ACTUALMATURITY,'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T1.ACTUALMATURITY,'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365
                END                                                                     AS RESIDUALM               -- 剩余期限
                ,CASE WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='A' THEN '01'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='B' THEN '02'       --十二级分类转为五级分类
                      WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='C' THEN '03'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='D' THEN '04'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='E' THEN '05'
                      ELSE NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))
                 END                                                                    AS RISKCLASSIFY            -- 风险分类
                ,'01'                                                                   AS EXPOSURESTATUS          -- 风险暴露状态  01-正常
                ,T1.OVERDUEDAYS                                                         AS OVERDUEDAYS             -- 逾期天数
                ,0                                                                      AS SPECIALPROVISION        -- 专项准备金-规则处理
                ,0                                                                      AS GENERALPROVISION        -- 一般准备金
                ,0                                                                      AS ESPECIALPROVISION       -- 特别准备金
                ,T1.CANCELSUM                                                           AS WRITTENOFFAMOUNT        -- 已核销金额
                ,''                                                                     AS OffExpoSource           -- 表外暴露来源
                ,''                                                                     AS OffBusinessType         -- 表外业务类型
                ,''                                                                     AS OffBusinessSdvsSTD      -- 权重法表外业务类型细分
                ,'1'                                                                    AS UncondCancelFlag        -- 是否可随时无条件撤销
                ,''                                                                     AS CCFLevel                -- 信用转换系数级别
                ,T6.CCFVALUE                                                            AS CCFAIRB                 -- 高级法信用转换系数
                ,'01'                                                                   AS CLAIMSLEVEL             -- 债权级别
                ,'0'                                                                    AS BONDFLAG                -- 是否为债券
                ,'02'                                                                   AS BONDISSUEINTENT         -- 债券发行目的
                ,'0'                                                                    AS NSUREALPROPERTYFLAG     -- 是否非自用不动产
                ,''                                                                     AS REPASSETTERMTYPE        -- 抵债资产期限类型
                ,'0'                                                                    AS DEPENDONFPOBFLAG        -- 是否依赖于银行未来盈利
                ,''                                                                     AS IRATING                 -- 内部评级
                ,T6.PDVALUE                                                             AS PD                      -- 违约概率
                ,''                                                                     AS LGDLEVEL                -- 违约损失率级别
                ,T6.LGDVALUE                                                            AS LGDAIRB                 -- 高级法违约损失率
                ,0                                                                      AS MAIRB                   -- 高级法有效期限
                ,T1.BALANCE                                                            AS EADAIRB                 -- 高级法违约风险暴露
                ,CASE WHEN T6.DEFAULTFLAG='1' THEN '1'
                      ELSE '0'
                 END                                                                    AS DEFAULTFLAG             -- 违约标识
                ,T6.BEELVALUE                                                           AS BEEL                    -- 已违约暴露预期损失比率
                ,T6.LGDVALUE                                                            AS DEFAULTLGD              -- 已违约暴露违约损失率
                ,'0'                                                                    AS EQUITYEXPOFLAG          -- 股权暴露标识
                ,''                                                                     AS EQUITYINVESTTYPE        -- 股权投资对象类型
                ,''                                                                     AS EQUITYINVESTCAUSE       -- 股权投资形成原因
                ,'0'                                                                    AS SLFLAG                  -- 专业贷款标识       专业贷款相关字段一期先赋空
                ,''                                                                     AS SLTYPE                  -- 专业贷款类型
                ,''                                                                     AS PFPHASE                 -- 项目融资阶段
                ,'01'                                                                   AS REGURATING              -- 监管评级
                ,''                                                                     AS CBRCMPRATINGFLAG        -- 银监会认定评级是否更为审慎
                ,''                                                                     AS LARGEFLUCFLAG           -- 是否波动性较大
                ,'0'                                                                    AS LIQUEXPOFLAG            -- 是否清算过程中风险暴露
                ,''                                                                     AS PAYMENTDEALFLAG         -- 是否货款对付模式
                ,0                                                                      AS DELAYTRADINGDAYS        -- 延迟交易天数
                ,'0'                                                                    AS SECURITIESFLAG          -- 有价证券标识
                ,''                                                                     AS SECUISSUERID            -- 证券发行人ID
                ,''                                                                     AS RATINGDURATIONTYPE      -- 评级期限类型
                ,''                                                                     AS SECUISSUERATING         -- 证券发行等级
                ,0                                                                      AS SECURESIDUALM           -- 证券剩余期限
                ,1                                                                      AS SECUREVAFREQUENCY       -- 证券重估频率
                ,'0'                                                                    AS CCPTRANFLAG             -- 是否中央交易对手相关交易
                ,''                                                                     AS CCPID                   -- 中央交易对手ID
                ,'0'                                                                    AS QUALCCPFLAG             -- 是否合格中央交易对手
                ,''                                                                     AS BANKROLE                -- 银行角色
                ,''                                                                     AS CLEARINGMETHOD          -- 清算方式
                ,'0'                                                                    AS BANKASSETFLAG           -- 是否银行提交资产
                ,''                                                                     AS MATCHCONDITIONS         -- 符合条件情况
                ,'0'                                                                    AS SFTFLAG                 -- 证券融资交易标识
                ,''                                                                     AS MASTERNETAGREEFLAG      -- 净额结算主协议标识
                ,''                                                                     AS MASTERNETAGREEID        -- 净额结算主协议ID
                ,''                                                                     AS SFTTYPE                 -- 证券融资交易类型
                ,''                                                                     AS SECUOWNERTRANSFLAG      -- 证券所有权是否转移
                ,'0'                                                                    AS OTCFLAG                 -- 场外衍生工具标识
                ,''                                                                     AS VALIDNETTINGFLAG        -- 有效净额结算协议标识
                ,''                                                                     AS VALIDNETAGREEMENTID     -- 有效净额结算协议ID
                ,''                                                                     AS OTCTYPE                 -- 场外衍生工具类型
                ,''                                                                     AS DEPOSITRISKPERIOD       -- 保证金风险期间
                ,0                                                                      AS MTM                     -- 重置成本
                ,''                                                                     AS MTMCURRENCY             -- 重置成本币种
                ,''                                                                     AS BUYERORSELLER           -- 买方卖方
                ,''                                                                     AS QUALROFLAG              -- 合格参照资产标识
                ,''                                                                     AS ROISSUERPERFORMFLAG     -- 参照资产发行人是否能履约
                ,''                                                                     AS BUYERINSOLVENCYFLAG     -- 信用保护买方是否破产
                ,0                                                                      AS NONPAYMENTFEES          -- 尚未支付费用
                ,'1'                                                                    AS RETAILEXPOFLAG          -- 零售暴露标识
                ,CASE WHEN T6.RISK_EXPOSURE='01' THEN '020401'
                      WHEN T6.RISK_EXPOSURE='02' THEN '020403'
                      ELSE '020402'
                 END                                                                    AS RETAILCLAIMTYPE         -- 零售债权类型
                ,CASE WHEN T6.RISK_EXPOSURE='01' THEN '01'
                      ELSE '02'
                 END                                                                    AS MORTGAGETYPE            -- 住房抵押贷款类型
                ,1                                                                      AS EXPONUMBER              -- 风险暴露个数
                ,0.8                                                                    AS LTV                     --贷款价值比  统一更新
                ,T6.MOB                                                                 AS AGING                   --账龄
                ,CASE WHEN T1.NEWDEFAULTFLAG='0' THEN '1'
                      ELSE '0'
                 END                                                                    AS NEWDEFAULTDEBTFLAG      --新增违约债项标识
                ,T6.PDMODELCODE                                                         AS PDPOOLMODELID           -- PD分池模型ID
                ,T6.LGDMODELCODE                                                        AS LGDPOOLMODELID          -- LGD分池模型ID
                ,T6.CCFMODELCODE                                                        AS CCFPOOLMODELID          -- CCF分池模型ID
                ,T6.PDCODE                                                              AS PDPOOLID                -- 所属PD池ID
                ,T6.LGDCODE                                                             AS LGDPOOLID               -- 所属LGD池ID
                ,T6.CCFCODE                                                             AS CCFPOOLID               -- 所属CCF池ID
                ,CASE WHEN T10.PROJECTNO IS NULL THEN '0'
                      ELSE '1'
                 END                                                                    AS ABSUAFLAG           --资产证券化基础资产标识
                ,CASE WHEN T10.PROJECTNO IS NULL THEN ''
                      ELSE T10.PROJECTNO
                 END                                                                    AS ABSPOOLID           --证券化资产池ID
                ,''                                                                     AS GROUPID                 -- 分组编号
                ,CASE WHEN T6.DEFAULTFLAG='1' THEN TO_DATE(T6.UPDATETIME,'YYYYMMDD')
                      ELSE NULL
                 END                                                                    AS DefaultDate             -- 违约时点
                ,0                                                                      AS ABSPROPORTION           --资产证券化比重
                ,0                                                                      AS DEBTORNUMBER            --借款人个数
                ,'WLD'
    FROM        RWA_DEV.NCM_BUSINESS_DUEBILL T1
    INNER JOIN  RWA_DEV.NCM_BUSINESS_CONTRACT T3
    ON          T1.RELATIVESERIALNO2 = T3.SERIALNO        --对账以借据为准，所以关联合同时，不应该加合同的有效条件
    AND         T1.DATANO = T3.DATANO
    INNER JOIN  rwa_dev.brd_loan_nor t31
    ON          t1.serialno=t31.crdt_acct_no
    AND         t31.cur_bal > 0 
    AND         t31.DATANO = P_DATA_DT_STR
  and     t31.sbjt_cd in('13030206','13030106') --13030106/13030206 个人短期/中长期微粒贷贷款本金 
    LEFT JOIN   RWA.ORG_INFO T4
    ON           decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID) = T4.ORGID
    LEFT JOIN   RWA.CODE_LIBRARY T5
    ON          T3.DIRECTION = T5.ITEMNO
    AND         T5.CODENO = 'IndustryType'
    LEFT JOIN   RWA_DEV.RWA_TEMP_LGDLEVEL T6              --零售债项评级
    ON          T1.RELATIVESERIALNO2 = T6.BUSINESSID
    AND         T6.BUSINESSTYPE <> 'CREDITCARD'               -- 不取信用卡
    LEFT JOIN   (SELECT DISTINCT AA.PROJECTNO,AA.CONTRACTNO   --判断是否资产证券化
                   FROM RWA_DEV.NCM_ABS_PROJECT_ASSET AA
             INNER JOIN RWA_DEV.NCM_ABS_PROJECT_INFO BB
                     ON AA.PROJECTNO = BB.PROJECTNO
                    AND BB.DATANO = P_DATA_DT_STR
                    AND BB.PROJECTSTATUS = '0401'             --交割成功
                  WHERE AA.DATANO = P_DATA_DT_STR
                ) T10
    ON          T3.SERIALNO = T10.CONTRACTNO
    LEFT JOIN   RWA_DEV.ncm_rwa_risk_expo_rst T11
    ON          T1.SERIALNO = T11.OBJECTNO
    AND         T11.OBJECTTYPE = 'BusinessDuebill'
    AND         T11.DATANO = P_DATA_DT_STR
    LEFT JOIN   RWA_DEV.RWA_CD_CODE_MAPPING T12
    ON          T11.RISKEXPOSURERESULT=T12.SITEMNO
    AND         T12.SCODENO = 'RwaResultType'
    LEFT JOIN   RWA_DEV.NCM_CLASSIFY_RECORD T13
    ON          T1.RELATIVESERIALNO2=T13.OBJECTNO
    AND         T13.OBJECTTYPE = 'TwelveClassify'
    AND         T13.ISWORK = '1'
    AND         T13.DATANO = P_DATA_DT_STR
    LEFT JOIN   RWA_DEV.NCM_CUSTOMER_INFO T16
    ON          T1.CUSTOMERID = T16.CUSTOMERID
    AND         T1.DATANO = T16.DATANO
    LEFT JOIN   (
                select OBJECTNO, DIRECTION
                  from (select T.OBJECTNO,
                               T.DIRECTION,
                               ROW_NUMBER() OVER(PARTITION BY T.OBJECTNO order by T.SERIALNO DESC) AS RM
                          from RWA_DEV.NCM_PUTOUT_SCHEME T
                         where T.DATANO = P_DATA_DT_STR
                           and T.OBJECTTYPE = 'BusinessContract'
                           and T.DIRECTION IS NOT NULL)
                 where RM = 1
                ) CPS                 --额度类业务的行业投向需从提用表取
    ON          T3.SERIALNO = CPS.OBJECTNO
    LEFT JOIN   RWA.CODE_LIBRARY CL
    ON          CPS.DIRECTION = CL.ITEMNO
    AND         CL.CODENO = 'IndustryType'
    WHERE       T1.DATANO=P_DATA_DT_STR
    ;
    commit;
    
    /*4 插入主要是信用证，保函，贷款承诺(都是对公业务)*/ 
    INSERT INTO RWA_DEV.RWA_XD_EXPOSURE(
                DATADATE                        --数据日期
               ,DATANO                          --数据流水号
               ,EXPOSUREID                      --风险暴露ID
               ,DUEID                           --债项ID
               ,SSYSID                          --源系统ID
               ,CONTRACTID                      --合同ID
               ,CLIENTID                        --参与主体ID
               ,SORGID                          --源机构ID
               ,SORGNAME                        --源机构名称
               ,ORGSORTNO                       --所属机构排序号
               ,ORGID                           --所属机构ID
               ,ORGNAME                         --所属机构名称
               ,ACCORGID                        --账务机构ID
               ,ACCORGNAME                      --账务机构名称
               ,INDUSTRYID                      --所属行业代码
               ,INDUSTRYNAME                    --所属行业名称
               ,BUSINESSLINE                    --业务条线
               ,ASSETTYPE                       --资产大类
               ,ASSETSUBTYPE                    --资产小类
               ,BUSINESSTYPEID                  --业务品种代码
               ,BUSINESSTYPENAME                --业务品种名称
               ,CREDITRISKDATATYPE              --信用风险数据类型
               ,ASSETTYPEOFHAIRCUTS             --折扣系数对应资产类别
               ,BUSINESSTYPESTD                 --权重法业务类型
               ,EXPOCLASSSTD                    --权重法暴露大类
               ,EXPOSUBCLASSSTD                 --权重法暴露小类
               ,EXPOCLASSIRB                    --内评法暴露大类
               ,EXPOSUBCLASSIRB                 --内评法暴露小类
               ,EXPOBELONG                      --暴露所属标识
               ,BOOKTYPE                        --账户类别
               ,REGUTRANTYPE                    --监管交易类型
               ,REPOTRANFLAG                    --回购交易标识
               ,REVAFREQUENCY                   --重估频率
               ,CURRENCY                        --币种
               ,NORMALPRINCIPAL                 --正常本金余额
               ,OVERDUEBALANCE                  --逾期余额
               ,NONACCRUALBALANCE               --非应计余额
               ,ONSHEETBALANCE                  --表内余额
               ,NORMALINTEREST                  --正常利息
               ,ONDEBITINTEREST                 --表内欠息
               ,OFFDEBITINTEREST                --表外欠息
               ,EXPENSERECEIVABLE               --应收费用
               ,ASSETBALANCE                    --资产余额
               ,ACCSUBJECT1                     --科目一
               ,ACCSUBJECT2                     --科目二
               ,ACCSUBJECT3                     --科目三
               ,STARTDATE                       --起始日期
               ,DUEDATE                         --到期日期
               ,ORIGINALMATURITY                --原始期限
               ,RESIDUALM                       --剩余期限
               ,RISKCLASSIFY                    --风险分类
               ,EXPOSURESTATUS                  --风险暴露状态
               ,OVERDUEDAYS                     --逾期天数
               ,SPECIALPROVISION                --专项准备金
               ,GENERALPROVISION                --一般准备金
               ,ESPECIALPROVISION               --特别准备金
               ,WRITTENOFFAMOUNT                --已核销金额
               ,OFFEXPOSOURCE                   --表外暴露来源
               ,OFFBUSINESSTYPE                 --表外业务类型
               ,OFFBUSINESSSDVSSTD              --权重法表外业务类型细分
               ,UNCONDCANCELFLAG                --是否可随时无条件撤销
               ,CCFLEVEL                        --信用转换系数级别
               ,CCFAIRB                         --高级法信用转换系数
               ,CLAIMSLEVEL                     --债权级别
               ,BONDFLAG                        --是否为债券
               ,BONDISSUEINTENT                 --债券发行目的
               ,NSUREALPROPERTYFLAG             --是否非自用不动产
               ,REPASSETTERMTYPE                --抵债资产期限类型
               ,DEPENDONFPOBFLAG                --是否依赖于银行未来盈利
               ,IRATING                         --内部评级
               ,PD                              --违约概率
               ,LGDLEVEL                        --违约损失率级别
               ,LGDAIRB                         --高级法违约损失率
               ,MAIRB                           --高级法有效期限
               ,EADAIRB                         --高级法违约风险暴露
               ,DEFAULTFLAG                     --违约标识
               ,BEEL                            --已违约暴露预期损失比率
               ,DEFAULTLGD                      --已违约暴露违约损失率
               ,EQUITYEXPOFLAG                  --股权暴露标识
               ,EQUITYINVESTTYPE                --股权投资对象类型
               ,EQUITYINVESTCAUSE               --股权投资形成原因
               ,SLFLAG                          --专业贷款标识
               ,SLTYPE                          --专业贷款类型
               ,PFPHASE                         --项目融资阶段
               ,REGURATING                      --监管评级
               ,CBRCMPRATINGFLAG                --银监会认定评级是否更为审慎
               ,LARGEFLUCFLAG                   --是否波动性较大
               ,LIQUEXPOFLAG                    --是否清算过程中风险暴露
               ,PAYMENTDEALFLAG                 --是否货款对付模式
               ,DELAYTRADINGDAYS                --延迟交易天数
               ,SECURITIESFLAG                  --有价证券标识
               ,SECUISSUERID                    --证券发行人ID
               ,RATINGDURATIONTYPE              --评级期限类型
               ,SECUISSUERATING                 --证券发行等级
               ,SECURESIDUALM                   --证券剩余期限
               ,SECUREVAFREQUENCY               --证券重估频率
               ,CCPTRANFLAG                     --是否中央交易对手相关交易
               ,CCPID                           --中央交易对手ID
               ,QUALCCPFLAG                     --是否合格中央交易对手
               ,BANKROLE                        --银行角色
               ,CLEARINGMETHOD                  --清算方式
               ,BANKASSETFLAG                   --是否银行提交资产
               ,MATCHCONDITIONS                 --符合条件情况
               ,SFTFLAG                         --证券融资交易标识
               ,MASTERNETAGREEFLAG              --净额结算主协议标识
               ,MASTERNETAGREEID                --净额结算主协议ID
               ,SFTTYPE                         --证券融资交易类型
               ,SECUOWNERTRANSFLAG              --证券所有权是否转移
               ,OTCFLAG                         --场外衍生工具标识
               ,VALIDNETTINGFLAG                --有效净额结算协议标识
               ,VALIDNETAGREEMENTID             --有效净额结算协议ID
               ,OTCTYPE                         --场外衍生工具类型
               ,DEPOSITRISKPERIOD               --保证金风险期间
               ,MTM                             --重置成本
               ,MTMCURRENCY                     --重置成本币种
               ,BUYERORSELLER                   --买方卖方
               ,QUALROFLAG                      --合格参照资产标识
               ,ROISSUERPERFORMFLAG             --参照资产发行人是否能履约
               ,BUYERINSOLVENCYFLAG             --信用保护买方是否破产
               ,NONPAYMENTFEES                  --尚未支付费用
               ,RETAILEXPOFLAG                  --零售暴露标识
               ,RETAILCLAIMTYPE                 --零售债权类型
               ,MORTGAGETYPE                    --住房抵押贷款类型
               ,EXPONUMBER                      --风险暴露个数
               ,LTV                             --贷款价值比
               ,AGING                           --账龄
               ,NEWDEFAULTDEBTFLAG              --新增违约债项标识
               ,PDPOOLMODELID                   --PD分池模型ID
               ,LGDPOOLMODELID                  --LGD分池模型ID
               ,CCFPOOLMODELID                  --CCF分池模型ID
               ,PDPOOLID                        --所属PD池ID
               ,LGDPOOLID                       --所属LGD池ID
               ,CCFPOOLID                       --所属CCF池ID
               ,ABSUAFLAG                       --资产证券化基础资产标识
               ,ABSPOOLID                       --证券化资产池ID
               ,GROUPID                         --分组编号
               ,DefaultDate                     --违约时点
               ,ABSPROPORTION                   --资产证券化比重
               ,DEBTORNUMBER                    --借款人个数
               ,flag
    )
    SELECT
                TO_DATE(T1.DATANO,'YYYYMMDD')                                           AS DATADATE                -- 数据日期
                ,T1.DATANO                                                              AS DATANO                  -- 数据流水号
                ,T1.SERIALNO                                                            AS EXPOSUREID              -- 风险暴露ID
                ,T1.SERIALNO                                                            AS DUEID                   -- 债项ID
                ,'XD'                                                                   AS SSYSID                  -- 源系统ID
                ,T1.RELATIVESERIALNO2                                                   AS CONTRACTID              -- 合同ID
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN DECODE(T4.LINETYPE,'0030','XN-GRKH','XN-YBGS')                 --若客户ID为空，条线为个人就虚拟为个人客户，否则为一般公司
                      ELSE T1.CUSTOMERID
                 END                                                                    AS CLIENTID                -- 参与主体ID
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS SORGID                  -- 源机构ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T3.ORGNAME,'总行')                                                              AS SORGNAME                -- 源机构名称
                ,nvl(T3.SORTNO,'1010')                                                              AS ORGSORTNO               -- 所属机构排序号
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS ORGID                   -- 所属机构ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T3.ORGNAME,'总行')                                                             AS ORGNAME                 -- 所属机构名称
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS ACCORGID                -- 账务机构ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T3.ORGNAME,'总行')                                                             AS ACCORGNAME              -- 账务机构名称
                ,NVL(T4.DIRECTION,CPS.DIRECTION)                                        AS INDUSTRYID              -- 所属行业代码
                ,NVL(T5.ITEMNAME,CL.ITEMNAME)                                           AS INDUSTRYNAME            -- 所属行业名称
                ,CASE WHEN T1.BUSINESSTYPE IN('10202010','10201010','1035102010','1035102020') THEN '0102'        --进口信用证，跨境保函   归到 大中-贸金部
                      WHEN T1.BUSINESSTYPE IN('1035101020','1035101010') THEN '0101' --融资性保函 非融资性保函 归到大中-客户 by chengang
                      WHEN T4.LINETYPE='0010' THEN '0101'
                      WHEN T4.LINETYPE='0020' THEN '0201'
                      WHEN T4.LINETYPE='0030' THEN '0301'
                      WHEN T4.LINETYPE='0040' THEN '0401'
                      ELSE '0101'
                 END                                                                    AS BUSINESSLINE            -- 条线
                ,''                                                                     AS ASSETTYPE               -- 资产大类
                ,''                                                                     AS ASSETSUBTYPE            -- 资产小类
                ,T1.BUSINESSTYPE                                                        AS BUSINESSTYPEID          -- 业务品种代码
                ,T2.TYPENAME                                                            AS BUSINESSTYPENAME        -- 业务品种名称
                --,'01'                                                                   AS CREDITRISKDATATYPE      -- 信用风险数据类型          01-一般非零售
                ,CASE WHEN T16.CUSTOMERTYPE = '0310' OR T16.CERTTYPE LIKE 'Ind%'
                      THEN '02' --零售
                      ELSE '01' --非零售
                  END                                                                   AS CREDITRISKDATATYPE  --信用风险数据类型
                ,'01'                                                                   AS ASSETTYPEOFHAIRCUTS     -- 折扣系数对应资产类别     01-现金及现金等价物
                ,'07'                                                                   AS BUSINESSTYPESTD         -- 权重法业务类型
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN '0112'                                    --若客户ID为空，默认 其他(0112)
                      ELSE ''
                 END                                                                    AS EXPOCLASSSTD            -- 权重法暴露大类
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN DECODE(T4.LINETYPE,'0030','011215','011216')                          --若客户ID为空，条线为个人就默认 其他适用75%风险权重的资产(011215)，否则默认 其他适用100%风险权重的资产(011216)
                      ELSE ''
                 END                                                                    AS EXPOSUBCLASSSTD         -- 权重法暴露小类
                ,SUBSTR(T12.DITEMNO,1,4)                                                AS EXPOCLASSIRB            -- 内评法暴露大类
                ,T12.DITEMNO                                                            AS EXPOSUBCLASSIRB         -- 内评法暴露小类
                ,CASE WHEN T2.OFFSHEETFLAG='EntOff' THEN '02'   --  02-表外
                      ELSE '01'                                 --  01-表内
                 END                                                                   AS EXPOBELONG              -- 暴露所属标识
                ,'01'                                                                  AS BOOKTYPE                -- 账户类别           01-银行账户
                ,'03'                                                                  AS REGUTRANTYPE            -- 监管交易类型      03-抵押贷款
                ,'0'                                                                   AS REPOTRANFLAG            -- 回购交易标识       0-否
                ,1                                                                     AS REVAFREQUENCY           -- 重估频率
                ,NVL(T1.BUSINESSCURRENCY,'CNY')                                        AS CURRENCY                -- 币种
                ,T31.BALANCE                                                           AS NORMALPRINCIPAL         -- 正常本金余额
                ,0                                                                     AS OVERDUEBALANCE          -- 逾期余额
                ,0                                                                     AS NONACCRUALBALANCE       -- 非应计余额
                ,T31.BALANCE                                                           AS ONSHEETBALANCE          -- 表内余额
                ,0                                                                     AS NORMALINTEREST          -- 正常利息
                ,0                                                                     AS ONDEBITINTEREST         -- 表内欠息
                ,0                                                                     AS OFFDEBITINTEREST        -- 表外欠息
                ,0                                                                     AS EXPENSERECEIVABLE       -- 应收费用
                ,T31.BALANCE                                                           AS ASSETBALANCE            -- 资产余额
                ,CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010') THEN '70010100'  --信用证
                      /*T1.BUSINESSTYPE IN ('10201010','10202010')                     --信用证
                      THEN CASE WHEN (TO_DATE(T4.MATURITY,'YYYYMMDD')-TO_DATE(T4.OCCURDATE,'YYYYMMDD'))<=365      --经黄征确认，信用证期限用 到期日-发生日期
                                THEN '70010100'                                        --即期信用证
                                ELSE '70010200'                                        --远期信用证
                           END*/
                      WHEN T1.BUSINESSTYPE IN ('1035101010','1035102010') THEN '70180001'  --融资保函
                      WHEN T1.BUSINESSTYPE IN ('1035101020','1035102020') THEN '70180002'  --非融资保函
                      --WHEN SUBSTR(T1.BUSINESSTYPE,1,6) ='103510' THEN '70180000'       --保函
                      WHEN T1.BUSINESSTYPE='103550' THEN '70030000'                    --贷款承诺
                      WHEN T1.BUSINESSTYPE='1040101040' THEN '11112000'                --买入返售其他金融资产
                      ELSE '13070100'                                                  --垫款-承兑垫款
                 END                                                                   AS ACCSUBJECT1             -- 科目一
                ,''                                                                    AS ACCSUBJECT2             -- 科目二
                ,''                                                                    AS ACCSUBJECT3             -- 科目三
                ,NVL(T4.PUTOUTDATE,T4.OCCURDATE)                                       AS STARTDATE               -- 起始日期
                ,T4.MATURITY                                                           AS DUEDATE                 --到期日期
                ,CASE WHEN MONTHS_BETWEEN(TO_DATE(T4.MATURITY,'YYYYMMDD'),TO_DATE(NVL(T4.PUTOUTDATE,T4.OCCURDATE),'YYYYMMDD'))/12<0
                      THEN 0
                      ELSE MONTHS_BETWEEN(TO_DATE(T4.MATURITY,'YYYYMMDD'),TO_DATE(NVL(T4.PUTOUTDATE,T4.OCCURDATE),'YYYYMMDD'))/12
                 END AS OriginalMaturity    --原始期限
                 /*
                ,CASE WHEN MONTHS_BETWEEN(TO_DATE(T4.MATURITY,'YYYYMMDD'),TO_DATE(T1.DATANO,'YYYYMMDD'))/12<0
                                THEN 0
                                ELSE MONTHS_BETWEEN(TO_DATE(T4.MATURITY,'YYYYMMDD'),TO_DATE(T1.DATANO,'YYYYMMDD'))/12
                 END                                                                   AS ResidualM           --剩余期限
                
                
  ------20191127   BY  WZB  系统所有期限按365天计算
                CASE WHEN (TO_DATE(T4.MATURITY,'YYYYMMDD')-TO_DATE(CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010') THEN NVL(T4.PUTOUTDATE,T4.OCCURDATE)
                                                                       ELSE T4.PUTOUTDATE
                                                                    END,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T4.MATURITY,'YYYYMMDD')-TO_DATE(CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010') THEN NVL(T4.PUTOUTDATE,T4.OCCURDATE)
                                                                       ELSE T4.PUTOUTDATE
                                                                    END,'YYYYMMDD'))/365
                END                                                                     AS ORIGINALMATURITY        -- 原始期限
                */
                ,CASE WHEN (TO_DATE(T4.MATURITY,'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T4.MATURITY,'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365
                END                                                                 AS RESIDUALM               -- 剩余期限
                ,CASE WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='A' THEN '01'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='B' THEN '02'       --十二级分类转为五级分类
                      WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='C' THEN '03'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='D' THEN '04'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='E' THEN '05'
                      ELSE NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))
                 END                                                                   AS RISKCLASSIFY            -- 风险分类
                ,'01'                                                                  AS EXPOSURESTATUS          -- 风险暴露状态  01-正常
                ,0                                                                     AS OVERDUEDAYS             -- 逾期天数
                ,0                                                                     AS SPECIALPROVISION        -- 专项准备金-规则处理
                ,0                                                                     AS GENERALPROVISION        -- 一般准备金
                ,0                                                                     AS ESPECIALPROVISION       -- 特别准备金
                ,0                                                                     AS WRITTENOFFAMOUNT        -- 已核销金额
                ,CASE WHEN T2.OFFSHEETFLAG='EntOff'  THEN '03'                         --  03-实际表外业务
                      ELSE ''
                 END                                                                   AS OffExpoSource           -- 表外暴露来源
                ,''                                                                    AS OffBusinessType         -- 表外业务类型
                ,''                                                                    AS OffBusinessSdvsSTD      -- 权重法表外业务类型细分
                ,'1'                                                                   AS UncondCancelFlag        -- 是否可随时无条件撤销
                ,''                                                                    AS CCFLevel                -- 信用转换系数级别
                ,NULL                                                                  AS CCFAIRB                 -- 高级法信用转换系数
                ,'01'                                                                  AS CLAIMSLEVEL             -- 债权级别
                ,'0'                                                                   AS BONDFLAG                -- 是否为债券
                ,'02'                                                                  AS BONDISSUEINTENT         -- 债券发行目的
                ,'0'                                                                   AS NSUREALPROPERTYFLAG     -- 是否非自用不动产
                ,''                                                                    AS REPASSETTERMTYPE        -- 抵债资产期限类型
                ,'0'                                                                   AS DEPENDONFPOBFLAG        -- 是否依赖于银行未来盈利
                ,T6.PDADJLEVEL                                                         AS IRATING                 -- 内部评级
                ,T6.PD                                                                 AS PD                      -- 违约概率
                ,NULL                                                                  AS LGDLEVEL                -- 违约损失率级别
                ,0                                                                     AS LGDAIRB                 -- 高级法违约损失率
                ,0                                                                     AS MAIRB                   -- 高级法有效期限
                ,T31.BALANCE                                                           AS EADAIRB                 -- 高级法违约风险暴露
                ,CASE WHEN T6.PDADJCODE='D' THEN '1'
                      ELSE '0'
                 END                                                                   AS DEFAULTFLAG             -- 违约标识
                ,0                                                                     AS BEEL                    -- 已违约暴露预期损失比率
                ,0                                                                     AS DEFAULTLGD              -- 已违约暴露违约损失率
                ,'0'                                                                   AS EQUITYEXPOFLAG          -- 股权暴露标识
                ,''                                                                    AS EQUITYINVESTTYPE        -- 股权投资对象类型
                ,''                                                                    AS EQUITYINVESTCAUSE       -- 股权投资形成原因
                ,'0'                                                                   AS SLFLAG                  -- 专业贷款标识       专业贷款相关字段一期先赋空
                ,''                                                                    AS SLTYPE                  -- 专业贷款类型
                ,''                                                                    AS PFPHASE                 -- 项目融资阶段
                ,'01'                                                                  AS REGURATING              -- 监管评级
                ,''                                                                    AS CBRCMPRATINGFLAG        -- 银监会认定评级是否更为审慎
                ,''                                                                    AS LARGEFLUCFLAG           -- 是否波动性较大
                ,'0'                                                                   AS LIQUEXPOFLAG            -- 是否清算过程中风险暴露
                ,''                                                                    AS PAYMENTDEALFLAG         -- 是否货款对付模式
                ,0                                                                     AS DELAYTRADINGDAYS        -- 延迟交易天数
                ,'0'                                                                   AS SECURITIESFLAG          -- 有价证券标识
                ,''                                                                    AS SECUISSUERID            -- 证券发行人ID
                ,''                                                                    AS RATINGDURATIONTYPE      -- 评级期限类型
                ,''                                                                    AS SECUISSUERATING         -- 证券发行等级
                ,0                                                                     AS SECURESIDUALM           -- 证券剩余期限
                ,1                                                                     AS SECUREVAFREQUENCY       -- 证券重估频率
                ,'0'                                                                   AS CCPTRANFLAG             -- 是否中央交易对手相关交易
                ,''                                                                    AS CCPID                   -- 中央交易对手ID
                ,'0'                                                                   AS QUALCCPFLAG             -- 是否合格中央交易对手
                ,''                                                                    AS BANKROLE                -- 银行角色
                ,''                                                                    AS CLEARINGMETHOD          -- 清算方式
                ,'0'                                                                   AS BANKASSETFLAG           -- 是否银行提交资产
                ,''                                                                    AS MATCHCONDITIONS         -- 符合条件情况
                ,'0'                                                                   AS SFTFLAG                 -- 证券融资交易标识
                ,''                                                                    AS MASTERNETAGREEFLAG      -- 净额结算主协议标识
                ,''                                                                    AS MASTERNETAGREEID        -- 净额结算主协议ID
                ,''                                                                    AS SFTTYPE                 -- 证券融资交易类型
                ,''                                                                    AS SECUOWNERTRANSFLAG      -- 证券所有权是否转移
                ,'0'                                                                   AS OTCFLAG                 -- 场外衍生工具标识
                ,''                                                                    AS VALIDNETTINGFLAG        -- 有效净额结算协议标识
                ,''                                                                    AS VALIDNETAGREEMENTID     -- 有效净额结算协议ID
                ,''                                                                    AS OTCTYPE                 -- 场外衍生工具类型
                ,''                                                                    AS DEPOSITRISKPERIOD       -- 保证金风险期间
                ,0                                                                     AS MTM                     -- 重置成本
                ,''                                                                    AS MTMCURRENCY             -- 重置成本币种
                ,''                                                                    AS BUYERORSELLER           -- 买方卖方
                ,''                                                                    AS QUALROFLAG              -- 合格参照资产标识
                ,''                                                                    AS ROISSUERPERFORMFLAG     -- 参照资产发行人是否能履约
                ,''                                                                    AS BUYERINSOLVENCYFLAG     -- 信用保护买方是否破产
                ,0                                                                     AS NONPAYMENTFEES          -- 尚未支付费用
                ,'0'                                                                   AS RETAILEXPOFLAG          -- 零售暴露标识
                ,''                                                                    AS RETAILCLAIMTYPE         -- 零售债权类型
                ,''                                                                    AS MORTGAGETYPE            -- 住房抵押贷款类型
                ,1                                                                     AS EXPONUMBER              -- 风险暴露个数
                ,0.8                                                                   AS LTV                     --贷款价值比
                ,0                                                                     AS AGING                   --账龄
                ,''                                                                    AS NEWDEFAULTDEBTFLAG      --新增违约债项标识
                ,''                                                                    AS PDPOOLMODELID           -- PD分池模型ID
                ,''                                                                    AS LGDPOOLMODELID          -- LGD分池模型ID
                ,''                                                                    AS CCFPOOLMODELID          -- CCF分池模型ID
                ,''                                                                    AS PDPOOLID                -- 所属PD池ID
                ,''                                                                    AS LGDPOOLID               -- 所属LGD池ID
                ,''                                                                    AS CCFPOOLID               -- 所属CCF池ID
                ,CASE WHEN T10.PROJECTNO IS NULL THEN '0'
                      ELSE '1'
                 END                                                                   AS ABSUAFLAG           --资产证券化基础资产标识
                ,CASE WHEN T10.PROJECTNO IS NULL THEN ''
                      ELSE T10.PROJECTNO
                 END                                                                   AS ABSPOOLID           --证券化资产池ID
                ,''                                                                    AS GROUPID                 -- 分组编号
                ,CASE WHEN T6.PDADJCODE='D' THEN TO_DATE(T6.PDVAVLIDDATE,'YYYY/MM/DD')
                      ELSE NULL
                 END                                                                   AS DefaultDate             -- 违约时点
                ,0                                                                     AS ABSPROPORTION           --资产证券化比重
                ,0                                                                     AS DEBTORNUMBER            --借款人个数
                ,'BW'
    FROM        RWA_DEV.NCM_BUSINESS_DUEBILL T1
    INNER JOIN  RWA_DEV.NCM_BUSINESS_TYPE T2
    ON          T1.BUSINESSTYPE = T2.TYPENO
    AND         T1.DATANO = T2.DATANO
    INNER JOIN  RWA_DEV.NCM_BUSINESS_CONTRACT T4
    ON          T1.RELATIVESERIALNO2 = T4.SERIALNO
    AND         T1.DATANO = T4.DATANO
    INNER JOIN  RWA_DEV.NCM_BUSINESS_HISTORY T31
    ON          T1.SERIALNO = T31.SERIALNO
    AND         T31.BALANCE > 0
    AND         T31.DATANO = P_DATA_DT_STR
                /*rwa_dev.brd_loan_nor t31
    ON          t1.serialno = t31.CRDT_ACCT_NO
    AND         t31.CUR_BAL > 0
    AND         T31.DATANO = P_DATA_DT_STR*/
    LEFT JOIN   RWA.ORG_INFO T3
    ON          decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)  = T3.ORGID
    LEFT JOIN   RWA.CODE_LIBRARY T5
    ON          T4.DIRECTION = T5.ITEMNO
    AND         T5.CODENO = 'IndustryType'
    LEFT JOIN   RWA_DEV.RWA_TEMP_PDLEVEL T6
    ON          T1.CUSTOMERID=T6.CUSTID
    LEFT JOIN   (SELECT DISTINCT AA.PROJECTNO,AA.CONTRACTNO   --判断是否资产证券化
                   FROM RWA_DEV.NCM_ABS_PROJECT_ASSET AA
             INNER JOIN RWA_DEV.NCM_ABS_PROJECT_INFO BB
                     ON AA.PROJECTNO = BB.PROJECTNO
                    AND BB.DATANO = P_DATA_DT_STR
                    AND BB.PROJECTSTATUS = '0401'            --交割成功
                  WHERE AA.DATANO = P_DATA_DT_STR
                ) T10
    ON          T1.RELATIVESERIALNO2 = T10.CONTRACTNO
    LEFT JOIN   RWA_DEV.ncm_rwa_risk_expo_rst T11
    ON          T1.SERIALNO = T11.OBJECTNO
    AND         T11.OBJECTTYPE = 'BusinessDuebill'
    AND         T11.DATANO = P_DATA_DT_STR
    LEFT JOIN   RWA_DEV.RWA_CD_CODE_MAPPING T12
    ON          T11.RISKEXPOSURERESULT = T12.SITEMNO
    AND         T12.SCODENO = 'RwaResultType'
    LEFT JOIN   RWA_DEV.NCM_CLASSIFY_RECORD T13
    ON          T1.RELATIVESERIALNO2 = T13.OBJECTNO
    AND         T13.OBJECTTYPE = 'TwelveClassify'
    AND         T13.ISWORK = '1'
    AND         T13.DATANO = P_DATA_DT_STR
    LEFT JOIN   RWA_DEV.NCM_CUSTOMER_INFO T16
    ON          T1.CUSTOMERID = T16.CUSTOMERID
    AND         T1.DATANO = T16.DATANO
    LEFT JOIN   (
                select OBJECTNO, DIRECTION
                  from (select T.OBJECTNO,
                               T.DIRECTION,
                               ROW_NUMBER() OVER(PARTITION BY T.OBJECTNO order by T.SERIALNO DESC) AS RM
                          from RWA_DEV.NCM_PUTOUT_SCHEME T
                         where T.DATANO = P_DATA_DT_STR
                           and T.OBJECTTYPE = 'BusinessContract'
                           and T.DIRECTION IS NOT NULL)
                 where RM = 1
                ) CPS                 --额度类业务的行业投向需从提用表取
    ON          T4.SERIALNO = CPS.OBJECTNO
    LEFT JOIN   RWA.CODE_LIBRARY CL
    ON          CPS.DIRECTION = CL.ITEMNO
    AND         CL.CODENO = 'IndustryType'
    WHERE       T1.DATANO = P_DATA_DT_STR
    AND         T1.SERIALNO <> 'BD2014110400000001'
    AND         T1.BUSINESSTYPE IN ('10201010','10202010','1035101010','1035101020','1035102010','1035102020','103550','1040101040'
                --,'10352010','10352020'--承兑汇票已经包含在上面对公业务里面
                ) 
    --20170131期次后bl2017041800000067该笔买入返售其他金融资产提前到期，但借据和BH表的余额不一致(balance)，故强制排除该笔数据
    --20190820 该两笔买入返售其他金融资产提前到期'bl2017041800000065', 'bl2017041800000063' ,强制过滤
    AND         CASE WHEN P_DATA_DT_STR > '20171031' AND T1.SERIALNO IN('bl2017041800000065', 'bl2017041800000063', 'bl2017041800000067') THEN '0' ELSE '1' END = '1'       
    --AND T1.BALANCE>0
    ;
    COMMIT;
    
    /*5 插入 垫款(承兑垫款+保函垫款+信用证垫款) 对公业务flag=DK*/
    INSERT INTO RWA_DEV.RWA_XD_EXPOSURE(
               DATADATE         --01 数据日期
              ,DATANO           --02 数据流水号
              ,EXPOSUREID       --03 风险暴露ID
              ,DUEID            --04 债项ID
              ,SSYSID           --05 源系统ID
              ,CONTRACTID       --06 合同ID
              ,CLIENTID         --07 参与主体ID
              ,SORGID           --08 源机构ID
              ,SORGNAME         --09 源机构名称
              ,ORGSORTNO        --10 所属机构排序号
              ,ORGID            --11 所属机构ID
              ,ORGNAME          --12 所属机构名称
              ,ACCORGID         --13 账务机构ID
              ,ACCORGNAME       --14 账务机构名称
              ,INDUSTRYID       --15 所属行业代码
              ,INDUSTRYNAME     --16 所属行业名称
              ,BUSINESSLINE     --17 业务条线
              ,ASSETTYPE        --18 资产大类
              ,ASSETSUBTYPE     --19 资产小类
              ,BUSINESSTYPEID   --20 业务品种代码
              ,BUSINESSTYPENAME --21 业务品种名称
              ,CREDITRISKDATATYPE    --22 信用风险数据类型
              ,ASSETTYPEOFHAIRCUTS   --23 折扣系数对应资产类别
              ,BUSINESSTYPESTD       --24 权重法业务类型
              ,EXPOCLASSSTD          --25 权重法暴露大类
              ,EXPOSUBCLASSSTD       --26 权重法暴露小类
              ,EXPOCLASSIRB          --27 内评法暴露大类
              ,EXPOSUBCLASSIRB       --28 内评法暴露小类
              ,EXPOBELONG            --29 暴露所属标识
              ,BOOKTYPE              --30 账户类别
              ,REGUTRANTYPE          --31 监管交易类型
              ,REPOTRANFLAG          --32 回购交易标识
              ,REVAFREQUENCY         --33 重估频率
              ,CURRENCY              --34 币种
              ,NORMALPRINCIPAL       --35 正常本金余额
              ,OVERDUEBALANCE        --36 逾期余额
              ,NONACCRUALBALANCE     --37 非应计余额
              ,ONSHEETBALANCE        --38 表内余额
              ,NORMALINTEREST        --39 正常利息
              ,ONDEBITINTEREST       --40 表内欠息
              ,OFFDEBITINTEREST      --41 表外欠息
              ,EXPENSERECEIVABLE     --42 应收费用
              ,ASSETBALANCE          --43 资产余额
              ,ACCSUBJECT1           --44 科目一
              ,ACCSUBJECT2           --45 科目二
              ,ACCSUBJECT3           --46 科目三
              ,STARTDATE             --47 起始日期
              ,DUEDATE               --48 到期日期
              ,ORIGINALMATURITY      --49 原始期限
              ,RESIDUALM             --50 剩余期限
              ,RISKCLASSIFY          --51 风险分类
              ,EXPOSURESTATUS        --52 风险暴露状态
              ,OVERDUEDAYS           --53 逾期天数
              ,SPECIALPROVISION      --54 专项准备金
              ,GENERALPROVISION      --55 一般准备金
              ,ESPECIALPROVISION     --56 特别准备金
              ,WRITTENOFFAMOUNT      --57 已核销金额
              ,OFFEXPOSOURCE         --58 表外暴露来源
              ,OFFBUSINESSTYPE       --59 表外业务类型
              ,OFFBUSINESSSDVSSTD    --60 权重法表外业务类型细分
              ,UNCONDCANCELFLAG      --61 是否可随时无条件撤销
              ,CCFLEVEL              --62 信用转换系数级别
              ,CCFAIRB               --63 高级法信用转换系数
              ,CLAIMSLEVEL           --64 债权级别
              ,BONDFLAG              --65 是否为债券
              ,BONDISSUEINTENT       --66 债券发行目的
              ,NSUREALPROPERTYFLAG   --67 是否非自用不动产
              ,REPASSETTERMTYPE      --68 抵债资产期限类型
              ,DEPENDONFPOBFLAG      --69 是否依赖于银行未来盈利
              ,IRATING               --70 内部评级
              ,PD                    --71 违约概率
              ,LGDLEVEL              --72 违约损失率级别
              ,LGDAIRB               --73 高级法违约损失率
              ,MAIRB                 --74 高级法有效期限
              ,EADAIRB               --75 高级法违约风险暴露
              ,DEFAULTFLAG           --76 违约标识
              ,BEEL                  --77 已违约暴露预期损失比率
              ,DEFAULTLGD            --78 已违约暴露违约损失率
              ,EQUITYEXPOFLAG        --79 股权暴露标识
              ,EQUITYINVESTTYPE      --80 股权投资对象类型
              ,EQUITYINVESTCAUSE     --81 股权投资形成原因
              ,SLFLAG                --82 专业贷款标识
              ,SLTYPE                --83 专业贷款类型
              ,PFPHASE               --84 项目融资阶段
              ,REGURATING            --85 监管评级
              ,CBRCMPRATINGFLAG      --86 银监会认定评级是否更为审慎
              ,LARGEFLUCFLAG         --87 是否波动性较大
              ,LIQUEXPOFLAG          --88 是否清算过程中风险暴露
              ,PAYMENTDEALFLAG       --89 是否货款对付模式
              ,DELAYTRADINGDAYS      --90 延迟交易天数
              ,SECURITIESFLAG        --91 有价证券标识
              ,SECUISSUERID          --92 证券发行人ID
              ,RATINGDURATIONTYPE    --93 评级期限类型
              ,SECUISSUERATING       --94 证券发行等级
              ,SECURESIDUALM         --95 证券剩余期限
              ,SECUREVAFREQUENCY     --96 证券重估频率
              ,CCPTRANFLAG           --97 是否中央交易对手相关交易
              ,CCPID                 --98 中央交易对手ID
              ,QUALCCPFLAG           --99 是否合格中央交易对手
              ,BANKROLE              --100 银行角色
              ,CLEARINGMETHOD        --101 清算方式
              ,BANKASSETFLAG         --102 是否银行提交资产
              ,MATCHCONDITIONS       --103 符合条件情况
              ,SFTFLAG               --104 证券融资交易标识
              ,MASTERNETAGREEFLAG    --105 净额结算主协议标识
              ,MASTERNETAGREEID      --106 净额结算主协议ID
              ,SFTTYPE               --107 证券融资交易类型
              ,SECUOWNERTRANSFLAG    --108 证券所有权是否转移
              ,OTCFLAG               --109 场外衍生工具标识
              ,VALIDNETTINGFLAG      --110 有效净额结算协议标识
              ,VALIDNETAGREEMENTID   --111 有效净额结算协议ID
              ,OTCTYPE               --112 场外衍生工具类型
              ,DEPOSITRISKPERIOD     --113 保证金风险期间
              ,MTM                   --114 重置成本
              ,MTMCURRENCY           --115 重置成本币种
              ,BUYERORSELLER         --116 买方卖方
              ,QUALROFLAG            --117 合格参照资产标识
              ,ROISSUERPERFORMFLAG   --118 参照资产发行人是否能履约
              ,BUYERINSOLVENCYFLAG   --119 信用保护买方是否破产
              ,NONPAYMENTFEES        --120 尚未支付费用
              ,RETAILEXPOFLAG        --121 零售暴露标识
              ,RETAILCLAIMTYPE       --122 零售债权类型
              ,MORTGAGETYPE          --123 住房抵押贷款类型
              ,EXPONUMBER            --124 风险暴露个数
              ,LTV                   --125 贷款价值比
              ,AGING                 --126 账龄
              ,NEWDEFAULTDEBTFLAG    --127 新增违约债项标识
              ,PDPOOLMODELID         --128 PD分池模型ID
              ,LGDPOOLMODELID        --129 LGD分池模型ID
              ,CCFPOOLMODELID        --130 CCF分池模型ID
              ,PDPOOLID              --131 所属PD池ID
              ,LGDPOOLID             --132 所属LGD池ID
              ,CCFPOOLID             --133 所属CCF池ID
              ,ABSUAFLAG             --134 资产证券化基础资产标识
              ,ABSPOOLID             --135 证券化资产池ID
              ,GROUPID               --136 分组编号
              ,DefaultDate           --137 违约时点
              ,ABSPROPORTION         --138 资产证券化比重
              ,DEBTORNUMBER          --139 借款人个数
              ,flag
    
    )
    SELECT
                TO_DATE(T1.DATANO,'YYYYMMDD')                                           AS DATADATE                -- 数据日期
                ,T1.DATANO                                                              AS DATANO                  -- 数据流水号
                ,T1.SERIALNO                                                            AS EXPOSUREID              -- 风险暴露ID
                ,T1.SERIALNO                                                            AS DUEID                   -- 债项ID
                ,'XD'                                                                   AS SSYSID                  -- 源系统ID
                ,T1.RELATIVESERIALNO2                                                   AS CONTRACTID              -- 合同ID
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN DECODE(T3.LINETYPE,'0030','XN-GRKH','XN-YBGS')                 --若客户ID为空，条线为个人就虚拟为个人客户，否则为一般公司
                      ELSE T1.CUSTOMERID
                 END                                                                    AS CLIENTID                -- 参与主体ID
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS SORGID                  -- 源机构ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T4.ORGNAME,'总行')                                                             AS SORGNAME                -- 源机构名称
                ,nvl(T4.SORTNO,'1010')                                                              AS ORGSORTNO               -- 所属机构排序号
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS ORGID                   -- 所属机构ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T4.ORGNAME,'总行')  
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS ACCORGID                -- 账务机构ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T4.ORGNAME,'总行')                                                             AS ACCORGNAME              -- 账务机构名称
                ,NVL(T3.DIRECTION,CPS.DIRECTION)                                        AS INDUSTRYID              -- 所属行业代码
                ,NVL(T5.ITEMNAME,CL.ITEMNAME)                                           AS INDUSTRYNAME            -- 所属行业名称
                ,CASE WHEN T1.BUSINESSCURRENCY<>'CNY' THEN '0102'                            --外币的表内业务   大中-贸金部
                      WHEN T1.BUSINESSTYPE IN('10302010','10302015','10302020') THEN '0401'  --贴现业务        同业金融市场部
                      WHEN T3.LINETYPE='0010' THEN '0101'
                      WHEN T3.LINETYPE='0020' THEN '0201'
                      WHEN T3.LINETYPE='0030' THEN '0301'
                      WHEN T3.LINETYPE='0040' THEN '0401'
                      ELSE '0101'
                 END                                                                    AS BUSINESSLINE            -- 条线
                ,''                                                                     AS ASSETTYPE               -- 资产大类
                ,''                                                                     AS ASSETSUBTYPE            -- 资产小类
                ,T1.BUSINESSTYPE                                                        AS BUSINESSTYPEID          -- 业务品种代码
                ,T2.TYPENAME                                                            AS BUSINESSTYPENAME        -- 业务品种名称
                /*,CASE WHEN T1.SERIALNO='20170125c0000373' THEN '02'
                      ELSE '01'
                 END                                                                    AS CREDITRISKDATATYPE      -- 信用风险数据类型          01-一般非零售
                */
                ,CASE WHEN T16.CUSTOMERTYPE = '0310' OR T16.CERTTYPE LIKE 'Ind%'
                      THEN '02' --零售
                      ELSE '01' --非零售
                  END                                                                   AS CREDITRISKDATATYPE      --信用风险数据类型
                ,'01'                                                                   AS ASSETTYPEOFHAIRCUTS     -- 折扣系数对应资产类别     01-现金及现金等价物
                ,''                                                                     AS BUSINESSTYPESTD         -- 权重法业务类型
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN '0112'                                    --若客户ID为空，默认 其他(0112)
                      ELSE ''
                 END                                                                    AS EXPOCLASSSTD            -- 权重法暴露大类
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN DECODE(T3.LINETYPE,'0030','011215','011216')                          --若客户ID为空，条线为个人就默认 其他适用75%风险权重的资产(011215)，否则默认 其他适用100%风险权重的资产(011216)
                      ELSE ''
                 END                                                                    AS EXPOSUBCLASSSTD         -- 权重法暴露小类
                ,SUBSTR(T10.DITEMNO,1,4)                                                AS EXPOCLASSIRB            -- 内评法暴露大类
                ,T10.DITEMNO                                                            AS EXPOSUBCLASSIRB         -- 内评法暴露小类
                ,CASE WHEN T2.OFFSHEETFLAG='EntOff' THEN '02'   --  02-表外
                      ELSE '01'                                 --  01-表内
                 END                                                                   AS EXPOBELONG              -- 暴露所属标识
                ,'01'                                                                  AS BOOKTYPE                -- 账户类别           01-银行账户
                ,'03'                                                                  AS REGUTRANTYPE            -- 监管交易类型      03-抵押贷款
                ,'0'                                                                   AS REPOTRANFLAG            -- 回购交易标识       0-否
                ,1                                                                     AS REVAFREQUENCY           -- 重估频率
                ,NVL(T1.BUSINESSCURRENCY,'CNY')                                        AS CURRENCY                -- 币种
                ,t1.balance                                                            AS NORMALPRINCIPAL         -- 正常本金余额
                ,0                                                                     AS OVERDUEBALANCE          -- 逾期余额
                ,0                                                                     AS NONACCRUALBALANCE       -- 非应计余额
                ,t1.balance                                                            AS ONSHEETBALANCE          -- 表内余额
                ,0                                                                     AS NORMALINTEREST          -- 正常利息
                ,0                                                                     AS ONDEBITINTEREST         -- 表内欠息
                ,0                                                                     AS OFFDEBITINTEREST        -- 表外欠息
                ,0                                                                     AS EXPENSERECEIVABLE       -- 应收费用
                ,t1.balance                                                            AS ASSETBALANCE            -- 资产余额
                ,T31.sbjt_cd                                                          AS ACCSUBJECT1             -- 科目一
                ,''                                                                    AS ACCSUBJECT2             -- 科目二
                ,''                                                                    AS ACCSUBJECT3             -- 科目三
                ,NVL(T1.PUTOUTDATE,T3.PUTOUTDATE)                                      AS STARTDATE               -- 起始日期
                ,NVL(T1.ACTUALMATURITY,T3.MATURITY)                                    AS DUEDATE                 -- 到期日期
                ,CASE WHEN (TO_DATE(NVL(T1.ACTUALMATURITY,T3.MATURITY),'YYYYMMDD')-TO_DATE(NVL(T1.PUTOUTDATE,T3.PUTOUTDATE),'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(NVL(T1.ACTUALMATURITY,T3.MATURITY),'YYYYMMDD')-TO_DATE(NVL(T1.PUTOUTDATE,T3.PUTOUTDATE),'YYYYMMDD'))/365
                END                                                                    AS ORIGINALMATURITY        -- 原始期限
                ,CASE WHEN (TO_DATE(NVL(T1.ACTUALMATURITY,T3.MATURITY),'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(NVL(T1.ACTUALMATURITY,T3.MATURITY),'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365
                END                                                                    AS RESIDUALM               -- 剩余期限
                ,CASE WHEN NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))='A' THEN '01'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))='B' THEN '02'       --十二级分类转为五级分类
                      WHEN NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))='C' THEN '03'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))='D' THEN '04'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))='E' THEN '05'
                      ELSE NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))
                 END                                                                   AS RISKCLASSIFY            -- 风险分类
                ,'01'                                                                  AS EXPOSURESTATUS          -- 风险暴露状态  01-正常
                ,T1.OVERDUEDAYS                                                        AS OVERDUEDAYS             -- 逾期天数
                ,0                                                                     AS SPECIALPROVISION        -- 专项准备金-规则处理
                ,0                                                                     AS GENERALPROVISION        -- 一般准备金
                ,0                                                                     AS ESPECIALPROVISION       -- 特别准备金
                ,T1.CANCELSUM                                                          AS WRITTENOFFAMOUNT        -- 已核销金额
                ,CASE WHEN T2.OFFSHEETFLAG='EntOff'  THEN '03'                         --  03-实际表外业务
                      ELSE ''
                 END                                                                   AS OffExpoSource           -- 表外暴露来源
                ,''                                                                    AS OffBusinessType         -- 表外业务类型
                ,''                                                                    AS OffBusinessSdvsSTD      -- 权重法表外业务类型细分
                ,'1'                                                                   AS UncondCancelFlag        -- 是否可随时无条件撤销
                ,''                                                                    AS CCFLevel                -- 信用转换系数级别
                ,NULL                                                                  AS CCFAIRB                 -- 高级法信用转换系数
                ,'01'                                                                  AS CLAIMSLEVEL             -- 债权级别
                ,'0'                                                                   AS BONDFLAG                -- 是否为债券
                ,'02'                                                                  AS BONDISSUEINTENT         -- 债券发行目的
                ,'0'                                                                   AS NSUREALPROPERTYFLAG     -- 是否非自用不动产
                ,''                                                                    AS REPASSETTERMTYPE        -- 抵债资产期限类型
                ,'0'                                                                   AS DEPENDONFPOBFLAG        -- 是否依赖于银行未来盈利
                ,T6.PDADJLEVEL                                                         AS IRATING                 -- 内部评级
                ,T6.PD                                                                 AS PD                      -- 违约概率
                ,''                                                                    AS LGDLEVEL                -- 违约损失率级别
                ,0                                                                     AS LGDAIRB                 -- 高级法违约损失率
                ,0                                                                     AS MAIRB                   -- 高级法有效期限
                ,T1.Balance                                                            AS EADAIRB                 -- 高级法违约风险暴露
                ,CASE WHEN T6.PDADJCODE='D' THEN '1'
                      ELSE '0'
                 END                                                                   AS DEFAULTFLAG             -- 违约标识
                ,0                                                                     AS BEEL                    -- 已违约暴露预期损失比率
                ,0                                                                     AS DEFAULTLGD              -- 已违约暴露违约损失率
                ,'0'                                                                   AS EQUITYEXPOFLAG          -- 股权暴露标识
                ,''                                                                    AS EQUITYINVESTTYPE        -- 股权投资对象类型
                ,''                                                                    AS EQUITYINVESTCAUSE       -- 股权投资形成原因
                ,'0'                                                                   AS SLFLAG                  -- 专业贷款标识       专业贷款相关字段一期先赋空
                ,''                                                                    AS SLTYPE                  -- 专业贷款类型
                ,''                                                                    AS PFPHASE                 -- 项目融资阶段
                ,'01'                                                                  AS REGURATING              -- 监管评级
                ,''                                                                    AS CBRCMPRATINGFLAG        -- 银监会认定评级是否更为审慎
                ,''                                                                    AS LARGEFLUCFLAG           -- 是否波动性较大
                ,'0'                                                                   AS LIQUEXPOFLAG            -- 是否清算过程中风险暴露
                ,''                                                                    AS PAYMENTDEALFLAG         -- 是否货款对付模式
                ,0                                                                     AS DELAYTRADINGDAYS        -- 延迟交易天数
                ,'0'                                                                   AS SECURITIESFLAG          -- 有价证券标识
                ,''                                                                    AS SECUISSUERID            -- 证券发行人ID
                ,''                                                                    AS RATINGDURATIONTYPE      -- 评级期限类型
                ,''                                                                    AS SECUISSUERATING         -- 证券发行等级
                ,0                                                                     AS SECURESIDUALM           -- 证券剩余期限
                ,1                                                                     AS SECUREVAFREQUENCY       -- 证券重估频率
                ,'0'                                                                   AS CCPTRANFLAG             -- 是否中央交易对手相关交易
                ,''                                                                    AS CCPID                   -- 中央交易对手ID
                ,'0'                                                                   AS QUALCCPFLAG             -- 是否合格中央交易对手
                ,''                                                                    AS BANKROLE                -- 银行角色
                ,''                                                                    AS CLEARINGMETHOD          -- 清算方式
                ,'0'                                                                   AS BANKASSETFLAG           -- 是否银行提交资产
                ,''                                                                    AS MATCHCONDITIONS         -- 符合条件情况
                ,'0'                                                                   AS SFTFLAG                 -- 证券融资交易标识
                ,''                                                                    AS MASTERNETAGREEFLAG      -- 净额结算主协议标识
                ,''                                                                    AS MASTERNETAGREEID        -- 净额结算主协议ID
                ,''                                                                    AS SFTTYPE                 -- 证券融资交易类型
                ,''                                                                    AS SECUOWNERTRANSFLAG      -- 证券所有权是否转移
                ,'0'                                                                   AS OTCFLAG                 -- 场外衍生工具标识
                ,''                                                                    AS VALIDNETTINGFLAG        -- 有效净额结算协议标识
                ,''                                                                    AS VALIDNETAGREEMENTID     -- 有效净额结算协议ID
                ,''                                                                    AS OTCTYPE                 -- 场外衍生工具类型
                ,''                                                                    AS DEPOSITRISKPERIOD       -- 保证金风险期间
                ,0                                                                     AS MTM                     -- 重置成本
                ,''                                                                    AS MTMCURRENCY             -- 重置成本币种
                ,''                                                                    AS BUYERORSELLER           -- 买方卖方
                ,''                                                                    AS QUALROFLAG              -- 合格参照资产标识
                ,''                                                                    AS ROISSUERPERFORMFLAG     -- 参照资产发行人是否能履约
                ,''                                                                    AS BUYERINSOLVENCYFLAG     -- 信用保护买方是否破产
                ,0                                                                     AS NONPAYMENTFEES          -- 尚未支付费用
                ,'0'                                                                   AS RETAILEXPOFLAG          -- 零售暴露标识
                ,''                                                                    AS RETAILCLAIMTYPE         -- 零售债权类型
                ,''                                                                    AS MORTGAGETYPE            -- 住房抵押贷款类型
                ,1                                                                     AS EXPONUMBER              -- 风险暴露个数
                ,0.8                                                                   AS LTV                     --贷款价值比
                ,0                                                                     AS AGING                   --账龄
                ,''                                                                    AS NEWDEFAULTDEBTFLAG      --新增违约债项标识
                ,''                                                                    AS PDPOOLMODELID           -- PD分池模型ID
                ,''                                                                    AS LGDPOOLMODELID          -- LGD分池模型ID
                ,''                                                                    AS CCFPOOLMODELID          -- CCF分池模型ID
                ,''                                                                    AS PDPOOLID                -- 所属PD池ID
                ,''                                                                    AS LGDPOOLID               -- 所属LGD池ID
                ,''                                                                    AS CCFPOOLID               -- 所属CCF池ID
                ,CASE WHEN T9.PROJECTNO IS NULL THEN '0'
                      ELSE '1'
                 END                                                                   AS ABSUAFLAG           --资产证券化基础资产标识
                ,CASE WHEN T9.PROJECTNO IS NULL THEN ''
                      ELSE T9.PROJECTNO
                 END                                                                   AS ABSPOOLID           --证券化资产池ID
                ,''                                                                    AS GROUPID                 -- 分组编号
                ,CASE WHEN T6.PDADJCODE='D' THEN TO_DATE(T6.PDVAVLIDDATE,'YYYYMMDD')
                      ELSE NULL
                 END                                                                   AS DefaultDate             -- 违约时点
                ,0                                                                     AS ABSPROPORTION           --资产证券化比重
                ,0                                                                     AS DEBTORNUMBER            --借款人个数
                ,'DK'
    FROM        RWA_DEV.NCM_BUSINESS_DUEBILL T1
    INNER JOIN  RWA_DEV.NCM_BUSINESS_TYPE T2
    ON          T1.BUSINESSTYPE = T2.TYPENO
    AND         T1.DATANO = T2.DATANO
    --AND         T2.ATTRIBUTE1 <> '2'                    --只取对公业务
    --AND         T2.TYPENO NOT LIKE '30%'                --排除额度类业务
    INNER JOIN  RWA_DEV.NCM_BUSINESS_CONTRACT T3
    ON          T1.RELATIVESERIALNO2 = T3.SERIALNO      --对账以借据为准，所以关联合同时，不应该加合同的有效条件
    AND         T1.DATANO = T3.DATANO
    INNER JOIN  rwa_dev.BRD_LOAN_NOR T31                  --支付集市正常贷款
    ON          T1.SERIALNO = T31.CRDT_ACCT_NO
    AND         t31.cur_bal > 0
    AND         t31.datano = P_DATA_DT_STR
    AND         T31.SBJT_CD LIKE '1307%'  --只取垫款   
    LEFT JOIN   RWA.ORG_INFO T4
    ON          decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)  = T4.ORGID
    LEFT JOIN   RWA.CODE_LIBRARY T5
    ON          T3.DIRECTION = T5.ITEMNO
    AND         T5.CODENO = 'IndustryType'
    LEFT JOIN   RWA_DEV.RWA_TEMP_PDLEVEL T6
    ON          T1.CUSTOMERID = T6.CUSTID
    LEFT JOIN   (SELECT DISTINCT AA.PROJECTNO,AA.CONTRACTNO   --判断是否资产证券化
                   FROM RWA_DEV.NCM_ABS_PROJECT_ASSET AA --项目内资产
             INNER JOIN RWA_DEV.NCM_ABS_PROJECT_INFO BB  --项目基本信息
                     ON AA.PROJECTNO = BB.PROJECTNO
                    AND BB.DATANO = P_DATA_DT_STR
                    AND BB.PROJECTSTATUS = '0401'             --交割成功
                  WHERE AA.DATANO = P_DATA_DT_STR
                ) T9
    ON          T3.SERIALNO = T9.CONTRACTNO
    LEFT JOIN   RWA_DEV.ncm_rwa_risk_expo_rst T3 --风险暴露结果信息表
    ON          T1.SERIALNO = T3.OBJECTNO
    AND         T3.OBJECTTYPE = 'BusinessDuebill'
    AND         T3.DATANO = P_DATA_DT_STR
    LEFT JOIN   RWA_DEV.RWA_CD_CODE_MAPPING T10 --代码映射转换表
    ON          T3.RISKEXPOSURERESULT = T10.SITEMNO
    AND         T10.SCODENO = 'RwaResultType'
    LEFT JOIN   RWA_DEV.NCM_CLASSIFY_RECORD T11 --五级分类信息表
    ON          T1.RELATIVESERIALNO2 = T11.OBJECTNO
    AND         T11.OBJECTTYPE = 'TwelveClassify'
    AND         T11.ISWORK = '1'
    AND         T11.DATANO = P_DATA_DT_STR
    LEFT JOIN   RWA_DEV.NCM_CUSTOMER_INFO T16
    ON          T1.CUSTOMERID = T16.CUSTOMERID
    AND         T1.DATANO = T16.DATANO
    LEFT JOIN   (
                select OBJECTNO, DIRECTION
                  from (select T.OBJECTNO,
                               T.DIRECTION,
                               ROW_NUMBER() OVER(PARTITION BY T.OBJECTNO order by T.SERIALNO DESC) AS RM
                          from RWA_DEV.NCM_PUTOUT_SCHEME T  --额度提用表
                         where T.DATANO = P_DATA_DT_STR
                           and T.OBJECTTYPE = 'BusinessContract'
                           and T.DIRECTION IS NOT NULL)
                 where RM = 1
                ) CPS                 --额度类业务的行业投向需从提用表取
    ON          T3.SERIALNO = CPS.OBJECTNO
    LEFT JOIN   RWA.CODE_LIBRARY CL
    ON          CPS.DIRECTION = CL.ITEMNO
    AND         CL.CODENO = 'IndustryType'
    WHERE       T1.DATANO = P_DATA_DT_STR
    AND         T1.BALANCE>0
   ;
    COMMIT; 

    /*6 插入 信贷系统零售借据 到目标表*/    --插入逾期贷款（微粒贷）明细暴露 
    INSERT INTO RWA_DEV.RWA_XD_EXPOSURE(
                DATADATE                        --数据日期
               ,DATANO                          --数据流水号
               ,EXPOSUREID                      --风险暴露ID
               ,DUEID                           --债项ID
               ,SSYSID                          --源系统ID
               ,CONTRACTID                      --合同ID
               ,CLIENTID                        --参与主体ID
               ,SORGID                          --源机构ID
               ,SORGNAME                        --源机构名称
               ,ORGSORTNO                       --所属机构排序号
               ,ORGID                           --所属机构ID
               ,ORGNAME                         --所属机构名称
               ,ACCORGID                        --账务机构ID
               ,ACCORGNAME                      --账务机构名称
               ,INDUSTRYID                      --所属行业代码
               ,INDUSTRYNAME                    --所属行业名称
               ,BUSINESSLINE                    --业务条线
               ,ASSETTYPE                       --资产大类
               ,ASSETSUBTYPE                    --资产小类
               ,BUSINESSTYPEID                  --业务品种代码
               ,BUSINESSTYPENAME                --业务品种名称
               ,CREDITRISKDATATYPE              --信用风险数据类型
               ,ASSETTYPEOFHAIRCUTS             --折扣系数对应资产类别
               ,BUSINESSTYPESTD                 --权重法业务类型
               ,EXPOCLASSSTD                    --权重法暴露大类
               ,EXPOSUBCLASSSTD                 --权重法暴露小类
               ,EXPOCLASSIRB                    --内评法暴露大类
               ,EXPOSUBCLASSIRB                 --内评法暴露小类
               ,EXPOBELONG                      --暴露所属标识
               ,BOOKTYPE                        --账户类别
               ,REGUTRANTYPE                    --监管交易类型
               ,REPOTRANFLAG                    --回购交易标识
               ,REVAFREQUENCY                   --重估频率
               ,CURRENCY                        --币种
               ,NORMALPRINCIPAL                 --正常本金余额
               ,OVERDUEBALANCE                  --逾期余额
               ,NONACCRUALBALANCE               --非应计余额
               ,ONSHEETBALANCE                  --表内余额
               ,NORMALINTEREST                  --正常利息
               ,ONDEBITINTEREST                 --表内欠息
               ,OFFDEBITINTEREST                --表外欠息
               ,EXPENSERECEIVABLE               --应收费用
               ,ASSETBALANCE                    --资产余额
               ,ACCSUBJECT1                     --科目一
               ,ACCSUBJECT2                     --科目二
               ,ACCSUBJECT3                     --科目三
               ,STARTDATE                       --起始日期
               ,DUEDATE                         --到期日期
               ,ORIGINALMATURITY                --原始期限
               ,RESIDUALM                       --剩余期限
               ,RISKCLASSIFY                    --风险分类
               ,EXPOSURESTATUS                  --风险暴露状态
               ,OVERDUEDAYS                     --逾期天数
               ,SPECIALPROVISION                --专项准备金
               ,GENERALPROVISION                --一般准备金
               ,ESPECIALPROVISION               --特别准备金
               ,WRITTENOFFAMOUNT                --已核销金额
               ,OFFEXPOSOURCE                   --表外暴露来源
               ,OFFBUSINESSTYPE                 --表外业务类型
               ,OFFBUSINESSSDVSSTD              --权重法表外业务类型细分
               ,UNCONDCANCELFLAG                --是否可随时无条件撤销
               ,CCFLEVEL                        --信用转换系数级别
               ,CCFAIRB                         --高级法信用转换系数
               ,CLAIMSLEVEL                     --债权级别
               ,BONDFLAG                        --是否为债券
               ,BONDISSUEINTENT                 --债券发行目的
               ,NSUREALPROPERTYFLAG             --是否非自用不动产
               ,REPASSETTERMTYPE                --抵债资产期限类型
               ,DEPENDONFPOBFLAG                --是否依赖于银行未来盈利
               ,IRATING                         --内部评级
               ,PD                              --违约概率
               ,LGDLEVEL                        --违约损失率级别
               ,LGDAIRB                         --高级法违约损失率
               ,MAIRB                           --高级法有效期限
               ,EADAIRB                         --高级法违约风险暴露
               ,DEFAULTFLAG                     --违约标识
               ,BEEL                            --已违约暴露预期损失比率
               ,DEFAULTLGD                      --已违约暴露违约损失率
               ,EQUITYEXPOFLAG                  --股权暴露标识
               ,EQUITYINVESTTYPE                --股权投资对象类型
               ,EQUITYINVESTCAUSE               --股权投资形成原因
               ,SLFLAG                          --专业贷款标识
               ,SLTYPE                          --专业贷款类型
               ,PFPHASE                         --项目融资阶段
               ,REGURATING                      --监管评级
               ,CBRCMPRATINGFLAG                --银监会认定评级是否更为审慎
               ,LARGEFLUCFLAG                   --是否波动性较大
               ,LIQUEXPOFLAG                    --是否清算过程中风险暴露
               ,PAYMENTDEALFLAG                 --是否货款对付模式
               ,DELAYTRADINGDAYS                --延迟交易天数
               ,SECURITIESFLAG                  --有价证券标识
               ,SECUISSUERID                    --证券发行人ID
               ,RATINGDURATIONTYPE              --评级期限类型
               ,SECUISSUERATING                 --证券发行等级
               ,SECURESIDUALM                   --证券剩余期限
               ,SECUREVAFREQUENCY               --证券重估频率
               ,CCPTRANFLAG                     --是否中央交易对手相关交易
               ,CCPID                           --中央交易对手ID
               ,QUALCCPFLAG                     --是否合格中央交易对手
               ,BANKROLE                        --银行角色
               ,CLEARINGMETHOD                  --清算方式
               ,BANKASSETFLAG                   --是否银行提交资产
               ,MATCHCONDITIONS                 --符合条件情况
               ,SFTFLAG                         --证券融资交易标识
               ,MASTERNETAGREEFLAG              --净额结算主协议标识
               ,MASTERNETAGREEID                --净额结算主协议ID
               ,SFTTYPE                         --证券融资交易类型
               ,SECUOWNERTRANSFLAG              --证券所有权是否转移
               ,OTCFLAG                         --场外衍生工具标识
               ,VALIDNETTINGFLAG                --有效净额结算协议标识
               ,VALIDNETAGREEMENTID             --有效净额结算协议ID
               ,OTCTYPE                         --场外衍生工具类型
               ,DEPOSITRISKPERIOD               --保证金风险期间
               ,MTM                             --重置成本
               ,MTMCURRENCY                     --重置成本币种
               ,BUYERORSELLER                   --买方卖方
               ,QUALROFLAG                      --合格参照资产标识
               ,ROISSUERPERFORMFLAG             --参照资产发行人是否能履约
               ,BUYERINSOLVENCYFLAG             --信用保护买方是否破产
               ,NONPAYMENTFEES                  --尚未支付费用
               ,RETAILEXPOFLAG                  --零售暴露标识
               ,RETAILCLAIMTYPE                 --零售债权类型
               ,MORTGAGETYPE                    --住房抵押贷款类型
               ,EXPONUMBER                      --风险暴露个数
               ,LTV                             --贷款价值比
               ,AGING                           --账龄
               ,NEWDEFAULTDEBTFLAG              --新增违约债项标识
               ,PDPOOLMODELID                   --PD分池模型ID
               ,LGDPOOLMODELID                  --LGD分池模型ID
               ,CCFPOOLMODELID                  --CCF分池模型ID
               ,PDPOOLID                        --所属PD池ID
               ,LGDPOOLID                       --所属LGD池ID
               ,CCFPOOLID                       --所属CCF池ID
               ,ABSUAFLAG                       --资产证券化基础资产标识
               ,ABSPOOLID                       --证券化资产池ID
               ,GROUPID                         --分组编号
               ,DefaultDate                     --违约时点
               ,ABSPROPORTION                   --资产证券化比重
               ,DEBTORNUMBER                    --借款人个数
               ,flag
    )
    SELECT
                TO_DATE(T1.DATANO,'YYYYMMDD')                                           AS DATADATE                -- 数据日期
                ,T1.DATANO                                                              AS DATANO                  -- 数据流水号
                ,T1.SERIALNO                                                            AS EXPOSUREID              -- 风险暴露ID
                ,T1.SERIALNO                                                            AS DUEID                   -- 债项ID
                ,'XD'                                                                   AS SSYSID                  -- 源系统ID
                ,T1.RELATIVESERIALNO2                                                   AS CONTRACTID              -- 合同ID
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN DECODE(T3.LINETYPE,'0030','XN-GRKH','XN-YBGS')                 --若客户ID为空，条线为个人就虚拟为个人客户，否则为一般公司
                      ELSE T1.CUSTOMERID
                 END                                                                    AS CLIENTID                -- 参与主体ID
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS SORGID                  -- 源机构ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T4.ORGNAME,'总行')                                                             AS SORGNAME                -- 源机构名称
                ,nvl(T4.SORTNO,'1010')                                                              AS ORGSORTNO               -- 所属机构排序号
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS ORGID                   -- 所属机构ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T4.ORGNAME,'总行')                                                             AS ORGNAME                 -- 所属机构名称
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS ACCORGID                -- 账务机构ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T4.ORGNAME,'总行')                                                             AS ACCORGNAME              -- 账务机构名称
                ,NVL(T3.DIRECTION,CPS.DIRECTION)                                        AS INDUSTRYID              -- 所属行业代码
                ,NVL(T5.ITEMNAME,CL.ITEMNAME)                                           AS INDUSTRYNAME            -- 所属行业名称
                ,CASE WHEN T1.BUSINESSCURRENCY<>'CNY' THEN '0102'                            --外币的表内业务   大中-贸金部
                      WHEN T1.BUSINESSTYPE IN('10302010','10302015','10302020') THEN '0401'  --贴现业务        同业金融市场部
                      WHEN T3.LINETYPE='0010' THEN '0101'
                      WHEN T3.LINETYPE='0020' THEN '0201'
                      WHEN T3.LINETYPE='0030' THEN '0301'
                      WHEN T3.LINETYPE='0040' THEN '0401'
                      ELSE '0101'
                 END                                                                    AS BUSINESSLINE            -- 条线
                ,''                                                                     AS ASSETTYPE               -- 资产大类
                ,''                                                                     AS ASSETSUBTYPE            -- 资产小类
                ,'11103030'                                                             AS BUSINESSTYPEID          --业务品种代码
                ,'微粒贷'                                                               AS BUSINESSTYPENAME        -- 业务品种名称
                --,'02'                                                                   AS CREDITRISKDATATYPE      -- 信用风险数据类型          01-一般非零售
                ,CASE WHEN T16.CUSTOMERTYPE = '0310' OR T16.CERTTYPE LIKE 'Ind%'
                      THEN '02' --零售
                      ELSE '01' --非零售
                  END                                                                   AS CREDITRISKDATATYPE  --信用风险数据类型
                ,'01'                                                                   AS ASSETTYPEOFHAIRCUTS     -- 折扣系数对应资产类别     01-现金及现金等价物
                ,''                                                                     AS BUSINESSTYPESTD         -- 权重法业务类型
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN '0112'                                    --若客户ID为空，默认 其他(0112)
                      ELSE ''
                 END                                                                    AS EXPOCLASSSTD            -- 权重法暴露大类
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN DECODE(T3.LINETYPE,'0030','011215','011216')                          --若客户ID为空，条线为个人就默认 其他适用75%风险权重的资产(011215)，否则默认 其他适用100%风险权重的资产(011216)
                      ELSE ''
                 END                                                                    AS EXPOSUBCLASSSTD         -- 权重法暴露小类
                ,SUBSTR(T12.DITEMNO,1,4)                                                AS EXPOCLASSIRB            -- 内评法暴露大类
                ,T12.DITEMNO                                                            AS EXPOSUBCLASSIRB         -- 内评法暴露小类
                ,'01'                                                                   AS EXPOBELONG              -- 暴露所属标识
                ,'01'                                                                   AS BOOKTYPE                -- 账户类别           01-银行账户
                ,'03'                                                                   AS REGUTRANTYPE            -- 监管交易类型      03-抵押贷款
                ,'0'                                                                    AS REPOTRANFLAG            -- 回购交易标识       0-否
                ,1                                                                      AS REVAFREQUENCY           -- 重估频率
                ,t31.ccy_cd                                                             AS CURRENCY                -- 币种
                ,T1.BALANCE                                                            AS NORMALPRINCIPAL         -- 正常本金余额
                ,0                                                                      AS OVERDUEBALANCE          -- 逾期余额
                ,0                                                                      AS NONACCRUALBALANCE       -- 非应计余额
                ,T1.BALANCE                                                            AS ONSHEETBALANCE          -- 表内余额
                ,0                                                                      AS NORMALINTEREST          -- 正常利息
                ,0                                                                      AS ONDEBITINTEREST         -- 表内欠息
                ,0                                                                      AS OFFDEBITINTEREST        -- 表外欠息
                ,0                                                                      AS EXPENSERECEIVABLE       -- 应收费用
                ,T1.BALANCE                                                            AS ASSETBALANCE            -- 资产余额
                ,'13100001'                                                             AS ACCSUBJECT1             -- 科目一
                ,''                                                                     AS ACCSUBJECT2             -- 科目二
                ,''                                                                     AS ACCSUBJECT3             -- 科目三
                ,T1.PUTOUTDATE                                                          AS STARTDATE               -- 起始日期
                ,T1.ACTUALMATURITY                                                      AS DUEDATE                 -- 到期日期
                ,CASE WHEN (TO_DATE(T1.ACTUALMATURITY,'YYYYMMDD')-TO_DATE(T1.PUTOUTDATE,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T1.ACTUALMATURITY,'YYYYMMDD')-TO_DATE(T1.PUTOUTDATE,'YYYYMMDD'))/365
                END                                                                     AS ORIGINALMATURITY        -- 原始期限
                ,CASE WHEN (TO_DATE(T1.ACTUALMATURITY,'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T1.ACTUALMATURITY,'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365
                END                                                                     AS RESIDUALM               -- 剩余期限
                ,CASE WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='A' THEN '01'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='B' THEN '02'       --十二级分类转为五级分类
                      WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='C' THEN '03'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='D' THEN '04'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))='E' THEN '05'
                      ELSE NVL(T1.Classifyresult,SUBSTR(T13.FINALLYRESULT,1,1))
                 END                                                                    AS RISKCLASSIFY            -- 风险分类
                ,'01'                                                                   AS EXPOSURESTATUS          -- 风险暴露状态  01-正常
                ,T1.OVERDUEDAYS                                                         AS OVERDUEDAYS             -- 逾期天数
                ,0                                                                      AS SPECIALPROVISION        -- 专项准备金-规则处理
                ,0                                                                      AS GENERALPROVISION        -- 一般准备金
                ,0                                                                      AS ESPECIALPROVISION       -- 特别准备金
                ,T1.CANCELSUM                                                           AS WRITTENOFFAMOUNT        -- 已核销金额
                ,''                                                                     AS OffExpoSource           -- 表外暴露来源
                ,''                                                                     AS OffBusinessType         -- 表外业务类型
                ,''                                                                     AS OffBusinessSdvsSTD      -- 权重法表外业务类型细分
                ,'1'                                                                    AS UncondCancelFlag        -- 是否可随时无条件撤销
                ,''                                                                     AS CCFLevel                -- 信用转换系数级别
                ,T6.CCFVALUE                                                            AS CCFAIRB                 -- 高级法信用转换系数
                ,'01'                                                                   AS CLAIMSLEVEL             -- 债权级别
                ,'0'                                                                    AS BONDFLAG                -- 是否为债券
                ,'02'                                                                   AS BONDISSUEINTENT         -- 债券发行目的
                ,'0'                                                                    AS NSUREALPROPERTYFLAG     -- 是否非自用不动产
                ,''                                                                     AS REPASSETTERMTYPE        -- 抵债资产期限类型
                ,'0'                                                                    AS DEPENDONFPOBFLAG        -- 是否依赖于银行未来盈利
                ,''                                                                     AS IRATING                 -- 内部评级
                ,T6.PDVALUE                                                             AS PD                      -- 违约概率
                ,''                                                                     AS LGDLEVEL                -- 违约损失率级别
                ,T6.LGDVALUE                                                            AS LGDAIRB                 -- 高级法违约损失率
                ,0                                                                      AS MAIRB                   -- 高级法有效期限
                ,T1.BALANCE                                                            AS EADAIRB                 -- 高级法违约风险暴露
                ,CASE WHEN T6.DEFAULTFLAG='1' THEN '1'
                      ELSE '0'
                 END                                                                    AS DEFAULTFLAG             -- 违约标识
                ,T6.BEELVALUE                                                           AS BEEL                    -- 已违约暴露预期损失比率
                ,T6.LGDVALUE                                                            AS DEFAULTLGD              -- 已违约暴露违约损失率
                ,'0'                                                                    AS EQUITYEXPOFLAG          -- 股权暴露标识
                ,''                                                                     AS EQUITYINVESTTYPE        -- 股权投资对象类型
                ,''                                                                     AS EQUITYINVESTCAUSE       -- 股权投资形成原因
                ,'0'                                                                    AS SLFLAG                  -- 专业贷款标识       专业贷款相关字段一期先赋空
                ,''                                                                     AS SLTYPE                  -- 专业贷款类型
                ,''                                                                     AS PFPHASE                 -- 项目融资阶段
                ,'01'                                                                   AS REGURATING              -- 监管评级
                ,''                                                                     AS CBRCMPRATINGFLAG        -- 银监会认定评级是否更为审慎
                ,''                                                                     AS LARGEFLUCFLAG           -- 是否波动性较大
                ,'0'                                                                    AS LIQUEXPOFLAG            -- 是否清算过程中风险暴露
                ,''                                                                     AS PAYMENTDEALFLAG         -- 是否货款对付模式
                ,0                                                                      AS DELAYTRADINGDAYS        -- 延迟交易天数
                ,'0'                                                                    AS SECURITIESFLAG          -- 有价证券标识
                ,''                                                                     AS SECUISSUERID            -- 证券发行人ID
                ,''                                                                     AS RATINGDURATIONTYPE      -- 评级期限类型
                ,''                                                                     AS SECUISSUERATING         -- 证券发行等级
                ,0                                                                      AS SECURESIDUALM           -- 证券剩余期限
                ,1                                                                      AS SECUREVAFREQUENCY       -- 证券重估频率
                ,'0'                                                                    AS CCPTRANFLAG             -- 是否中央交易对手相关交易
                ,''                                                                     AS CCPID                   -- 中央交易对手ID
                ,'0'                                                                    AS QUALCCPFLAG             -- 是否合格中央交易对手
                ,''                                                                     AS BANKROLE                -- 银行角色
                ,''                                                                     AS CLEARINGMETHOD          -- 清算方式
                ,'0'                                                                    AS BANKASSETFLAG           -- 是否银行提交资产
                ,''                                                                     AS MATCHCONDITIONS         -- 符合条件情况
                ,'0'                                                                    AS SFTFLAG                 -- 证券融资交易标识
                ,''                                                                     AS MASTERNETAGREEFLAG      -- 净额结算主协议标识
                ,''                                                                     AS MASTERNETAGREEID        -- 净额结算主协议ID
                ,''                                                                     AS SFTTYPE                 -- 证券融资交易类型
                ,''                                                                     AS SECUOWNERTRANSFLAG      -- 证券所有权是否转移
                ,'0'                                                                    AS OTCFLAG                 -- 场外衍生工具标识
                ,''                                                                     AS VALIDNETTINGFLAG        -- 有效净额结算协议标识
                ,''                                                                     AS VALIDNETAGREEMENTID     -- 有效净额结算协议ID
                ,''                                                                     AS OTCTYPE                 -- 场外衍生工具类型
                ,''                                                                     AS DEPOSITRISKPERIOD       -- 保证金风险期间
                ,0                                                                      AS MTM                     -- 重置成本
                ,''                                                                     AS MTMCURRENCY             -- 重置成本币种
                ,''                                                                     AS BUYERORSELLER           -- 买方卖方
                ,''                                                                     AS QUALROFLAG              -- 合格参照资产标识
                ,''                                                                     AS ROISSUERPERFORMFLAG     -- 参照资产发行人是否能履约
                ,''                                                                     AS BUYERINSOLVENCYFLAG     -- 信用保护买方是否破产
                ,0                                                                      AS NONPAYMENTFEES          -- 尚未支付费用
                ,'1'                                                                    AS RETAILEXPOFLAG          -- 零售暴露标识
                ,CASE WHEN T6.RISK_EXPOSURE='01' THEN '020401'
                      WHEN T6.RISK_EXPOSURE='02' THEN '020403'
                      ELSE '020402'
                 END                                                                    AS RETAILCLAIMTYPE         -- 零售债权类型
                ,CASE WHEN T6.RISK_EXPOSURE='01' THEN '01'
                      ELSE '02'
                 END                                                                    AS MORTGAGETYPE            -- 住房抵押贷款类型
                ,1                                                                      AS EXPONUMBER              -- 风险暴露个数
                ,0.8                                                                    AS LTV                     --贷款价值比  统一更新
                ,T6.MOB                                                                 AS AGING                   --账龄
                ,CASE WHEN T1.NEWDEFAULTFLAG='0' THEN '1'
                      ELSE '0'
                 END                                                                    AS NEWDEFAULTDEBTFLAG      --新增违约债项标识
                ,T6.PDMODELCODE                                                         AS PDPOOLMODELID           -- PD分池模型ID
                ,T6.LGDMODELCODE                                                        AS LGDPOOLMODELID          -- LGD分池模型ID
                ,T6.CCFMODELCODE                                                        AS CCFPOOLMODELID          -- CCF分池模型ID
                ,T6.PDCODE                                                              AS PDPOOLID                -- 所属PD池ID
                ,T6.LGDCODE                                                             AS LGDPOOLID               -- 所属LGD池ID
                ,T6.CCFCODE                                                             AS CCFPOOLID               -- 所属CCF池ID
                ,CASE WHEN T10.PROJECTNO IS NULL THEN '0'
                      ELSE '1'
                 END                                                                    AS ABSUAFLAG           --资产证券化基础资产标识
                ,CASE WHEN T10.PROJECTNO IS NULL THEN ''
                      ELSE T10.PROJECTNO
                 END                                                                    AS ABSPOOLID           --证券化资产池ID
                ,''                                                                     AS GROUPID                 -- 分组编号
                ,CASE WHEN T6.DEFAULTFLAG='1' THEN TO_DATE(T6.UPDATETIME,'YYYYMMDD')
                      ELSE NULL
                 END                                                                    AS DefaultDate             -- 违约时点
                ,0                                                                      AS ABSPROPORTION           --资产证券化比重
                ,0                                                                      AS DEBTORNUMBER            --借款人个数
                ,'YQWLD'
    FROM        RWA_DEV.NCM_BUSINESS_DUEBILL T1
    INNER JOIN  RWA_DEV.NCM_BUSINESS_CONTRACT T3
    ON          T1.RELATIVESERIALNO2 = T3.SERIALNO        --对账以借据为准，所以关联合同时，不应该加合同的有效条件
    AND         T1.DATANO = T3.DATANO
    INNER JOIN  /*RWA_DEV.NCM_BUSINESS_HISTORY T31
    ON          T1.SERIALNO = T31.SERIALNO
    AND         (T31.OVERDUEBALANCE + T31.DULLBALANCE + T31.BADBALANCE) > 0   --取到逾期的记录
    AND         T31.DATANO = P_DATA_DT_STR*/
                (SELECT CRDT_ACCT_NO LNCBCERNO, -- 信贷系统借据号
                        L.SBJT_CD,--科目
                        CASE WHEN CCY_CD = '01' OR CCY_CD IS NULL THEN 'CNY' ELSE CCY_CD END as CCY_CD, --币种
                        CUR_BAL BALANCE --余额
                   FROM rwa_dev.BRD_LOAN_NOR L --BRD_LOAN_NOR-正常贷款
                  WHERE L.SBJT_CD = '13100001' --微粒贷逾期贷款
                  AND   L.DATANO = P_DATA_DT_STR
                  AND   L.CUR_BAL>0
                    ) T31
    ON           T1.SERIALNO = T31.LNCBCERNO
    LEFT JOIN   RWA.ORG_INFO T4
    ON           decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID) = T4.ORGID
    LEFT JOIN   RWA.CODE_LIBRARY T5
    ON          T3.DIRECTION = T5.ITEMNO
    AND         T5.CODENO = 'IndustryType'
    LEFT JOIN   RWA_DEV.RWA_TEMP_LGDLEVEL T6    --零售债项评级
    ON          T1.RELATIVESERIALNO2 = T6.BUSINESSID
    AND         T6.BUSINESSTYPE <> 'CREDITCARD'                       -- 不取信用卡
    LEFT JOIN   (SELECT DISTINCT AA.PROJECTNO,AA.CONTRACTNO   --判断是否资产证券化
                   FROM RWA_DEV.NCM_ABS_PROJECT_ASSET AA
             INNER JOIN RWA_DEV.NCM_ABS_PROJECT_INFO BB
                     ON AA.PROJECTNO = BB.PROJECTNO
                    AND BB.DATANO = P_DATA_DT_STR
                    AND BB.PROJECTSTATUS = '0401'            --交割成功
                  WHERE AA.DATANO = P_DATA_DT_STR
                ) T10
    ON          T3.SERIALNO = T10.CONTRACTNO
    LEFT JOIN   RWA_DEV.ncm_rwa_risk_expo_rst T11
    ON          T1.SERIALNO = T11.OBJECTNO
    AND         T11.OBJECTTYPE = 'BusinessDuebill'
    AND         T11.DATANO = P_DATA_DT_STR
    LEFT JOIN   RWA_DEV.RWA_CD_CODE_MAPPING T12
    ON          T11.RISKEXPOSURERESULT = T12.SITEMNO
    AND         T12.SCODENO = 'RwaResultType'
    LEFT JOIN   RWA_DEV.NCM_CLASSIFY_RECORD T13
    ON          T1.RELATIVESERIALNO2=T13.OBJECTNO
    AND         T13.OBJECTTYPE = 'TwelveClassify'
    AND         T13.ISWORK = '1'
    AND         T13.DATANO = P_DATA_DT_STR
    LEFT JOIN   RWA_DEV.NCM_CUSTOMER_INFO T16
    ON          T1.CUSTOMERID = T16.CUSTOMERID
    AND         T1.DATANO = T16.DATANO
    LEFT JOIN   (
                select OBJECTNO, DIRECTION
                  from (select T.OBJECTNO,
                               T.DIRECTION,
                               ROW_NUMBER() OVER(PARTITION BY T.OBJECTNO order by T.SERIALNO DESC) AS RM
                          from RWA_DEV.NCM_PUTOUT_SCHEME T
                         where T.DATANO = P_DATA_DT_STR
                           and T.OBJECTTYPE = 'BusinessContract'
                           and T.DIRECTION IS NOT NULL)
                 where RM = 1
                ) CPS                 --额度类业务的行业投向需从提用表取
    ON          T3.SERIALNO = CPS.OBJECTNO
    LEFT JOIN   RWA.CODE_LIBRARY CL
    ON          CPS.DIRECTION = CL.ITEMNO
    AND         CL.CODENO = 'IndustryType'
    WHERE       T1.DATANO = P_DATA_DT_STR
    --AND         T1.BUSINESSTYPE = '11103030'  --只取微粒贷业务
    ;
    COMMIT;

    /*7 插入 普通业务的逾期贷款明细 插入到目标表*/ 
    INSERT INTO RWA_DEV.RWA_XD_EXPOSURE(
                DATADATE                        --数据日期
               ,DATANO                          --数据流水号
               ,EXPOSUREID                      --风险暴露ID
               ,DUEID                           --债项ID
               ,SSYSID                          --源系统ID
               ,CONTRACTID                      --合同ID
               ,CLIENTID                        --参与主体ID
               ,SORGID                          --源机构ID
               ,SORGNAME                        --源机构名称
               ,ORGSORTNO                       --所属机构排序号
               ,ORGID                           --所属机构ID
               ,ORGNAME                         --所属机构名称
               ,ACCORGID                        --账务机构ID
               ,ACCORGNAME                      --账务机构名称
               ,INDUSTRYID                      --所属行业代码
               ,INDUSTRYNAME                    --所属行业名称
               ,BUSINESSLINE                    --业务条线
               ,ASSETTYPE                       --资产大类
               ,ASSETSUBTYPE                    --资产小类
               ,BUSINESSTYPEID                  --业务品种代码
               ,BUSINESSTYPENAME                --业务品种名称
               ,CREDITRISKDATATYPE              --信用风险数据类型
               ,ASSETTYPEOFHAIRCUTS             --折扣系数对应资产类别
               ,BUSINESSTYPESTD                 --权重法业务类型
               ,EXPOCLASSSTD                    --权重法暴露大类
               ,EXPOSUBCLASSSTD                 --权重法暴露小类
               ,EXPOCLASSIRB                    --内评法暴露大类
               ,EXPOSUBCLASSIRB                 --内评法暴露小类
               ,EXPOBELONG                      --暴露所属标识
               ,BOOKTYPE                        --账户类别
               ,REGUTRANTYPE                    --监管交易类型
               ,REPOTRANFLAG                    --回购交易标识
               ,REVAFREQUENCY                   --重估频率
               ,CURRENCY                        --币种
               ,NORMALPRINCIPAL                 --正常本金余额
               ,OVERDUEBALANCE                  --逾期余额
               ,NONACCRUALBALANCE               --非应计余额
               ,ONSHEETBALANCE                  --表内余额
               ,NORMALINTEREST                  --正常利息
               ,ONDEBITINTEREST                 --表内欠息
               ,OFFDEBITINTEREST                --表外欠息
               ,EXPENSERECEIVABLE               --应收费用
               ,ASSETBALANCE                    --资产余额
               ,ACCSUBJECT1                     --科目一
               ,ACCSUBJECT2                     --科目二
               ,ACCSUBJECT3                     --科目三
               ,STARTDATE                       --起始日期
               ,DUEDATE                         --到期日期
               ,ORIGINALMATURITY                --原始期限
               ,RESIDUALM                       --剩余期限
               ,RISKCLASSIFY                    --风险分类
               ,EXPOSURESTATUS                  --风险暴露状态
               ,OVERDUEDAYS                     --逾期天数
               ,SPECIALPROVISION                --专项准备金
               ,GENERALPROVISION                --一般准备金
               ,ESPECIALPROVISION               --特别准备金
               ,WRITTENOFFAMOUNT                --已核销金额
               ,OFFEXPOSOURCE                   --表外暴露来源
               ,OFFBUSINESSTYPE                 --表外业务类型
               ,OFFBUSINESSSDVSSTD              --权重法表外业务类型细分
               ,UNCONDCANCELFLAG                --是否可随时无条件撤销
               ,CCFLEVEL                        --信用转换系数级别
               ,CCFAIRB                         --高级法信用转换系数
               ,CLAIMSLEVEL                     --债权级别
               ,BONDFLAG                        --是否为债券
               ,BONDISSUEINTENT                 --债券发行目的
               ,NSUREALPROPERTYFLAG             --是否非自用不动产
               ,REPASSETTERMTYPE                --抵债资产期限类型
               ,DEPENDONFPOBFLAG                --是否依赖于银行未来盈利
               ,IRATING                         --内部评级
               ,PD                              --违约概率
               ,LGDLEVEL                        --违约损失率级别
               ,LGDAIRB                         --高级法违约损失率
               ,MAIRB                           --高级法有效期限
               ,EADAIRB                         --高级法违约风险暴露
               ,DEFAULTFLAG                     --违约标识
               ,BEEL                            --已违约暴露预期损失比率
               ,DEFAULTLGD                      --已违约暴露违约损失率
               ,EQUITYEXPOFLAG                  --股权暴露标识
               ,EQUITYINVESTTYPE                --股权投资对象类型
               ,EQUITYINVESTCAUSE               --股权投资形成原因
               ,SLFLAG                          --专业贷款标识
               ,SLTYPE                          --专业贷款类型
               ,PFPHASE                         --项目融资阶段
               ,REGURATING                      --监管评级
               ,CBRCMPRATINGFLAG                --银监会认定评级是否更为审慎
               ,LARGEFLUCFLAG                   --是否波动性较大
               ,LIQUEXPOFLAG                    --是否清算过程中风险暴露
               ,PAYMENTDEALFLAG                 --是否货款对付模式
               ,DELAYTRADINGDAYS                --延迟交易天数
               ,SECURITIESFLAG                  --有价证券标识
               ,SECUISSUERID                    --证券发行人ID
               ,RATINGDURATIONTYPE              --评级期限类型
               ,SECUISSUERATING                 --证券发行等级
               ,SECURESIDUALM                   --证券剩余期限
               ,SECUREVAFREQUENCY               --证券重估频率
               ,CCPTRANFLAG                     --是否中央交易对手相关交易
               ,CCPID                           --中央交易对手ID
               ,QUALCCPFLAG                     --是否合格中央交易对手
               ,BANKROLE                        --银行角色
               ,CLEARINGMETHOD                  --清算方式
               ,BANKASSETFLAG                   --是否银行提交资产
               ,MATCHCONDITIONS                 --符合条件情况
               ,SFTFLAG                         --证券融资交易标识
               ,MASTERNETAGREEFLAG              --净额结算主协议标识
               ,MASTERNETAGREEID                --净额结算主协议ID
               ,SFTTYPE                         --证券融资交易类型
               ,SECUOWNERTRANSFLAG              --证券所有权是否转移
               ,OTCFLAG                         --场外衍生工具标识
               ,VALIDNETTINGFLAG                --有效净额结算协议标识
               ,VALIDNETAGREEMENTID             --有效净额结算协议ID
               ,OTCTYPE                         --场外衍生工具类型
               ,DEPOSITRISKPERIOD               --保证金风险期间
               ,MTM                             --重置成本
               ,MTMCURRENCY                     --重置成本币种
               ,BUYERORSELLER                   --买方卖方
               ,QUALROFLAG                      --合格参照资产标识
               ,ROISSUERPERFORMFLAG             --参照资产发行人是否能履约
               ,BUYERINSOLVENCYFLAG             --信用保护买方是否破产
               ,NONPAYMENTFEES                  --尚未支付费用
               ,RETAILEXPOFLAG                  --零售暴露标识
               ,RETAILCLAIMTYPE                 --零售债权类型
               ,MORTGAGETYPE                    --住房抵押贷款类型
               ,EXPONUMBER                      --风险暴露个数
               ,LTV                             --贷款价值比
               ,AGING                           --账龄
               ,NEWDEFAULTDEBTFLAG              --新增违约债项标识
               ,PDPOOLMODELID                   --PD分池模型ID
               ,LGDPOOLMODELID                  --LGD分池模型ID
               ,CCFPOOLMODELID                  --CCF分池模型ID
               ,PDPOOLID                        --所属PD池ID
               ,LGDPOOLID                       --所属LGD池ID
               ,CCFPOOLID                       --所属CCF池ID
               ,ABSUAFLAG                       --资产证券化基础资产标识
               ,ABSPOOLID                       --证券化资产池ID
               ,GROUPID                         --分组编号
               ,DefaultDate                     --违约时点
               ,ABSPROPORTION                   --资产证券化比重
               ,DEBTORNUMBER                    --借款人个数
               ,flag
    )WITH TEMP_EXPOSURE AS (
                 --调整取余额逻辑 pxl 2019/04/11                           
                 SELECT CRDT_ACCT_NO LNCBCERNO, -- 信贷系统借据号
                        L.SBJT_CD,--科目
                        CASE
                          WHEN CCY_CD = '01' OR CCY_CD IS NULL THEN
                           'CNY' ELSE CCY_CD
                        END CCY_CD, --币种
                        CUR_BAL BALANCE --余额
                   FROM rwa_dev.BRD_LOAN_NOR L --BRD_LOAN_NOR-正常贷款
                  WHERE substr(L.SBJT_CD,1,4) = '1310' 
                    AND L.SBJT_CD <> '13100001' ----所有不含微粒贷的逾期贷款
                    AND L.DATANO = P_DATA_DT_STR
                    AND L.CUR_BAL>0
                           )
    SELECT
                TO_DATE(T1.DATANO,'YYYYMMDD')                                           AS DATADATE                -- 数据日期
                ,T1.DATANO                                                              AS DATANO                  -- 数据流水号
                ,'YQ'||T1.SERIALNO                                                      AS EXPOSUREID              -- 风险暴露ID
                ,T1.SERIALNO                                                            AS DUEID                   -- 债项ID
                ,'XD'                                                                   AS SSYSID                  -- 源系统ID
                ,T1.RELATIVESERIALNO2                                                   AS CONTRACTID              -- 合同ID
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN DECODE(T3.LINETYPE,'0030','XN-GRKH','XN-YBGS')                 --若客户ID为空，条线为个人就虚拟为个人客户，否则为一般公司
                      ELSE T1.CUSTOMERID
                 END                                                                    AS CLIENTID                -- 参与主体ID
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS SORGID                  -- 源机构ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T4.ORGNAME,'总行')                                                             AS SORGNAME                -- 源机构名称
                ,nvl(T4.SORTNO,'1010')                                                              AS ORGSORTNO               -- 所属机构排序号
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS ORGID                   -- 所属机构ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T4.ORGNAME,'总行')                                                             AS ORGNAME                 -- 所属机构名称
                --,decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)                      AS ACCORGID                -- 账务机构ID
                ,case when substr(decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID),1,1)='@' then '01000000'
                     else decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)
                 end
                ,nvl(T4.ORGNAME,'总行')                                                             AS ACCORGNAME              -- 账务机构名称
                ,NVL(T3.DIRECTION,CPS.DIRECTION)                                        AS INDUSTRYID              -- 所属行业代码
                ,NVL(T5.ITEMNAME,CL.ITEMNAME)                                           AS INDUSTRYNAME            -- 所属行业名称
                ,CASE WHEN T1.BUSINESSCURRENCY<>'CNY' THEN '0102'                            --外币的表内业务   大中-贸金部
                      WHEN T1.BUSINESSTYPE IN('10302010','10302015','10302020') THEN '0401'  --贴现业务        同业金融市场部
                      WHEN T3.LINETYPE='0010' THEN '0101'
                      WHEN T3.LINETYPE='0020' THEN '0201'
                      WHEN T3.LINETYPE='0030' THEN '0301'
                      WHEN T3.LINETYPE='0040' THEN '0401'
                      ELSE '0101'
                 END                                                                    AS BUSINESSLINE            -- 条线
                ,''                                                                     AS ASSETTYPE               -- 资产大类
                ,''                                                                     AS ASSETSUBTYPE            -- 资产小类
                ,T1.BUSINESSTYPE                                                        AS BUSINESSTYPEID          -- 业务品种代码
                ,T2.TYPENAME                                                            AS BUSINESSTYPENAME        -- 业务品种名称
                /*,CASE WHEN T1.SERIALNO='20170125c0000373' THEN '02'
                      ELSE '01'
                 END                                                                    AS CREDITRISKDATATYPE      -- 信用风险数据类型          01-一般非零售
                */
                ,CASE WHEN T16.CUSTOMERTYPE = '0310' OR T16.CERTTYPE LIKE 'Ind%'
                      THEN '02' --零售
                      ELSE '01' --非零售
                  END                                                                   AS CREDITRISKDATATYPE  --信用风险数据类型
                ,'01'                                                                   AS ASSETTYPEOFHAIRCUTS     -- 折扣系数对应资产类别     01-现金及现金等价物
                ,''                                                                     AS BUSINESSTYPESTD         -- 权重法业务类型
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN '0112'                                    --若客户ID为空，默认 其他(0112)
                      ELSE ''
                 END                                                                    AS EXPOCLASSSTD            -- 权重法暴露大类
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN DECODE(T3.LINETYPE,'0030','011215','011216')                          --若客户ID为空，条线为个人就默认 其他适用75%风险权重的资产(011215)，否则默认 其他适用100%风险权重的资产(011216)
                      ELSE ''
                 END                                                                    AS EXPOSUBCLASSSTD         -- 权重法暴露小类
                ,SUBSTR(T10.DITEMNO,1,4)                                                AS EXPOCLASSIRB            -- 内评法暴露大类
                ,T10.DITEMNO                                                            AS EXPOSUBCLASSIRB         -- 内评法暴露小类
                ,CASE WHEN T2.OFFSHEETFLAG='EntOff' THEN '02'   --  02-表外
                      ELSE '01'                                 --  01-表内
                 END                                                                   AS EXPOBELONG              -- 暴露所属标识
                ,'01'                                                                  AS BOOKTYPE                -- 账户类别           01-银行账户
                ,'03'                                                                  AS REGUTRANTYPE            -- 监管交易类型      03-抵押贷款
                ,'0'                                                                   AS REPOTRANFLAG            -- 回购交易标识       0-否
                ,1                                                                     AS REVAFREQUENCY           -- 重估频率
                ,t31.CCY_CD/*NVL(T1.BUSINESSCURRENCY,'CNY')*/                                        AS CURRENCY                -- 币种
                ,T1.BALANCE                                                           AS NORMALPRINCIPAL         -- 正常本金余额
                ,0                                                                     AS OVERDUEBALANCE          -- 逾期余额
                ,0                                                                     AS NONACCRUALBALANCE       -- 非应计余额
                ,T1.BALANCE                                                           AS ONSHEETBALANCE          -- 表内余额
                ,0                                                                     AS NORMALINTEREST          -- 正常利息
                ,0                                                                     AS ONDEBITINTEREST         -- 表内欠息
                ,0                                                                     AS OFFDEBITINTEREST        -- 表外欠息
                ,0                                                                     AS EXPENSERECEIVABLE       -- 应收费用
                ,T1.BALANCE                                                           AS ASSETBALANCE            -- 资产余额
                ,'13100000'                                                            AS ACCSUBJECT1             -- 科目一
                ,''                                                                    AS ACCSUBJECT2             -- 科目二
                ,''                                                                    AS ACCSUBJECT3             -- 科目三
                ,NVL(T1.PUTOUTDATE,T3.PUTOUTDATE)                                      AS STARTDATE               -- 起始日期
                ,NVL(T1.ACTUALMATURITY,T3.MATURITY)                                    AS DUEDATE                 -- 到期日期
                ,CASE WHEN (TO_DATE(NVL(T1.ACTUALMATURITY,T3.MATURITY),'YYYYMMDD')-TO_DATE(NVL(T1.PUTOUTDATE,T3.PUTOUTDATE),'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(NVL(T1.ACTUALMATURITY,T3.MATURITY),'YYYYMMDD')-TO_DATE(NVL(T1.PUTOUTDATE,T3.PUTOUTDATE),'YYYYMMDD'))/365
                END                                                                    AS ORIGINALMATURITY        -- 原始期限
                ,CASE WHEN (TO_DATE(NVL(T1.ACTUALMATURITY,T3.MATURITY),'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(NVL(T1.ACTUALMATURITY,T3.MATURITY),'YYYYMMDD')-TO_DATE(T1.DATANO,'YYYYMMDD'))/365
                END                                                                    AS RESIDUALM               -- 剩余期限
                ,CASE WHEN NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))='A' THEN '01'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))='B' THEN '02'       --十二级分类转为五级分类
                      WHEN NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))='C' THEN '03'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))='D' THEN '04'
                      WHEN NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))='E' THEN '05'
                      ELSE NVL(T1.Classifyresult,SUBSTR(T11.FINALLYRESULT,1,1))
                 END                                                                   AS RISKCLASSIFY            -- 风险分类
                ,'01'                                                                  AS EXPOSURESTATUS          -- 风险暴露状态  01-正常
                ,T1.OVERDUEDAYS                                                        AS OVERDUEDAYS             -- 逾期天数
                ,0                                                                     AS SPECIALPROVISION        -- 专项准备金-规则处理
                ,0                                                                     AS GENERALPROVISION        -- 一般准备金
                ,0                                                                     AS ESPECIALPROVISION       -- 特别准备金
                ,T1.CANCELSUM                                                          AS WRITTENOFFAMOUNT        -- 已核销金额
                ,CASE WHEN T2.OFFSHEETFLAG='EntOff'  THEN '03'                         --  03-实际表外业务
                      ELSE ''
                 END                                                                   AS OffExpoSource           -- 表外暴露来源
                ,''                                                                    AS OffBusinessType         -- 表外业务类型
                ,''                                                                    AS OffBusinessSdvsSTD      -- 权重法表外业务类型细分
                ,'1'                                                                   AS UncondCancelFlag        -- 是否可随时无条件撤销
                ,''                                                                    AS CCFLevel                -- 信用转换系数级别
                ,NULL                                                                  AS CCFAIRB                 -- 高级法信用转换系数
                ,'01'                                                                  AS CLAIMSLEVEL             -- 债权级别
                ,'0'                                                                   AS BONDFLAG                -- 是否为债券
                ,'02'                                                                  AS BONDISSUEINTENT         -- 债券发行目的
                ,'0'                                                                   AS NSUREALPROPERTYFLAG     -- 是否非自用不动产
                ,''                                                                    AS REPASSETTERMTYPE        -- 抵债资产期限类型
                ,'0'                                                                   AS DEPENDONFPOBFLAG        -- 是否依赖于银行未来盈利
                ,T6.PDADJLEVEL                                                         AS IRATING                 -- 内部评级
                ,T6.PD                                                                 AS PD                      -- 违约概率
                ,''                                                                    AS LGDLEVEL                -- 违约损失率级别
                ,0                                                                     AS LGDAIRB                 -- 高级法违约损失率
                ,0                                                                     AS MAIRB                   -- 高级法有效期限
                ,T1.BALANCE                                                           AS EADAIRB                 -- 高级法违约风险暴露
                ,CASE WHEN T6.PDADJCODE='D' THEN '1'
                      ELSE '0'
                 END                                                                   AS DEFAULTFLAG             -- 违约标识
                ,0                                                                  AS BEEL                    -- 已违约暴露预期损失比率
                ,0                                                                     AS DEFAULTLGD              -- 已违约暴露违约损失率
                ,'0'                                                                   AS EQUITYEXPOFLAG          -- 股权暴露标识
                ,''                                                                    AS EQUITYINVESTTYPE        -- 股权投资对象类型
                ,''                                                                    AS EQUITYINVESTCAUSE       -- 股权投资形成原因
                ,'0'                                                                   AS SLFLAG                  -- 专业贷款标识       专业贷款相关字段一期先赋空
                ,''                                                                    AS SLTYPE                  -- 专业贷款类型
                ,''                                                                    AS PFPHASE                 -- 项目融资阶段
                ,'01'                                                                  AS REGURATING              -- 监管评级
                ,''                                                                    AS CBRCMPRATINGFLAG        -- 银监会认定评级是否更为审慎
                ,''                                                                    AS LARGEFLUCFLAG           -- 是否波动性较大
                ,'0'                                                                   AS LIQUEXPOFLAG            -- 是否清算过程中风险暴露
                ,''                                                                    AS PAYMENTDEALFLAG         -- 是否货款对付模式
                ,0                                                                     AS DELAYTRADINGDAYS        -- 延迟交易天数
                ,'0'                                                                   AS SECURITIESFLAG          -- 有价证券标识
                ,''                                                                    AS SECUISSUERID            -- 证券发行人ID
                ,''                                                                    AS RATINGDURATIONTYPE      -- 评级期限类型
                ,''                                                                    AS SECUISSUERATING         -- 证券发行等级
                ,0                                                                     AS SECURESIDUALM           -- 证券剩余期限
                ,1                                                                     AS SECUREVAFREQUENCY       -- 证券重估频率
                ,'0'                                                                   AS CCPTRANFLAG             -- 是否中央交易对手相关交易
                ,''                                                                    AS CCPID                   -- 中央交易对手ID
                ,'0'                                                                   AS QUALCCPFLAG             -- 是否合格中央交易对手
                ,''                                                                    AS BANKROLE                -- 银行角色
                ,''                                                                    AS CLEARINGMETHOD          -- 清算方式
                ,'0'                                                                   AS BANKASSETFLAG           -- 是否银行提交资产
                ,''                                                                    AS MATCHCONDITIONS         -- 符合条件情况
                ,'0'                                                                   AS SFTFLAG                 -- 证券融资交易标识
                ,''                                                                    AS MASTERNETAGREEFLAG      -- 净额结算主协议标识
                ,''                                                                    AS MASTERNETAGREEID        -- 净额结算主协议ID
                ,''                                                                    AS SFTTYPE                 -- 证券融资交易类型
                ,''                                                                    AS SECUOWNERTRANSFLAG      -- 证券所有权是否转移
                ,'0'                                                                   AS OTCFLAG                 -- 场外衍生工具标识
                ,''                                                                    AS VALIDNETTINGFLAG        -- 有效净额结算协议标识
                ,''                                                                    AS VALIDNETAGREEMENTID     -- 有效净额结算协议ID
                ,''                                                                    AS OTCTYPE                 -- 场外衍生工具类型
                ,''                                                                    AS DEPOSITRISKPERIOD       -- 保证金风险期间
                ,0                                                                     AS MTM                     -- 重置成本
                ,''                                                                    AS MTMCURRENCY             -- 重置成本币种
                ,''                                                                    AS BUYERORSELLER           -- 买方卖方
                ,''                                                                    AS QUALROFLAG              -- 合格参照资产标识
                ,''                                                                    AS ROISSUERPERFORMFLAG     -- 参照资产发行人是否能履约
                ,''                                                                    AS BUYERINSOLVENCYFLAG     -- 信用保护买方是否破产
                ,0                                                                     AS NONPAYMENTFEES          -- 尚未支付费用
                ,'0'                                                                   AS RETAILEXPOFLAG          -- 零售暴露标识
                ,''                                                                    AS RETAILCLAIMTYPE         -- 零售债权类型
                ,''                                                                    AS MORTGAGETYPE            -- 住房抵押贷款类型
                ,1                                                                     AS EXPONUMBER              -- 风险暴露个数
                ,0.8                                                                   AS LTV                     --贷款价值比
                ,0                                                                     AS AGING                   --账龄
                ,''                                                                    AS NEWDEFAULTDEBTFLAG      --新增违约债项标识
                ,''                                                                    AS PDPOOLMODELID           -- PD分池模型ID
                ,''                                                                    AS LGDPOOLMODELID          -- LGD分池模型ID
                ,''                                                                    AS CCFPOOLMODELID          -- CCF分池模型ID
                ,''                                                                    AS PDPOOLID                -- 所属PD池ID
                ,''                                                                    AS LGDPOOLID               -- 所属LGD池ID
                ,''                                                                    AS CCFPOOLID               -- 所属CCF池ID
                ,CASE WHEN T9.PROJECTNO IS NULL THEN '0'
                      ELSE '1'
                 END                                                                   AS ABSUAFLAG           --资产证券化基础资产标识
                ,CASE WHEN T9.PROJECTNO IS NULL THEN ''
                      ELSE T9.PROJECTNO
                 END                                                                   AS ABSPOOLID           --证券化资产池ID
                ,''                                                                    AS GROUPID                 -- 分组编号
                ,CASE WHEN T6.PDADJCODE='D' THEN TO_DATE(T6.PDVAVLIDDATE,'YYYYMMDD')
                      ELSE NULL
                 END                                                                   AS DefaultDate             -- 违约时点
                ,0                                                                     AS ABSPROPORTION           --资产证券化比重
                ,0                                                                     AS DEBTORNUMBER            --借款人个数
                ,'YQ'
    FROM        RWA_DEV.NCM_BUSINESS_DUEBILL T1
    INNER JOIN  RWA_DEV.NCM_BUSINESS_TYPE T2
    ON          T1.BUSINESSTYPE = T2.TYPENO
    AND         T1.DATANO = T2.DATANO
    INNER JOIN  RWA_DEV.NCM_BUSINESS_CONTRACT T3
    ON          T1.RELATIVESERIALNO2 = T3.SERIALNO          --对账以借据为准，所以关联合同时，不应该加合同的有效条件
    AND         T1.DATANO = T3.DATANO
    INNER JOIN  TEMP_EXPOSURE T31                           --关联临时表取到余额
    ON          T1.SERIALNO = T31.LNCBCERNO
    LEFT JOIN   RWA.ORG_INFO T4
    ON          decode(t1.MFORGID,'@',t1.OPERATEORGID,t1.MFORGID)  = T4.ORGID
    LEFT JOIN   RWA.CODE_LIBRARY T5
    ON          T3.DIRECTION = T5.ITEMNO
    AND         T5.CODENO = 'IndustryType'
    LEFT JOIN   RWA_DEV.RWA_TEMP_PDLEVEL T6
    ON          T1.CUSTOMERID = T6.CUSTID
    LEFT JOIN   (SELECT DISTINCT AA.PROJECTNO,AA.CONTRACTNO   --判断是否资产证券化
                   FROM RWA_DEV.NCM_ABS_PROJECT_ASSET AA
             INNER JOIN RWA_DEV.NCM_ABS_PROJECT_INFO BB
                     ON AA.PROJECTNO = BB.PROJECTNO
                    AND BB.DATANO = P_DATA_DT_STR
                    AND BB.PROJECTSTATUS = '0401'            --交割成功
                  WHERE AA.DATANO = P_DATA_DT_STR
                ) T9
    ON          T3.SERIALNO = T9.CONTRACTNO
    LEFT JOIN   RWA_DEV.ncm_rwa_risk_expo_rst T13
    ON          T1.SERIALNO = T13.OBJECTNO
    AND         T13.OBJECTTYPE = 'BusinessDuebill'
    AND         T13.DATANO = P_DATA_DT_STR
    LEFT JOIN   RWA_DEV.RWA_CD_CODE_MAPPING T10
    ON          T13.RISKEXPOSURERESULT = T10.SITEMNO
    AND         T10.SCODENO = 'RwaResultType'
    LEFT JOIN   RWA_DEV.NCM_CLASSIFY_RECORD T11
    ON          T1.RELATIVESERIALNO2 = T11.OBJECTNO
    AND         T11.OBJECTTYPE = 'TwelveClassify'
    AND         T11.ISWORK = '1'
    AND         T11.DATANO = P_DATA_DT_STR
    LEFT JOIN   RWA_DEV.NCM_CUSTOMER_INFO T16
    ON          T1.CUSTOMERID = T16.CUSTOMERID
    AND         T1.DATANO = T16.DATANO
    LEFT JOIN   (
                select OBJECTNO, DIRECTION
                  from (select T.OBJECTNO,
                               T.DIRECTION,
                               ROW_NUMBER() OVER(PARTITION BY T.OBJECTNO order by T.SERIALNO DESC) AS RM
                          from RWA_DEV.NCM_PUTOUT_SCHEME T
                         where T.DATANO = P_DATA_DT_STR
                           and T.OBJECTTYPE = 'BusinessContract'
                           and T.DIRECTION IS NOT NULL)
                 where RM = 1
                ) CPS                 --额度类业务的行业投向需从提用表取
    ON          T3.SERIALNO = CPS.OBJECTNO
    LEFT JOIN   RWA.CODE_LIBRARY CL
    ON          CPS.DIRECTION = CL.ITEMNO
    AND         CL.CODENO = 'IndustryType'
    WHERE       T1.DATANO = P_DATA_DT_STR
    ;
    COMMIT;
    
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_XD_EXPOSURE',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_XD_EXPOSURE;

    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功'||'-'||v_count1;
    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '信贷系统借据信息(RWA_XD_EXPOSURE)ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_XD_EXPOSURE;
/

