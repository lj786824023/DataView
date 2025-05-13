CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_HG_WSIB(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_HG_WSIB
    实现功能:核心系统-回购-补录铺底(从数据源核心系统将业务相关信息全量导入RWA回购补录铺底表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-06-06
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.CBS_BND|债券投资登记簿
    源  表2 :RWA_DEV.CBS_IAC|通用分户帐
    源  表3 :RWA.ORG_INFO|机构信息表
    源  表4 :RWA.RWA_WS_B_BILLREPURCHASE|买入返售票据回购补录表
    源  表5 :RWA.RWA_WS_B_BONDREPURCHASE|买入返售债券回购补录表
    源  表6 :RWA.RWA_WS_S_BONDREPURCHASE|卖出回购债券回购补录表
    源  表7 :RWA.RWA_WP_SUPPTASKORG|补录任务机构分发配置表
    源  表8 :RWA.RWA_WP_SUPPTASK|补录任务发布表
    源  表9 :RWA.RWA_WS_B_BILLREPURCHASE|买入返售票据回购补录补录表
    源  表10:RWA_DEV.BL_CUSTOMER_INFO|补录客户汇总表
    目标表1 :RWA.RWA_WSIB_B_BILLREPURCHASE|买入返售票据回购补录铺底表
    目标表2 :RWA.RWA_WSIB_B_BONDREPURCHASE|买入返售债券回购补录铺底表
    目标表3 :RWA.RWA_WSIB_S_BONDREPURCHASE|卖出回购债券回购补录铺底表
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_HG_WSIB';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER := 0;
  v_count1 INTEGER;
  v_count2 INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    --清空买入返售票据回购铺底表
    DELETE FROM RWA.RWA_WSIB_B_BILLREPURCHASE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --清空买入返售债券回购铺底表
    DELETE FROM RWA.RWA_WSIB_B_BONDREPURCHASE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --清空卖出回购债券回购铺底表
    DELETE FROM RWA.RWA_WSIB_S_BONDREPURCHASE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    COMMIT;

    --2.将满足条件的数据从源表插入到目标表中
    --2.1 核心系统-买入反售票据回购业务(铺底上期补录数据)
    INSERT INTO RWA.RWA_WSIB_B_BILLREPURCHASE(
                DATADATE                               --数据日期
                ,ORGID                             	 	 --机构ID
                ,INVACCNO                          	 	 --交易批次号
                ,BELONGORGCODE                         --业务所属机构
                ,SUBJECTNO                       	 	 	 --科目
                ,BALANCE          	               	 	 --交易业务余额
                ,INTEREST                          	 	 --应收利息
                ,CURRENCY                          	 	 --交易币种
                ,BEGINDATE        	               	 	 --交易开始日期
                ,ENDDATE                           	 	 --交易到期日期
                ,BILLNO                            	 	 --票据编号
                ,CLIENTNAME       	               	 	 --交易对手名称
                ,ORGANIZATIONCODE                  	 	 --交易对手组织机构代码
                ,COUNTRYCODE      	               	 	 --交易对手注册国家代码
                ,INDUSTRYID       	         			 	 	 --交易对手所属行业代码
                ,ISSUERNAME                        	 	 --承兑行名称
                ,ISSUERORGCODE    	               	 	 --承兑行组织机构代码
                ,ISSUERCOUNTRYCODE										 --承兑行注册国家代码
                ,ISSUERINDUSTRYID                      --承兑行所属行业代码
                ,BILLBEGINDATE                         --票据发行日期
                ,BILLENDDATE                           --票据到期日期
                ,BILLCURRENCY                          --票据币种
                ,BILLVALUE                             --票面金额
                ,CLIENTCATEGORY												 --交易对手客户类型
                ,ACCEPTCATEGORY												 --承兑行客户类型
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,T1.SUPPORGID						                     AS ORGID                    --机构ID
                ,T1.INVACCNO         						 						 AS INVACCNO                 --交易批次号
                ,T1.BELONGORGCODE            		 						 AS BELONGORGCODE            --业务所属机构
                ,T1.SUBJECTNO        					 	 						 AS SUBJECTNO                --科目
                ,T1.BALANCE               			 						 AS BALANCE          	       --交易业务余额
                ,T1.INTEREST                 		 						 AS INTEREST                 --应收利息
                ,T1.CURRENCY            				 						 AS CURRENCY                 --交易币种
                ,T1.BEGINDATE                    						 AS BEGINDATE        	       --交易开始日期
                ,T1.ENDDATE              				 						 AS ENDDATE                  --交易到期日期
                ,T1.BILLNO                   		 						 AS BILLNO                   --票据编号
                ,T1.CLIENTNAME                 	 						 AS CLIENTNAME       	       --交易对手名称
                ,T1.ORGANIZATIONCODE         	 	 						 AS ORGANIZATIONCODE         --交易对手组织机构代码
                ,T1.COUNTRYCODE                  						 AS COUNTRYCODE      	       --交易对手注册国家代码
                ,T1.INDUSTRYID                   						 AS INDUSTRYID       	       --交易对手所属行业代码
                ,T1.ISSUERNAME       						 						 AS ISSUERNAME               --承兑行名称
                ,T1.ISSUERORGCODE    						 						 AS ISSUERORGCODE    	       --承兑行组织机构代码
                ,T1.ISSUERCOUNTRYCODE						 						 AS ISSUERCOUNTRYCODE				 --承兑行注册国家代码
                ,T1.ISSUERINDUSTRYID             						 AS ISSUERINDUSTRYID         --承兑行所属行业代码
                ,T1.BILLBEGINDATE                						 AS BILLBEGINDATE            --票据发行日期
                ,T1.BILLENDDATE                  						 AS BILLENDDATE              --票据到期日期
                ,T1.BILLCURRENCY                 						 AS BILLCURRENCY             --票据币种
                ,T1.BILLVALUE                    						 AS BILLVALUE                --票面金额
                ,T1.CLIENTCATEGORY													 AS CLIENTCATEGORY					 --交易对手客户类型
                ,T1.ACCEPTCATEGORY													 AS ACCEPTCATEGORY					 --承兑行客户类型

    FROM				RWA.RWA_WS_B_BILLREPURCHASE T1	             --买入返售票据回购补录补录表，取最近一期补录数据铺底
		WHERE				T1.DATADATE = (SELECT MAX(DATADATE) FROM RWA.RWA_WS_B_BILLREPURCHASE WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
		ORDER BY		T1.INVACCNO, T1.BILLNO
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA',tabname => 'RWA_WSIB_B_BILLREPURCHASE',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    --SELECT COUNT(1) INTO v_count FROM RWA.RWA_WSIB_B_BILLREPURCHASE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA.RWA_WSIB_B_BILLREPURCHASE表当前插入的核心系统-买入返售票据回购铺底数据记录为: ' || v_count || ' 条');


    --2.2 核心系统-买入返售债券回购业务
    INSERT INTO RWA.RWA_WSIB_B_BONDREPURCHASE(
                DATADATE                               --数据日期
                ,ORGID                             	 	 --机构ID
                ,INVACCNO						               	 	 --交易账号
                ,BELONGORGCODE                         --业务所属机构
                ,CLRDATE															 --交易起始日
                ,FDATE																 --交易到期日
                ,REPURCHASEVALUE			             	 	 --交易回购金额
                ,REPURCHASETYPE												 --回购类型
                ,CLIENTNAME					               	 	 --交易对手名称
                ,CLIENTCATEGORY												 --交易对手客户类型
                ,ORGANIZATIONCODE		               	 	 --交易对手组织机构代码
                ,COUNTRYCODE					             	 	 --交易对手国家代码
                ,INDUSTRYID					               	 	 --交易对手行业代码
                ,BONDCODE							             	 	 --债券代码
                ,BONDISSUEINTENT			             	 	 --债券发行目的
                ,ISSUERNAME					               	 	 --债券发行人名称
                ,ISSUERCATEGORY												 --债券发行人客户类型
                ,ISSUERORGCODE				             	 	 --债券发行人组织机构代码
                ,ISSUERCOUNTRYCODE		             	 	 --债券发行人注册国家代码
                ,ISSUERINDUSTRYID		               	 	 --债券发行人所属国标行业
                ,ISSUERSCOPE					       			 	 	 --债券发行人企业规模
                ,ISSUERRATINGORGCODE	             	 	 --债券发行人评级机构
                ,ISSUERRATING				  								 --债券发行人评级
                ,ISSUERRATINGDATE											 --债券发行人评级日期
                ,ISSUERRATINGORGCODE2	             	 	 --债券发行人评级机构2
                ,ISSUERRATING2				  							 --债券发行人评级2
                ,ISSUERRATINGDATE2										 --债券发行人评级日期2
                ,BONDRATINGORGCODE		             	 	 --债券评级机构
                ,BONDRATINGTYPE			               	 	 --债券评级期限类型
                ,BONDRATING					               	 	 --债券发行等级
                ,BONDRATINGDATE												 --债券评级日期
                ,BONDRATINGORGCODE2		             	 	 --债券评级机构2
                ,BONDRATINGTYPE2		               	 	 --债券评级期限类型2
                ,BONDRATING2				               	 	 --债券发行等级2
                ,BONDRATINGDATE2											 --债券评级日期2
                ,BONDBEGINDATE												 --债券发行日期
                ,BONDENDDATE													 --债券到期日期
                ,BONDCURRENCY				               	 	 --债券币种
                ,BONDVALUE						             	 	 --债券价值
                ,BALANCE															 --业务余额

    )
    WITH TMP_SUPPORG AS (
								SELECT T1.ORGID AS ORGID
										   ,CASE WHEN T3.ORGLEVEL > 2 THEN T4.SORTNO ELSE T3.SORTNO END AS SORTNO
								  FROM RWA.RWA_WP_SUPPTASKORG T1
						INNER JOIN RWA.RWA_WP_SUPPTASK T2
								    ON T1.SUPPTASKID = T2.SUPPTASKID
								   AND T2.ENABLEFLAG = '01'
						 LEFT JOIN RWA.ORG_INFO T3
								    ON T1.ORGID = T3.ORGID
						 LEFT JOIN RWA.ORG_INFO T4
	                	ON T3.BELONGORGID = T4.ORGID
								 WHERE T1.SUPPTMPLID = 'M-0064'
							ORDER BY T3.SORTNO
		)
		, TMP_BL_CUST AS (
								SELECT CUSTOMERNAME, CERTID
									FROM RWA_DEV.BL_CUSTOMER_INFO
								WHERE CERTTYPE = 'Ent01'
									AND ROWID IN (SELECT MAX(ROWID)
											            FROM RWA_DEV.BL_CUSTOMER_INFO
											           WHERE CERTTYPE = 'Ent01'
											           GROUP BY CUSTOMERNAME)
		)
		SELECT
								DATADATE                               --数据日期
                ,ORGID                             	 	 --机构ID
                ,INVACCNO						               	 	 --交易账号
                ,BELONGORGCODE                         --业务所属机构
                ,CLRDATE															 --交易起始日
                ,FDATE																 --交易到期日
                ,REPURCHASEVALUE			             	 	 --交易回购金额
                ,REPURCHASETYPE												 --回购类型
                ,CLIENTNAME					               	 	 --交易对手名称
                ,CLIENTCATEGORY												 --交易对手客户类型
                ,ORGANIZATIONCODE		               	 	 --交易对手组织机构代码
                ,COUNTRYCODE					             	 	 --交易对手国家代码
                ,INDUSTRYID					               	 	 --交易对手行业代码
                ,BONDCODE							             	 	 --债券代码
                ,BONDISSUEINTENT			             	 	 --债券发行目的
                ,ISSUERNAME					               	 	 --债券发行人名称
                ,ISSUERCATEGORY												 --债券发行人客户类型
                ,ISSUERORGCODE				             	 	 --债券发行人组织机构代码
                ,ISSUERCOUNTRYCODE		             	 	 --债券发行人注册国家代码
                ,ISSUERINDUSTRYID		               	 	 --债券发行人所属国标行业
                ,ISSUERSCOPE					       			 	 	 --债券发行人企业规模
                ,ISSUERRATINGORGCODE	             	 	 --债券发行人评级机构
                ,ISSUERRATING				  								 --债券发行人评级
                ,ISSUERRATINGDATE											 --债券发行人评级日期
                ,ISSUERRATINGORGCODE2	             	 	 --债券发行人评级机构2
                ,ISSUERRATING2				  							 --债券发行人评级2
                ,ISSUERRATINGDATE2										 --债券发行人评级日期2
                ,BONDRATINGORGCODE		             	 	 --债券评级机构
                ,BONDRATINGTYPE			               	 	 --债券评级期限类型
                ,BONDRATING					               	 	 --债券发行等级
                ,BONDRATINGDATE												 --债券评级日期
                ,BONDRATINGORGCODE2		             	 	 --债券评级机构2
                ,BONDRATINGTYPE2		               	 	 --债券评级期限类型2
                ,BONDRATING2				               	 	 --债券发行等级2
                ,BONDRATINGDATE2											 --债券评级日期2
                ,BONDBEGINDATE												 --债券发行日期
                ,BONDENDDATE													 --债券到期日期
                ,BONDCURRENCY				               	 	 --债券币种
                ,BONDVALUE						             	 	 --债券价值
                ,BALANCE															 --业务余额
		FROM (
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,NVL(T7.ORGID,(SELECT ORGID FROM TMP_SUPPORG WHERE ROWNUM = 1))
                												                     AS ORGID										 --机构ID                     		按照补录任务分配情况，分配给金融市场部及其分部
                ,RANK() OVER(PARTITION BY T1.INVACCNO ORDER BY LENGTH(NVL(T7.SORTNO,(SELECT SORTNO FROM TMP_SUPPORG WHERE ROWNUM = 1))) DESC)
                																						 AS RECORDNO								 --数据序号
                ,T1.INVACCNO                             		 AS INVACCNO								 --交易账号
                ,T2.IACGACBR																 AS BELONGORGCODE            --业务所属机构
                ,TO_CHAR(TO_DATE(T1.CLRDATE,'YYYYMMDD'),'YYYY-MM-DD')
                																						 AS CLRDATE									 --交易起始日
                ,TO_CHAR(TO_DATE(T1.FDATE,'YYYYMMDD'),'YYYY-MM-DD')
                																						 AS FDATE										 --交易到期日
                ,ABS(T2.IACCURBAL)	             				 		 AS REPURCHASEVALUE		    	 --交易回购金额
                ,CASE WHEN T2.IACITMNO = '11110301' THEN '01'														 --质押式
                			WHEN T2.IACITMNO = '11110302' THEN '02'														 --买断式
                			ELSE NVL(T4.REPURCHASETYPE,'')
                 END																				 AS REPURCHASETYPE					 --回购类型
                ,NVL(T4.CLIENTNAME,T1.BNDNAME)             	 AS CLIENTNAME							 --交易对手名称
                ,NVL(T4.CLIENTCATEGORY,'0202')             	 AS CLIENTCATEGORY					 --交易对手客户类型								默认0202-中国商业银行
                ,NVL(T4.ORGANIZATIONCODE,T8.CERTID)      		 AS ORGANIZATIONCODE		  	 --交易对手组织机构代码
                ,NVL(T4.COUNTRYCODE,'CHN')                   AS COUNTRYCODE			         --交易对手国家代码    						默认CHN-中国
                ,NVL(T4.INDUSTRYID,'J66')            				 AS INDUSTRYID				       --交易对手行业代码    						默认J66-货币金融服务
                ,T4.BONDCODE                          			 AS BONDCODE						     --债券代码
                ,T4.BONDISSUEINTENT                          AS BONDISSUEINTENT		    	 --债券发行目的
                ,T4.ISSUERNAME                           		 AS ISSUERNAME					  	 --债券发行人名称
                ,NVL(T4.ISSUERCATEGORY,'0202')             	 AS ISSUERCATEGORY					 --债券发行人客户类型							默认0202-中国商业银行
                ,T4.ISSUERORGCODE    												 AS ISSUERORGCODE			    	 --债券发行人组织机构代码
                ,NVL(T4.ISSUERCOUNTRYCODE,'CHN')             AS ISSUERCOUNTRYCODE	    	 --债券发行人注册国家代码					默认CHN-中国
                ,NVL(T4.ISSUERINDUSTRYID,'J66')              AS ISSUERINDUSTRYID		  	 --债券发行人所属国标行业					默认J66-货币金融服务
                ,T4.ISSUERSCOPE															 AS ISSUERSCOPE				  		 --债券发行人企业规模
                ,T4.ISSUERRATINGORGCODE											 AS ISSUERRATINGORGCODE 		 --债券发行人评级机构
                ,T4.ISSUERRATING														 AS ISSUERRATING						 --债券发行人评级
                ,T4.ISSUERRATINGDATE												 AS ISSUERRATINGDATE				 --债券发行人评级日期
                ,T4.ISSUERRATINGORGCODE2										 AS ISSUERRATINGORGCODE2 		 --债券发行人评级机构2
                ,T4.ISSUERRATING2														 AS ISSUERRATING2						 --债券发行人评级2
                ,T4.ISSUERRATINGDATE2												 AS ISSUERRATINGDATE2				 --债券发行人评级日期2
                ,T4.BONDRATINGORGCODE                        AS BONDRATINGORGCODE	    	 --债券评级机构
                ,T4.BONDRATINGTYPE                           AS BONDRATINGTYPE			  	 --债券评级期限类型
                ,T4.BONDRATING															 AS BONDRATING							 --债券发行等级
                ,T4.BONDRATINGDATE													 AS BONDRATINGDATE					 --债券评级日期
                ,T4.BONDRATINGORGCODE2                       AS BONDRATINGORGCODE2    	 --债券评级机构2
                ,T4.BONDRATINGTYPE2                          AS BONDRATINGTYPE2			  	 --债券评级期限类型2
                ,T4.BONDRATING2															 AS BONDRATING2							 --债券发行等级2
                ,T4.BONDRATINGDATE2													 AS BONDRATINGDATE2					 --债券评级日期2
                ,T4.BONDBEGINDATE                            AS BONDBEGINDATE			    	 --债券发行日期
                ,T4.BONDENDDATE                              AS BONDENDDATE				    	 --债券到期日期
                ,NVL(T4.BONDCURRENCY,'CNY')									 AS BONDCURRENCY				  	 --债券币种              					默认CNY-人民币
                ,T4.BONDVALUE																 AS BONDVALUE					    	 --债券价值
                ,ABS(T2.IACCURBAL)													 AS BALANCE									 --业务余额

    FROM				RWA_DEV.CBS_BND T1	             		 											--债券投资登记簿,获取有效的债券信息
		INNER JOIN 	RWA_DEV.CBS_IAC T2																				--通用分户帐
		ON 					T1.INVACCNO = T2.IACAC_NO
		AND					T2.IACCURBAL <> 0																					--余额不等于0
		AND					T2.IACITMNO LIKE '111103%'																--买入返售(逆回购)债券
		AND					T2.DATANO = p_data_dt_str
		LEFT	JOIN	RWA.ORG_INFO T3																						--机构信息表
	  ON					T2.IACGACBR = T3.ORGID
	  LEFT	JOIN	(SELECT INVACCNO
	  									 ,REPURCHASETYPE
	  									 ,CLIENTNAME
	  									 ,CLIENTCATEGORY
	  									 ,ORGANIZATIONCODE
	  									 ,COUNTRYCODE
	  									 ,INDUSTRYID
	  									 ,BONDCODE
	  									 ,BONDISSUEINTENT
	  									 ,ISSUERNAME
	  									 ,ISSUERCATEGORY
	  									 ,ISSUERORGCODE
	  									 ,ISSUERCOUNTRYCODE
	  									 ,ISSUERINDUSTRYID
	  									 ,ISSUERSCOPE
	  									 ,ISSUERRATINGORGCODE
	  									 ,ISSUERRATING
	  									 ,ISSUERRATINGDATE
	  									 ,ISSUERRATINGORGCODE2
	  									 ,ISSUERRATING2
	  									 ,ISSUERRATINGDATE2
	  									 ,BONDRATINGORGCODE
	  									 ,BONDRATINGTYPE
	  									 ,BONDRATING
	  									 ,BONDRATINGDATE
	  									 ,BONDRATINGORGCODE2
	  									 ,BONDRATINGTYPE2
	  									 ,BONDRATING2
	  									 ,BONDRATINGDATE2
	  									 ,BONDBEGINDATE
	  									 ,BONDENDDATE
	  									 ,BONDCURRENCY
	  									 ,BONDVALUE
	  							 FROM RWA.RWA_WS_B_BONDREPURCHASE
	  							WHERE DATADATE = (SELECT MAX(DATADATE) FROM RWA.RWA_WS_B_BONDREPURCHASE WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
	  						) T4																											--取最近一期补录数据铺底
	  ON					T1.INVACCNO = T4.INVACCNO
	  LEFT	JOIN  TMP_SUPPORG T7
    ON          T3.SORTNO LIKE T7.SORTNO || '%'
    LEFT	JOIN	TMP_BL_CUST T8
    ON					T1.BNDNAME = T8.CUSTOMERNAME
		WHERE				T1.STATUS = '00'																					--账户状态正常 00-正常；11-销户或归还
		AND 				T1.TYPE IN ('2','3')																			--债券类型：2-正回购；3-逆回购(买入)
		AND					T1.DATANO = p_data_dt_str
		ORDER BY		T1.INVACCNO
		)
		WHERE RECORDNO = 1
		ORDER BY		INVACCNO,BONDCODE
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA',tabname => 'RWA_WSIB_B_BONDREPURCHASE',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count1 FROM RWA.RWA_WSIB_B_BONDREPURCHASE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA.RWA_WSIB_B_BONDREPURCHASE表当前插入的核心系统-买入返售债券回购铺底数据记录为: ' || v_count || ' 条');


    --2.3 核心系统-卖出回购债券回购业务
    INSERT INTO RWA.RWA_WSIB_S_BONDREPURCHASE(
                DATADATE                               --数据日期
                ,ORGID                             	 	 --机构ID
                ,INVACCNO				                 	 	 	 --交易账号
                ,CLRDATE															 --交易起始日
                ,FDATE																 --交易到期日
                ,BELONGORGCODE                         --业务所属机构
                ,BONDCODE						             	 	 	 --债券代码
                ,REPURCHASETYPE	  	             	 	 	 --回购类型
                ,REPURCHASEVALUE	               	 	 	 --交易回购金额
                ,CLIENTNAME			                 	 	 	 --交易对手名称
                ,ORGANIZATIONCODE 	             	 	 	 --交易对手组织机构代码
                ,COUNTRYCODE			               	 	 	 --交易对手国家代码
                ,INDUSTRYID			  	             	 	 	 --交易对手行业代码
                ,BONDCURRENCY				               	 	 --债券币种
                ,BONDVALUE						             	 	 --债券市值
                ,CLIENTCATEGORY			             	 	 	 --交易对手客户类型
    )
    WITH TMP_SUPPORG AS (
								SELECT T1.ORGID AS ORGID
										   ,CASE WHEN T3.ORGLEVEL > 2 THEN T4.SORTNO ELSE T3.SORTNO END AS SORTNO
								  FROM RWA.RWA_WP_SUPPTASKORG T1
						INNER JOIN RWA.RWA_WP_SUPPTASK T2
								    ON T1.SUPPTASKID = T2.SUPPTASKID
								   AND T2.ENABLEFLAG = '01'
						 LEFT JOIN RWA.ORG_INFO T3
								    ON T1.ORGID = T3.ORGID
						 LEFT JOIN RWA.ORG_INFO T4
	                	ON T3.BELONGORGID = T4.ORGID
								 WHERE T1.SUPPTMPLID = 'M-0065'
							ORDER BY T3.SORTNO
		)
		, TMP_BL_CUST AS (
								SELECT CUSTOMERNAME, CERTID
									FROM RWA_DEV.BL_CUSTOMER_INFO
								WHERE CERTTYPE = 'Ent02'
									AND ROWID IN (SELECT MAX(ROWID)
											            FROM RWA_DEV.BL_CUSTOMER_INFO
											           WHERE CERTTYPE = 'Ent02'
											           GROUP BY CUSTOMERNAME)
		)
		SELECT
								DATADATE                               --数据日期
                ,ORGID                             	 	 --机构ID
                ,INVACCNO				                 	 	 	 --交易账号
                ,CLRDATE															 --交易起始日
                ,FDATE																 --交易到期日
                ,BELONGORGCODE                         --业务所属机构
                ,BONDCODE						             	 	 	 --债券代码
                ,REPURCHASETYPE	  	             	 	 	 --回购类型
                ,REPURCHASEVALUE	               	 	 	 --交易回购金额
                ,CLIENTNAME			                 	 	 	 --交易对手名称
                ,ORGANIZATIONCODE 	             	 	 	 --交易对手组织机构代码
                ,COUNTRYCODE			               	 	 	 --交易对手国家代码
                ,INDUSTRYID			  	             	 	 	 --交易对手行业代码
                ,BONDCURRENCY				               	 	 --债券币种
                ,BONDVALUE						             	 	 --债券市值
                ,CLIENTCATEGORY			             	 	 	 --交易对手客户类型
		FROM (
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,NVL(T7.ORGID,(SELECT ORGID FROM TMP_SUPPORG WHERE ROWNUM = 1))
                												                     AS ORGID										 --机构ID                     		按照补录任务分配情况，分配给金融市场部及其分部
                ,RANK() OVER(PARTITION BY T1.INVACCNO ORDER BY LENGTH(NVL(T7.SORTNO,(SELECT SORTNO FROM TMP_SUPPORG WHERE ROWNUM = 1))) DESC)
                																						 AS RECORDNO								 --数据序号
                ,T1.INVACCNO                             		 AS INVACCNO				  		 	 --交易账号
                ,TO_CHAR(TO_DATE(T1.CLRDATE,'YYYYMMDD'),'YYYY-MM-DD')
                																						 AS CLRDATE									 --交易起始日
                ,TO_CHAR(TO_DATE(T1.FDATE,'YYYYMMDD'),'YYYY-MM-DD')
                																						 AS FDATE										 --交易到期日
                ,T2.IACGACBR																 AS BELONGORGCODE            --业务所属机构
                ,T4.BONDCODE                           			 AS BONDCODE						     --债券代码
                ,T4.REPURCHASETYPE               				 		 AS REPURCHASETYPE	    	 	 --回购类型
                ,T4.REPURCHASEVALUE                        	 AS REPURCHASEVALUE	  		 	 --交易回购金额
                ,NVL(T4.CLIENTNAME,T1.BNDNAME)             	 AS CLIENTNAME							 --交易对手名称
                ,NVL(T4.ORGANIZATIONCODE,T8.CERTID)      		 AS ORGANIZATIONCODE		  	 --交易对手组织机构代码
                ,NVL(T4.COUNTRYCODE,'CHN')               		 AS COUNTRYCODE			    	 	 --交易对手国家代码    						默认CHN-中国
                ,NVL(T4.INDUSTRYID,'J66')                    AS INDUSTRYID			    	 	 --交易对手行业代码    						默认J66-货币金融服务
                ,NVL(T4.BONDCURRENCY,'CNY')									 AS BONDCURRENCY				     --债券币种												默认CNY-人民币
                ,T4.BONDVALUE																 AS BONDVALUE						     --债券市值
                ,NVL(T4.CLIENTCATEGORY,'0202')   						 AS CLIENTCATEGORY	    	 	 --交易对手客户类型         			默认0202-中国商业银行

    FROM				RWA_DEV.CBS_BND T1	             		 											--债券投资登记簿,获取有效的债券信息
		INNER JOIN 	RWA_DEV.CBS_IAC T2																				--通用分户帐
		ON 					T1.INVACCNO = T2.IACAC_NO
		AND					T2.IACCURBAL <> 0																					--余额不等于0
		AND					T2.IACITMNO LIKE '211103%'																--卖出回购(正回购)债券
		AND					T2.DATANO = p_data_dt_str
		LEFT	JOIN	RWA.ORG_INFO T3																						--机构信息表
	  ON					T2.IACGACBR = T3.ORGID
	  LEFT	JOIN	(SELECT INVACCNO
	  									 ,BONDCODE
	  									 ,REPURCHASETYPE
	  									 ,REPURCHASEVALUE
	  									 ,CLIENTNAME
	  									 ,ORGANIZATIONCODE
	  									 ,COUNTRYCODE
	  									 ,INDUSTRYID
	  									 ,BONDCURRENCY
	  									 ,BONDVALUE
	  									 ,CLIENTCATEGORY
	  							 FROM RWA.RWA_WS_S_BONDREPURCHASE
	  							WHERE DATADATE = (SELECT MAX(DATADATE) FROM RWA.RWA_WS_S_BONDREPURCHASE WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
	  						) T4																											--取最近一期补录数据铺底
	  ON					T1.INVACCNO = T4.INVACCNO
	  LEFT	JOIN  TMP_SUPPORG T7
    ON          T3.SORTNO LIKE T7.SORTNO || '%'
    LEFT	JOIN	TMP_BL_CUST T8
    ON					T1.BNDNAME = T8.CUSTOMERNAME
		WHERE				T1.STATUS = '00'																					--账户状态正常 00-正常；11-销户或归还
		AND 				T1.TYPE IN ('2','3')																			--债券类型：2-正回购；3-逆回购(买入)
		AND					T1.DATANO = p_data_dt_str
		)
		WHERE RECORDNO = 1
		ORDER BY		INVACCNO,BONDCODE
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA',tabname => 'RWA_WSIB_S_BONDREPURCHASE',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count2 FROM RWA.RWA_WSIB_S_BONDREPURCHASE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA.RWA_WSIB_S_BONDREPURCHASE表当前插入的核心系统-卖出回购债券回购铺底数据记录为: ' || v_count || ' 条');



    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || (v_count + v_count1 + v_count2);
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '回购业务补录数据铺底('|| v_pro_name ||')处理失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_HG_WSIB;
/

