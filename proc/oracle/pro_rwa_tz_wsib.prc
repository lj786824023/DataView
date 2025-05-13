CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_TZ_WSIB(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_TZ_WSIB
    实现功能:财务系统-投资-补录铺底(从数据源财务系统将业务相关信息全量导入RWA投资补录铺底表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2017-07-03
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.FNS_BND_INFO_B|财务系统债券信息表
    源  表2 :RWA_DEV.FNS_BND_BOOK_B|财务系统账面活动表
    源  表3 :RWA.ORG_INFO|机构信息表
    源  表4 :RWA.RWA_WS_RESERVE|应收款投资准备金补录表
    源  表5 :RWA.RWA_WP_SUPPTASKORG|补录任务机构分发配置表
    源  表6 :RWA.RWA_WP_SUPPTASK|补录任务发布表
    目标表1 :RWA.RWA_WSIB_RESERVE|应收款投资准备金补录铺底表
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_TZ_WSIB';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    --清空应收款投资准备金补录铺底表
    --EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA.RWA_WSIB_RESERVE';
    --DELETE FROM RWA.RWA_WSIB_RESERVE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    --清空债券投资货币基金铺底表
    --EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA.RWA_WSIB_BONDTRADE_MF';
    DELETE FROM RWA.RWA_WSIB_BONDTRADE_MF WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    COMMIT;

    --2.将满足条件的数据从源表插入到目标表中
    --2.1 财务系统-应收款投资业务准备金
    /*
    INSERT INTO RWA.RWA_WSIB_RESERVE(
                DATADATE                               --数据日期
                ,ORGID                                 --机构ID
                ,BOND_ID                       				 --债券内码
                ,BOND_CODE                     				 --债券代码
                ,BOND_NAME                     				 --债券名称
                ,DEPARTMENT   								 				 --记账机构
                ,EFFECT_DATE  								 				 --起始日期
                ,MATURITY_DATE                 				 --到期日期
                ,CURRENCY_CODE                 				 --币种
                ,BOND_BAL                      				 --余额
                ,RESERVESUM                    				 --计提金额
    )
    WITH TEMP_BND_BOOK AS (
    										SELECT BOND_ID,
												       NVL(INITIAL_COST, 0) + NVL(MKT_VALUE_CHANGE, 0) + NVL(ACCOUNTABLE_INT, 0) AS BOND_BAL
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
		, TMP_SUPPORG AS (
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
								 WHERE T1.SUPPTMPLID = 'M-0210'
							ORDER BY T3.SORTNO
		)
		SELECT
								DATADATE                               --数据日期
                ,ORGID                                 --机构ID
                ,BOND_ID                       				 --债券内码
                ,BOND_CODE                     				 --债券代码
                ,BOND_NAME                     				 --债券名称
                ,DEPARTMENT   								 				 --记账机构
                ,EFFECT_DATE  								 				 --起始日期
                ,MATURITY_DATE                 				 --到期日期
                ,CURRENCY_CODE                 				 --币种
                ,BOND_BAL                      				 --余额
                ,RESERVESUM                    				 --计提金额
		FROM (
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,NVL(T8.ORGID,(SELECT ORGID FROM TMP_SUPPORG WHERE ROWNUM = 1))
                												                     AS ORGID										 --机构ID                     		按照补录任务分配情况，分配给金融市场部及其分部
                ,RANK() OVER(PARTITION BY T1.BOND_ID ORDER BY LENGTH(NVL(T8.SORTNO,(SELECT SORTNO FROM TMP_SUPPORG WHERE ROWNUM = 1))) DESC)
                																						 AS RECORDNO								 --数据序号
                ,T1.BOND_ID                             		 AS BOND_ID                  --债券内码
                ,T1.BOND_CODE 										 					 AS BOND_CODE                --债券代码
                ,T1.BOND_NAME                           		 AS BOND_NAME                --债券名称
                ,T1.DEPARTMENT															 AS DEPARTMENT   						 --记账机构
                ,TO_CHAR(TO_DATE(T1.EFFECT_DATE,'YYYYMMDD'),'YYYY-MM-DD')
                																						 AS EFFECT_DATE  						 --起始日期
                ,TO_CHAR(TO_DATE(T1.MATURITY_DATE,'YYYYMMDD'),'YYYY-MM-DD')
                													                   AS MATURITY_DATE            --到期日期
                ,NVL(T1.CURRENCY_CODE,'CNY')                 AS CURRENCY_CODE            --币种
                ,T3.BOND_BAL														     AS BOND_BAL                 --余额
                ,T4.RESERVESUM		         									 AS RESERVESUM               --计提金额

    FROM				RWA_DEV.FNS_BND_INFO_B T1
		INNER JOIN	TEMP_BND_BOOK T3
		ON 					T1.BOND_ID = T3.BOND_ID
		LEFT JOIN		RWA.ORG_INFO T6
		ON					T1.DEPARTMENT = T6.ORGID
		LEFT JOIN   (SELECT BOND_ID
											 ,RESERVESUM
									 FROM RWA.RWA_WS_RESERVE
									WHERE DATADATE = (SELECT MAX(DATADATE) FROM RWA.RWA_WS_RESERVE WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
								) T4																								--取最近一期补录数据铺底
    ON          T1.BOND_ID = T4.BOND_ID
    LEFT	JOIN  TMP_SUPPORG T8
    ON          T6.SORTNO LIKE T8.SORTNO || '%'
		WHERE 			T1.ASSET_CLASS IN ('50','60')												--通过资产分类来确定债券还是应收款投资。
																																		--50 应收款项类投资
																																		--60 应收款项类投资-商票
		AND					T1.BOND_TYPE1 <> '060'
		AND					T1.BOND_ID NOT IN
								(SELECT ZQNM FROM RWA.RWA_WSIB_ABS_ISSUE_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
								 UNION ALL
								 SELECT ZQNM FROM RWA.RWA_WSIB_ABS_INVEST_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
								)
		AND 				T1.DATANO = p_data_dt_str														--债券信息表,获取有效的债券信息
		AND					T1.BOND_CODE IS NOT NULL														--排除无效的债券数据
		)
		WHERE RECORDNO = 1
		ORDER BY		BOND_ID,BOND_CODE
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA',tabname => 'RWA_WSIB_RESERVE',cascade => true);
    */

    --2.2 财务系统-债券投资业务-货币基金
    INSERT INTO RWA.RWA_WSIB_BONDTRADE_MF(
                DATADATE                                --数据日期
                ,ORGID                             	 		--机构ID
                ,BOND_ID                           	 		--内部代码
                ,BOND_CODE                         	 		--投资交易统一代码
                ,BOND_NAME                         	 		--投资交易名称
                ,BONDTYPE            								 		--债券分类
                ,BONDTYPE2           								 		--债券分类2
                ,BONDCURRENCY                      	 		--债券币种
                ,BONDBAL                           	 		--债券余额
                ,ISSUERID                          	 		--债券发行人ID
                ,ISSUERNAME                        	 		--债券发行人名称
                ,ISSUERCATEGORY                    	 		--债券发行人客户类型
                ,BELONGORGCODE                     	 		--业务所属机构
                ,EFFECT_DATE                       	 		--生效日
                ,MATURITY_DATE       								 		--到日期
                ,GUARANTYTYPE                       	  --缓释物类型
                ,LETTERTYPE                         	  --存单类型
                ,LCISSUERTYPE        								 		--保本理财产品发行机构
                ,BONDISSUEINTENT                   	 		--债券发行目的
                ,GUARANTORNAME                     	 		--缓释人的客户名称
                ,GUARANTORCATEGORY                    	--缓释人的客户类型
                ,GUARANTORCOUNTRYCODE								 		--缓释人的注册国家代码
                ,GUARANTYCURRENCYCODE               	 	--担保币种
                ,GUARANTYSUM                        	 	--担保价值（元）
    )
    WITH TEMP_BND_BOOK AS (
    						SELECT BOND_ID,
								       NVL(INITIAL_COST, 0) + NVL(MKT_VALUE_CHANGE, 0) + NVL(INT_ADJUST, 0) + NVL(ACCOUNTABLE_INT, 0) AS BOND_BAL
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
								   AND NVL(INITIAL_COST, 0) + NVL(MKT_VALUE_CHANGE, 0) + NVL(INT_ADJUST, 0) + NVL(ACCOUNTABLE_INT, 0) <> 0
		)
		, TMP_SUPPORG AS (
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
								 WHERE T1.SUPPTMPLID = 'M-0071'
							ORDER BY T3.SORTNO
		)
		SELECT
								DATADATE                               	--数据日期
                ,ORGID                             	 		--机构ID
                ,BOND_ID                           	 		--内部代码
                ,BOND_CODE                         	 		--投资交易统一代码
                ,BOND_NAME                         	 		--投资交易名称
                ,BONDTYPE            								 		--债券分类
                ,BONDTYPE2           								 		--债券分类2
                ,BONDCURRENCY                      	 		--债券币种
                ,BONDBAL                           	 		--债券余额
                ,ISSUERID                          	 		--债券发行人ID
                ,ISSUERNAME                        	 		--债券发行人名称
                ,ISSUERCATEGORY                    	 		--债券发行人客户类型
                ,BELONGORGCODE                     	 		--业务所属机构
                ,EFFECT_DATE                       	 		--生效日
                ,MATURITY_DATE       								 		--到日期
                ,GUARANTYTYPE                       	  --缓释物类型
                ,LETTERTYPE                         	  --存单类型
                ,LCISSUERTYPE        								 		--保本理财产品发行机构
                ,BONDISSUEINTENT                   	 		--债券发行目的
                ,GUARANTORNAME                     	 		--缓释人的客户名称
                ,GUARANTORCATEGORY                    	--缓释人的客户类型
                ,GUARANTORCOUNTRYCODE								 		--缓释人的注册国家代码
                ,GUARANTYCURRENCYCODE               	 	--担保币种
                ,GUARANTYSUM                        	 	--担保价值（元）
		FROM (
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,NVL(T8.ORGID,(SELECT ORGID FROM TMP_SUPPORG WHERE ROWNUM = 1))
                												                     AS ORGID										 --机构ID                     		按照补录任务分配情况，分配给金融市场部及其分部
                ,RANK() OVER(PARTITION BY T1.BOND_ID ORDER BY LENGTH(NVL(T8.SORTNO,(SELECT SORTNO FROM TMP_SUPPORG WHERE ROWNUM = 1))) DESC)
                																						 AS RECORDNO								 --数据序号
                ,T1.BOND_ID                             		 AS BOND_ID               	 --内部代码
                ,T1.BOND_CODE 										 					 AS BOND_CODE                --投资交易统一代码
                ,T1.BOND_NAME                           		 AS BOND_NAME                --投资交易名称
                ,T1.BOND_TYPE1															 AS BONDTYPE            		 --债券分类
                ,T1.BOND_TYPE2															 AS BONDTYPE2                --债券分类2
                ,NVL(T1.CURRENCY_CODE,'CNY')                 AS BONDCURRENCY             --债券币种
                ,T3.BOND_BAL		 						                 AS BONDBAL                  --债券余额
                ,CASE WHEN T1.BOND_ID = 'B200801010095' THEN 'MZXXZ'																																														--毛主席像章默认参与主体
                			WHEN T9.BUSINESSTYPE = '1040102040' AND (T10.ISCOUNTTR = '1' OR T10.BONDNAME LIKE '%国债%') THEN 'ZGZYZF'																	--人民币债券投资国债时默认发行人为中国中央政府
                			WHEN T9.BUSINESSTYPE = '1040202010' AND T10.BONDFLAG04 = '1' AND T10.MARKETSCATEGORY = '01' THEN T10.BONDPUBLISHCOUNTRY || 'ZYZF'					--外币债券投资境外中央政府
                			WHEN T9.BUSINESSTYPE = '1040202010' AND T10.BONDFLAG04 = '1' AND T10.MARKETSCATEGORY = '02' THEN T10.BONDPUBLISHCOUNTRY || 'ZYYH'					--外币债券投资境外中央银行
                			WHEN T9.BUSINESSTYPE = '1040202010' AND T10.BONDFLAG04 = '1' AND T10.MARKETSCATEGORY = '03' THEN T10.BONDPUBLISHCOUNTRY || 'BMST'					--外币债券投资境外国家或地区注册的公共部门实体
                			WHEN REPLACE(T10.BONDPUBLISHID,'NCM_','') IS NULL THEN 'XN-YBGS'
                 ELSE T10.BONDPUBLISHID
                 END					     													 AS ISSUERID                 --债券发行人ID
                ,CASE WHEN T1.BOND_ID = 'B200801010095' THEN '毛主席像'																																													--毛主席像章默认参与主体
                			WHEN T9.BUSINESSTYPE = '1040102040' AND (T10.ISCOUNTTR = '1' OR T10.BONDNAME LIKE '%国债%') THEN 'ZGZYZF'																	--人民币债券投资国债时默认发行人为中国中央政府
                			WHEN T9.BUSINESSTYPE = '1040202010' AND T10.BONDFLAG04 = '1' AND T10.MARKETSCATEGORY = '01' THEN T10.BONDPUBLISHCOUNTRY || '中央政府'			--外币债券投资境外中央政府
                			WHEN T9.BUSINESSTYPE = '1040202010' AND T10.BONDFLAG04 = '1' AND T10.MARKETSCATEGORY = '02' THEN T10.BONDPUBLISHCOUNTRY || '中央银行'			--外币债券投资境外中央银行
                			WHEN T9.BUSINESSTYPE = '1040202010' AND T10.BONDFLAG04 = '1' AND T10.MARKETSCATEGORY = '03' THEN T10.BONDPUBLISHCOUNTRY || '公共部门实体'	--外币债券投资境外国家或地区注册的公共部门实体
                			WHEN REPLACE(T10.BONDPUBLISHID,'NCM_','') IS NULL THEN '虚拟一般公司'
                 ELSE T11.CUSTOMERNAME
                 END														             AS ISSUERNAME               --债券发行人名称
                ,CASE WHEN T1.BOND_ID = 'B200801010095' THEN '0205'																																															--毛主席像章默认参与主体
                			WHEN T9.BUSINESSTYPE = '1040102040' AND (T10.ISCOUNTTR = '1' OR T10.BONDNAME LIKE '%国债%') THEN '0101'																		--人民币债券投资国债时默认发行人为中国中央政府
                			WHEN T9.BUSINESSTYPE = '1040202010' AND T10.BONDFLAG04 = '1' AND T10.MARKETSCATEGORY = '01' THEN '0102'																		--外币债券投资境外中央政府
                			WHEN T9.BUSINESSTYPE = '1040202010' AND T10.BONDFLAG04 = '1' AND T10.MARKETSCATEGORY = '02' THEN '0104'																		--外币债券投资境外中央银行
                			WHEN T9.BUSINESSTYPE = '1040202010' AND T10.BONDFLAG04 = '1' AND T10.MARKETSCATEGORY = '03' THEN '0107'																		--外币债券投资境外国家或地区注册的公共部门实体
                			WHEN REPLACE(T10.BONDPUBLISHID,'NCM_','') IS NULL THEN '0301'
                 ELSE T11.RWACUSTOMERTYPE
                 END													               AS ISSUERCATEGORY           --债券发行人客户类型
                ,T1.DEPARTMENT			              					 AS BELONGORGCODE            --业务所属机构
                ,TO_CHAR(TO_DATE(T1.EFFECT_DATE,'YYYYMMDD'),'YYYY-MM-DD')
                																						 AS EFFECT_DATE         		 --生效日
                ,TO_CHAR(TO_DATE(T1.MATURITY_DATE,'YYYYMMDD'),'YYYY-MM-DD')
                																			       AS MATURITY_DATE            --到日期
                ,T7.GUARANTYTYPE		              					 AS GUARANTYTYPE             --缓释物类型
                ,T7.LETTERTYPE															 AS LETTERTYPE          		 --存单类型
                ,T7.LCISSUERTYPE					 		 							 AS LCISSUERTYPE        	   --保本理财产品发行机构
                ,NVL(T7.BONDISSUEINTENT,'02')  							 AS BONDISSUEINTENT          --债券发行目的
                ,T7.GUARANTORNAME                            AS GUARANTORNAME            --缓释人的客户名称
                ,T7.GUARANTORCATEGORY												 AS GUARANTORCATEGORY   		 --缓释人的客户类型
                ,NVL(T7.GUARANTORCOUNTRYCODE,'CHN')					 AS GUARANTORCOUNTRYCODE		 --缓释人的注册国家代码
                ,NVL(T7.GUARANTYCURRENCYCODE,'CNY')					 AS GUARANTYCURRENCYCODE     --担保币种
                ,T7.GUARANTYSUM                              AS GUARANTYSUM              --担保价值（元）


    FROM				RWA_DEV.FNS_BND_INFO_B T1
		INNER JOIN	TEMP_BND_BOOK T3
		ON 					T1.BOND_ID = T3.BOND_ID
		LEFT JOIN		RWA.ORG_INFO T6
		ON					T1.DEPARTMENT = T6.ORGID
		LEFT JOIN   (SELECT BOND_ID
											 ,GUARANTYTYPE
											 ,LETTERTYPE
											 ,LCISSUERTYPE
											 ,BONDISSUEINTENT
											 ,GUARANTORNAME
											 ,GUARANTORCATEGORY
											 ,GUARANTORCOUNTRYCODE
											 ,GUARANTYCURRENCYCODE
											 ,GUARANTYSUM
									 FROM RWA.RWA_WS_BONDTRADE_MF
									WHERE DATADATE = (SELECT MAX(DATADATE) FROM RWA.RWA_WS_BONDTRADE_MF WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
								) T7																								--取最近一期补录数据铺底
    ON          T1.BOND_ID = T7.BOND_ID
    LEFT	JOIN  TMP_SUPPORG T8
    ON          T6.SORTNO LIKE T8.SORTNO || '%'
    LEFT JOIN		NCM_BUSINESS_DUEBILL T9
    ON					'CW_IMPORTDATA' || T1.BOND_ID = T9.THIRDPARTYACCOUNTS
    AND					T9.DATANO = p_data_dt_str
    LEFT JOIN		NCM_BOND_INFO T10
    ON					T10.OBJECTNO = T9.RELATIVESERIALNO2
    AND					T10.OBJECTTYPE = 'BusinessContract'
    AND					T10.DATANO = p_data_dt_str
    LEFT JOIN		NCM_CUSTOMER_INFO T11
    ON					DECODE(T10.BONDPUBLISHID,'NCM_',T9.CUSTOMERID,'',T9.CUSTOMERID,T10.BONDPUBLISHID) = T11.CUSTOMERID
    AND					T11.DATANO = p_data_dt_str
		WHERE 			(T1.ASSET_CLASS = '20' OR
								 (T1.ASSET_CLASS = '40' AND T1.BOND_TYPE2 NOT IN ('30','50')) OR 										  --T1.BOND_TYPE1 NOT IN ('091','099')
								 (T1.ASSET_CLASS = '40' AND T1.BOND_TYPE2 IN ('30','50') AND T1.CLOSED = '1')         --T1.BOND_TYPE1 IN ('091','099')
								)
		AND					T1.BOND_TYPE2 = '50'																--仅取货币基金数据
		AND					T1.BOND_TYPE1 <> '060'
		AND					T1.BOND_ID NOT IN
								(SELECT ZQNM FROM RWA.RWA_WSIB_ABS_ISSUE_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
								 UNION ALL
								 SELECT ZQNM FROM RWA.RWA_WSIB_ABS_INVEST_EXPOSURE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
								)
		AND 				T1.DATANO = p_data_dt_str														--债券信息表,获取有效的债券信息
		)
		WHERE RECORDNO = 1
		ORDER BY		BOND_ID,BOND_CODE
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA',tabname => 'RWA_WSIB_BONDTRADE_MF',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA.RWA_WSIB_BONDTRADE_MF WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA.RWA_WSIB_BONDTRADE_MF表当前插入的财务系统-债券投资-货币基金铺底数据记录为: ' || v_count || ' 条');


    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '投资业务补录数据铺底('|| v_pro_name ||')处理失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_TZ_WSIB;
/

