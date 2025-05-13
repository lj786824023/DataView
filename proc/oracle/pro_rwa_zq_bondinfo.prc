CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ZQ_BONDINFO(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_ZQ_BONDINFO
    实现功能:财务系统-债券-市场风险-债券信息(从数据源财务系统将业务相关信息全量导入RWA市场风险债券接口表债券信息表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-04-12
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.FNS_BND_INFO_B|财务系统债券信息表
    源  表2 :RWA_DEV.FNS_BND_BOOK_B|财务系统账面活动表
    源  表3 :RWA.RWA_WS_BONDTRADE|债券投资补录信息表
    源  表4 :RWA.RWA_WP_DATASUPPLEMENT|补录任务明细信息表
    目标表1 :RWA_DEV.RWA_ZQ_BONDINFO|财务系统债券信息表
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ZQ_BONDINFO';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_ZQ_BONDINFO';

    --2.将满足条件的数据从源表插入到目标表中
    --2.1 财务系统-债券-人民币
    INSERT INTO RWA_DEV.RWA_ZQ_BONDINFO(
                DATADATE                               --数据日期
                ,BONDID                                --债券ID
                ,BONDNAME                              --债券名称
                ,BONDTYPE                              --债券类型
                ,ERATING                               --外部评级
                ,ISSUERID                              --发行人ID
                ,ISSUERNAME                            --发行人名称
                ,ISSUERTYPE                            --发行人大类
                ,ISSUERSUBTYPE                         --发行人小类
                ,ISSUERREGISTSTATE                     --发行人注册国家
                ,ISSUERSMBFLAG                         --发行人小微企业标识
                ,BONDISSUEINTENT                       --债券发行目的
                ,REABSFLAG                             --再资产证券化标识
                ,ORIGINATORFLAG                        --是否发起机构
                ,STARTDATE                             --起始日期
                ,DUEDATE                               --到期日期
                ,ORIGINALMATURITY                      --原始期限
                ,RESIDUALM                             --剩余期限
                ,RATETYPE                              --利率类型
                ,EXECUTIONRATE                         --执行利率
                ,NEXTREPRICEDATE                       --下次重定价日
                ,NEXTREPRICEM                          --下次重定价期限
                ,MODIFIEDDURATION                      --修正久期
                ,DENOMINATION                          --面额
                ,CURRENCY                              --币种

    )
    WITH TEMP_BND_BOOK AS (
    										SELECT BOND_ID,
												       INITIAL_COST,
												       INT_ADJUST,
												       MKT_VALUE_CHANGE,
												       RECEIVABLE_INT,
												       ACCOUNTABLE_INT,
												       PAR_VALUE
												  FROM (SELECT BOND_ID,
												               INITIAL_COST,
												               INT_ADJUST,
												               MKT_VALUE_CHANGE,
												               RECEIVABLE_INT,
												               ACCOUNTABLE_INT,
												               PAR_VALUE,
												               ROW_NUMBER() OVER(PARTITION BY BOND_ID ORDER BY SORT_SEQ DESC) RM
												          FROM FNS_BND_BOOK_B
												         WHERE AS_OF_DATE <= p_data_dt_str
												           AND DATANO = p_data_dt_str)
												 WHERE RM = 1
												   AND NVL(INITIAL_COST, 0) + NVL(INT_ADJUST, 0) + NVL(MKT_VALUE_CHANGE, 0) + NVL(ACCOUNTABLE_INT, 0) <> 0
		)
		, TMP_ABS_BOND AS (
												SELECT ABSR.ZQNM AS BOND_ID, ABSR.ZZCZQHBZ AS REABSFLAG
												  FROM RWA.RWA_WS_ABS_INVEST_EXPOSURE ABSR
												 INNER JOIN RWA.RWA_WP_DATASUPPLEMENT RWD
												    ON ABSR.SUPPORGID = RWD.ORGID
												   AND RWD.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
												   AND RWD.SUPPTMPLID = 'M-0140'
												   AND RWD.SUBMITFLAG = '1'
												 WHERE ABSR.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    											 AND ABSR.YHJS = '02'																			--02 投资机构 其它全放到发行机构暴露中
												UNION
												SELECT RWAIE.ZQNM AS BOND_ID, RWAIE.ZZCZQHBZ AS REABSFLAG
												  FROM RWA.RWA_WS_ABS_ISSUE_EXPOSURE RWAIE
												 INNER JOIN RWA.RWA_WP_DATASUPPLEMENT RWD
												    ON RWAIE.SUPPORGID = RWD.ORGID
												   AND RWD.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
												   AND RWD.SUPPTMPLID = 'M-0131'
												   AND RWD.SUBMITFLAG = '1'
												 WHERE RWAIE.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    											 AND RWAIE.YHJS <> '02'																		--除了 02 投资机构  其它全放到发行机构暴露中
		)
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,T1.BOND_ID														     	 AS BONDID                   --债券ID
                ,T4.BONDNAME                    				 		 AS BONDNAME                 --债券名称
                ,T1.BOND_TYPE1                           		 AS BONDTYPE                 --债券类型
                ,RWA_DEV.GETSTANDARDRATING1(T4.BONDRATING)   AS ERATING                  --外部评级          					 转换为标普
                ,CASE WHEN T1.BOND_ID = 'B200801010095' THEN 'MZXXZ'																														--毛主席像章默认参与主体
                			WHEN T3.BUSINESSTYPE = '1040102040' AND (T4.ISCOUNTTR = '1' OR T4.BONDNAME LIKE '%国债%') THEN 'ZGZYZF'														--人民币债券投资国债时默认发行人为中国中央政府
                			WHEN T3.BUSINESSTYPE = '1040202010' AND T4.BONDFLAG04 = '1' AND T4.MARKETSCATEGORY = '01' THEN T4.BONDPUBLISHCOUNTRY || 'ZYZF'		--外币债券投资境外中央政府
                			WHEN T3.BUSINESSTYPE = '1040202010' AND T4.BONDFLAG04 = '1' AND T4.MARKETSCATEGORY = '02' THEN T4.BONDPUBLISHCOUNTRY || 'ZYYH'		--外币债券投资境外中央银行
                			WHEN T3.BUSINESSTYPE = '1040202010' AND T4.BONDFLAG04 = '1' AND T4.MARKETSCATEGORY = '03' THEN T4.BONDPUBLISHCOUNTRY || 'BMST'		--外币债券投资境外国家或地区注册的公共部门实体
                			WHEN REPLACE(T4.BONDPUBLISHID,'NCM_','') IS NULL THEN 'XN-YBGS'
                 ELSE T4.BONDPUBLISHID
                 END						                      			 AS ISSUERID                 --发行人ID
                ,CASE WHEN T1.BOND_ID = 'B200801010095' THEN '毛主席像'																																	--毛主席像章默认参与主体
                			WHEN T3.BUSINESSTYPE = '1040102040' AND (T4.ISCOUNTTR = '1' OR T4.BONDNAME LIKE '%国债%') THEN '中国中央政府'															--人民币债券投资国债时默认发行人为中国中央政府
                			WHEN T3.BUSINESSTYPE = '1040202010' AND T4.BONDFLAG04 = '1' AND T4.MARKETSCATEGORY = '01' THEN T4.BONDPUBLISHCOUNTRY || '中央政府'				--外币债券投资境外中央政府
                			WHEN T3.BUSINESSTYPE = '1040202010' AND T4.BONDFLAG04 = '1' AND T4.MARKETSCATEGORY = '02' THEN T4.BONDPUBLISHCOUNTRY || '中央银行'				--外币债券投资境外中央银行
                			WHEN T3.BUSINESSTYPE = '1040202010' AND T4.BONDFLAG04 = '1' AND T4.MARKETSCATEGORY = '03' THEN T4.BONDPUBLISHCOUNTRY || '公共部门实体'		--外币债券投资境外国家或地区注册的公共部门实体
                			WHEN REPLACE(T4.BONDPUBLISHID,'NCM_','') IS NULL THEN '虚拟一般公司'
                 ELSE T5.CUSTOMERNAME
                 END								                         AS ISSUERNAME               --发行人名称
                ,CASE WHEN T1.BOND_ID = 'B200801010095' THEN '02'																	--毛主席像章默认参与主体
                			WHEN T3.BUSINESSTYPE = '1040102040' AND (T4.ISCOUNTTR = '1' OR T4.BONDNAME LIKE '%国债%') THEN '01'					--人民币债券投资国债时默认发行人为中国中央政府
                			WHEN T3.BUSINESSTYPE = '1040202010' AND T4.BONDFLAG04 = '1' THEN '01'				--外币债券投资境外中央政府
                			WHEN REPLACE(T4.BONDPUBLISHID,'NCM_','') IS NULL THEN '03'
                 ELSE SUBSTR(T5.RWACUSTOMERTYPE,1,2)
                 END													               AS ISSUERTYPE               --发行人大类        					 规则映射
                ,CASE WHEN T1.BOND_ID = 'B200801010095' THEN '0205'																--毛主席像章默认参与主体
                			WHEN T3.BUSINESSTYPE = '1040102040' AND (T4.ISCOUNTTR = '1' OR T4.BONDNAME LIKE '%国债%') THEN '0101'
                			WHEN T3.BUSINESSTYPE = '1040202010' AND T4.BONDFLAG04 = '1' AND T4.MARKETSCATEGORY = '01' THEN '0102'				--外币债券投资境外中央政府
                			WHEN T3.BUSINESSTYPE = '1040202010' AND T4.BONDFLAG04 = '1' AND T4.MARKETSCATEGORY = '02' THEN '0104'				--外币债券投资境外中央银行
                			WHEN T3.BUSINESSTYPE = '1040202010' AND T4.BONDFLAG04 = '1' AND T4.MARKETSCATEGORY = '03' THEN '0107'				--外币债券投资境外国家或地区注册的公共部门实体
                			WHEN REPLACE(T4.BONDPUBLISHID,'NCM_','') IS NULL THEN '0301'
                 ELSE T5.RWACUSTOMERTYPE
                 END							                           AS ISSUERSUBTYPE            --发行人小类        					 规则映射
                ,CASE WHEN NVL(T5.COUNTRYCODE,'CHN') = 'CHN' THEN '01'
                 ELSE '02'
                 END								                  			 AS ISSUERREGISTSTATE        --发行人注册国家
                ,NVL(T5.ISSUPERVISESTANDARSMENT,'0')         AS ISSUERSMBFLAG            --发行人小微企业标识					 默认：否(0)
                ,SUBSTR(NVL(T4.BONDPUBLISHPURPOSE,'0020'),2,2)
                																             AS BONDISSUEINTENT          --债券发行目的      					 默认：其他(02)
                ,NVL(T7.REABSFLAG,'0')                       AS REABSFLAG                --再资产证券化标识  					 默认：否(0)
                ,CASE WHEN REPLACE(T5.CERTID,'-','') = '202869177' THEN '1'
                 ELSE '0'
                 END																				 AS ORIGINATORFLAG   				 --是否发起机构      					 1. 发行人名称＝重庆银行(202869177)，则为是： 2. 否则为否
                ,T1.ORIGINATION_DATE                         AS STARTDATE                --起始日期          					 以起息日填充(FNS_BND_INFO_B.ORIGINATION_DATE)
                ,T1.MATURITY_DATE                            AS DUEDATE                  --到期日期
                ,CASE WHEN (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(T1.ORIGINATION_DATE,'YYYYMMDD')) / 365<0
                      THEN 0
                      ELSE (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(T1.ORIGINATION_DATE,'YYYYMMDD')) / 365
                 END                                         AS ORIGINALMATURITY         --原始期限
                ,CASE WHEN (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                      THEN 0
                      ELSE (TO_DATE(T1.MATURITY_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                         AS RESIDUALM                --剩余期限
                ,CASE WHEN T1.RATE_TYPE	= '10' THEN '01'																 --固定利率
                 ELSE '02'																															 --浮动利率(含自动重订价和手工重订价)
                 END				                                 AS RATETYPE                 --利率类型(补录)
                ,NVL(T1.PAR_RATE,0) / 100                    AS EXECUTIONRATE            --执行利率
                ,CASE WHEN T1.RATE_TYPE = '10' OR T1.REPRICE_DATE < p_data_dt_str THEN T1.MATURITY_DATE
                 ELSE T1.REPRICE_DATE
                 END                                         AS NEXTREPRICEDATE          --下次重定价日      					1. 若利率类型＝固定，则下次重定价日＝到期日期；2. 否则取系统字段
                ,CASE WHEN T1.RATE_TYPE = '10' THEN NULL
                 ELSE CASE WHEN (TO_DATE(CASE WHEN T1.REPRICE_DATE < p_data_dt_str THEN T1.MATURITY_DATE ELSE T1.REPRICE_DATE END,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0 THEN 0
                 			ELSE (TO_DATE(CASE WHEN T1.REPRICE_DATE < p_data_dt_str THEN T1.MATURITY_DATE ELSE T1.REPRICE_DATE END,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 			END
                 END																				 AS NEXTREPRICEM             --下次重定价期限							 1. 若利率类型＝固定，则默认为：NULL；2. 否则取下次重订价日-数据日期，单位：年
                ,NULL                                        AS MODIFIEDDURATION         --修正久期
                ,T2.PAR_VALUE            										 AS DENOMINATION             --面额              					 补录
                ,NVL(T1.CURRENCY_CODE,'CNY')                 AS CURRENCY                 --币种

    FROM				RWA_DEV.FNS_BND_INFO_B T1
		INNER JOIN	TEMP_BND_BOOK T2
		ON					T1.BOND_ID = T2.BOND_ID
		LEFT JOIN		RWA_DEV.NCM_BUSINESS_DUEBILL T3														--信贷借据表
		ON					'CW_IMPORTDATA' || T1.BOND_ID = T3.THIRDPARTYACCOUNTS
		--AND					T3.BUSINESSTYPE IN ('1040102040','1040202010')						--1040102040-人民币债券投资;1040202010-外币债券投资
		AND					T3.DATANO = p_data_dt_str
		LEFT JOIN		RWA_DEV.NCM_BOND_INFO T4																	--信贷债券信息表
	  ON					T3.RELATIVESERIALNO2 = T4.OBJECTNO
		AND					T4.OBJECTTYPE = 'BusinessContract'
		AND					T4.DATANO = p_data_dt_str
		LEFT JOIN		RWA_DEV.NCM_CUSTOMER_INFO T5															--统一客户信息表
	  ON					T4.BONDPUBLISHID = T5.CUSTOMERID
	  AND					T5.DATANO = p_data_dt_str
		LEFT JOIN		TMP_ABS_BOND T7
		ON					T1.BOND_ID = T7.BOND_ID
		AND					T7.REABSFLAG = '1'
		WHERE 			T1.ASSET_CLASS = '10'																			--仅交易性账户进入市场风险
		AND					T1.DATANO = p_data_dt_str
		AND 				T1.BOND_CODE IS NOT NULL																	--排除无效的债券数据
	  ;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_ZQ_BONDINFO',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_ZQ_BONDINFO;
    --Dbms_output.Put_line('RWA_DEV.RWA_ZQ_BONDINFO表当前插入的财务系统-债券(市场风险)-债券信息记录为: ' || v_count || ' 条');



    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '债券信息('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_ZQ_BONDINFO;
/

