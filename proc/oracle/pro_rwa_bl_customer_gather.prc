CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_BL_CUSTOMER_GATHER(
                             p_data_dt_str  IN  VARCHAR2,    --数据日期 yyyyMMdd
                             p_po_rtncode  OUT  VARCHAR2,    --返回编号 1 成功,0 失败
                            p_po_rtnmsg    OUT  VARCHAR2    --返回描述
        )
  /*
    存储过程名称:RWA_DEV.PRO_RWA_BL_CUSTOMER_GATHER
    实现功能:RWA系统-补录-客户信息汇总(从RWA系统各个补录表中把客户信息汇总去重后并入到统一客户表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-06-12
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA.RWA_WS_XD_ISSUER|信贷系统抵质押品发行人信息补录表
    源  表2 :RWA.RWA_WS_DSBANK_ADV|直销银行垫款补录表
    源  表3 :RWA.RWA_WS_BO_BILLREDISCOUNT|外部票据转帖现_银票补录表
    源  表4 :RWA.RWA_WS_CO_BILLREDISCOUNT|外部票据转帖现_商票补录表
    源  表5 :RWA.RWA_WS_BI_BILLREDISCOUNT|内部票据转帖现_银票补录表
    源  表6 :RWA.RWA_WS_INNERBANK|同业拆借补录表
    源  表7 :RWA.RWA_WS_B_BILLREPURCHASE|票据回购类投资补录表
    源  表8 :RWA.RWA_WS_B_BONDREPURCHASE|债券回购类投资补录表
    源  表9 :RWA.RWA_WS_S_BONDREPURCHASE|债券回购类投资补录表
    源  表10:RWA.RWA_WS_BONDTRADE|债券投资补录表
    源  表11:RWA.RWA_WS_RECEIVABLE|应收款投资补录表
    源  表12:RWA.RWA_WS_B_RECEIVABLE|买入返售应收款投资补录表
    源  表13:RWA.RWA_WS_FCII_BOND|理财投资债券投资业务信息补录表
    源  表14:RWA.RWA_WS_FCII_PLAN|理财投资资管计划业务信息补录表
    源  表15:RWA_DEV.RWA_EI_UNCONSFIINVEST|股权投资页面补录表
    源  表16:RWA_DEV.CBS_LNM|贷款户主档
    源  表17:RWA_DEV.CBS_IAC|通用分户帐
    源  表18:RWA_DEV.CMS_CUSTOMER_INFO|集市统一客户表
    源  表19:RWA.RWA_WP_DATASUPPLEMENT|补录任务明细信息表
    目标表  :RWA_DEV.BL_CUSTOMER_INFO|统一客户信息补录表
    临时表  :RWA_DEV.RWA_TMP_CUSTOMER_INFO|统一客户信息补录临时表
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_BL_CUSTOMER_GATHER';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除临时表中的原有记录
    --1.1 清空补录客户信息临时表
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TMP_CUSTOMER_INFO';

    --1.2 更新债券回购交易对手、债券发行人的组织机构代码
    --债券回购补录根据发行人名称默认组织机构代码。财政部-00001318-6(中华人民共和国财政部)、中国进出口银行-10001644-8、国家开发银行-00001845-4(默认国家开发银行股份有限公司)、中国农业发展银行-10001704-5
    UPDATE RWA.RWA_WS_B_BONDREPURCHASE SET ORGANIZATIONCODE = CASE WHEN CLIENTNAME LIKE '%财政部%' THEN '00001318-6' WHEN CLIENTNAME LIKE '%进出口%行%' THEN '10001644-8' WHEN CLIENTNAME LIKE '%国%开%行%' THEN '00001845-4' WHEN CLIENTNAME LIKE '%农发%行%' OR CLIENTNAME LIKE '%农业发%行%' THEN '10001704-5' ELSE ORGANIZATIONCODE END WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD');

    UPDATE RWA.RWA_WS_B_BONDREPURCHASE SET ISSUERORGCODE = CASE WHEN ISSUERNAME LIKE '%财政部%' THEN '00001318-6' WHEN ISSUERNAME LIKE '%进出口%行%' THEN '10001644-8' WHEN ISSUERNAME LIKE '%国%开%行%' THEN '00001845-4' WHEN ISSUERNAME LIKE '%农发%行%' OR ISSUERNAME LIKE '%农业发%行%' THEN '10001704-5' ELSE ISSUERORGCODE END WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD');

		UPDATE RWA.RWA_WS_S_BONDREPURCHASE SET ORGANIZATIONCODE = CASE WHEN CLIENTNAME LIKE '%财政部%' THEN '00001318-6' WHEN CLIENTNAME LIKE '%进出口%行%' THEN '10001644-8' WHEN CLIENTNAME LIKE '%国%开%行%' THEN '00001845-4' WHEN CLIENTNAME LIKE '%农发%行%' OR CLIENTNAME LIKE '%农业发%行%' THEN '10001704-5' ELSE ORGANIZATIONCODE END WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD');

		COMMIT;

    --2. 将满足条件的数据从源表插入到临时表中
    --2.1 更新核心-内部银票转贴现-承兑方汇总后的客户编号
    UPDATE RWA.RWA_WS_BI_BILLREDISCOUNT T
		   SET T.CUSTID1 =
		       (WITH TMP_CUST AS (SELECT LNUCAR_NO,LNUCARNO,SUPPSERIALNO, 'NBYPZTCD' || p_data_dt_str || lpad(rownum, 4, '0') AS CUSTID
															  FROM (SELECT LNUCAR_NO,LNUCARNO,SUPPSERIALNO
															          FROM RWA.RWA_WS_BI_BILLREDISCOUNT
															         WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
															         ORDER BY LNUCAR_NO,LNUCARNO,SUPPSERIALNO))
		         SELECT T1.CUSTID
		           FROM TMP_CUST T1
		          WHERE T1.LNUCAR_NO = T.LNUCAR_NO
		          	AND T1.LNUCARNO = T.LNUCARNO
		          	AND T1.SUPPSERIALNO = T.SUPPSERIALNO)
		          WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		;

		COMMIT;

    --2.2 插入核心-内部银票转贴现-承兑人信息
    INSERT INTO RWA_DEV.RWA_TMP_CUSTOMER_INFO(
                DATANO                             --数据期次
                ,CUSTOMERID                        --客户编号
                ,CUSTOMERNAME                      --客户名称
                ,CUSTOMERTYPE                      --客户类型
                ,CERTTYPE                          --证件类型
                ,CERTID                            --证件号码
                ,COUNTRYCODE                       --所在国家代码
                ,INDUSTRYTYPE                      --行业类型
                ,ORGNATURE                         --对公客户类型
                ,FINANCETYPE                       --金融机构类型
                ,SCOPE                             --工信部企业规模
                ,ORGID                             --客户归属机构
                ,CUSTOMERCATEGORY                  --客户类别
                ,ERATINGORG                        --外部评级机构
                ,ERATINGTYPE                       --外部评级期限
                ,ERATING                           --外部评级结果
                ,IRATING                           --内部评级结果
                ,UPDATEDATE                        --更新日期
                ,UPDATETIME                        --更新时间
    )
    SELECT
                p_data_dt_str                                  AS DATANO                    --数据期次
                ,T1.CUSTID1                                    AS CUSTOMERID                --客户编号
                ,T1.Acceptor                                   AS CUSTOMERNAME              --客户名称
                ,'0321000002'                                  AS CUSTOMERTYPE              --客户类型
                ,'Ent02'		                                   AS CERTTYPE                  --证件类型
                ,T1.Acceptorgcode                              AS CERTID                    --证件号码
                ,T1.Acceptcountrycode                          AS COUNTRYCODE               --所在国家代码
                ,T1.Acceptindustryid                           AS INDUSTRYTYPE              --行业类型
                ,''                                            AS ORGNATURE                 --对公客户类型
                ,''                                            AS FINANCETYPE               --金融机构类型
                ,''					                                   AS SCOPE                     --工信部企业规模
                ,T1.BELONGORGCODE                              AS ORGID                     --客户归属机构
                ,T1.CLIENTCATEGORY                             AS CUSTOMERCATEGORY          --客户类别
                ,''                                            AS ERATINGORG                --外部评级机构
                ,''                                            AS ERATINGTYPE               --外部评级期限
                ,''                                            AS ERATING                   --外部评级结果
                ,''                                            AS IRATING                   --内部评级结果
                ,''                                            AS UPDATEDATE                --更新日期
                ,''                                            AS UPDATETIME                --更新时间

    FROM        RWA.RWA_WS_BI_BILLREDISCOUNT T1
    WHERE 			T1.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
    AND					T1.CUSTID1 IS NOT NULL
    ;

    COMMIT;

		--2.3 更新核心-销银行垫款交易对手汇总后的客户编号
    UPDATE RWA.RWA_WS_DSBANK_ADV T
		   SET T.CUSTID1 =
		       (WITH TMP_CUST AS (SELECT IACAC_NO,SUPPSERIALNO, 'ZXYHDKJY' || p_data_dt_str || lpad(rownum, 4, '0') AS CUSTID
															  FROM (SELECT IACAC_NO,SUPPSERIALNO
															          FROM RWA.RWA_WS_DSBANK_ADV
															         WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
															         ORDER BY IACAC_NO,SUPPSERIALNO))
		         SELECT T1.CUSTID
		           FROM TMP_CUST T1
		          WHERE T1.IACAC_NO = T.IACAC_NO
		          	AND T1.SUPPSERIALNO = T.SUPPSERIALNO)
		          WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		;

		COMMIT;

    --2.4 插入核心-直销银行垫款交易对手信息
    INSERT INTO RWA_DEV.RWA_TMP_CUSTOMER_INFO(
                DATANO                             --数据期次
                ,CUSTOMERID                        --客户编号
                ,CUSTOMERNAME                      --客户名称
                ,CUSTOMERTYPE                      --客户类型
                ,CERTTYPE                          --证件类型
                ,CERTID                            --证件号码
                ,COUNTRYCODE                       --所在国家代码
                ,INDUSTRYTYPE                      --行业类型
                ,ORGNATURE                         --对公客户类型
                ,FINANCETYPE                       --金融机构类型
                ,SCOPE                             --工信部企业规模
                ,ORGID                             --客户归属机构
                ,CUSTOMERCATEGORY                  --客户类别
                ,ERATINGORG                        --外部评级机构
                ,ERATINGTYPE                       --外部评级期限
                ,ERATING                           --外部评级结果
                ,IRATING                           --内部评级结果
                ,UPDATEDATE                        --更新日期
                ,UPDATETIME                        --更新时间
    )
    SELECT
                p_data_dt_str                                  AS DATANO                    --数据期次
                ,T1.CUSTID1                                    AS CUSTOMERID                --客户编号
                ,T1.CLIENTNAME                                 AS CUSTOMERNAME              --客户名称
                ,'0321000002'		                               AS CUSTOMERTYPE              --客户类型
                ,'Ent02'		                                   AS CERTTYPE                  --证件类型
                ,T1.ORGANIZATIONCODE                           AS CERTID                    --证件号码
                ,T1.COUNTRYCODE                                AS COUNTRYCODE               --所在国家代码
                ,T1.INDUSTRYID	                               AS INDUSTRYTYPE              --行业类型
                ,''                                            AS ORGNATURE                 --对公客户类型
                ,''                                            AS FINANCETYPE               --金融机构类型
                ,''					                                   AS SCOPE                     --工信部企业规模
                ,T1.BELONGORGCODE                              AS ORGID                     --客户归属机构
                ,T1.CLIENTCATEGORY                             AS CUSTOMERCATEGORY          --客户类别
                ,''                                            AS ERATINGORG                --外部评级机构
                ,''                                            AS ERATINGTYPE               --外部评级期限
                ,''                                            AS ERATING                   --外部评级结果
                ,''                                            AS IRATING                   --内部评级结果
                ,''                                            AS UPDATEDATE                --更新日期
                ,''                                            AS UPDATETIME                --更新时间

    FROM        RWA.RWA_WS_DSBANK_ADV T1
    WHERE 			T1.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
    AND					T1.CUSTID1 IS NOT NULL
    ;

    COMMIT;

    --2.5 更新核心-同业拆借存放-交易对手汇总后的客户编号
    UPDATE RWA.RWA_WS_INNERBANK T
		   SET T.CUSTID1 =
		       (WITH TMP_CUST AS (SELECT ACCSERIALNO,SUPPSERIALNO, 'TYCJCFJY' || p_data_dt_str || lpad(rownum, 4, '0') AS CUSTID
															  FROM (SELECT ACCSERIALNO,SUPPSERIALNO
															          FROM RWA.RWA_WS_INNERBANK
															         WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
															         ORDER BY ACCSERIALNO,SUPPSERIALNO))
		         SELECT T1.CUSTID
		           FROM TMP_CUST T1
		          WHERE T1.ACCSERIALNO = T.ACCSERIALNO
		          	AND T1.SUPPSERIALNO = T.SUPPSERIALNO)
		          WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		;

		COMMIT;

    --2.6 插入核心-同业拆借存放-交易对手信息
    INSERT INTO RWA_DEV.RWA_TMP_CUSTOMER_INFO(
                DATANO                             --数据期次
                ,CUSTOMERID                        --客户编号
                ,CUSTOMERNAME                      --客户名称
                ,CUSTOMERTYPE                      --客户类型
                ,CERTTYPE                          --证件类型
                ,CERTID                            --证件号码
                ,COUNTRYCODE                       --所在国家代码
                ,INDUSTRYTYPE                      --行业类型
                ,ORGNATURE                         --对公客户类型
                ,FINANCETYPE                       --金融机构类型
                ,SCOPE                             --工信部企业规模
                ,ORGID                             --客户归属机构
                ,CUSTOMERCATEGORY                  --客户类别
                ,ERATINGORG                        --外部评级机构
                ,ERATINGTYPE                       --外部评级期限
                ,ERATING                           --外部评级结果
                ,IRATING                           --内部评级结果
                ,UPDATEDATE                        --更新日期
                ,UPDATETIME                        --更新时间
    )
    SELECT
                p_data_dt_str                                  AS DATANO                    --数据期次
                ,T1.CUSTID1                                    AS CUSTOMERID                --客户编号
                ,T1.CLIENTNAME                                 AS CUSTOMERNAME              --客户名称
                ,'0321000002'                                  AS CUSTOMERTYPE              --客户类型
                ,'Ent02'                                   		 AS CERTTYPE                  --证件类型
                ,T1.ORGANIZATIONCODE                           AS CERTID                    --证件号码
                ,T1.COUNTRYCODE                                AS COUNTRYCODE               --所在国家代码
                ,T1.INDUSTRYID	                               AS INDUSTRYTYPE              --行业类型
                ,''                                            AS ORGNATURE                 --对公客户类型
                ,''                                            AS FINANCETYPE               --金融机构类型
                ,''                                            AS SCOPE                     --工信部企业规模
                ,T1.BELONGORGCODE                              AS ORGID                     --客户归属机构
                ,T1.CLIENTCATEGORY                             AS CUSTOMERCATEGORY          --客户类别
                ,''                                            AS ERATINGORG                --外部评级机构
                ,''                                            AS ERATINGTYPE               --外部评级期限
                ,''                                            AS ERATING                   --外部评级结果
                ,''                                            AS IRATING                   --内部评级结果
                ,''                                            AS UPDATEDATE                --更新日期
                ,''                                            AS UPDATETIME                --更新时间

    FROM        RWA.RWA_WS_INNERBANK T1
    WHERE 			T1.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
    AND					T1.CUSTID1 IS NOT NULL
    ;

    COMMIT;

    --2.7 更新核心-买入返售票据回购-交易对手汇总后的客户编号
    UPDATE RWA.RWA_WS_B_BILLREPURCHASE T
		   SET T.CUSTID1 =
		       (WITH TMP_CUST AS (SELECT INVACCNO, 'MRFSPJJY' || p_data_dt_str || lpad(rownum, 4, '0') AS CUSTID
															  FROM (SELECT INVACCNO
															          FROM RWA.RWA_WS_B_BILLREPURCHASE
															         WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
															       	 GROUP BY INVACCNO
															         ORDER BY INVACCNO))
		         SELECT T1.CUSTID
		           FROM TMP_CUST T1
		          WHERE T1.INVACCNO = T.INVACCNO)
		          WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		;

		COMMIT;

    --2.8 插入核心-买入返售票据回购-交易对手信息
    INSERT INTO RWA_DEV.RWA_TMP_CUSTOMER_INFO(
                DATANO                             --数据期次
                ,CUSTOMERID                        --客户编号
                ,CUSTOMERNAME                      --客户名称
                ,CUSTOMERTYPE                      --客户类型
                ,CERTTYPE                          --证件类型
                ,CERTID                            --证件号码
                ,COUNTRYCODE                       --所在国家代码
                ,INDUSTRYTYPE                      --行业类型
                ,ORGNATURE                         --对公客户类型
                ,FINANCETYPE                       --金融机构类型
                ,SCOPE                             --工信部企业规模
                ,ORGID                             --客户归属机构
                ,CUSTOMERCATEGORY                  --客户类别
                ,ERATINGORG                        --外部评级机构
                ,ERATINGTYPE                       --外部评级期限
                ,ERATING                           --外部评级结果
                ,IRATING                           --内部评级结果
                ,UPDATEDATE                        --更新日期
                ,UPDATETIME                        --更新时间
    )
    SELECT
                p_data_dt_str                                  AS DATANO                    --数据期次
                ,T1.CUSTID1                                    AS CUSTOMERID                --客户编号
                ,T1.CLIENTNAME                                 AS CUSTOMERNAME              --客户名称
                ,'0321000002'                                  AS CUSTOMERTYPE              --客户类型
                ,'Ent02'		                                   AS CERTTYPE                  --证件类型
                ,T1.ORGANIZATIONCODE                           AS CERTID                    --证件号码
                ,T1.COUNTRYCODE                                AS COUNTRYCODE               --所在国家代码
                ,T1.INDUSTRYID	                               AS INDUSTRYTYPE              --行业类型
                ,''                                            AS ORGNATURE                 --对公客户类型
                ,''                                            AS FINANCETYPE               --金融机构类型
                ,''                                            AS SCOPE                     --工信部企业规模
                ,T1.BELONGORGCODE                              AS ORGID                     --客户归属机构
                ,T1.CLIENTCATEGORY                             AS CUSTOMERCATEGORY          --客户类别
                ,''                                            AS ERATINGORG                --外部评级机构
                ,''                                            AS ERATINGTYPE               --外部评级期限
                ,''                                            AS ERATING                   --外部评级结果
                ,''                                            AS IRATING                   --内部评级结果
                ,''                                            AS UPDATEDATE                --更新日期
                ,''                                            AS UPDATETIME                --更新时间

    FROM        RWA.RWA_WS_B_BILLREPURCHASE T1
    WHERE 			T1.ROWID IN  (SELECT MAX(T3.ROWID)
                               FROM RWA.RWA_WS_B_BILLREPURCHASE T3
						                  WHERE T3.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
						                  	AND T3.CUSTID1 IS NOT NULL
                           GROUP BY T3.INVACCNO)
    AND 				T1.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
    AND					T1.CUSTID1 IS NOT NULL
    ;

    COMMIT;

    --2.9 更新核心-买入返售票据回购-承兑方汇总后的客户编号
    UPDATE RWA.RWA_WS_B_BILLREPURCHASE T
		   SET T.CUSTID2 =
		       (WITH TMP_CUST AS (SELECT INVACCNO,BILLNO,SUPPSERIALNO, 'MRFSPJCD' || p_data_dt_str || lpad(rownum, 4, '0') AS CUSTID
															  FROM (SELECT INVACCNO,BILLNO,SUPPSERIALNO
															          FROM RWA.RWA_WS_B_BILLREPURCHASE
															         WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
															         ORDER BY INVACCNO,BILLNO,SUPPSERIALNO))
		         SELECT T1.CUSTID
		           FROM TMP_CUST T1
		          WHERE T1.INVACCNO = T.INVACCNO
		          	AND T1.BILLNO = T.BILLNO
		          	AND T1.SUPPSERIALNO = T.SUPPSERIALNO)
		          WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		;

		COMMIT;

    --2.10 插入核心-买入返售票据回购-承兑方信息
    INSERT INTO RWA_DEV.RWA_TMP_CUSTOMER_INFO(
                DATANO                             --数据期次
                ,CUSTOMERID                        --客户编号
                ,CUSTOMERNAME                      --客户名称
                ,CUSTOMERTYPE                      --客户类型
                ,CERTTYPE                          --证件类型
                ,CERTID                            --证件号码
                ,COUNTRYCODE                       --所在国家代码
                ,INDUSTRYTYPE                      --行业类型
                ,ORGNATURE                         --对公客户类型
                ,FINANCETYPE                       --金融机构类型
                ,SCOPE                             --工信部企业规模
                ,ORGID                             --客户归属机构
                ,CUSTOMERCATEGORY                  --客户类别
                ,ERATINGORG                        --外部评级机构
                ,ERATINGTYPE                       --外部评级期限
                ,ERATING                           --外部评级结果
                ,IRATING                           --内部评级结果
                ,UPDATEDATE                        --更新日期
                ,UPDATETIME                        --更新时间
    )
    SELECT
                p_data_dt_str                                  AS DATANO                    --数据期次
                ,T1.CUSTID2                                    AS CUSTOMERID                --客户编号
                ,T1.ISSUERNAME                                 AS CUSTOMERNAME              --客户名称
                ,'0321000002'                                  AS CUSTOMERTYPE              --客户类型
                ,'Ent02'		                                   AS CERTTYPE                  --证件类型
                ,T1.ISSUERORGCODE                              AS CERTID                    --证件号码
                ,T1.ISSUERCOUNTRYCODE                          AS COUNTRYCODE               --所在国家代码
                ,T1.ISSUERINDUSTRYID                           AS INDUSTRYTYPE              --行业类型
                ,''                                            AS ORGNATURE                 --对公客户类型
                ,''                                            AS FINANCETYPE               --金融机构类型
                ,''                                            AS SCOPE                     --工信部企业规模
                ,T1.BELONGORGCODE                              AS ORGID                     --客户归属机构
                ,T1.ACCEPTCATEGORY                             AS CUSTOMERCATEGORY          --客户类别
                ,''                                            AS ERATINGORG                --外部评级机构
                ,''                                            AS ERATINGTYPE               --外部评级期限
                ,''                                            AS ERATING                   --外部评级结果
                ,''                                            AS IRATING                   --内部评级结果
                ,''                                            AS UPDATEDATE                --更新日期
                ,''                                            AS UPDATETIME                --更新时间

    FROM        RWA.RWA_WS_B_BILLREPURCHASE T1
    WHERE 			T1.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
    AND					T1.CUSTID2 IS NOT NULL
    ;

    COMMIT;

    --2.11 更新核心-买入返售债券回购-交易对手汇总后的客户编号
    UPDATE RWA.RWA_WS_B_BONDREPURCHASE T
		   SET T.CUSTID1 =
		       (WITH TMP_CUST AS (SELECT INVACCNO, 'MRFSZQJY' || p_data_dt_str || lpad(rownum, 4, '0') AS CUSTID
															  FROM (SELECT INVACCNO
															          FROM RWA.RWA_WS_B_BONDREPURCHASE
															         WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
															       	 GROUP BY INVACCNO
															         ORDER BY INVACCNO))
		         SELECT T1.CUSTID
		           FROM TMP_CUST T1
		          WHERE T1.INVACCNO = T.INVACCNO)
		          WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		;

		COMMIT;

    --2.12 插入核心-买入返售债券回购-交易对手信息
    INSERT INTO RWA_DEV.RWA_TMP_CUSTOMER_INFO(
                DATANO                             --数据期次
                ,CUSTOMERID                        --客户编号
                ,CUSTOMERNAME                      --客户名称
                ,CUSTOMERTYPE                      --客户类型
                ,CERTTYPE                          --证件类型
                ,CERTID                            --证件号码
                ,COUNTRYCODE                       --所在国家代码
                ,INDUSTRYTYPE                      --行业类型
                ,ORGNATURE                         --对公客户类型
                ,FINANCETYPE                       --金融机构类型
                ,SCOPE                             --工信部企业规模
                ,ORGID                             --客户归属机构
                ,CUSTOMERCATEGORY                  --客户类别
                ,ERATINGORG                        --外部评级机构
                ,ERATINGTYPE                       --外部评级期限
                ,ERATING                           --外部评级结果
                ,IRATING                           --内部评级结果
                ,UPDATEDATE                        --更新日期
                ,UPDATETIME                        --更新时间
    )
    SELECT
                p_data_dt_str                                  AS DATANO                    --数据期次
                ,T1.CUSTID1                                    AS CUSTOMERID                --客户编号
                ,T1.CLIENTNAME                                 AS CUSTOMERNAME              --客户名称
                ,'0321000002'                                  AS CUSTOMERTYPE              --客户类型
                ,'Ent02'		                                   AS CERTTYPE                  --证件类型
                ,T1.ORGANIZATIONCODE                           AS CERTID                    --证件号码
                ,T1.COUNTRYCODE                                AS COUNTRYCODE               --所在国家代码
                ,T1.INDUSTRYID	                               AS INDUSTRYTYPE              --行业类型
                ,''                                            AS ORGNATURE                 --对公客户类型
                ,''                                            AS FINANCETYPE               --金融机构类型
                ,''                                            AS SCOPE                     --工信部企业规模
                ,T1.BELONGORGCODE                              AS ORGID                     --客户归属机构
                ,T1.CLIENTCATEGORY                             AS CUSTOMERCATEGORY          --客户类别
                ,''                                            AS ERATINGORG                --外部评级机构
                ,''                                            AS ERATINGTYPE               --外部评级期限
                ,''                                            AS ERATING                   --外部评级结果
                ,''                                            AS IRATING                   --内部评级结果
                ,''                                            AS UPDATEDATE                --更新日期
                ,''                                            AS UPDATETIME                --更新时间

    FROM        RWA.RWA_WS_B_BONDREPURCHASE T1
    WHERE 			T1.ROWID IN  (SELECT MAX(T3.ROWID)
                               FROM RWA.RWA_WS_B_BONDREPURCHASE T3
						                  WHERE T3.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
						                  	AND T3.CUSTID1 IS NOT NULL
                           GROUP BY T3.INVACCNO)
    AND 				T1.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
    AND					T1.CUSTID1 IS NOT NULL
    ;

    COMMIT;

    --2.13 更新核心-买入返售债券回购-发行人汇总后的客户编号
    UPDATE RWA.RWA_WS_B_BONDREPURCHASE T
		   SET T.CUSTID2 =
		       (WITH TMP_CUST AS (SELECT INVACCNO,BONDCODE,SUPPSERIALNO, 'MRFSZQFX' || p_data_dt_str || lpad(rownum, 4, '0') AS CUSTID
															  FROM (SELECT INVACCNO,BONDCODE,SUPPSERIALNO
															          FROM RWA.RWA_WS_B_BONDREPURCHASE
															         WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
															         ORDER BY INVACCNO,BONDCODE,SUPPSERIALNO))
		         SELECT T1.CUSTID
		           FROM TMP_CUST T1
		          WHERE T1.INVACCNO = T.INVACCNO
		          	AND T1.BONDCODE = T.BONDCODE
		          	AND T1.SUPPSERIALNO = T.SUPPSERIALNO)
		          WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		;

		COMMIT;

    --2.14 插入核心-买入返售债券回购-发行人信息
    INSERT INTO RWA_DEV.RWA_TMP_CUSTOMER_INFO(
                DATANO                             --数据期次
                ,CUSTOMERID                        --客户编号
                ,CUSTOMERNAME                      --客户名称
                ,CUSTOMERTYPE                      --客户类型
                ,CERTTYPE                          --证件类型
                ,CERTID                            --证件号码
                ,COUNTRYCODE                       --所在国家代码
                ,INDUSTRYTYPE                      --行业类型
                ,ORGNATURE                         --对公客户类型
                ,FINANCETYPE                       --金融机构类型
                ,SCOPE                             --工信部企业规模
                ,ORGID                             --客户归属机构
                ,CUSTOMERCATEGORY                  --客户类别
                ,ERATINGORG                        --外部评级机构
                ,ERATINGTYPE                       --外部评级期限
                ,ERATING                           --外部评级结果
                ,IRATING                           --内部评级结果
                ,UPDATEDATE                        --更新日期
                ,UPDATETIME                        --更新时间
    )
    SELECT
                p_data_dt_str                                  AS DATANO                    --数据期次
                ,T1.CUSTID2                                    AS CUSTOMERID                --客户编号
                ,T1.ISSUERNAME                                 AS CUSTOMERNAME              --客户名称
                ,'0321000002'                                  AS CUSTOMERTYPE              --客户类型
                ,'Ent02'		                                   AS CERTTYPE                  --证件类型
                ,T1.ISSUERORGCODE                              AS CERTID                    --证件号码
                ,T1.ISSUERCOUNTRYCODE                          AS COUNTRYCODE               --所在国家代码
                ,T1.ISSUERINDUSTRYID                           AS INDUSTRYTYPE              --行业类型
                ,''                                            AS ORGNATURE                 --对公客户类型
                ,''                                            AS FINANCETYPE               --金融机构类型
                ,T1.ISSUERSCOPE                                AS SCOPE                     --工信部企业规模
                ,T1.BELONGORGCODE                              AS ORGID                     --客户归属机构
                ,T1.ISSUERCATEGORY                             AS CUSTOMERCATEGORY          --客户类别
                ,''                                            AS ERATINGORG                --外部评级机构
                ,''                                            AS ERATINGTYPE               --外部评级期限
                ,''                                            AS ERATING                   --外部评级结果
                ,''                                            AS IRATING                   --内部评级结果
                ,''                                            AS UPDATEDATE                --更新日期
                ,''                                            AS UPDATETIME                --更新时间

    FROM        RWA.RWA_WS_B_BONDREPURCHASE T1
    WHERE 			T1.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
    ;

    COMMIT;

    --2.15 更新核心-卖出回购债券回购-交易对手汇总后的客户编号
    UPDATE RWA.RWA_WS_S_BONDREPURCHASE T
		   SET T.CUSTID1 =
		       (WITH TMP_CUST AS (SELECT INVACCNO, 'MCHGZQJY' || p_data_dt_str || lpad(rownum, 4, '0') AS CUSTID
															  FROM (SELECT INVACCNO
															          FROM RWA.RWA_WS_S_BONDREPURCHASE
															         WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
															         	 AND REPURCHASETYPE = '02'
															       	 GROUP BY INVACCNO
															         ORDER BY INVACCNO))
		         SELECT T1.CUSTID
		           FROM TMP_CUST T1
		          WHERE T1.INVACCNO = T.INVACCNO)
		          WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		;

		COMMIT;

    --2.16 插入核心-卖出回购债券回购-交易对手信息
    INSERT INTO RWA_DEV.RWA_TMP_CUSTOMER_INFO(
                DATANO                             --数据期次
                ,CUSTOMERID                        --客户编号
                ,CUSTOMERNAME                      --客户名称
                ,CUSTOMERTYPE                      --客户类型
                ,CERTTYPE                          --证件类型
                ,CERTID                            --证件号码
                ,COUNTRYCODE                       --所在国家代码
                ,INDUSTRYTYPE                      --行业类型
                ,ORGNATURE                         --对公客户类型
                ,FINANCETYPE                       --金融机构类型
                ,SCOPE                             --工信部企业规模
                ,ORGID                             --客户归属机构
                ,CUSTOMERCATEGORY                  --客户类别
                ,ERATINGORG                        --外部评级机构
                ,ERATINGTYPE                       --外部评级期限
                ,ERATING                           --外部评级结果
                ,IRATING                           --内部评级结果
                ,UPDATEDATE                        --更新日期
                ,UPDATETIME                        --更新时间
    )
    SELECT
                p_data_dt_str                                  AS DATANO                    --数据期次
                ,T1.CUSTID1                                    AS CUSTOMERID                --客户编号
                ,T1.CLIENTNAME                                 AS CUSTOMERNAME              --客户名称
                ,'0321000002'                                  AS CUSTOMERTYPE              --客户类型
                ,'Ent02'		                                   AS CERTTYPE                  --证件类型
                ,T1.ORGANIZATIONCODE                           AS CERTID                    --证件号码
                ,T1.COUNTRYCODE                                AS COUNTRYCODE               --所在国家代码
                ,T1.INDUSTRYID	                               AS INDUSTRYTYPE              --行业类型
                ,''                                            AS ORGNATURE                 --对公客户类型
                ,''                                            AS FINANCETYPE               --金融机构类型
                ,''                                            AS SCOPE                     --工信部企业规模
                ,T1.BELONGORGCODE                              AS ORGID                     --客户归属机构
                ,T1.CLIENTCATEGORY                             AS CUSTOMERCATEGORY          --客户类别
                ,''                                            AS ERATINGORG                --外部评级机构
                ,''                                            AS ERATINGTYPE               --外部评级期限
                ,''                                            AS ERATING                   --外部评级结果
                ,''                                            AS IRATING                   --内部评级结果
                ,''                                            AS UPDATEDATE                --更新日期
                ,''                                            AS UPDATETIME                --更新时间

    FROM        RWA.RWA_WS_S_BONDREPURCHASE T1
    WHERE 			T1.ROWID IN  (SELECT MAX(T3.ROWID)
                               FROM RWA.RWA_WS_S_BONDREPURCHASE T3
						                  WHERE T3.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
						                  	AND	T3.REPURCHASETYPE = '02'
						                  	AND T3.CUSTID1 IS NOT NULL
                           GROUP BY T3.INVACCNO)
    AND 				T1.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
    AND					T1.REPURCHASETYPE = '02'
    AND					T1.CUSTID1 IS NOT NULL
    ;

    COMMIT;

		--2.17 更新台账-股权投资-交易对手汇总后的客户编号
    UPDATE RWA_DEV.RWA_EI_UNCONSFIINVEST T
		   SET T.CUSTID1 =
		       (WITH TMP_CUST AS (SELECT SERIALNO, 'TZGQTZJY' || p_data_dt_str || lpad(rownum, 4, '0') AS CUSTID
															  FROM (SELECT SERIALNO
															          FROM RWA_DEV.RWA_EI_UNCONSFIINVEST
															         WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
															         	 AND EQUITYINVESTTYPE LIKE '03%'
															         ORDER BY SERIALNO))
		         SELECT T1.CUSTID
		           FROM TMP_CUST T1
		          WHERE T1.SERIALNO = T.SERIALNO)
		          WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		;

		COMMIT;

    --2.18 插入台账-股权投资-交易对手信息
    INSERT INTO RWA_DEV.RWA_TMP_CUSTOMER_INFO(
                DATANO                             --数据期次
                ,CUSTOMERID                        --客户编号
                ,CUSTOMERNAME                      --客户名称
                ,CUSTOMERTYPE                      --客户类型
                ,CERTTYPE                          --证件类型
                ,CERTID                            --证件号码
                ,COUNTRYCODE                       --所在国家代码
                ,INDUSTRYTYPE                      --行业类型
                ,ORGNATURE                         --对公客户类型
                ,FINANCETYPE                       --金融机构类型
                ,SCOPE                             --工信部企业规模
                ,ORGID                             --客户归属机构
                ,CUSTOMERCATEGORY                  --客户类别
                ,ERATINGORG                        --外部评级机构
                ,ERATINGTYPE                       --外部评级期限
                ,ERATING                           --外部评级结果
                ,IRATING                           --内部评级结果
                ,UPDATEDATE                        --更新日期
                ,UPDATETIME                        --更新时间
    )
    SELECT
                p_data_dt_str                                  AS DATANO                    --数据期次
                ,T1.CUSTID1                                    AS CUSTOMERID                --客户编号
                ,T1.investeename                               AS CUSTOMERNAME              --客户名称
                ,'0321000002'                                  AS CUSTOMERTYPE              --客户类型
                ,'Ent02'		                                   AS CERTTYPE                  --证件类型
                ,T1.organizationcode                           AS CERTID                    --证件号码
                ,'CHN'				                                 AS COUNTRYCODE               --所在国家代码
                ,''                                            AS INDUSTRYTYPE              --行业类型
                ,''                                            AS ORGNATURE                 --对公客户类型
                ,''                                            AS FINANCETYPE               --金融机构类型
                ,''                                            AS SCOPE                     --工信部企业规模
                ,T1.orgId                                      AS ORGID                     --客户归属机构
                ,T1.equityinvesttype                           AS CUSTOMERCATEGORY          --客户类别
                ,''                                            AS ERATINGORG                --外部评级机构
                ,''                                            AS ERATINGTYPE               --外部评级期限
                ,''                                            AS ERATING                   --外部评级结果
                ,''                                            AS IRATING                   --内部评级结果
                ,''                                            AS UPDATEDATE                --更新日期
                ,''                                            AS UPDATETIME                --更新时间

    FROM        RWA_DEV.RWA_EI_UNCONSFIINVEST T1
    WHERE 			T1.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
    AND 				T1.EQUITYINVESTTYPE LIKE '03%'
    AND					T1.CUSTID1 IS NOT NULL
    ;

    COMMIT;

    --2.19 债券投资-货币基金-缓释人ID
		UPDATE RWA.RWA_WS_BONDTRADE_MF T
		   SET T.CUSTID1 =
		       (WITH TMP_BOND AS (SELECT BOND_ID,SUPPSERIALNO, 'ZQTZHBJJ' || p_data_dt_str || lpad(rownum, 4, '0') AS CUSTID1
															  FROM (SELECT BOND_ID,SUPPSERIALNO
															          FROM RWA.RWA_WS_BONDTRADE_MF
															         WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
															         	 AND GUARANTYTYPE IS NOT NULL
															         	 AND GUARANTORNAME IS NOT NULL
															         ORDER BY BOND_ID,SUPPSERIALNO))
		         SELECT T1.CUSTID1
		           FROM TMP_BOND T1
		          WHERE T1.BOND_ID = T.BOND_ID
		          	AND T1.SUPPSERIALNO = T.SUPPSERIALNO)
		          WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		;

		COMMIT;

		--2.20 债券投资-货币基金-缓释人信息
    INSERT INTO RWA_DEV.RWA_TMP_CUSTOMER_INFO(
                DATANO                             --数据期次
                ,CUSTOMERID                        --客户编号
                ,CUSTOMERNAME                      --客户名称
                ,CUSTOMERTYPE                      --客户类型
                ,CERTTYPE                          --证件类型
                ,CERTID                            --证件号码
                ,COUNTRYCODE                       --所在国家代码
                ,INDUSTRYTYPE                      --行业类型
                ,ORGNATURE                         --对公客户类型
                ,FINANCETYPE                       --金融机构类型
                ,SCOPE                             --工信部企业规模
                ,ORGID                             --客户归属机构
                ,CUSTOMERCATEGORY                  --客户类别
                ,ERATINGORG                        --外部评级机构
                ,ERATINGTYPE                       --外部评级期限
                ,ERATING                           --外部评级结果
                ,IRATING                           --内部评级结果
                ,UPDATEDATE                        --更新日期
                ,UPDATETIME                        --更新时间
    )
    SELECT
                p_data_dt_str                                  AS DATANO                    --数据期次
                ,T1.CUSTID1                                    AS CUSTOMERID                --客户编号
                ,T1.GUARANTORNAME                              AS CUSTOMERNAME              --客户名称
                ,'0321000002'                                  AS CUSTOMERTYPE              --客户类型
                ,''					                                   AS CERTTYPE                  --证件类型
                ,''				                                     AS CERTID                    --证件号码
                ,T1.GUARANTORCOUNTRYCODE                       AS COUNTRYCODE               --所在国家代码
                ,'J66'                                         AS INDUSTRYTYPE              --行业类型
                ,''                                            AS ORGNATURE                 --对公客户类型
                ,''                                            AS FINANCETYPE               --金融机构类型
                ,'00'                                          AS SCOPE                     --工信部企业规模
                ,T1.BELONGORGCODE                              AS ORGID                     --客户归属机构
                ,T1.GUARANTORCATEGORY                          AS CUSTOMERCATEGORY          --客户类别
                ,''                                            AS ERATINGORG                --外部评级机构
                ,''                                            AS ERATINGTYPE               --外部评级期限
                ,''                                            AS ERATING                   --外部评级结果
                ,''                                            AS IRATING                   --内部评级结果
                ,''                                            AS UPDATEDATE                --更新日期
                ,''                                            AS UPDATETIME                --更新时间

    FROM        RWA.RWA_WS_BONDTRADE_MF T1
    WHERE       T1.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		AND					T1.CUSTID1 IS NOT NULL
    ;

    COMMIT;


    /*目标表数据统计*/
    --统计插入的记录数
    --SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_TMP_CUSTOMER_INFO;
    --Dbms_output.Put_line('RWA_DEV.RWA_TMP_CUSTOMER_INFO表当前插入的补录客户汇总信息数据记录为: ' || v_count || ' 条');

    --3.清除目标表中的原有记录
    --清空补录客户信息目标表同期数据
    DELETE FROM RWA_DEV.BL_CUSTOMER_INFO WHERE DATANO = p_data_dt_str;

    COMMIT;

    --4.将满足条件的数据从源表插入到补录客户信息表中
    --4.1 插入本期补录的客户汇总信息
    INSERT INTO RWA_DEV.BL_CUSTOMER_INFO(
                DATANO                             --数据期次
                ,CUSTOMERID                        --客户编号
                ,CUSTOMERNAME                      --客户名称
                ,CUSTOMERTYPE                      --客户类型
                ,CERTTYPE                          --证件类型
                ,CERTID                            --证件号码
                ,COUNTRYCODE                       --所在国家代码
                ,INDUSTRYTYPE                      --行业类型
                ,ORGNATURE                         --对公客户类型
                ,FINANCETYPE                       --金融机构类型
                ,SCOPE                             --工信部企业规模
                ,ORGID                             --客户归属机构
                ,CUSTOMERCATEGORY                  --客户类别
                ,ERATINGORG                        --外部评级机构
                ,ERATINGTYPE                       --外部评级期限
                ,ERATING                           --外部评级结果
                ,IRATING                           --内部评级结果
                ,UPDATEDATE                        --更新日期
                ,UPDATETIME                        --更新时间
    )
    SELECT
                p_data_dt_str                                 AS DATANO                    --数据期次
                ,T1.CUSTOMERID                                AS CUSTOMERID                --客户编号
                ,T1.CUSTOMERNAME         											AS CUSTOMERNAME              --客户名称
                ,T1.CUSTOMERTYPE											        AS CUSTOMERTYPE              --客户类型
                ,T1.CERTTYPE								                  AS CERTTYPE                  --证件类型
                ,T1.CERTID                                    AS CERTID                    --证件号码
                ,T1.COUNTRYCODE										            AS COUNTRYCODE               --所在国家代码
                ,T1.INDUSTRYTYPE										          AS INDUSTRYTYPE              --行业类型
                ,''					                                  AS ORGNATURE                 --对公客户类型
                ,''						                                AS FINANCETYPE               --金融机构类型
                ,T1.SCOPE							      									AS SCOPE                     --工信部企业规模
                ,T1.ORGID							      									AS ORGID                     --客户归属机构
                ,T1.CUSTOMERCATEGORY													AS CUSTOMERCATEGORY          --客户类别
                ,T1.ERATINGORG                                AS ERATINGORG                --外部评级机构
                ,T1.ERATINGTYPE                               AS ERATINGTYPE               --外部评级期限
                ,T1.ERATING                                   AS ERATING                   --外部评级结果
                ,''						                                AS IRATING                   --内部评级结果
                ,T1.UPDATEDATE                                AS UPDATEDATE                --更新日期
                ,T1.UPDATETIME                                AS UPDATETIME                --更新时间

    FROM        RWA_DEV.RWA_TMP_CUSTOMER_INFO T1
    ;

    COMMIT;

    --5.清空补录客户信息临时表
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TMP_CUSTOMER_INFO';

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'BL_CUSTOMER_INFO',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.BL_CUSTOMER_INFO WHERE DATANO = p_data_dt_str;
    --Dbms_output.Put_line('RWA_DEV.BL_CUSTOMER_INFO表当前插入的补录客户汇总信息数据记录为: ' || v_count || ' 条');

    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功' || '-' || v_count;
    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '补录客户去重汇总('|| v_pro_name ||')处理失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_BL_CUSTOMER_GATHER;
/

