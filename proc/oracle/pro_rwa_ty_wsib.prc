CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_TY_WSIB(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_TY_WSIB
    实现功能:核心系统-同业-补录铺底(从数据源核心系统将业务相关信息全量导入RWA拆借与存放同业补录铺底表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-06-20
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.CBS_BRW|资金拆借登记簿
    源  表2 :RWA_DEV.CBS_BBR|同业账户登记簿
    源  表3 :RWA_DEV.CBS_IAC|通用分户帐
    源  表4 :RWA.ORG_INFO|机构信息表
    源  表5 :RWA.RWA_WS_INNERBANK|拆借与存放同业补录表
    源  表6 :RWA.RWA_WP_SUPPTASKORG|补录任务机构分发配置表
    源  表7 :RWA.RWA_WP_SUPPTASK|补录任务发布表
    源  表8 :RWA_DEV.BL_CUSTOMER_INFO|补录客户汇总表
    目标表1 :RWA.RWA_WSIB_INNERBANK|拆借与存放同业铺底表
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_TY_WSIB';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    --清空拆借与存放同业铺底表
    DELETE FROM RWA.RWA_WSIB_INNERBANK WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    COMMIT;

    --2.将满足条件的数据从源表插入到目标表中
    --2.1 核心系统-拆借同业业务
    INSERT INTO RWA.RWA_WSIB_INNERBANK(
                DATADATE                               --数据日期
                ,ORGID                             	 	 --机构ID
                ,ACCSERIALNO                        	 --交易账号
                ,BUSINESSTYPE													 --业务类型
                ,BELONGORGCODE                         --业务所属机构
                ,IACCURBAL														 --当前余额
                ,IACCRTDAT														 --起始日
                ,IACDLTDAT														 --到期日
                ,CLIENTNAME                         	 --交易对手名称
                ,ORGANIZATIONCODE                   	 --交易对手组织机构代码
                ,COUNTRYCODE                        	 --交易对手国家代码
                ,INDUSTRYID                         	 --交易对手行业代码
                ,CLIENTCATEGORY												 --客户类型
    )
    WITH TMP_SUPPORG AS (
								SELECT T1.ORGID AS ORGID
										   ,CASE WHEN T3.ORGLEVEL > 2 AND T3.SORTNO LIKE '%610' THEN T4.SORTNO WHEN T1.ORGID = '01370000' THEN '1100000' ELSE T3.SORTNO END AS SORTNO  --仅金融市场部需要提到分行层级，总行清算中心强制分到总行金融同业管理部
								  FROM RWA.RWA_WP_SUPPTASKORG T1
						INNER JOIN RWA.RWA_WP_SUPPTASK T2
								    ON T1.SUPPTASKID = T2.SUPPTASKID
								   AND T2.ENABLEFLAG = '01'
						 LEFT JOIN RWA.ORG_INFO T3
								    ON T1.ORGID = T3.ORGID
						 LEFT JOIN RWA.ORG_INFO T4
	                	ON T3.BELONGORGID = T4.ORGID
								 WHERE T1.SUPPTMPLID = 'M-0050'
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
                ,ACCSERIALNO                        	 --交易账号
                ,BUSINESSTYPE													 --业务类型
                ,BELONGORGCODE                         --业务所属机构
                ,IACCURBAL														 --当前余额
                ,IACCRTDAT														 --起始日
                ,IACDLTDAT														 --到期日
                ,CLIENTNAME                         	 --交易对手名称
                ,ORGANIZATIONCODE                   	 --交易对手组织机构代码
                ,COUNTRYCODE                        	 --交易对手国家代码
                ,INDUSTRYID                         	 --交易对手行业代码
                ,CLIENTCATEGORY												 --客户类型
		FROM (
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,NVL(T7.ORGID,(SELECT ORGID FROM TMP_SUPPORG WHERE ROWNUM = 1))
                												                     AS ORGID										 --机构ID                     		按照补录任务分配情况，分配给金融市场部及其分部
                ,RANK() OVER(PARTITION BY T1.ACCNO ORDER BY LENGTH(NVL(T7.SORTNO,(SELECT SORTNO FROM TMP_SUPPORG WHERE ROWNUM = 1))) DESC)
                																						 AS RECORDNO								 --数据序号
                ,T1.ACCNO		                             		 AS ACCSERIALNO              --交易账号
                ,'01'																				 AS BUSINESSTYPE						 --业务类型												默认：01-拆借
                ,T2.IACGACBR																 AS BELONGORGCODE            --业务所属机构
                ,ABS(T2.IACCURBAL)													 AS IACCURBAL								 --当前余额
                ,TO_CHAR(TO_DATE(T1.SDATE,'YYYYMMDD'),'YYYY-MM-DD')
                																						 AS IACCRTDAT								 --起始日
                ,TO_CHAR(TO_DATE(T1.EDATE,'YYYYMMDD'),'YYYY-MM-DD')
                																						 AS IACDLTDAT								 --到期日
                ,NVL(T4.CLIENTNAME,T1.CUSTNAME)              AS CLIENTNAME               --交易对手名称
                ,NVL(T4.ORGANIZATIONCODE,T8.CERTID)          AS ORGANIZATIONCODE         --交易对手组织机构代码
                ,NVL(T4.COUNTRYCODE,'CHN')                   AS COUNTRYCODE              --交易对手国家代码    						默认CHN-中国
                ,NVL(T4.INDUSTRYID,'J66')					           AS INDUSTRYID               --交易对手行业代码 							默认J66-货币金融服务
                ,NVL(T4.CLIENTCATEGORY,'0202')					     AS CLIENTCATEGORY           --客户类型					 							默认0202-中国商业银行

    FROM				RWA_DEV.CBS_BRW T1	             		 											--资金拆借登记簿
		INNER JOIN 	RWA_DEV.CBS_IAC T2																				--通用分户帐
		ON 					T1.ACCNO = T2.IACAC_NO
		AND					T2.IACCURBAL <> 0																					--账户余额不等于0
		AND					T2.DATANO = p_data_dt_str
		LEFT	JOIN	RWA.ORG_INFO T3																						--机构信息表
	  ON					T2.IACGACBR = T3.ORGID
	  LEFT JOIN   (SELECT ACCSERIALNO
	  									 ,CLIENTNAME
	  									 ,ORGANIZATIONCODE
	  									 ,COUNTRYCODE
	  									 ,INDUSTRYID
	  									 ,CLIENTCATEGORY
	  							 FROM RWA.RWA_WS_INNERBANK
	  							WHERE BUSINESSTYPE = '01'
    								AND DATADATE = (SELECT MAX(DATADATE) FROM RWA.RWA_WS_INNERBANK WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD') AND BUSINESSTYPE = '01')
    						) T4																											--取最近一期补录数据铺底
    ON          T1.ACCNO = T4.ACCSERIALNO
    LEFT	JOIN  TMP_SUPPORG T7
    ON          T3.SORTNO LIKE T7.SORTNO || '%'
    LEFT	JOIN	TMP_BL_CUST T8
    ON					T1.CUSTNAME = T8.CUSTOMERNAME
		WHERE 			T1.FLAG = '1'																							--拆借标志 业务方向0：拆入；1、拆出
    AND 				T1.STATUS = '00'																					--状态 00：正常、01：已归还
    AND NOT EXISTS (SELECT 1 FROM RWA_DEV.NCM_BUSINESS_DUEBILL CBD WHERE 'BRW' || T1.CNTRNO = CBD.THIRDPARTYACCOUNTS AND CBD.DATANO = p_data_dt_str)
    AND 				T1.DATANO = p_data_dt_str
    )
    WHERE RECORDNO = 1
    ORDER BY		ACCSERIALNO
		;

    COMMIT;

    /*目标表数据统计*/
    --统计插入的记录数
    --SELECT COUNT(1) INTO v_count FROM RWA.RWA_WSIB_INNERBANK WHERE BUSINESSTYPE = '01' AND DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA.RWA_WSIB_INNERBANK表当前插入的核心系统-拆借同业铺底数据记录为: ' || v_count || ' 条');


    --2.2 核心系统-存放同业业务
    INSERT INTO RWA.RWA_WSIB_INNERBANK(
                DATADATE                               --数据日期
                ,ORGID                             	 	 --机构ID
                ,ACCSERIALNO                        	 --交易账号
                ,BUSINESSTYPE													 --业务类型
                ,BELONGORGCODE                         --业务所属机构
                ,IACCURBAL														 --当前余额
                ,IACCRTDAT														 --起始日
                ,IACDLTDAT														 --到期日
                ,CLIENTNAME                         	 --交易对手名称
                ,ORGANIZATIONCODE                   	 --交易对手组织机构代码
                ,COUNTRYCODE                        	 --交易对手国家代码
                ,INDUSTRYID                         	 --交易对手行业代码
                ,CLIENTCATEGORY												 --客户类型
    )
    WITH TMP_SUPPORG AS (
								SELECT T1.ORGID AS ORGID
										   ,CASE WHEN T3.ORGLEVEL > 2 AND T3.SORTNO LIKE '%610' THEN T4.SORTNO WHEN T1.ORGID = '01370000' THEN '1100000' ELSE T3.SORTNO END AS SORTNO  --仅金融市场部需要提到分行层级，总行清算中心强制分到总行金融同业管理部
								  FROM RWA.RWA_WP_SUPPTASKORG T1
						INNER JOIN RWA.RWA_WP_SUPPTASK T2
								    ON T1.SUPPTASKID = T2.SUPPTASKID
								   AND T2.ENABLEFLAG = '01'
						 LEFT JOIN RWA.ORG_INFO T3
								    ON T1.ORGID = T3.ORGID
						 LEFT JOIN RWA.ORG_INFO T4
	                	ON T3.BELONGORGID = T4.ORGID
								 WHERE T1.SUPPTMPLID = 'M-0050'
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
                ,ACCSERIALNO                        	 --交易账号
                ,BUSINESSTYPE													 --业务类型
                ,BELONGORGCODE                         --业务所属机构
                ,IACCURBAL														 --当前余额
                ,IACCRTDAT														 --起始日
                ,IACDLTDAT														 --到期日
                ,CLIENTNAME                         	 --交易对手名称
                ,ORGANIZATIONCODE                   	 --交易对手组织机构代码
                ,COUNTRYCODE                        	 --交易对手国家代码
                ,INDUSTRYID                         	 --交易对手行业代码
                ,CLIENTCATEGORY												 --客户类型
		FROM (
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,NVL(T7.ORGID,(SELECT ORGID FROM TMP_SUPPORG WHERE ROWNUM = 1))
                												                     AS ORGID										 --机构ID                     		按照补录任务分配情况，分配给金融市场部及其分部
                ,RANK() OVER(PARTITION BY T1.IACAC_NO ORDER BY LENGTH(NVL(T7.SORTNO,(SELECT SORTNO FROM TMP_SUPPORG WHERE ROWNUM = 1))) DESC)
                																						 AS RECORDNO								 --数据序号
                ,T1.IACAC_NO								                 AS ACCSERIALNO              --交易账号
                ,'02'																				 AS BUSINESSTYPE						 --业务类型												默认：02-存放
                ,T1.IACGACBR																 AS BELONGORGCODE            --业务所属机构
                ,ABS(T1.IACCURBAL)													 AS IACCURBAL								 --当前余额
                ,NVL(T4.IACCRTDAT,TO_CHAR(TO_DATE(NVL(T2.SDATE,T1.IACCRTDAT),'YYYYMMDD'),'YYYY-MM-DD'))
                																				 		 AS IACCRTDAT								 --起始日
                ,NVL(T4.IACDLTDAT,TO_CHAR(TO_DATE(NVL(T2.EDATE,T1.IACDLTDAT),'YYYYMMDD'),'YYYY-MM-DD'))
                																				 		 AS IACDLTDAT								 --到期日
                ,NVL(T4.CLIENTNAME,T1.IACAC_NAM)	           AS CLIENTNAME               --交易对手名称
                ,NVL(T4.ORGANIZATIONCODE,T8.CERTID)          AS ORGANIZATIONCODE         --交易对手组织机构代码
                ,NVL(T4.COUNTRYCODE,'CHN')                   AS COUNTRYCODE              --交易对手国家代码    						默认CHN-中国
                ,NVL(T4.INDUSTRYID,'J66')					           AS INDUSTRYID               --交易对手行业代码 							默认J66-货币金融服务
                ,NVL(T4.CLIENTCATEGORY,'0202')					     AS CLIENTCATEGORY           --客户类型					 							默认0202-中国商业银行

    FROM				RWA_DEV.CBS_IAC T1	             		 											--通用分户帐
    LEFT JOIN		RWA_DEV.CBS_BBR T2
    ON					T1.IACAC_NO = T2.ACCNO
		AND					T2.TYPE IN ('1','0')																			--同业类型为 0-存放同业、1-存放央行
    AND 				T2.STATUS = '00'																					--账户状态为 00-正常
    AND 				T2.DATANO = p_data_dt_str
		LEFT JOIN		RWA.ORG_INFO T3																						--机构信息表
	  ON					T1.IACGACBR = T3.ORGID
	  LEFT JOIN   (SELECT ACCSERIALNO
	  									 ,CLIENTNAME
	  									 ,ORGANIZATIONCODE
	  									 ,COUNTRYCODE
	  									 ,INDUSTRYID
	  									 ,IACCRTDAT
	  									 ,IACDLTDAT
	  									 ,CLIENTCATEGORY
	  							 FROM RWA.RWA_WS_INNERBANK
	  							WHERE BUSINESSTYPE = '02'
    								AND DATADATE = (SELECT MAX(DATADATE) FROM RWA.RWA_WS_INNERBANK WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD') AND BUSINESSTYPE = '02')
    						) T4																											--取最近一期补录数据铺底
    ON          T1.IACAC_NO = T4.ACCSERIALNO
    LEFT	JOIN  TMP_SUPPORG T7
    ON          T3.SORTNO LIKE T7.SORTNO || '%'
    LEFT	JOIN	TMP_BL_CUST T8
    ON					T1.IACAC_NAM = T8.CUSTOMERNAME
		WHERE 			T1.IACAC_STS = '2'																				--账务状态为：2-正常
    AND					T1.IACCURBAL <> 0																					--账户余额不等于0
    AND					T1.IACITMNO LIKE '1011%'																	--1003-存放中央银行款项(因为参与主体都是央行，统一虚拟);1011-存放同业
    AND NOT EXISTS (SELECT 1 FROM RWA_DEV.NCM_BUSINESS_DUEBILL CBD WHERE 'BBR' || T2.ACCNO || '_' || T2.ACCNOSEQ = CBD.THIRDPARTYACCOUNTS AND CBD.DATANO = p_data_dt_str)
		AND					T1.DATANO = p_data_dt_str
    )
    WHERE RECORDNO = 1
    ORDER BY		ACCSERIALNO
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA',tabname => 'RWA_WSIB_INNERBANK',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA.RWA_WSIB_INNERBANK WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --SELECT COUNT(1) INTO v_count FROM RWA.RWA_WSIB_INNERBANK WHERE BUSINESSTYPE = '02' AND DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA.RWA_WSIB_INNERBANK表当前插入的核心系统-存放同业铺底数据记录为: ' || v_count || ' 条');



    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '拆借与存放同业补录数据铺底('|| v_pro_name ||')处理失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_TY_WSIB;
/

