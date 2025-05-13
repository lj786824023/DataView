CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_LC_WSIB(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_LC_WSIB
    实现功能:理财系统-理财-补录铺底(从数据源理财系统将业务相关信息全量导入RWA理财补录铺底表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-06-06
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.ZGS_INVESTASSETDETAIL|交易明细表
    源  表2 :RWA_DEV.ZGS_FINANCING_INFO|产品信息表
    源  表3 :RWA_DEV.ZGS_ATBOND|债券信息表
    源  表4 :RWA_DEV.ZGS_ATINTRUST_PLAN|资产管理计划表
    源  表5 :RWA.RWA_WS_FCII_BOND|债券理财投资补录表
    源  表6 :RWA.RWA_WS_FCII_PLAN|资管计划理财投资补录表
    源  表7 :RWA.RWA_WP_SUPPTASKORG|补录任务机构分发配置表
    源  表8 :RWA.RWA_WP_SUPPTASK|补录任务发布表
    目标表1 :RWA.RWA_WSIB_FCII_BOND|债券理财投资补录铺底表
    目标表2 :RWA.RWA_WSIB_FCII_PLAN|资管计划理财投资补录铺底表
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_LC_WSIB';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;
  v_count1 INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    --清空债券理财投资铺底表
    --EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA.RWA_WSIB_FCII_BOND';
    DELETE FROM RWA.RWA_WSIB_FCII_BOND WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    --清空资管计划理财投资铺底表
    --EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA.RWA_WSIB_FCII_PLAN';
    DELETE FROM RWA.RWA_WSIB_FCII_PLAN WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    COMMIT;

    --2.将满足条件的数据从源表插入到目标表中
    --2.1 理财系统-债券理财投资业务
    INSERT INTO RWA.RWA_WSIB_FCII_BOND(
                DATADATE                               --数据日期
                ,ORGID                              	 --机构ID
                ,C_BOND_CODE       	       	 				 	 --债券内码
                ,C_BOND_ID         	       	 				 	 --债券代码
                ,C_BOND_NAME       	       	 				 	 --债券名称
                ,C_BOND_TYPE													 --债券分类
                ,BELONGORGCODE		 		     	 				 	 --业务所属机构
                ,ISSUERNAME                	 				 	 --债券发行人名称
                ,ISSUERORGCODE             	 				 	 --债券发行人组织机构代码
                ,ISSUERCOUNTRYCODE         	 				 	 --债券发行人注册国家代码
                ,ISSUERINDUSTRYID          	 				 	 --债券发行人所属国标行业
                ,ISSUERMSMBFLAG            	 				 	 --债券发行人企业规模
                ,ISSUERRATINGORGCODE       	 				 	 --债券发行人外部评级机构
                ,ISSUERRATING              	 				 	 --债券发行人外部评级结果
                ,ISSUERRATINGDATE											 --债券发行人评级日期
                ,ISSUERRATINGORGCODE2      	 				 	 --债券发行人外部评级机构2
                ,ISSUERRATING2             	 				 	 --债券发行人外部评级结果2
                ,ISSUERRATINGDATE2										 --债券发行人评级日期2
                ,BONDRATINGORGCODE         	 				 	 --债券评级机构
                ,BONDRATINGTYPE       		 	 				 	 --债券评级期限类型
                ,BONDRATING                	 				 	 --债券评级等级
                ,BONDRATINGDATE												 --债券评级日期
                ,BONDRATINGORGCODE2         	 				 --债券评级机构2
                ,BONDRATINGTYPE2       		 	 				 	 --债券评级期限类型2
                ,BONDRATING2                	 				 --债券评级等级2
                ,BONDRATINGDATE2											 --债券评级日期2
                ,BONDLEVEL						     	 				 	 --债券级别
                ,RATETYPE                  	 				 	 --利率类型
                ,BONDREDATE                	 				 	 --债券重订价日
                ,BONDREFREQUENCY              			 	 --重订价频率
    )
    WITH TEMP_INVESTASSETDETAIL AS (
        SELECT DISTINCT
        			 T3.FLD_ASSET_CODE						 AS FLD_ASSET_CODE
          FROM RWA_DEV.ZGS_INVESTASSETDETAIL T3
    INNER JOIN RWA_DEV.ZGS_FINANCING_INFO T4
            ON T3.FLD_FINANC_CODE = T4.FLD_FINANC_CODE
           AND T4.FLD_INCOME_TYPE <> '3'																		--3：排除非保本类型
           AND T4.DATANO = p_data_dt_str
         WHERE T3.FLD_ASSET_TYPE = '2'																			-- 2：债券，24：资产管理计划
           AND T3.FLD_ASSET_STATUS = '1' 																		--1：状态正常
           AND T3.FLD_ASSET_FLAG = '1'   																		--1：理财产品
           AND T3.FLD_DATE  = p_data_dt_str																	--有效的理财产品其估值日期每日更新
           AND T3.DATANO = p_data_dt_str
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
								 WHERE T1.SUPPTMPLID = 'M-0090'
							ORDER BY T3.SORTNO
		)
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,(SELECT ORGID FROM TMP_SUPPORG WHERE ROWNUM = 1)
                						                     				 		 AS ORGID                  	 --机构ID                			按照补录任务分配情况，默认为总行资产管理部(01160000)
                ,T1.C_BOND_CODE                              AS C_BOND_CODE       	     --债券内码
                ,T1.C_BOND_ID 										 					 AS C_BOND_ID         	     --债券代码
                ,T1.C_BOND_NAME                           	 AS C_BOND_NAME       	     --债券名称
                ,T1.C_BOND_TYPE															 AS C_BOND_TYPE							 --债券分类
                ,NVL(T3.BELONGORGCODE,'9998')				 		 AS BELONGORGCODE		 		     --业务所属机构            		 默认：9998(总行)
                ,NVL(T3.ISSUERNAME,
                 CASE WHEN T1.C_BOND_TYPE IN ('01','17','19') THEN '中国中央政府'				 --国债：默认中国中央政府
                 			WHEN T1.C_BOND_TYPE = '03' THEN '国家开发银行股份有限公司'					 --政策性金融债：默认国家开发银行股份有限公司
                 			ELSE ''
                 END)						                 	 					 AS ISSUERNAME               --债券发行人名称
                ,NVL(T3.ISSUERORGCODE,
                 CASE WHEN T1.C_BOND_TYPE IN ('01','17','19') THEN 'ZGZYZFZZJGDM'				 --国债：默认ZGZYZFZZJGDM
                 			WHEN T1.C_BOND_TYPE = '03' THEN '00001845-4'											 --政策性金融债：默认00001845-4
                 			ELSE ''
                 END)                        		 						 AS ISSUERORGCODE            --债券发行人组织机构代码
                ,NVL(T3.ISSUERCOUNTRYCODE,'CHN')     				 AS ISSUERCOUNTRYCODE        --债券发行人注册国家代码			 默认CHN-中国
                ,NVL(T3.ISSUERINDUSTRYID,
                 CASE WHEN T1.C_BOND_TYPE IN ('01','17','19') THEN '999999'							 --国债：默认未知
                 			ELSE 'J66'
                 END)          		 													 AS ISSUERINDUSTRYID         --债券发行人所属国标行业			 默认J66-货币金融服务
                ,T3.ISSUERMSMBFLAG                   				 AS ISSUERMSMBFLAG           --债券发行人企业规模
                ,T3.ISSUERRATINGORGCODE                  		 AS ISSUERRATINGORGCODE      --债券发行人外部评级机构
                ,T3.ISSUERRATING     												 AS ISSUERRATING             --债券发行人外部评级结果
                ,T3.ISSUERRATINGDATE												 AS ISSUERRATINGDATE				 --债券发行人评级日期
                ,T3.ISSUERRATINGORGCODE2                 		 AS ISSUERRATINGORGCODE2     --债券发行人外部评级机构2
                ,T3.ISSUERRATING2    												 AS ISSUERRATING2            --债券发行人外部评级结果2
                ,T3.ISSUERRATINGDATE2												 AS ISSUERRATINGDATE2				 --债券发行人评级日期2
                ,T3.BONDRATINGORGCODE                      	 AS BONDRATINGORGCODE        --债券评级机构
                ,T3.BONDRATINGTYPE                         	 AS BONDRATINGTYPE       		 --债券评级期限类型
                ,T3.BONDRATING	                             AS BONDRATING               --债券评级等级
                ,T3.BONDRATINGDATE													 AS BONDRATINGDATE					 --债券评级日期
                ,T3.BONDRATINGORGCODE2                     	 AS BONDRATINGORGCODE2       --债券评级机构2
                ,T3.BONDRATINGTYPE2                        	 AS BONDRATINGTYPE2      		 --债券评级期限类型2
                ,T3.BONDRATING2	                             AS BONDRATING2              --债券评级等级2
                ,T3.BONDRATINGDATE2													 AS BONDRATINGDATE2					 --债券评级日期2
                ,T3.BONDLEVEL                                AS BONDLEVEL						     --债券级别
                ,NVL(T3.RATETYPE,CASE WHEN T1.C_INTEREST_TYPE = '1' THEN '01'						 --固定利率
                 ELSE '02'																															 --浮动利率
                 END)																				 AS RATETYPE                 --利率类型
                ,T3.BONDREDATE 															 AS BONDREDATE               --债券重订价日
                ,T3.BONDREFREQUENCY                          AS BONDREFREQUENCY          --重订价频率

    FROM				RWA_DEV.ZGS_ATBOND T1
		INNER JOIN	TEMP_INVESTASSETDETAIL T2
		ON 					T1.C_BOND_CODE = T2.FLD_ASSET_CODE
		LEFT	JOIN	(SELECT C_BOND_CODE
											 ,BELONGORGCODE
											 ,ISSUERNAME
											 ,ISSUERORGCODE
											 ,ISSUERCOUNTRYCODE
											 ,ISSUERINDUSTRYID
											 ,ISSUERMSMBFLAG
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
											 ,BONDLEVEL
											 ,RATETYPE
											 ,BONDREDATE
											 ,BONDREFREQUENCY
									 FROM RWA.RWA_WS_FCII_BOND
									WHERE DATADATE = (SELECT MAX(DATADATE) FROM RWA.RWA_WS_FCII_BOND WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
								) T3																								--取最近一期补录数据铺底
		ON					T1.C_BOND_CODE = T3.C_BOND_CODE
		WHERE 			T1.DATANO = p_data_dt_str														--债券信息表,获取有效的债券信息
		ORDER BY		T1.C_BOND_CODE
		;

    COMMIT;

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA.RWA_WSIB_FCII_BOND WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA.RWA_WSIB_FCII_BOND表当前插入的理财系统-债券理财投资铺底数据记录为: ' || v_count || ' 条');


    --2.2 理财系统-资管计划投资业务
    INSERT INTO RWA.RWA_WSIB_FCII_PLAN(
                DATADATE                               --数据日期
                ,ORGID                                 --机构ID
                ,TRUSTCODE         	                   --信托编号
                ,BELONGORGCODE		 		                 --业务所属机构
                ,FINANCERNAME                          --交易对手名称
                ,FINANCERORGCODE                       --交易对手组织机构代码
                ,FINANCERCOUNTRYCODE                   --交易对手注册国家代码
                ,FINANCERINDUSTRYID                    --交易对手所属行业代码
                ,FINANCERMSMBFLAG                      --交易对手企业规模
                ,GUARANTEETYPE                         --担保类型
                ,GUARANTEEBEGINDATE                    --担保起始日
                ,GUARANTEEENDDATE                      --担保到期日
                ,GUARANTORNAME                         --担保人名称
                ,GUARANTORORGCODE                      --担保人组织机构代码/身份证号
                ,GUARANTORCOUNTRYCODE           			 --担保人注册国家代码
                ,GUARANTEECURRENCY                     --担保币种
                ,GUARANTEEVALUE                        --担保价值

    )
    WITH TEMP_INVESTASSETDETAIL AS (
        SELECT 	T3.FLD_ASSET_CODE						AS FLD_ASSET_CODE
        			 ,T3.FLD_FINANC_CODE					AS FLD_FINANC_CODE
          FROM RWA_DEV.ZGS_INVESTASSETDETAIL T3
    INNER JOIN RWA_DEV.ZGS_FINANCING_INFO T4
            ON T3.FLD_FINANC_CODE = T4.FLD_FINANC_CODE
           AND T4.FLD_INCOME_TYPE <> '3'																		--3：排除非保本类型
           AND T4.DATANO = p_data_dt_str
         WHERE T3.FLD_ASSET_TYPE = '24'																			-- 2：债券，24：资产管理计划
           AND T3.FLD_ASSET_STATUS = '1' 																		--1：状态正常
           AND T3.FLD_ASSET_FLAG = '1'   																		--1：理财产品
           AND T3.FLD_DATE  = p_data_dt_str																	--有效的理财产品其估值日期每日更新
           AND T3.DATANO = p_data_dt_str
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
								 WHERE T1.SUPPTMPLID = 'M-0100'
							ORDER BY T3.SORTNO
		)
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,(SELECT ORGID FROM TMP_SUPPORG WHERE ROWNUM = 1)
                						                     				 		 AS ORGID                  	 --机构ID                					按照补录任务分配情况，默认为总行资产管理部(01160000)
                ,T2.FLD_FINANC_CODE || T1.C_PRD_CODE       	 AS TRUSTCODE         	     --信托编号
                ,NVL(T3.BELONGORGCODE,'9998')						 AS BELONGORGCODE		 		     --业务所属机构
                ,T3.FINANCERNAME														 AS FINANCERNAME             --交易对手名称
                ,T3.FINANCERORGCODE													 AS FINANCERORGCODE          --交易对手组织机构代码
                ,NVL(T3.FINANCERCOUNTRYCODE,'CHN')			 		 AS FINANCERCOUNTRYCODE      --交易对手注册国家代码        		默认CHN-中国
                ,NVL(T3.FINANCERINDUSTRYID,'J66')          	 AS FINANCERINDUSTRYID       --交易对手所属行业代码        		默认J66-货币金融服务
                ,T3.FINANCERMSMBFLAG                     		 AS FINANCERMSMBFLAG         --交易对手企业规模
                ,T3.GUARANTEETYPE                            AS GUARANTEETYPE            --担保类型
                ,T3.GUARANTEEBEGINDATE											 AS GUARANTEEBEGINDATE       --担保起始日
                ,T3.GUARANTEEENDDATE                         AS GUARANTEEENDDATE         --担保到期日
                ,T3.GUARANTORNAME                       		 AS GUARANTORNAME            --担保人名称
                ,T3.GUARANTORORGCODE 												 AS GUARANTORORGCODE         --担保人组织机构代码/身份证号
                ,NVL(T3.GUARANTORCOUNTRYCODE,'CHN')        	 AS GUARANTORCOUNTRYCODE     --担保人注册国家代码           	默认CHN-中国
                ,NVL(T3.GUARANTEECURRENCY,'CNY')             AS GUARANTEECURRENCY        --担保币种                    		默认CNY-人民币
                ,T3.GUARANTEEVALUE                           AS GUARANTEEVALUE           --担保价值

    FROM				RWA_DEV.ZGS_ATINTRUST_PLAN T1
		INNER JOIN	TEMP_INVESTASSETDETAIL T2
		ON 					T1.C_PRD_CODE = T2.FLD_ASSET_CODE
		LEFT	JOIN	(SELECT TRUSTCODE
											 ,BELONGORGCODE
											 ,FINANCERNAME
											 ,FINANCERORGCODE
											 ,FINANCERCOUNTRYCODE
											 ,FINANCERINDUSTRYID
											 ,FINANCERMSMBFLAG
											 ,GUARANTEETYPE
											 ,GUARANTEEBEGINDATE
											 ,GUARANTEEENDDATE
											 ,GUARANTORNAME
											 ,GUARANTORORGCODE
											 ,GUARANTORCOUNTRYCODE
											 ,GUARANTEECURRENCY
											 ,GUARANTEEVALUE
									 FROM RWA.RWA_WS_FCII_PLAN
									WHERE DATADATE = (SELECT MAX(DATADATE) FROM RWA.RWA_WS_FCII_PLAN WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
								) T3																								--取最近一期补录数据铺底
		ON					T2.FLD_FINANC_CODE || T1.C_PRD_CODE = T3.TRUSTCODE
		WHERE 			T1.DATANO = p_data_dt_str														--债券信息表,获取有效的债券信息
		ORDER BY		T2.FLD_FINANC_CODE || T1.C_PRD_CODE
		;

    COMMIT;

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count1 FROM RWA.RWA_WSIB_FCII_PLAN WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA.RWA_WSIB_FCII_PLAN表当前插入的理财系统-资管计划投资铺底数据记录为: ' || v_count || ' 条');



    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || (v_count + v_count1);
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '理财投资业务补录数据铺底('|| v_pro_name ||')处理失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_LC_WSIB;
/

