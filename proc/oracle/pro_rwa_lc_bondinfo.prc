CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_LC_BONDINFO(
														p_data_dt_str  IN  VARCHAR2, --数据日期
                            p_po_rtncode   OUT VARCHAR2, --返回编号
                            p_po_rtnmsg    OUT VARCHAR2  --返回描述
)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_LC_BONDINFO
    实现功能:理财系统-债券理财投资-市场风险-债券信息(从数据源理财系统将业务相关信息全量导入RWA市场风险理财接口表债券信息表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :QHJIANG
    编写时间:2016-04-14
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.ZGS_ATBOND|债券信息表
    源  表2 :RWA.RWA_WS_FCII_BOND|债券理财投资补录表
    源  表3 :RWA_DEV.ZGS_INVESTASSETDETAI|资产详情表
    源  表4 :RWA_DEV.ZGS_FINANCING_INFO|产品信息表
    源  表5 :RWA.RWA_WP_DATASUPPLEMENT|补录任务明细信息表
    目标表1 :RWA_DEV.RWA_LC_BONDINFO|债券信息表
    辅助表  :无
    变更记录(修改人|修改时间|修改内容):
    */

  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_LC_BONDINFO';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN

    --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --清除目标表中的原有记录
    --DELETE FROM RWA_LC_BONDINFO WHERE DATADATE=TO_DATE(p_data_dt_str,'yyyyMMdd');
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_LC_BONDINFO';


    --DBMS_OUTPUT.PUT_LINE('开始：导入【债券信息表】' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    INSERT INTO RWA_DEV.RWA_LC_BONDINFO(
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
    WITH TEMP_INVESTASSETDETAIL AS (
        SELECT  DISTINCT
        				T3.FLD_ASSET_CODE						AS FLD_ASSET_CODE
          FROM RWA_DEV.ZGS_INVESTASSETDETAIL T3
    INNER JOIN RWA_DEV.ZGS_FINANCING_INFO T4
            ON T3.FLD_FINANC_CODE = T4.FLD_FINANC_CODE
           AND T4.FLD_INCOME_TYPE <> '3'																		--3：排除非保本类型
           AND T4.DATANO = p_data_dt_str
         WHERE T3.FLD_ASSET_TYPE = '2'																			-- 2：债券，24：资产管理计划
           AND T3.FLD_ASSET_STATUS = '1' 																		--1：状态正常
           AND T3.FLD_ASSET_FLAG = '1'   																		--1：理财产品
           AND T3.C_ACC_TYPE = 'D'																					--D：交易类，该部分数据作为市场风险
           AND T3.FLD_DATE = p_data_dt_str																	--有效的理财产品其估值日期每日更新
           AND T3.DATANO = p_data_dt_str
    )
    , TEMP_CUST_INFO AS (
    								SELECT CUSTOMERID
    											,CUSTOMERNAME
    											,CERTTYPE
    											,CERTID
    											,RWACUSTOMERTYPE
    											,ISSUPERVISESTANDARSMENT
    									FROM RWA_DEV.NCM_CUSTOMER_INFO
    								 WHERE ROWID IN (SELECT MAX(ROWID) FROM RWA_DEV.NCM_CUSTOMER_INFO WHERE DATANO = p_data_dt_str AND CERTTYPE IN ('Ent01','Ent02') GROUP BY CERTID)
    								 	 AND DATANO = p_data_dt_str
    )
    SELECT
        				TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,T1.C_BOND_CODE												     	 AS BONDID                   --债券ID
                ,T1.C_BOND_NAME                  				 		 AS BONDNAME                 --债券名称
                ,T1.C_BOND_TYPE                          		 AS BONDTYPE                 --债券类型
                ,T1.C_RISK_SCORE													   AS ERATING                  --外部评级          					 补录，通过期限、机构、等级转换为标普
                ,CASE WHEN T1.C_BOND_TYPE IN ('01','17','19') THEN 'ZGZYZF'							 --国债参与主体默认为中国中央政府()
        				 ELSE 'LC' || T1.C_ISSUER_IDENTIFICATION_TYPE || T1.C_ISSUER_IDENTIFICATION_NO
        				 END				                           			 AS ISSUERID                 --发行人ID          					 补录
                ,CASE WHEN T1.C_BOND_TYPE IN ('01','17','19') THEN '中国中央政府'				 --国债参与主体默认为中国中央政府()
        				 ELSE NVL(T1.C_RWA_PUBLISHNAME,T4.C_ORG_NAME)
        				 END															           AS ISSUERNAME               --发行人名称        					 补录
                ,T10.DITEMNO									               AS ISSUERTYPE               --发行人大类        					 规则映射
                ,T11.DITEMNO																 AS ISSUERSUBTYPE            --发行人小类        					  规则映射
                ,CASE WHEN NVL(T1.C_ISSUER_REGCOUNTRY_CODE,'CHN') = 'CHN' THEN '01'
                 ELSE '02'
                 END								                  			 AS ISSUERREGISTSTATE        --发行人注册国家    					 	默认：01
                ,CASE WHEN T1.C_ISSUER_ENTERPRISE_SIZE IN ('02','03') THEN '1'
                 ELSE '0'
                 END
                																		         AS ISSUERSMBFLAG            --发行人小微企业标识					 默认：否(0)
                ,CASE WHEN T1.C_RELEASE_PURPOSE = '0' THEN '01'
                 ELSE '02'
                 END													               AS BONDISSUEINTENT          --债券发行目的      					 默认：其他(02)
                ,'0'						                             AS REABSFLAG                --再资产证券化标识  					 	默认：否(0)
                ,CASE WHEN REPLACE(T1.C_ISSUER_IDENTIFICATION_NO,'-','') = '202869177' THEN '1'
                 ELSE '0'
                 END																				 AS ORIGINATORFLAG   				 --是否发起机构      					 1. 发行人名称＝重庆银行(202869177)，则为是： 2. 否则为否
                ,T1.D_VALUE_DATE                             AS STARTDATE                --起始日期          					 补录
                ,T1.D_END_DATE	                             AS DUEDATE                  --到期日期
                ,CASE WHEN (TO_DATE(T1.D_END_DATE,'YYYYMMDD') - TO_DATE(T1.D_VALUE_DATE,'YYYYMMDD')) / 365<0
                      THEN 0
                      ELSE (TO_DATE(T1.D_END_DATE,'YYYYMMDD') - TO_DATE(T1.D_VALUE_DATE,'YYYYMMDD')) / 365
                END                                          AS ORIGINALMATURITY         --原始期限
                ,CASE WHEN (TO_DATE(T1.D_END_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                      THEN 0
                      ELSE (TO_DATE(T1.D_END_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                END                                          AS RESIDUALM                --剩余期限
                ,CASE WHEN T1.C_INTEREST_TYPE = '1' THEN '01'														 --固定利率
                 ELSE '02'
                 END							                    		 	 AS RATETYPE                 --利率类型
                ,T1.F_BOND_RATE                              AS EXECUTIONRATE            --执行利率
                ,CASE WHEN T1.C_INTEREST_TYPE = '1' OR T1.C_REPRICING_DATE < p_data_dt_str THEN T1.D_END_DATE
                 ELSE T1.C_REPRICING_DATE
                 END                                   		 	 AS NEXTREPRICEDATE          --下次重定价日      					1. 若利率类型＝固定，则下次重定价日＝到期日期；2. 否则取系统字段 补录
                /*,CASE WHEN T2.RATETYPE = '01' THEN NULL
                ELSE CASE WHEN REPLACE(T2.BONDREDATE,'-','') < p_data_dt_str THEN 12
                		 ELSE ROUND(TO_NUMBER(T2.BONDREFREQUENCY),0)
                		 END
                END								                           AS NEXTREPRICEM             --下次重定价期限(补录)   			1. 若利率类型＝固定，则默认为：NULL；2. 否则取系统字段，单位：月
                */
                ,CASE WHEN T1.C_INTEREST_TYPE = '1' THEN NULL
                 ELSE CASE WHEN (TO_DATE(CASE WHEN T1.C_REPRICING_DATE < p_data_dt_str THEN T1.D_END_DATE ELSE T1.C_REPRICING_DATE END,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0 THEN 0
                 			ELSE (TO_DATE(CASE WHEN T1.C_REPRICING_DATE < p_data_dt_str THEN T1.D_END_DATE ELSE T1.C_REPRICING_DATE END,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 			END
                 END																				 AS NEXTREPRICEM             --下次重定价期限							 1. 若利率类型＝固定，则默认为：NULL；2. 否则取下次重订价日-数据日期，单位：年
                ,NULL                                        AS MODIFIEDDURATION         --修正久期
                ,T1.F_PAR_VAL                                AS DENOMINATION             --面额
                ,T1.C_CURR_TYPE 	                           AS CURRENCY                 --币种

   	FROM				RWA_DEV.ZGS_ATBOND T1																							--债券信息表
   	INNER JOIN	TEMP_INVESTASSETDETAIL T2																					--交易明细表的最新记录
   	ON					T1.C_BOND_CODE = T2.FLD_ASSET_CODE
    LEFT JOIN		TEMP_CUST_INFO T3																									--
    ON					REPLACE(T1.C_ISSUER_IDENTIFICATION_NO,'-','') = REPLACE(T3.CERTID,'-','')
    LEFT JOIN		RWA_DEV.ZGS_ATTYORG T4
    ON					T1.C_PUBLISHER = T4.C_ORG_ID
    AND					T4.DATANO = p_data_dt_str
    LEFT JOIN		RWA_DEV.RWA_CD_CODE_MAPPING T10
	  ON					T1.C_ISSUERTYPE_1 = T10.SITEMNO
	  AND					T10.SCODENO = 'ClientCategory'
	  AND					T10.SYSID = 'LC'
	  LEFT JOIN		RWA_DEV.RWA_CD_CODE_MAPPING T11
	  ON					T1.C_ISSUERTYPE_2 = T11.SITEMNO
	  AND					T11.SCODENO = 'ClientCategory'
	  AND					T11.SYSID = 'LC'
    WHERE				T1.DATANO = p_data_dt_str
 		;

 		COMMIT;

 		dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_LC_BONDINFO',cascade => true);


    --DBMS_OUTPUT.PUT_LINE('结束：导入【债券信息表】' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_LC_BONDINFO;
    --Dbms_output.Put_line('RWA_DEV.RWA_LC_BONDINFO表当前插入的理财系统-债券理财投资(市场风险)-债券信息记录为: ' || v_count || ' 条');



    --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功' || '-' || v_count;

    --定义异常
    EXCEPTION WHEN OTHERS THEN
        --DBMS_OUTPUT.PUT_LINE('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '债券信息('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;

    RETURN;

END PRO_RWA_LC_BONDINFO;
/

