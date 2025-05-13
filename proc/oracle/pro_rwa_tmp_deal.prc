CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_TMP_DEAL(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_TMP_DEAL
    实现功能:RWA系统-临时表数据准备
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2017-07-31
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.CBS_LNU|贴现卡片帐
    目标表1 :RWA_DEV.TMP_CURRENCY_CHANGE|币种转换临时表
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_TMP_DEAL';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1. 清除目标表中的原有记录
    --1.1 清空币种转换临时表当期数据
    DELETE FROM RWA_DEV.TMP_CURRENCY_CHANGE WHERE DATANO = p_data_dt_str;

    COMMIT;

    --2.将满足条件的数据从源表插入到目标表中
    --2.1 币种转换临时表
    INSERT INTO RWA_DEV.TMP_CURRENCY_CHANGE(
                DATANO                               	 --数据期次
                ,CURRENCYCODE                          --币种
                ,MIDDLEPRICE                           --中间价
    )
		SELECT
								p_data_dt_str                            		 AS DATANO           		 --期次
                ,T1.CCY																			 AS CURRENCYCODE         --币种
                ,MAX(T1.JZRAT)                         			 AS MIDDLEPRICE          --基准价作为中间价
		FROM 				RWA_DEV.NNS_JT_EXRATE T1	             		 	--汇率信息表
		WHERE 			T1.DATANO = p_data_dt_str
		GROUP BY		T1.CCY
		;

    COMMIT;

    --整理表信息
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'TMP_CURRENCY_CHANGE',cascade => true);

    --2.2 更新集团客户信息临时表
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.TMP_GROUP_MEMBERS';

    INSERT INTO RWA_DEV.TMP_GROUP_MEMBERS(GROUPID,MEMBERID)
    SELECT T2.GROUPID AS GROUPID,  --集团编号
           T2.MEMBERCUSTOMERID AS MEMBERID  --集团成员编号
		  FROM RWA_DEV.NCM_GROUP_INFO T1  --集团客户基本信息表
		 INNER JOIN (SELECT TT.GROUPID,TT.MEMBERCUSTOMERID,MAX(TT.VERSIONSEQ) AS VERSIONSEQ  --取到序号最新的一笔
                 FROM RWA_DEV.NCM_GROUP_FAMILY_MEMBER TT
                 WHERE DATANO = p_data_dt_str
                 GROUP BY TT.GROUPID,TT.MEMBERCUSTOMERID ) T2  --集团家谱成员
		    ON T1.GROUPID = T2.GROUPID
		   AND T1.REFVERSIONSEQ = T2.VERSIONSEQ
		 WHERE T1.STATUS = '1'
		   AND T1.FAMILYMAPSTATUS = '2'
		   AND T1.DATANO = p_data_dt_str
		UNION
		SELECT T2.GROUPID AS GROUPID, --集团编号
           T2.GROUPID AS MEMBERID --集团编号
		  FROM RWA_DEV.NCM_GROUP_INFO T1
		 INNER JOIN RWA_DEV.NCM_GROUP_FAMILY_MEMBER T2
		    ON T1.GROUPID = T2.GROUPID
		   AND T1.REFVERSIONSEQ = T2.VERSIONSEQ
		   AND T2.DATANO = p_data_dt_str
		 WHERE T1.STATUS = '1'
		   AND T1.FAMILYMAPSTATUS = '2'
		   AND T1.DATANO = p_data_dt_str
		UNION
		SELECT T2.GROUPID AS GROUPID, 
           T2.MEMBERCUSTOMERID AS MEMBERID
		  FROM RWA_DEV.NCM_GROUP_INFO T1
		 INNER JOIN (SELECT TT.GROUPID,TT.MEMBERCUSTOMERID,MAX(TT.VERSIONSEQ) AS VERSIONSEQ  --取到序号最新的一笔
                 FROM RWA_DEV.NCM_GROUP_FAMILY_MEMBER TT
                 WHERE DATANO = p_data_dt_str
                 GROUP BY TT.GROUPID,TT.MEMBERCUSTOMERID ) T2
		    ON T1.GROUPID = T2.GROUPID
		   AND T1.CURRENTVERSIONSEQ = T2.VERSIONSEQ
		 INNER JOIN RWA_DEV.NCM_GROUP_FAMILY_OPINION T3
		    ON T1.GROUPID = T3.GROUPID
		   AND T3.APPROVETYPE = '2'
		   AND T3.FAMILYSEQ = T1.CURRENTVERSIONSEQ
		   AND T3.DATANO = p_data_dt_str
		 WHERE T1.STATUS = '1'
		   AND T1.FAMILYMAPSTATUS IN ('0', '1')  --家谱版本状态
		   AND T1.DATANO = p_data_dt_str
		UNION
		SELECT T2.GROUPID AS GROUPID, 
           T2.GROUPID AS MEMBERID
		  FROM RWA_DEV.NCM_GROUP_INFO T1
		 INNER JOIN RWA_DEV.NCM_GROUP_FAMILY_MEMBER T2
		    ON T1.GROUPID = T2.GROUPID
		   AND T1.CURRENTVERSIONSEQ = T2.VERSIONSEQ
		   AND T2.DATANO = p_data_dt_str
		 INNER JOIN RWA_DEV.NCM_GROUP_FAMILY_OPINION T3
		    ON T1.GROUPID = T3.GROUPID
		   AND T3.APPROVETYPE = '2'
		   AND T3.FAMILYSEQ = T1.CURRENTVERSIONSEQ
		   AND T3.DATANO = p_data_dt_str
		 WHERE T1.STATUS = '1'
		   AND T1.FAMILYMAPSTATUS IN ('0', '1') --家谱版本状态
		   AND T1.DATANO = p_data_dt_str
		;

		COMMIT;

		--整理表信息
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'TMP_GROUP_MEMBERS',cascade => true);

    --------------------------------------------------------合同下保证金详细逻辑----------------------------------------------------
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TEMP_BAIL1';
    
    /*RWA二期，修改保证金逻辑，经庞小龙宋柯确认，信贷保证金都从核心保证金账户表获取*/
    --modify by yushuangjiang
    --核心保证金账户表可以跟借据表借据号关联，出账账号关联，合同号关联，优先顺序如下借据号，出账流水号，合同号
    --1.优先插入借据号可以关联的保证金信息
    INSERT INTO RWA_DEV.RWA_TEMP_BAIL1(
                CONTRACTNO
                ,BAILCURRENCY
                ,ISSUM
                ,BAILBALANCE
    )
    SELECT      T1.RELATIVESERIALNO2 AS  CONTRACTNO
                ,T2.BAILCURRENCY AS BAILCURRENCY
                ,'0'
                ,SUM(T2.BAILBALANCE) AS BAILBALANCE
    FROM RWA_DEV.NCM_BUSINESS_HISTORY T1
    INNER JOIN NCM_BAILACCOUNTINFO_TMP T2
    ON T1.Serialno=T2.RELATIVECONTRACTNO   --借据号关联
    AND T2.DATANO=p_data_dt_str
    AND T2.BAILBALANCE IS NOT NULL
    AND T2.BAILBALANCE>0
    WHERE T1.BALANCE>0
    AND T1.DATANO=p_data_dt_str
    AND T1.BUSINESSTYPE NOT IN ('1040101010','1040101020','1040101030','1040102010','1040102020','1040102040','1040105060','1040201010','1040201020','1040202010','105010','10303010'
                                ,'108010','10201080','10202091','11105010','11105020','11103030'
                                ,'10302020','10302030' --排除条件跟信贷借据一样，不然保证金会增多
                                )
    GROUP BY T1.RELATIVESERIALNO2,T2.BAILCURRENCY
    ;
    COMMIT; 
    
    --2.再插入出账流水号可以关联的保证金信息
    INSERT INTO RWA_DEV.RWA_TEMP_BAIL1(
                CONTRACTNO
                ,BAILCURRENCY
                ,ISSUM
                ,BAILBALANCE
    )
    SELECT      T1.RELATIVESERIALNO2 AS  CONTRACTNO
                ,T2.BAILCURRENCY AS BAILCURRENCY
                ,'0'
                ,SUM(T2.BAILBALANCE) AS BAILBALANCE
    FROM RWA_DEV.NCM_BUSINESS_HISTORY T1
    INNER JOIN NCM_BAILACCOUNTINFO_TMP T2
    ON T1.RELATIVESERIALNO1=T2.RELATIVECONTRACTNO   --出账流水号关联
    AND T2.DATANO=p_data_dt_str
    AND T2.BAILBALANCE IS NOT NULL
    AND T2.BAILBALANCE>0
    WHERE T1.BALANCE>0
    AND T1.DATANO=p_data_dt_str
    AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_TEMP_BAIL1 T3 WHERE T1.RELATIVESERIALNO2=T3.CONTRACTNO) --排除合同号已经在临时表中存在的记录，说明借据号已经关联上
    AND T1.BUSINESSTYPE NOT IN ('1040101010','1040101020','1040101030','1040102010','1040102020','1040102040','1040105060','1040201010','1040201020','1040202010','105010','10303010'
                                ,'108010','10201080','10202091','11105010','11105020','11103030'
                                ,'10302020','10302030' --排除条件跟信贷借据一样，不然保证金会增多
                                )
    GROUP BY T1.RELATIVESERIALNO2,T2.BAILCURRENCY
    ;
    COMMIT; 
    
    --3.最后插入合同号可以关联的保证金信息
    INSERT INTO RWA_DEV.RWA_TEMP_BAIL1(
                CONTRACTNO
                ,BAILCURRENCY
                ,ISSUM
                ,BAILBALANCE
    )
    SELECT      T1.RELATIVESERIALNO2 AS  CONTRACTNO
                ,T2.BAILCURRENCY AS BAILCURRENCY
                ,'0'
                ,SUM(T2.BAILBALANCE) AS BAILBALANCE
    FROM RWA_DEV.NCM_BUSINESS_HISTORY T1
    INNER JOIN NCM_BAILACCOUNTINFO_TMP T2
    ON T1.RELATIVESERIALNO2=T2.RELATIVECONTRACTNO   --合同号关联
    AND T2.DATANO=p_data_dt_str
    AND T2.BAILBALANCE IS NOT NULL  --保证金金额为空的排除掉
    AND T2.BAILBALANCE>0
    WHERE T1.BALANCE>0
    AND T1.DATANO=p_data_dt_str
    AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_TEMP_BAIL1 T3 WHERE T1.RELATIVESERIALNO2=T3.CONTRACTNO) --排除合同号已经在临时表中存在的记录，说明前面已经关联上
    AND T1.BUSINESSTYPE NOT IN ('1040101010','1040101020','1040101030','1040102010','1040102020','1040102040','1040105060','1040201010','1040201020','1040202010','105010','10303010'
                                ,'108010','10201080','10202091','11105010','11105020','11103030'
                                ,'10302020','10302030' --排除条件跟信贷借据一样，不然保证金会增多
                                )
    GROUP BY T1.RELATIVESERIALNO2,T2.BAILCURRENCY
    ;
    COMMIT; 
    
    /*--插入核心返回的保证金数据 - 额度层，注释段是一期逻辑，错误
    INSERT INTO RWA_DEV.RWA_TEMP_BAIL1(
                CONTRACTNO
                ,BAILCURRENCY
                ,ISSUM
                ,BAILBALANCE
    )
    SELECT
                T3.SERIALNO
                ,NVL(T1.BAILCURRENCY,T3.BUSINESSCURRENCY)
                ,'0'
                ,SUM(nvl(T1.BailBalance,0))
    FROM RWA_DEV.NCM_BAILACCOUNTINFO_TMP T1
    INNER JOIN RWA_DEV.NCM_CL_OCCUPY T2 --额度表
    ON T1.RELATIVECONTRACTNO=T2.OBJECTNO
    AND T2.OBJECTTYPE='BusinessContract'
    AND T2.DATANO=p_data_dt_str
    INNER JOIN RWA_DEV.NCM_Business_Contract T3
    ON T2.RELATIVESERIALNO=T3.SERIALNO
    AND T3.DATANO=p_data_dt_str 
    AND T3.businesstype not in('11103030', '11103035', '11103036') --不取 快E贷,快I贷,微粒贷
    AND NVL(T3.LINETYPE,'0010')<>'0040'
    INNER JOIN (SELECT DISTINCT relativeserialno2
                FROM RWA_DEV.NCM_BUSINESS_HISTORY WHERE DATANO=p_data_dt_str AND BALANCE>0 AND INPUTDATE=p_data_dt_str
                ) T4
    ON T1.RELATIVECONTRACTNO=T4.relativeserialno2
    WHERE T1.DATANO=p_data_dt_str
    GROUP BY T3.SERIALNO,NVL(T1.BAILCURRENCY,T3.BUSINESSCURRENCY),'0'
    ;
    COMMIT;

    --插入核心返回的保证金数据 - 合同层
    INSERT INTO RWA_DEV.RWA_TEMP_BAIL1(
                CONTRACTNO
                ,BAILCURRENCY
                ,ISSUM
                ,BAILBALANCE
    )
    SELECT
                T2.SERIALNO
                ,NVL(T1.BAILCURRENCY,T2.BUSINESSCURRENCY)
                ,'0'
                ,SUM(nvl(T1.BailBalance,0))
    FROM RWA_DEV.NCM_BAILACCOUNTINFO_TMP T1
    INNER JOIN RWA_DEV.NCM_Business_Contract T2
    ON T1.RELATIVECONTRACTNO=T2.SERIALNO
    AND T2.DATANO=p_data_dt_str
    AND T2.businesstype not in('11103030', '11103035', '11103036') --不取 快E贷,快I贷,微粒贷
    AND NVL(T2.LINETYPE,'0010')<>'0040'
    INNER JOIN (SELECT DISTINCT relativeserialno2
                FROM RWA_DEV.NCM_BUSINESS_HISTORY WHERE DATANO=p_data_dt_str AND BALANCE>0 AND INPUTDATE=p_data_dt_str
                ) T4
    ON T2.SERIALNO=T4.relativeserialno2
    WHERE T1.DATANO=p_data_dt_str
    GROUP BY T2.SERIALNO,NVL(T1.BAILCURRENCY,T2.BUSINESSCURRENCY),'0'
    ;
    COMMIT;

    --插入核心返回的保证金数据 - 出账层
    INSERT INTO RWA_DEV.RWA_TEMP_BAIL1(
                CONTRACTNO
                ,BAILCURRENCY
                ,ISSUM
                ,BAILBALANCE
    )
    SELECT
                T2.SERIALNO
                ,NVL(T1.BAILCURRENCY,T2.BUSINESSCURRENCY)
                ,'0'
                ,SUM(nvl(T1.BailBalance,0))
     FROM RWA_DEV.NCM_BAILACCOUNTINFO_TMP T1
     INNER JOIN (SELECT DISTINCT relativeserialno2,relativeserialno1
                 FROM RWA_DEV.NCM_BUSINESS_HISTORY WHERE DATANO=p_data_dt_str AND BALANCE>0 AND INPUTDATE=p_data_dt_str
                 ) T4
     ON T1.RELATIVECONTRACTNO=T4.relativeserialno1
     INNER JOIN RWA_DEV.NCM_Business_Contract T2
     ON T4.relativeserialno2=T2.SERIALNO
     AND T2.DATANO=p_data_dt_str
     AND T2.businesstype not in('11103030', '11103035', '11103036') --不取 快E贷,快I贷,微粒贷
     AND NVL(T2.LINETYPE,'0010')<>'0040'
     WHERE T1.DATANO=p_data_dt_str
     AND EXISTS(SELECT 1 FROM RWA_DEV.NCM_BUSINESS_PUTOUT T5
                 WHERE T1.RELATIVECONTRACTNO=T5.SERIALNO AND T2.SERIALNO=T5.CONTRACTSERIALNO
                 AND T5.DATANO=p_data_dt_str)
     GROUP BY T2.SERIALNO,NVL(T1.BAILCURRENCY,T2.BUSINESSCURRENCY),'0'
     ;
     COMMIT;

     --对核心取到的保证金按照合同号进行汇总(因为有可能是多层都记了保证金的，所以要汇总)
     INSERT INTO RWA_DEV.RWA_TEMP_BAIL1(
                CONTRACTNO
                ,BAILCURRENCY
                ,ISSUM
                ,BAILBALANCE
     )
     SELECT
                T1.CONTRACTNO
                ,T1.BAILCURRENCY
                ,'1'
                ,SUM(T1.BAILBALANCE)
     FROM RWA_DEV.RWA_TEMP_BAIL1 T1
     GROUP BY T1.CONTRACTNO,T1.BAILCURRENCY,'1'
     ;
     COMMIT;*/

     --整理表信息
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_TEMP_BAIL1',cascade => true);

     ---------------------------------------BP表，BC表，核心TEMP表，取保证金最大的值，作为合同最后的保证金
     /*--1.清除目标表中的原有记录
     EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TEMP_BAIL2';

     --插入存在BP表上的有效保证金
     INSERT INTO RWA_DEV.RWA_TEMP_BAIL2(
                 CONTRACTNO
                 ,BAILCURRENCY
                 ,ISMAX
                 ,BAILBALANCE
     )
     SELECT
                 T2.CONTRACTSERIALNO
                 ,NVL(T2.BAILCURRENCY,T1.BUSINESSCURRENCY)
                 ,'0'
                 ,SUM(T2.BAILSUM)
     FROM RWA_DEV.NCM_BUSINESS_HISTORY T1
     INNER JOIN RWA_DEV.NCM_BUSINESS_PUTOUT T2
     ON T1.RELATIVESERIALNO1=T2.SERIALNO
     AND T2.DATANO=p_data_dt_str
     AND T2.BAILSUM>0
     WHERE T1.DATANO=p_data_dt_str AND T1.BALANCE>0
     GROUP BY T2.CONTRACTSERIALNO,NVL(T2.BAILCURRENCY,T1.BUSINESSCURRENCY),'0'
     ;
     COMMIT;

     --插入存在BC表上的有效保证金
     INSERT INTO RWA_DEV.RWA_TEMP_BAIL2(
                 CONTRACTNO
                 ,BAILCURRENCY
                 ,ISMAX
                 ,BAILBALANCE
     )
     SELECT
                 T1.SERIALNO
                 ,NVL(T1.BAILCURRENCY,T1.BUSINESSCURRENCY)
                 ,'0'
                 ,T1.BAILSUM
     FROM RWA_DEV.NCM_BUSINESS_CONTRACT T1
     INNER JOIN (SELECT DISTINCT RELATIVESERIALNO2 FROM  RWA_DEV.NCM_BUSINESS_HISTORY
                 WHERE DATANO=p_data_dt_str
                 AND BALANCE>0) T2
     ON T1.SERIALNO=T2.RELATIVESERIALNO2
     WHERE T1.DATANO=p_data_dt_str AND T1.BAILSUM>0
     AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_TEMP_BAIL2 T3 WHERE T1.SERIALNO=T3.CONTRACTNO)
     ;
     COMMIT;

     --插入核心有效的保证金数据
     INSERT INTO RWA_DEV.RWA_TEMP_BAIL2(
                 CONTRACTNO
                 ,BAILCURRENCY
                 ,ISMAX
                 ,BAILBALANCE
     )
     SELECT
                 T1.CONTRACTNO
                 ,T1.BAILCURRENCY
                 ,'0'
                 ,T1.BAILBALANCE
     FROM RWA_DEV.RWA_TEMP_BAIL1 T1
     WHERE T1.BAILBALANCE>0 AND ISSUM='1'  --插入核心汇总过后并且余额大于0
     ;
     COMMIT;

     --这三段过来的保证金，取最大值，作为最终结果
     INSERT INTO RWA_DEV.RWA_TEMP_BAIL2(
                 CONTRACTNO
                 ,BAILCURRENCY
                 ,ISMAX
                 ,BAILBALANCE
     )
     SELECT
                 T1.CONTRACTNO
                 ,T1.BAILCURRENCY
                 ,'1'
                 ,MAX(T1.BAILBALANCE)
     FROM RWA_DEV.RWA_TEMP_BAIL2 T1
     WHERE T1.BAILBALANCE>0
     GROUP BY T1.CONTRACTNO,T1.BAILCURRENCY,'1'
     ;
     COMMIT;

    --整理表信息
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_TEMP_BAIL2',cascade => true);
*/
    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_TEMP_BAIL1;


    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '临时表数据准备('|| v_pro_name ||')处理失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_TMP_DEAL;
/

