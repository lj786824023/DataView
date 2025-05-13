CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_WH_FESPOTPOSITION(
                                                         p_data_dt_str      IN     VARCHAR2,       --数据日期 yyyyMMdd
                                                         p_po_rtncode       OUT    VARCHAR2,       --返回编号 1 成功,0 失败
                                                         p_po_rtnmsg        OUT    VARCHAR2        --返回描述
                )
  /*
    存储过程名称:RWA_DEV.PRO_RWA_WH_FESPOTPOSITION
    实现功能:国结系统-市场风险-外汇现货头寸(财务总账加工)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-04-12
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.FNS_GL_BALANCE|总账余额表(加工表)
    源  表2 :RWA.CODE_LIBRARY|代码表
    目标表  :RWA_DEV.RWA_WH_FESPOTPOSITION|国结系统外汇现货头寸表
    变更记录(修改人|修改时间|修改内容):
     pxl  2019/09/09 调整外汇现货头寸加工逻辑
    
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_WH_FESPOTPOSITION';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_WH_FESPOTPOSITION';

    --2.1 国结系统-外汇现货头寸表
    INSERT INTO RWA_DEV.RWA_WH_FESPOTPOSITION(
                 DATADATE                              --数据日期
                ,POSITIONID                            --头寸ID
                ,ACCORGID                              --账务机构ID 默认总行 10000000
                ,INSTRUMENTSTYPE                       --金融工具类型 默认为 ‘0501’（外汇现货）
                ,ACCSUBJECTS                           --会计科目
                ,BOOKTYPE                              --账户类别 码值：BookType 默认为 01 银行账户
                ,STRUCTURALEXPOFLAG                    --是否结构性敞口 默认否 1 是 0 否
                ,CURRENCY                              --币种
                ,POSITIONTYPE                          --头寸属性 码值：PositionType 资产为 01 多头；负债为 02 空头 损益类：大于等于0 01 多头，小于0 02 空头
                ,POSITION                              --头寸
    )
    SELECT
                 TEMP.DATADATE                         AS DATADATE                --数据日期
                ,p_data_dt_str||lpad(rownum, 10, '0')  AS POSITIONID              --头寸ID
                ,TEMP.ACCORGID                         AS ACCORGID                --账务机构ID
                ,TEMP.INSTRUMENTSTYPE                  AS INSTRUMENTSTYPE         --金融工具类型
                ,TEMP.ACCSUBJECTS                      AS ACCSUBJECTS             --会计科目
                ,TEMP.BOOKTYPE                         AS BOOKTYPE                --账户类别
                ,CASE 
                     WHEN FLAG = '4' THEN '1'  --结构性敞口 为是
                     ELSE '0'
                 END              AS STRUCTURALEXPOFLAG      --是否结构性敞口
                ,TEMP.CURRENCY                         AS CURRENCY                --币种
                ,TEMP.POSITIONTYPE                     AS POSITIONTYPE            --头寸属性
                ,ABS(
                  CASE WHEN FLAG='1'
                        THEN  TEMP.POSITION1
                             +NVL(DECODE(SIGN(TEMP.POSITION3001),1,TEMP.POSITION3001),0)
                             +NVL(DECODE(SIGN(TEMP.POSITION3002),1,TEMP.POSITION3002),0)
                             +NVL(DECODE(SIGN(TEMP.POSITION3003),1,TEMP.POSITION3003),0)
                             +NVL(DECODE(SIGN(TEMP.POSITION3004),1,TEMP.POSITION3004),0)
                        WHEN FLAG='2'
                        THEN  TEMP.POSITION1
                             +NVL(DECODE(SIGN(TEMP.POSITION3001),-1,TEMP.POSITION3001),0)
                             +NVL(DECODE(SIGN(TEMP.POSITION3002),-1,TEMP.POSITION3002),0)
                             +NVL(DECODE(SIGN(TEMP.POSITION3003),-1,TEMP.POSITION3003),0)
                             +NVL(DECODE(SIGN(TEMP.POSITION3004),-1,TEMP.POSITION3004),0)
                             +NVL(DECODE(SIGN(TEMP.POSITION_CD1-TEMP.POSITION_CD2),1,TEMP.POSITION_CD1-TEMP.POSITION_CD2),0)   --231401 委托存款 和 132101 委托贷款 的总和值必须大于0，小于0则为0
                        ELSE  TEMP.POSITION1 
                 END)         AS POSITION                --头寸
    FROM (
        --资产
        SELECT  TO_DATE(p_data_dt_str,'YYYYMMDD')   AS DATADATE           --数据日期
               ,'1'                                 AS FLAG               --数据类型标志
               ,'9998'                          AS ACCORGID           --账务机构ID 默认总行 9998
               ,'0501'                              AS INSTRUMENTSTYPE    --金融工具类型 默认为 ‘0501’（外汇现货）
               ,''                                  AS ACCSUBJECTS        --会计科目 默认为 空
               ,'01'                                AS BOOKTYPE           --账户类别 码值：BookType 默认为 01 银行账户
               ,'0'                                 AS STRUCTURALEXPOFLAG --是否结构性敞口 默认否 1 是 0 否
               ,CURRENCY_CODE                       AS CURRENCY           --币种
               ,'01'                                AS POSITIONTYPE       --头寸属性 码值：PositionType 资产为 01 多头；负债为 02 空头 损益类：大于等于0 01 多头，小于0 02 空头
               ,SUM(CASE WHEN    TE.SUBJECT_NO='1001' OR TE.SUBJECT_NO='1003' OR TE.SUBJECT_NO='1011' OR TE.SUBJECT_NO='1302'
                              OR TE.SUBJECT_NO='1101' OR TE.SUBJECT_NO='1111' OR TE.SUBJECT_NO='1132' OR TE.SUBJECT_NO='1301'
                              OR TE.SUBJECT_NO='1303' OR TE.SUBJECT_NO='1305' OR TE.SUBJECT_NO='1307' OR TE.SUBJECT_NO='1310'
                              OR TE.SUBJECT_NO='1503' OR TE.SUBJECT_NO='1222' OR TE.SUBJECT_NO='1501' OR TE.SUBJECT_NO='1511'
                              OR TE.SUBJECT_NO='1521' OR TE.SUBJECT_NO='1601' OR TE.SUBJECT_NO='1604' OR TE.SUBJECT_NO='1606'
                              OR TE.SUBJECT_NO='1701' OR TE.SUBJECT_NO='1811' OR TE.SUBJECT_NO='1124' OR TE.SUBJECT_NO='1221'
                              OR TE.SUBJECT_NO='1441'
                              OR TE.SUBJECT_NO='1801' OR TE.SUBJECT_NO='1802' OR TE.SUBJECT_NO='1901' OR TE.SUBJECT_NO='132101'
                              OR TE.SUBJECT_NO='132102' OR TE.SUBJECT_NO='132120' OR TE.SUBJECT_NO='1311'
                              OR TE.SUBJECT_NO='1231'
                         THEN BALANCE
                         WHEN    TE.SUBJECT_NO='123103'
                              OR TE.SUBJECT_NO='123101'
                              OR TE.SUBJECT_NO='123104'
                         THEN -ABS(BALANCE)
                         WHEN    TE.SUBJECT_NO='1304' OR TE.SUBJECT_NO='1504' OR TE.SUBJECT_NO='1502' OR TE.SUBJECT_NO='1512'
                              OR TE.SUBJECT_NO='1523' OR TE.SUBJECT_NO='160202' OR TE.SUBJECT_NO='160201'
                              OR TE.SUBJECT_NO='1603' OR TE.SUBJECT_NO='1702' OR TE.SUBJECT_NO='1703' OR TE.SUBJECT_NO='122109'
                              OR TE.SUBJECT_NO='1442' OR TE.SUBJECT_NO='1607'
                         THEN -ABS(BALANCE)
                         ELSE 0 END)                AS POSITION1     --头寸1
               ,SUM(CASE WHEN   TE.SUBJECT_NO='3001'
                         THEN BALANCE
                         ELSE 0 END)                AS POSITION3001  --头寸3001
               ,SUM(CASE WHEN   TE.SUBJECT_NO='3002'
                         THEN BALANCE
                         ELSE 0 END)                AS POSITION3002  --头寸3002
               ,SUM(CASE WHEN   TE.SUBJECT_NO='3003'
                         THEN BALANCE
                         ELSE 0 END)                AS POSITION3003  --头寸3003
               ,SUM(CASE WHEN   TE.SUBJECT_NO='3004'
                         THEN BALANCE
                         ELSE 0 END)                AS POSITION3004  --头寸3004
               ,0                                   AS POSITION_CD1  --头寸_委托存款
               ,0                                   AS POSITION_CD2  --头寸_委托贷款
        FROM (SELECT  TT.SUBJECT_NO
                     ,TT.CURRENCY_CODE
                     ,SUM(TT.BALANCE) AS BALANCE
              FROM  (SELECT  CASE WHEN   T1.SUBJECT_NO LIKE '1001%' OR T1.SUBJECT_NO LIKE '1003%' OR T1.SUBJECT_NO LIKE '1011%' OR T1.SUBJECT_NO LIKE '1302%'
                                      OR T1.SUBJECT_NO LIKE '1101%' OR T1.SUBJECT_NO LIKE '1111%' OR T1.SUBJECT_NO LIKE '1132%' OR T1.SUBJECT_NO LIKE '1301%'
                                      OR T1.SUBJECT_NO LIKE '1303%' OR T1.SUBJECT_NO LIKE '1305%' OR T1.SUBJECT_NO LIKE '1307%' OR T1.SUBJECT_NO LIKE '1310%'
                                      OR T1.SUBJECT_NO LIKE '1503%' OR T1.SUBJECT_NO LIKE '1222%' OR T1.SUBJECT_NO LIKE '1501%' OR T1.SUBJECT_NO LIKE '1511%'
                                      OR T1.SUBJECT_NO LIKE '1521%' OR T1.SUBJECT_NO LIKE '1601%' OR T1.SUBJECT_NO LIKE '1604%' OR T1.SUBJECT_NO LIKE '1606%'
                                      OR T1.SUBJECT_NO LIKE '1701%' OR T1.SUBJECT_NO LIKE '1811%' OR T1.SUBJECT_NO LIKE '1124%' OR T1.SUBJECT_NO LIKE '1221%'
                                      OR T1.SUBJECT_NO LIKE '1441%' OR T1.SUBJECT_NO LIKE '1442%' OR T1.SUBJECT_NO LIKE '1607%' OR T1.SUBJECT_NO LIKE '1311%'
                                      OR T1.SUBJECT_NO LIKE '1801%' OR T1.SUBJECT_NO LIKE '1802%' OR T1.SUBJECT_NO LIKE '1901%' OR (T1.SUBJECT_NO LIKE '1231%' AND T1.SUBJECT_NO NOT LIKE '123103%' AND  T1.SUBJECT_NO NOT LIKE '123101%' AND  T1.SUBJECT_NO NOT LIKE '123104%')
                                      OR T1.SUBJECT_NO LIKE '1304%' OR T1.SUBJECT_NO LIKE '1504%' OR T1.SUBJECT_NO LIKE '1502%' OR T1.SUBJECT_NO LIKE '1512%'
                                      OR T1.SUBJECT_NO LIKE '1523%' OR T1.SUBJECT_NO LIKE '1603%' OR T1.SUBJECT_NO LIKE '1702%' OR T1.SUBJECT_NO LIKE '1703%'
                                      OR T1.SUBJECT_NO LIKE '3001%' OR T1.SUBJECT_NO LIKE '3002%' OR T1.SUBJECT_NO LIKE '3003%' OR T1.SUBJECT_NO LIKE '3004%'
                                  THEN SUBSTR(T1.SUBJECT_NO,0,4)
                                  WHEN   T1.SUBJECT_NO LIKE '132101%' OR T1.SUBJECT_NO LIKE '132102%' OR T1.SUBJECT_NO LIKE '132120%'
                                      OR T1.SUBJECT_NO LIKE '123103%' OR T1.SUBJECT_NO LIKE '123101%' OR T1.SUBJECT_NO LIKE '123104%'
                                      OR T1.SUBJECT_NO LIKE '160201%' OR T1.SUBJECT_NO LIKE '160202%'
                                  THEN SUBSTR(T1.SUBJECT_NO,0,6)
                                  ELSE '不在范围' END AS SUBJECT_NO
                            ,T1.CURRENCY_CODE
                            ,SUM(CASE WHEN CL.ATTRIBUTE8='D-C' THEN T1.BALANCE_D - T1.BALANCE_C
                                      WHEN CL.ATTRIBUTE8='C-D' THEN T1.BALANCE_C - T1.BALANCE_D
                                      ELSE T1.BALANCE_D - T1.BALANCE_C END) AS BALANCE
                     FROM RWA_DEV.FNS_GL_BALANCE T1
                     LEFT JOIN RWA.CODE_LIBRARY CL
                     ON    CL.CODENO='NewSubject'
                     AND   T1.SUBJECT_NO=CL.ITEMNO
                     AND   CL.ISINUSE='1'
                     WHERE T1.CURRENCY_CODE IS NOT NULL
                     AND   T1.CURRENCY_CODE <> 'RMB'
                     AND   T1.DATANO = p_data_dt_str
                     GROUP BY T1.SUBJECT_NO, T1.CURRENCY_CODE) TT
              GROUP BY TT.SUBJECT_NO, TT.CURRENCY_CODE) TE
        GROUP BY CURRENCY_CODE
        --负债
        UNION ALL
        SELECT  TO_DATE(p_data_dt_str,'YYYYMMDD')      AS DATADATE           --数据日期
               ,'2'                                 AS FLAG               --数据类型标志
               ,'9998'                          AS ACCORGID           --账务机构ID 默认总行 9998
               ,'0501'                              AS INSTRUMENTSTYPE    --金融工具类型 默认为 ‘0501’（外汇现货）
               ,''                                  AS ACCSUBJECTS        --会计科目 默认为 空
               ,'01'                                AS BOOKTYPE           --账户类别 码值：BookType 默认为 01 银行账户
               ,'0'                                 AS STRUCTURALEXPOFLAG --是否结构性敞口 默认否 1 是 0 否
               ,CURRENCY_CODE                       AS CURRENCY           --币种
               ,'02'                                AS POSITIONTYPE       --头寸属性 码值：PositionType 资产为 01 多头；负债为 02 空头 损益类：大于等于0 01 多头，小于0 02 空头
               ,SUM(CASE WHEN    TE.SUBJECT_NO='2004' OR TE.SUBJECT_NO='2012' OR TE.SUBJECT_NO='2003' OR TE.SUBJECT_NO='2101'
                              OR TE.SUBJECT_NO='2111' OR TE.SUBJECT_NO='2002' OR TE.SUBJECT_NO='2011' OR TE.SUBJECT_NO='220402'
                              OR TE.SUBJECT_NO='231401' OR TE.SUBJECT_NO='132101' OR TE.SUBJECT_NO='2015' OR TE.SUBJECT_NO='2211'
                              OR TE.SUBJECT_NO='2221' OR TE.SUBJECT_NO='2231' OR TE.SUBJECT_NO='2801' OR TE.SUBJECT_NO='2502'
                              OR TE.SUBJECT_NO='2901' OR TE.SUBJECT_NO='2021' OR TE.SUBJECT_NO='220401' OR TE.SUBJECT_NO='2232'
                              OR TE.SUBJECT_NO='2241' OR TE.SUBJECT_NO='2313' OR TE.SUBJECT_NO='2701' OR TE.SUBJECT_NO='231420'
                              OR TE.SUBJECT_NO='2312' OR  TE.SUBJECT_NO='2240'
                         THEN BALANCE
                         ELSE 0 END)                AS POSITION1     --头寸1
               ,SUM(CASE WHEN   TE.SUBJECT_NO='3001'
                         THEN BALANCE
                         ELSE 0 END)                AS POSITION3001  --头寸3001
               ,SUM(CASE WHEN   TE.SUBJECT_NO='3002'
                         THEN BALANCE
                         ELSE 0 END)                AS POSITION3002  --头寸3002
               ,SUM(CASE WHEN   TE.SUBJECT_NO='3003'
                         THEN BALANCE
                         ELSE 0 END)                AS POSITION3003  --头寸3003
               ,SUM(CASE WHEN   TE.SUBJECT_NO='3004'
                         THEN BALANCE
                         ELSE 0 END)                AS POSITION3004  --头寸3004
               ,SUM(BALANCE_CD1)                    AS POSITION_CD1   --头寸_委托存款
               ,SUM(BALANCE_CD2)                    AS POSITION_CD2   --头寸_委托贷款
        FROM (SELECT  TT.SUBJECT_NO
                     ,TT.CURRENCY_CODE
                     ,SUM(TT.BALANCE) AS BALANCE
                     ,SUM(TT.BALANCE_CD1) AS BALANCE_CD1
                     ,SUM(TT.BALANCE_CD2) AS BALANCE_CD2
              FROM (SELECT  CASE WHEN   T1.SUBJECT_NO LIKE '2004%' OR T1.SUBJECT_NO LIKE '2012%' OR T1.SUBJECT_NO LIKE '2003%' OR T1.SUBJECT_NO LIKE '2101%'
                                     OR T1.SUBJECT_NO LIKE '2111%' OR T1.SUBJECT_NO LIKE '2002%' OR T1.SUBJECT_NO LIKE '2011%' OR T1.SUBJECT_NO LIKE '2015%'
                                     OR T1.SUBJECT_NO LIKE '2211%' OR T1.SUBJECT_NO LIKE '2221%' OR T1.SUBJECT_NO LIKE '2231%' OR T1.SUBJECT_NO LIKE '2801%'
                                     OR T1.SUBJECT_NO LIKE '2502%' OR T1.SUBJECT_NO LIKE '2901%' OR T1.SUBJECT_NO LIKE '2021%' OR T1.SUBJECT_NO LIKE '2232%'
                                     OR T1.SUBJECT_NO LIKE '2241%' OR T1.SUBJECT_NO LIKE '2313%' OR T1.SUBJECT_NO LIKE '2701%' OR T1.SUBJECT_NO LIKE '2312%'
                                     OR T1.SUBJECT_NO LIKE '3001%' OR T1.SUBJECT_NO LIKE '3002%' OR T1.SUBJECT_NO LIKE '3003%' OR T1.SUBJECT_NO LIKE '3004%'
                                     OR T1.SUBJECT_NO LIKE '2240%'
                                 THEN SUBSTR(T1.SUBJECT_NO,0,4)
                                 WHEN   T1.SUBJECT_NO LIKE '220402%' OR T1.SUBJECT_NO LIKE '231401%'
                                     OR T1.SUBJECT_NO LIKE '132101%' OR T1.SUBJECT_NO LIKE '220401%'
                                     OR T1.SUBJECT_NO LIKE '231420%'
                                 THEN SUBSTR(T1.SUBJECT_NO,0,6)
                                 ELSE '不在范围' END AS SUBJECT_NO
                           ,T1.CURRENCY_CODE
                           ,CASE WHEN T1.SUBJECT_NO LIKE '231401%' OR T1.SUBJECT_NO LIKE '132101%'
                                 THEN 0
                                 ELSE SUM(CASE WHEN CL.ATTRIBUTE8='D-C' THEN T1.BALANCE_D - T1.BALANCE_C
                                               WHEN CL.ATTRIBUTE8='C-D' THEN T1.BALANCE_C - T1.BALANCE_D
                                               ELSE T1.BALANCE_D - T1.BALANCE_C END) END AS BALANCE
                           ,CASE WHEN T1.SUBJECT_NO LIKE '231401%'
                                 THEN SUM(CASE WHEN CL.ATTRIBUTE8='D-C' THEN T1.BALANCE_D - T1.BALANCE_C
                                               WHEN CL.ATTRIBUTE8='C-D' THEN T1.BALANCE_C - T1.BALANCE_D
                                               ELSE T1.BALANCE_D - T1.BALANCE_C END)
                                 ELSE 0                                END AS BALANCE_CD1   --委托存款
                           ,CASE WHEN T1.SUBJECT_NO LIKE '132101%'
                                 THEN SUM(CASE WHEN CL.ATTRIBUTE8='D-C' THEN T1.BALANCE_D - T1.BALANCE_C
                                               WHEN CL.ATTRIBUTE8='C-D' THEN T1.BALANCE_C - T1.BALANCE_D
                                               ELSE T1.BALANCE_D - T1.BALANCE_C END)
                                 ELSE 0                                END AS BALANCE_CD2   --委托贷款
                    FROM RWA_DEV.FNS_GL_BALANCE T1
                    LEFT JOIN RWA.CODE_LIBRARY CL
                    ON    CL.CODENO='NewSubject'
                    AND   T1.SUBJECT_NO=CL.ITEMNO
                    AND   CL.ISINUSE='1'
                    WHERE T1.CURRENCY_CODE IS NOT NULL
                    AND   T1.CURRENCY_CODE <> 'RMB'
                    AND   T1.DATANO = p_data_dt_str
                    GROUP BY T1.SUBJECT_NO, T1.CURRENCY_CODE) TT
        GROUP BY TT.SUBJECT_NO, TT.CURRENCY_CODE) TE
        GROUP BY CURRENCY_CODE
        --损益
        UNION ALL
        SELECT  TO_DATE(p_data_dt_str,'YYYYMMDD')   AS DATADATE           --数据日期
               ,'3'                                 AS FLAG               --数据类型标志
               ,'9998'                          AS ACCORGID           --账务机构ID 默认总行 9998
               ,'0501'                              AS INSTRUMENTSTYPE    --金融工具类型 默认为 ‘0501’（外汇现货）
               ,TE.SUBJECT_NO                       AS ACCSUBJECTS        --会计科目 默认为 空
               ,'01'                                AS BOOKTYPE           --账户类别 码值：BookType 默认为 01 银行账户
               ,'0'                                 AS STRUCTURALEXPOFLAG --是否结构性敞口 默认否 1 是 0 否
               ,TE.CURRENCY_CODE                    AS CURRENCY           --币种
               ,CASE WHEN TE.BALANCE>=0
                     THEN '01'
                     ELSE '02' END                  AS POSITIONTYPE       --头寸属性 码值：PositionType 资产为 01 多头；负债为 02 空头 损益类：大于等于0 01 多头，小于0 02 空头
               ,TE.BALANCE                          AS POSITION1          --头寸1
               ,0                                   AS POSITION3001       --头寸3001
               ,0                                   AS POSITION3002       --头寸3002
               ,0                                   AS POSITION3003       --头寸3003
               ,0                                   AS POSITION3004       --头寸3004
               ,0                                   AS POSITION_CD1       --头寸_委托存款
               ,0                                   AS POSITION_CD2       --头寸_委托贷款
        FROM (
              --损益类科目特殊处理   
              SELECT T1.SUBJECT_NO,
                     T1.CURRENCY_CODE,
                     SUM(CASE
                           WHEN CL.ATTRIBUTE8 = 'D-C' THEN
                            T1.BALANCE_D_BEQ - T1.BALANCE_C_BEQ
                           WHEN CL.ATTRIBUTE8 = 'C-D' THEN
                            T1.BALANCE_C_BEQ - T1.BALANCE_D_BEQ
                           ELSE
                            T1.BALANCE_D_BEQ - T1.BALANCE_C_BEQ  --获取综合折人  币种保留本币  金额获取综合折人
                         END) AS BALANCE
                FROM RWA_DEV.FNS_GL_BALANCE T1
                LEFT JOIN RWA.CODE_LIBRARY CL
                  ON CL.CODENO = 'NewSubject'
                 AND T1.SUBJECT_NO = CL.ITEMNO
                 AND CL.ISINUSE = '1'
               WHERE T1.CURRENCY_CODE IS NOT NULL
                 AND T1.CURRENCY_CODE NOT IN ('RMB', 'CNY')
                 AND T1.DATANO = p_data_dt_str
                 AND T1.SUBJECT_NO IN ('60110200',
                                       '60110200',
                                       '60110200',
                                       '60110200',
                                       '60110200',
                                       '60110200',
                                       '60110200',
                                       '60110200',
                                       '60110200',
                                       '60110300',
                                       '60110300',
                                       '60110300',
                                       '60110301',
                                       '60110301',
                                       '60110310',
                                       '60110310',
                                       '60110500',
                                       '60110500',
                                       '60110500',
                                       '60110500',
                                       '60110500',
                                       '60110500',
                                       '60110500',
                                       '60110500',
                                       '60110600',
                                       '60110600',
                                       '60110600',
                                       '60110600',
                                       '60110600',
                                       '60110600',
                                       '60110801',
                                       '60110801',
                                       '60110801',
                                       '60110801',
                                       '60110801',
                                       '60110901',
                                       '60110901',
                                       '60110901',
                                       '60110901',
                                       '60110902',
                                       '60110902',
                                       '60110902',
                                       '60110902',
                                       '60110902',
                                       '60110902',
                                       '60110902',
                                       '60110902',
                                       '60110902',
                                       '60110902',
                                       '60110902',
                                       '60110902',
                                       '60110902',
                                       '60110903',
                                       '60110903',
                                       '60110903',
                                       '60110903',
                                       '60111200',
                                       '60111701',
                                       '60111701',
                                       '60111704',
                                       '60111801',
                                       '60112000',
                                       '60112000',
                                       '60112000',
                                       '60112300',
                                       '60113100',
                                       '60210100',
                                       '60210100',
                                       '60210100',
                                       '60210100',
                                       '60210100',
                                       '60210100',
                                       '60210100',
                                       '60210100',
                                       '60210100',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210500',
                                       '60210500',
                                       '60210500',
                                       '60210600',
                                       '60210600',
                                       '60210600',
                                       '60210600',
                                       '60210600',
                                       '60210600',
                                       '60210600',
                                       '60210600',
                                       '60210600',
                                       '60210600',
                                       '60210600',
                                       '60210600',
                                       '60210600',
                                       '60210600',
                                       '60210600',
                                       '60210600',
                                       '60210600',
                                       '60210700',
                                       '60210700',
                                       '60210700',
                                       '60210800',
                                       '60210800',
                                       '60210800',
                                       '60210800',
                                       '60211000',
                                       '60211000',
                                       '60211007',
                                       '60211010',
                                       '60211010',
                                       '60211302',
                                       '60211302',
                                       '60211303',
                                       '60211303',
                                       '60211303',
                                       '60211303',
                                       '60212000',
                                       '60212000',
                                       '60212800',
                                       '60212800',
                                       '60512000',
                                       '60512000',
                                       '60512000',
                                       '60512000',
                                       '60512000',
                                       '60512000',
                                       '60512000',
                                       '60512000',
                                       '60512000',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60612001',
                                       '60612002',
                                       '61110201',
                                       '61110201',
                                       '61110301',
                                       '61110301',
                                       '61110602',
                                       '64022000',
                                       '64110100',
                                       '64110100',
                                       '64110100',
                                       '64110100',
                                       '64110100',
                                       '64110300',
                                       '64110300',
                                       '64110300',
                                       '64110300',
                                       '64110300',
                                       '64110400',
                                       '64110400',
                                       '64110400',
                                       '64110500',
                                       '64110500',
                                       '64110500',
                                       '64110500',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110700',
                                       '64110700',
                                       '64110700',
                                       '64110700',
                                       '64110700',
                                       '64110700',
                                       '64110700',
                                       '64110700',
                                       '64110700',
                                       '64110700',
                                       '64110700',
                                       '64110700',
                                       '64110700',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64111100',
                                       '64111500',
                                       '64111500',
                                       '64111500',
                                       '64111500',
                                       '64111500',
                                       '64111500',
                                       '64111500',
                                       '64111500',
                                       '64111500',
                                       '64111500',
                                       '64112000',
                                       '64112000',
                                       '64210100',
                                       '64210100',
                                       '64210200',
                                       '64210200',
                                       '64210200',
                                       '64210200',
                                       '64210200',
                                       '64210200',
                                       '64210200',
                                       '64210200',
                                       '64210200',
                                       '64212000',
                                       '64212000',
                                       '64212000',
                                       '64212000',
                                       '64212000',
                                       '66020600',
                                       '66021000',
                                       '66021000',
                                       '66021400',
                                       '66021400',
                                       '66021400',
                                       '66021400',
                                       '66021800',
                                       '66022910',
                                       '66022910',
                                       '66024000',
                                       '67010401',
                                       '67010401',
                                       '67010401',
                                       '67112000')
               GROUP BY T1.SUBJECT_NO, T1.CURRENCY_CODE
             
        ) TE                     
                            
    ) TEMP
    WHERE CURRENCY <> 'CNY' --只考虑外币
    ;

    COMMIT;
    
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_WH_FESPOTPOSITION',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_WH_FESPOTPOSITION;
    
    p_po_rtncode := '1';
      p_po_rtnmsg  := '成功' || '-' || v_count;
        --定义异常
EXCEPTION
    WHEN OTHERS THEN
                 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
            ROLLBACK;
                p_po_rtncode := sqlcode;
                p_po_rtnmsg  := '外汇现货头寸('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_WH_FESPOTPOSITION;
/

