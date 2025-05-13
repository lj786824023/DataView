CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_CD_CUSTOMER_TYPE0910(p_data_dt_str IN  VARCHAR2, --数据日期 yyyyMMdd
                                                             p_po_rtncode  OUT VARCHAR2, --返回编号 1 成功,0 失败
                                                             p_po_rtnmsg   OUT VARCHAR2  --返回描述
                                                            )

/*
存储过程名称:PRO_RWA_CD_CUSTOMER_TYPE
实现功能:计算暴露分类.客户类型大小类分类
版  本  :V1.0.0
编写人  :QHJIANG
编写时间:2016-05-26
单  位  :上海安硕信息技术股份有限公司
源  表1 :RWA_DEV.RWA_EI_CLIENT
源  表2 :RWA_DEV.RWA_CD_CUSNAMELIST
源  表3 :RWA_DEV.BL_CUSTOMER_INFO
源  表4 :RWA_DEV.NCM_CUSTOMER_INFO
目标表1 :RWA_DEV.RWA_CD_CUSTOMER_TYPE
辅助表  :无
变更记录(修改人|修改时间|修改内容):
        QHJIANG|2017/03/30|二期开发_由于新信贷的客户数据的客户类型大小类信贷有生成，所以RWA只生成新信贷未生成客户类型大小类的数据
        QHJIANG|2017/05/02|二期开发_添加中小企业的客户分类
*/

AS
    --创建一个自治事务
    PRAGMA AUTONOMOUS_TRANSACTION;

    /*变量定义*/
    --定义存储过程名称并赋值
    v_pro_name VARCHAR2(200) := 'PRO_RWA_CD_CUSTOMER_TYPE';
    --定义异常变量
    v_raise EXCEPTION;
    --定义当前插入的记录数
    v_count INTEGER;



BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    --1 主权客户匹配
    --1.1 清空客户类型信息临时表
    DELETE FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE WHERE DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD');

    COMMIT;

		--3 个人客户较多，先分出个人客户
    --3.1 最先分出补录表的个人客户
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --参与主体ID
                ,CLIENTNAME         --参与主体名称
                ,RWACUSTYPE         --RWA参与主体大类
                ,RWACUSTYPE_NAME    --RWA参与主体大类名称
                ,RWACUSSUBTYPE      --RWA参与主体小类
                ,RWACUSSUBTYPE_NAME --RWA参与主体小类名称
                ,DATADATE           --数据日期
                ,DATANO             --数据流水号
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --参与主体ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --参与主体名称
           ,'04'                    AS CUSTYPE         --参与主体大类
           ,'个人'                  AS CUSTYPE_NAME    --参与主体大类名称
           ,'0401'                  AS CUSSUBTYPE      --参与主体小类
           ,'个人（自然人）'        AS CUSSUBTYPE_NAME --参与主体小类名称
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --数据日期
           ,p_data_dt_str        AS DATANO --数据流水号
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND		REC.SSYSID = 'XYK'						 --信用卡客户全是个人
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;
     COMMIT;

    --3 个人客户较多，先分出个人客户
    --3.1 最先分出补录表的个人客户
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --参与主体ID
                ,CLIENTNAME         --参与主体名称
                ,RWACUSTYPE         --RWA参与主体大类
                ,RWACUSTYPE_NAME    --RWA参与主体大类名称
                ,RWACUSSUBTYPE      --RWA参与主体小类
                ,RWACUSSUBTYPE_NAME --RWA参与主体小类名称
                ,DATADATE           --数据日期
                ,DATANO             --数据流水号
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --参与主体ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --参与主体名称
           ,'04'                    AS CUSTYPE         --参与主体大类
           ,'个人'                  AS CUSTYPE_NAME    --参与主体大类名称
           ,'0401'                  AS CUSSUBTYPE      --参与主体小类
           ,'个人（自然人）'        AS CUSSUBTYPE_NAME --参与主体小类名称
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --数据日期
           ,p_data_dt_str        AS DATANO --数据流水号
      FROM RWA_DEV.RWA_EI_CLIENT REC
      INNER JOIN  RWA_DEV.BL_CUSTOMER_INFO BCI          --补录客户表
      ON REC.CLIENTID=BCI.CUSTOMERID
      AND REC.DATANO=BCI.DATANO
      AND (BCI.CUSTOMERTYPE='0321000001' OR BCI.CERTTYPE LIKE 'Ind%')           --零售客户
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;
     COMMIT;
    --3 个人客户较多，先分出个人客户
    --3.1 然后分出源系统表的个人客户
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --参与主体ID
                ,CLIENTNAME         --参与主体名称
                ,RWACUSTYPE         --RWA参与主体大类
                ,RWACUSTYPE_NAME    --RWA参与主体大类名称
                ,RWACUSSUBTYPE      --RWA参与主体小类
                ,RWACUSSUBTYPE_NAME --RWA参与主体小类名称
                ,DATADATE           --数据日期
                ,DATANO             --数据流水号
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --参与主体ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --参与主体名称
           ,'04'                    AS CUSTYPE         --参与主体大类
           ,'个人'                  AS CUSTYPE_NAME    --参与主体大类名称
           ,'0401'                  AS CUSSUBTYPE      --参与主体小类
           ,'个人（自然人）'        AS CUSSUBTYPE_NAME --参与主体小类名称
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --数据日期
           ,p_data_dt_str                            AS DATANO   --数据流水号
      FROM RWA_DEV.RWA_EI_CLIENT REC
      INNER JOIN  RWA_DEV.NCM_CUSTOMER_INFO CCI           --源系统客户信息表
      ON CCI.CUSTOMERID=REC.CLIENTID
      AND CCI.DATANO=REC.DATANO
      AND (CCI.CUSTOMERTYPE='0321000001' OR CCI.CERTTYPE LIKE 'Ind%')           --零售客户
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --主权类客户，清单匹配
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --参与主体ID
                ,CLIENTNAME         --参与主体名称
                ,RWACUSTYPE         --RWA参与主体大类
                ,RWACUSTYPE_NAME    --RWA参与主体大类名称
                ,RWACUSSUBTYPE      --RWA参与主体小类
                ,RWACUSSUBTYPE_NAME --RWA参与主体小类名称
                ,DATADATE           --数据日期
                ,DATANO             --数据流水号
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --参与主体ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --参与主体名称
           ,RCC.ATTRIBUTE1          AS CUSTYPE         --参与主体大类
           ,'主权'                  AS CUSTYPE_NAME    --参与主体大类名称
           ,RCC.ATTRIBUTE2          AS CUSSUBTYPE      --参与主体小类
           ,RCC.LISTNAME            AS CUSSUBTYPE_NAME --参与主体小类名称
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --数据日期
           ,p_data_dt_str                            AS DATANO   --数据流水号
      FROM RWA_DEV.RWA_EI_CLIENT REC
      INNER JOIN RWA_DEV.RWA_CD_CUSNAMELIST RCC
      ON RCC.CUSTNAME=REC.CLIENTNAME
      AND RCC.ATTRIBUTE1='01'      --主权类客户全用清单全匹配
      AND RCC.ISINUSE = '1'
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --2.1.2 将匹配上的金融机构.政策性银行的金融机构存入临时表中
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --参与主体ID
                ,CLIENTNAME         --参与主体名称
                ,RWACUSTYPE         --RWA参与主体大类
                ,RWACUSTYPE_NAME    --RWA参与主体大类名称
                ,RWACUSSUBTYPE      --RWA参与主体小类
                ,RWACUSSUBTYPE_NAME --RWA参与主体小类名称
                ,DATADATE           --数据日期
                ,DATANO             --数据流水号
    )
    SELECT  REC.CLIENTID                                             AS CLIENTID        --参与主体ID
           ,REC.CLIENTNAME                                           AS CLIENTNAME      --参与主体名称
           ,'02'                                                     AS CUSTYPE         --参与主体大类
           ,'金融机构'                                               AS CUSTYPE_NAME    --参与主体大类名称
           ,'0201'                                                   AS CUSSUBTYPE      --参与主体小类
           ,'中国政策性银行'                                         AS CUSSUBTYPE_NAME --参与主体小类名称
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')                        AS DATADATE        --数据日期
           ,p_data_dt_str                                            AS DATANO          --数据流水号
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      --AND  REC.REGISTSTATE='01'                  --“所在国家地区”为‘中国’ 中国(01) 非中国(02)
      AND (REC.CLIENTNAME LIKE '%国家开发银行%'     --农村信用社，城市信用社，农村合作银行
           OR REC.CLIENTNAME LIKE '%国开行%'
           OR REC.CLIENTNAME LIKE '%中国进出口银行%'
           OR REC.CLIENTNAME LIKE '%进出口银行%'
           OR REC.CLIENTNAME LIKE '%中国农业发展银行%'
           OR REC.CLIENTNAME LIKE '%农发行%'
           )
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --2 金融机构客户匹配
    --2.1 境内客户类型匹配
    --2.1.1 将匹配上的金融机构.中国中央政府投资的金融资产管理公司存入临时表中
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --参与主体ID
                ,CLIENTNAME         --参与主体名称
                ,RWACUSTYPE         --RWA参与主体大类
                ,RWACUSTYPE_NAME    --RWA参与主体大类名称
                ,RWACUSSUBTYPE      --RWA参与主体小类
                ,RWACUSSUBTYPE_NAME --RWA参与主体小类名称
                ,DATADATE           --数据日期
                ,DATANO             --数据流水号
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --参与主体ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --参与主体名称
           ,RCC.ATTRIBUTE1          AS CUSTYPE         --参与主体大类
           ,'金融机构'              AS CUSTYPE_NAME    --参与主体大类名称
           ,case when REC.REGISTSTATE='01'
                 then RCC.ATTRIBUTE2
                 when REC.REGISTSTATE<>'01' and RCC.LISTTYPE='CCEN_AMC'
                 then '0208'
                 else '0206' end    AS CUSSUBTYPE      --参与主体小类
           ,case when REC.REGISTSTATE='01'
                 then RCC.LISTNAME
                 when REC.REGISTSTATE<>'01' and RCC.LISTTYPE='CCEN_AMC'
                 then '境外其他金融机构'
                 else '境外商业银行' end            AS CUSSUBTYPE_NAME --参与主体小类名称
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --数据日期
           ,p_data_dt_str                            AS DATANO   --数据流水号
      FROM RWA_DEV.RWA_EI_CLIENT REC
      INNER JOIN RWA_DEV.RWA_CD_CUSNAMELIST RCC
      ON RCC.CUSTNAME=REC.CLIENTNAME
      AND RCC.LISTTYPE ='CCEN_AMC'   --金融机构.中国中央政府投资的金融资产管理公司
      AND RCC.ISINUSE = '1'
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      --AND REC.REGISTSTATE='01'   --“所在国家地区”为‘中国’ 中国(01) 非中国(02)
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --2.1.2 将匹配上的金融机构.其它吸收公众存款的金融机构存入临时表中
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --参与主体ID
                ,CLIENTNAME         --参与主体名称
                ,RWACUSTYPE         --RWA参与主体大类
                ,RWACUSTYPE_NAME    --RWA参与主体大类名称
                ,RWACUSSUBTYPE      --RWA参与主体小类
                ,RWACUSSUBTYPE_NAME --RWA参与主体小类名称
                ,DATADATE           --数据日期
                ,DATANO             --数据流水号
    )
    SELECT  REC.CLIENTID                                             AS CLIENTID        --参与主体ID
           ,REC.CLIENTNAME                                           AS CLIENTNAME      --参与主体名称
           ,'02'                                                     AS CUSTYPE         --参与主体大类
           ,'金融机构'                                               AS CUSTYPE_NAME    --参与主体大类名称
           ,'0203'                                                   AS CUSSUBTYPE      --参与主体小类
           ,'中国农村合作银行、农村信用社等吸收公众存款的金融机构'   AS CUSSUBTYPE_NAME --参与主体小类名称
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')                        AS DATADATE        --数据日期
           ,p_data_dt_str                                            AS DATANO          --数据流水号
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND  REC.REGISTSTATE='01'                  --“所在国家地区”为‘中国’ 中国(01) 非中国(02)
      AND (REC.CLIENTNAME LIKE '%农村%信用社%'     --农村信用社，城市信用社，农村合作银行
           OR REC.CLIENTNAME LIKE '%城市%信用社%'
           OR REC.CLIENTNAME LIKE '%农村%合作%'
           OR REC.CLIENTNAME LIKE '%农%联%社%'
           OR REC.CLIENTNAME LIKE '%农%信%社%'
           OR REC.CLIENTNAME LIKE '%合行%'
           OR REC.CLIENTNAME LIKE '%信用社%')
      AND LENGTHB(CLIENTNAME)>9
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --2.1.3 将匹配上的金融机构.农村资金互助社|贷款公司|信托投资公司|财务公司|汽车金融服务公司|消费金融公司|证券公司|保险公司|企业集团财务公司|金融租赁公司|货币经纪公司|村镇银行的金融机构存入临时表中
    --2.1.3.1 将匹配上农村资金互助社存入临时表中
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --参与主体ID
                ,CLIENTNAME         --参与主体名称
                ,RWACUSTYPE         --RWA参与主体大类
                ,RWACUSTYPE_NAME    --RWA参与主体大类名称
                ,RWACUSSUBTYPE      --RWA参与主体小类
                ,RWACUSSUBTYPE_NAME --RWA参与主体小类名称
                ,DATADATE           --数据日期
                ,DATANO             --数据流水号
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --参与主体ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --参与主体名称
           ,'02'                    AS CUSTYPE         --参与主体大类
           ,'金融机构'              AS CUSTYPE_NAME    --参与主体大类名称
           ,'020501'                AS CUSSUBTYPE      --参与主体小类
           ,'农村资金互助社'        AS CUSSUBTYPE_NAME --参与主体小类名称
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --数据日期
           ,p_data_dt_str                            AS DATANO   --数据流水号
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='01'                --“所在国家地区”为‘中国’ 中国(01) 非中国(02)
      AND REC.CLIENTNAME LIKE '%农村%资金%互助社%'
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --2.1.3.2 将匹配上贷款公司存入临时表中
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --参与主体ID
                ,CLIENTNAME         --参与主体名称
                ,RWACUSTYPE         --RWA参与主体大类
                ,RWACUSTYPE_NAME    --RWA参与主体大类名称
                ,RWACUSSUBTYPE      --RWA参与主体小类
                ,RWACUSSUBTYPE_NAME --RWA参与主体小类名称
                ,DATADATE           --数据日期
                ,DATANO             --数据流水号
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --参与主体ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --参与主体名称
           ,'02'                    AS CUSTYPE         --参与主体大类
           ,'金融机构'              AS CUSTYPE_NAME    --参与主体大类名称
           ,'020502'                AS CUSSUBTYPE      --参与主体小类
           ,'贷款公司'              AS CUSSUBTYPE_NAME --参与主体小类名称
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --数据日期
           ,p_data_dt_str                            AS DATANO   --数据流水号
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='01'                --“所在国家地区”为‘中国’ 中国(01) 非中国(02)
      AND REC.CLIENTNAME LIKE '%贷款%公司%'
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --2.1.3.3 将匹配上信托投资公司存入临时表中
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --参与主体ID
                ,CLIENTNAME         --参与主体名称
                ,RWACUSTYPE         --RWA参与主体大类
                ,RWACUSTYPE_NAME    --RWA参与主体大类名称
                ,RWACUSSUBTYPE      --RWA参与主体小类
                ,RWACUSSUBTYPE_NAME --RWA参与主体小类名称
                ,DATADATE           --数据日期
                ,DATANO             --数据流水号
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --参与主体ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --参与主体名称
           ,'02'                    AS CUSTYPE         --参与主体大类
           ,'金融机构'              AS CUSTYPE_NAME    --参与主体大类名称
           ,'020503'                AS CUSSUBTYPE      --参与主体小类
           ,'信托投资公司'          AS CUSSUBTYPE_NAME --参与主体小类名称
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --数据日期
           ,p_data_dt_str                            AS DATANO   --数据流水号
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='01'                --“所在国家地区”为‘中国’ 中国(01) 非中国(02)
      AND (REC.CLIENTNAME LIKE '%信托%投资%' OR REC.CLIENTNAME LIKE '%信托%')
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

   COMMIT;

   --2.1.3.4 将匹配上企业集团财务公司存入临时表中
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --参与主体ID
                ,CLIENTNAME         --参与主体名称
                ,RWACUSTYPE         --RWA参与主体大类
                ,RWACUSTYPE_NAME    --RWA参与主体大类名称
                ,RWACUSSUBTYPE      --RWA参与主体小类
                ,RWACUSSUBTYPE_NAME --RWA参与主体小类名称
                ,DATADATE           --数据日期
                ,DATANO             --数据流水号
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --参与主体ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --参与主体名称
           ,'02'                    AS CUSTYPE         --参与主体大类
           ,'金融机构'              AS CUSTYPE_NAME    --参与主体大类名称
           ,'020509'                AS CUSSUBTYPE      --参与主体小类
           ,'企业集团财务公司'      AS CUSSUBTYPE_NAME --参与主体小类名称
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --数据日期
           ,p_data_dt_str                            AS DATANO   --数据流水号
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='01'                --“所在国家地区”为‘中国’ 中国(01) 非中国(02)
      AND REC.CLIENTNAME LIKE '%集团%财务%公司%'
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;


    --2.1.3.5 将匹配上汽车金融服务公司存入临时表中
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --参与主体ID
                ,CLIENTNAME         --参与主体名称
                ,RWACUSTYPE         --RWA参与主体大类
                ,RWACUSTYPE_NAME    --RWA参与主体大类名称
                ,RWACUSSUBTYPE      --RWA参与主体小类
                ,RWACUSSUBTYPE_NAME --RWA参与主体小类名称
                ,DATADATE           --数据日期
                ,DATANO             --数据流水号
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --参与主体ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --参与主体名称
           ,'02'                    AS CUSTYPE         --参与主体大类
           ,'金融机构'              AS CUSTYPE_NAME    --参与主体大类名称
           ,'020505'                AS CUSSUBTYPE      --参与主体小类
           ,'汽车金融服务公司'      AS CUSSUBTYPE_NAME --参与主体小类名称
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --数据日期
           ,p_data_dt_str                            AS DATANO   --数据流水号
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='01'                --“所在国家地区”为‘中国’ 中国(01) 非中国(02)
      AND REC.CLIENTNAME LIKE '%汽车%金融%公司%'
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --2.1.3.6 将匹配上消费金融公司存入临时表中
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --参与主体ID
                ,CLIENTNAME         --参与主体名称
                ,RWACUSTYPE         --RWA参与主体大类
                ,RWACUSTYPE_NAME    --RWA参与主体大类名称
                ,RWACUSSUBTYPE      --RWA参与主体小类
                ,RWACUSSUBTYPE_NAME --RWA参与主体小类名称
                ,DATADATE           --数据日期
                ,DATANO             --数据流水号
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --参与主体ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --参与主体名称
           ,'02'                    AS CUSTYPE         --参与主体大类
           ,'金融机构'              AS CUSTYPE_NAME    --参与主体大类名称
           ,'020506'                AS CUSSUBTYPE      --参与主体小类
           ,'消费金融公司'          AS CUSSUBTYPE_NAME --参与主体小类名称
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --数据日期
           ,p_data_dt_str                            AS DATANO   --数据流水号
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='01'                --“所在国家地区”为‘中国’ 中国(01) 非中国(02)
      AND REC.CLIENTNAME LIKE '%消费%金融%公司%'
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

   COMMIT;

    --2.1.3.7 将匹配上证券公司存入临时表中
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --参与主体ID
                ,CLIENTNAME         --参与主体名称
                ,RWACUSTYPE         --RWA参与主体大类
                ,RWACUSTYPE_NAME    --RWA参与主体大类名称
                ,RWACUSSUBTYPE      --RWA参与主体小类
                ,RWACUSSUBTYPE_NAME --RWA参与主体小类名称
                ,DATADATE           --数据日期
                ,DATANO             --数据流水号
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --参与主体ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --参与主体名称
           ,'02'                    AS CUSTYPE         --参与主体大类
           ,'金融机构'              AS CUSTYPE_NAME    --参与主体大类名称
           ,'020507'                AS CUSSUBTYPE      --参与主体小类
           ,'证券公司'              AS CUSSUBTYPE_NAME --参与主体小类名称
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --数据日期
           ,p_data_dt_str                            AS DATANO   --数据流水号
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='01'                --“所在国家地区”为‘中国’ 中国(01) 非中国(02)
      AND (REC.CLIENTNAME NOT LIKE '%银行%' AND REC.CLIENTNAME LIKE '%证券%'）
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --2.1.3.8 将匹配上保险公司存入临时表中
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --参与主体ID
                ,CLIENTNAME         --参与主体名称
                ,RWACUSTYPE         --RWA参与主体大类
                ,RWACUSTYPE_NAME    --RWA参与主体大类名称
                ,RWACUSSUBTYPE      --RWA参与主体小类
                ,RWACUSSUBTYPE_NAME --RWA参与主体小类名称
                ,DATADATE           --数据日期
                ,DATANO             --数据流水号
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --参与主体ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --参与主体名称
           ,'02'                    AS CUSTYPE         --参与主体大类
           ,'金融机构'              AS CUSTYPE_NAME    --参与主体大类名称
           ,'020508'                AS CUSSUBTYPE      --参与主体小类
           ,'保险公司'              AS CUSSUBTYPE_NAME --参与主体小类名称
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --数据日期
           ,p_data_dt_str                            AS DATANO   --数据流水号
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='01'                --“所在国家地区”为‘中国’ 中国(01) 非中国(02)
      AND REC.CLIENTNAME LIKE '%保险%公司%'
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --2.1.3.9 将匹配上财务公司存入临时表中
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --参与主体ID
                ,CLIENTNAME         --参与主体名称
                ,RWACUSTYPE         --RWA参与主体大类
                ,RWACUSTYPE_NAME    --RWA参与主体大类名称
                ,RWACUSSUBTYPE      --RWA参与主体小类
                ,RWACUSSUBTYPE_NAME --RWA参与主体小类名称
                ,DATADATE           --数据日期
                ,DATANO             --数据流水号
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --参与主体ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --参与主体名称
           ,'02'                    AS CUSTYPE         --参与主体大类
           ,'金融机构'              AS CUSTYPE_NAME    --参与主体大类名称
           ,'020504'                AS CUSSUBTYPE      --参与主体小类
           ,'财务公司'              AS CUSSUBTYPE_NAME --参与主体小类名称
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --数据日期
           ,p_data_dt_str                            AS DATANO   --数据流水号
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='01'                --“所在国家地区”为‘中国’ 中国(01) 非中国(02)
      AND REC.CLIENTNAME LIKE '%财务%公司%'
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --2.1.3.10 将匹配上金融租赁公司存入临时表中
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --参与主体ID
                ,CLIENTNAME         --参与主体名称
                ,RWACUSTYPE         --RWA参与主体大类
                ,RWACUSTYPE_NAME    --RWA参与主体大类名称
                ,RWACUSSUBTYPE      --RWA参与主体小类
                ,RWACUSSUBTYPE_NAME --RWA参与主体小类名称
                ,DATADATE           --数据日期
                ,DATANO             --数据流水号
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --参与主体ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --参与主体名称
           ,'02'                    AS CUSTYPE         --参与主体大类
           ,'金融机构'              AS CUSTYPE_NAME    --参与主体大类名称
           ,'020510'                AS CUSSUBTYPE      --参与主体小类
           ,'金融租赁公司'          AS CUSSUBTYPE_NAME --参与主体小类名称
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --数据日期
           ,p_data_dt_str                            AS DATANO   --数据流水号
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='01'                --“所在国家地区”为‘中国’ 中国(01) 非中国(02)
      AND REC.CLIENTNAME LIKE '%金融%租赁%公司%'
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --2.1.3.11 将匹配上货币经纪公司存入临时表中
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --参与主体ID
                ,CLIENTNAME         --参与主体名称
                ,RWACUSTYPE         --RWA参与主体大类
                ,RWACUSTYPE_NAME    --RWA参与主体大类名称
                ,RWACUSSUBTYPE      --RWA参与主体小类
                ,RWACUSSUBTYPE_NAME --RWA参与主体小类名称
                ,DATADATE           --数据日期
                ,DATANO             --数据流水号
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --参与主体ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --参与主体名称
           ,'02'                    AS CUSTYPE         --参与主体大类
           ,'金融机构'              AS CUSTYPE_NAME    --参与主体大类名称
           ,'020511'                AS CUSSUBTYPE      --参与主体小类
           ,'货币经纪公司'          AS CUSSUBTYPE_NAME --参与主体小类名称
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --数据日期
           ,p_data_dt_str                            AS DATANO   --数据流水号
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='01'                --“所在国家地区”为‘中国’ 中国(01) 非中国(02)
      AND REC.CLIENTNAME LIKE '%货币%经纪%公司%'
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --2.1.3.12 将匹配上村镇银行存入临时表中
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --参与主体ID
                ,CLIENTNAME         --参与主体名称
                ,RWACUSTYPE         --RWA参与主体大类
                ,RWACUSTYPE_NAME    --RWA参与主体大类名称
                ,RWACUSSUBTYPE      --RWA参与主体小类
                ,RWACUSSUBTYPE_NAME --RWA参与主体小类名称
                ,DATADATE           --数据日期
                ,DATANO             --数据流水号
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --参与主体ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --参与主体名称
           ,'02'                    AS CUSTYPE         --参与主体大类
           ,'金融机构'              AS CUSTYPE_NAME    --参与主体大类名称
           ,'020512'                AS CUSSUBTYPE      --参与主体小类
           ,'村镇银行'              AS CUSSUBTYPE_NAME --参与主体小类名称
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --数据日期
           ,p_data_dt_str                            AS DATANO   --数据流水号
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='01'                --“所在国家地区”为‘中国’ 中国(01) 非中国(02)
      AND REC.CLIENTNAME LIKE '%村镇%银行%'
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --2.1.4 通过关键词“银行”模糊匹配商业银行
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --参与主体ID
                ,CLIENTNAME         --参与主体名称
                ,RWACUSTYPE         --RWA参与主体大类
                ,RWACUSTYPE_NAME    --RWA参与主体大类名称
                ,RWACUSSUBTYPE      --RWA参与主体小类
                ,RWACUSSUBTYPE_NAME --RWA参与主体小类名称
                ,DATADATE           --数据日期
                ,DATANO             --数据流水号
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --参与主体ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --参与主体名称
           ,'02'                    AS CUSTYPE         --参与主体大类
           ,'金融机构'              AS CUSTYPE_NAME    --参与主体大类名称
           ,'0202'                  AS CUSSUBTYPE      --参与主体小类
           ,'中国商业银行'          AS CUSSUBTYPE_NAME --参与主体小类名称
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --数据日期
           ,p_data_dt_str                            AS DATANO   --数据流水号
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='01'                --“所在国家地区”为‘中国’ 中国(01) 非中国(02)
      AND (REC.CLIENTNAME LIKE '%银行%' OR REC.CLIENTNAME LIKE '%支行%' --OR REC.CLIENTNAME LIKE '%商行%'  经宋科确认删掉这个关键字
            OR REC.CLIENTNAME LIKE '%分行%' OR REC.CLIENTNAME LIKE '%农商行%' OR REC.CLIENTNAME LIKE '%建行%' OR REC.CLIENTNAME LIKE '%农行%'
            OR REC.CLIENTNAME LIKE '%花旗纽约%' OR REC.CLIENTNAME LIKE '%中银香港%')
      AND LENGTHB(CLIENTNAME)>9
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --2.1.3.12 将匹配上村镇银行存入临时表中
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --参与主体ID
                ,CLIENTNAME         --参与主体名称
                ,RWACUSTYPE         --RWA参与主体大类
                ,RWACUSTYPE_NAME    --RWA参与主体大类名称
                ,RWACUSSUBTYPE      --RWA参与主体小类
                ,RWACUSSUBTYPE_NAME --RWA参与主体小类名称
                ,DATADATE           --数据日期
                ,DATANO             --数据流水号
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --参与主体ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --参与主体名称
           ,'02'                    AS CUSTYPE         --参与主体大类
           ,'金融机构'              AS CUSTYPE_NAME    --参与主体大类名称
           ,'0205'                  AS CUSSUBTYPE      --参与主体小类
           ,'中国其他金融机构'      AS CUSSUBTYPE_NAME --参与主体小类名称
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --数据日期
           ,p_data_dt_str                            AS DATANO   --数据流水号
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='01'                --“所在国家地区”为‘中国’ 中国(01) 非中国(02)
      AND (REC.CLIENTNAME LIKE '%金融资产管理%' OR REC.CLIENTNAME LIKE '%清算%中心%'
           OR REC.CLIENTNAME LIKE '%存放同业%' OR REC.CLIENTNAME LIKE '%基金%公司%' OR REC.CLIENTNAME LIKE '%银联%')
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --2.1.5 不满足标题2.1以上条件，所在国家地区为'中国'，则客户类型标识为中国其他金融机构
   /* INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(    --经黄征确认，这段删除
                 CLIENTID           --参与主体ID
                ,CLIENTNAME         --参与主体名称
                ,RWACUSTYPE         --RWA参与主体大类
                ,RWACUSTYPE_NAME    --RWA参与主体大类名称
                ,RWACUSSUBTYPE      --RWA参与主体小类
                ,RWACUSSUBTYPE_NAME --RWA参与主体小类名称
                ,DATADATE           --数据日期
                ,DATANO             --数据流水号
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --参与主体ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --参与主体名称
           ,'02'                    AS CUSTYPE         --参与主体大类
           ,'金融机构'              AS CUSTYPE_NAME    --参与主体大类名称
           ,'0205'                  AS CUSSUBTYPE      --参与主体小类
           ,'中国其他金融机构'      AS CUSSUBTYPE_NAME --参与主体小类名称
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --数据日期
           ,p_data_dt_str                            AS DATANO   --数据流水号
      FROM RWA_DEV.RWA_EI_CLIENT REC
      INNER JOIN RWA_DEV.NCM_CUSTOMER_INFO CCI
      ON CCI.CUSTOMERID=REC.CLIENTID
      AND REC.DATANO=CCI.DATANO
      AND CCI.CUSTOMERTYPE='0321000003'
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='01'    --“所在国家地区”为‘中国’ 中国(01) 非中国(02)
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;  */

    --2.2 境外客户类型匹配
    --2.2.1 将匹配上的金融机构.多边开发银行、国际清算银行和国际货币基金组织存入临时表中
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --参与主体ID
                ,CLIENTNAME         --参与主体名称
                ,RWACUSTYPE         --RWA参与主体大类
                ,RWACUSTYPE_NAME    --RWA参与主体大类名称
                ,RWACUSSUBTYPE      --RWA参与主体小类
                ,RWACUSSUBTYPE_NAME --RWA参与主体小类名称
                ,DATADATE           --数据日期
                ,DATANO             --数据流水号
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --参与主体ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --参与主体名称
           ,RCC.ATTRIBUTE1          AS CUSTYPE         --参与主体大类
           ,'金融机构'              AS CUSTYPE_NAME    --参与主体大类名称
           ,RCC.ATTRIBUTE2          AS CUSSUBTYPE      --参与主体小类
           ,RCC.LISTNAME            AS CUSSUBTYPE_NAME --参与主体小类名称
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --数据日期
           ,p_data_dt_str                            AS DATANO   --数据流水号
      FROM RWA_DEV.RWA_EI_CLIENT REC
      INNER JOIN RWA_DEV.RWA_CD_CUSNAMELIST RCC
      ON REC.CLIENTNAME=RCC.CUSTNAME
      AND RCC.LISTTYPE='MDB'                   --多边开发银行、国际清算银行和国际货币基金组织（MDB）名单
      AND RCC.ISINUSE = '1'
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      --AND REC.REGISTSTATE='02'
      ;

    COMMIT;

    --2.2.2 通过关键词“银行”模糊匹配商业银行
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --参与主体ID
                ,CLIENTNAME         --参与主体名称
                ,RWACUSTYPE         --RWA参与主体大类
                ,RWACUSTYPE_NAME    --RWA参与主体大类名称
                ,RWACUSSUBTYPE      --RWA参与主体小类
                ,RWACUSSUBTYPE_NAME --RWA参与主体小类名称
                ,DATADATE           --数据日期
                ,DATANO             --数据流水号
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --参与主体ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --参与主体名称
           ,'02'                    AS CUSTYPE         --参与主体大类
           ,'金融机构'              AS CUSTYPE_NAME    --参与主体大类名称
           ,'0206'                  AS CUSSUBTYPE      --参与主体小类
           ,'境外商业银行'          AS CUSSUBTYPE_NAME --参与主体小类名称
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --数据日期
           ,p_data_dt_str                            AS DATANO   --数据流水号
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='02'                 --“所在国家地区”为‘中国’ 中国(01) 非中国(02)
      AND (REC.CLIENTNAME LIKE '%银行%' OR REC.CLIENTNAME LIKE '%花旗纽约%' OR REC.CLIENTNAME LIKE '%中银香港%' OR REC.CLIENTNAME LIKE '%三井住友%')      --通过关键词“银行”模糊匹配商业银行
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    --2.2.3 不满足标题2.1以上条件，客户类型为'金融机构' 所在国家地区为'非中国'，则客户类型标识为境外其他金融机构
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --参与主体ID
                ,CLIENTNAME         --参与主体名称
                ,RWACUSTYPE         --RWA参与主体大类
                ,RWACUSTYPE_NAME    --RWA参与主体大类名称
                ,RWACUSSUBTYPE      --RWA参与主体小类
                ,RWACUSSUBTYPE_NAME --RWA参与主体小类名称
                ,DATADATE           --数据日期
                ,DATANO             --数据流水号
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --参与主体ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --参与主体名称
           ,'02'                    AS CUSTYPE         --参与主体大类
           ,'金融机构'              AS CUSTYPE_NAME    --参与主体大类名称
           ,'0208'                  AS CUSSUBTYPE      --参与主体小类
           ,'境外其他金融机构'      AS CUSSUBTYPE_NAME --参与主体小类名称
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --数据日期
           ,p_data_dt_str                            AS DATANO   --数据流水号
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND REC.REGISTSTATE='02'    --“所在国家地区”为‘中国’ 中国(01) 非中国(02)
      AND (REC.CLIENTNAME LIKE '%保险%'OR REC.CLIENTNAME LIKE '%财务%' OR REC.CLIENTNAME LIKE '%信托%'
           OR REC.CLIENTNAME LIKE '%证券%' OR REC.CLIENTNAME LIKE '%货币%' OR REC.CLIENTNAME LIKE '%贷款%' OR REC.CLIENTNAME LIKE '%信用社%'
           OR REC.CLIENTNAME LIKE '%互助社%'  OR REC.CLIENTNAME LIKE '%花旗纽约%' OR REC.CLIENTNAME LIKE '%中银香港%'
           OR REC.CLIENTNAME LIKE '%农村%资金%互助社%' OR REC.CLIENTNAME LIKE '%汽车%金融%公司%' OR REC.CLIENTNAME LIKE '%消费%金融%公司%' --add by qhjiang 2017/03/30 二期开发 添加境外其他金融机构匹配条件
           OR REC.CLIENTNAME LIKE '%金融%租赁%公司%' OR REC.CLIENTNAME LIKE '%村镇%银行%' --add by qhjiang 2017/03/30 二期开发 添加境外其他金融机构匹配条件
           OR EXISTS(SELECT 1 --add by qhjiang 2017/03/30 二期开发 添加境外其他金融机构匹配条件
                       FROM RWA_DEV.NCM_CUSTOMER_INFO CCI
                      WHERE CCI.CUSTOMERID=REC.CLIENTID
                        AND REC.DATANO=CCI.DATANO
                        AND CCI.CUSTOMERTYPE='0321000003')
           )
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;


    --4 公司客户分类，剩下的都分到公司客户
    INSERT INTO RWA_DEV.RWA_CD_CUSTOMER_TYPE(
                 CLIENTID           --参与主体ID
                ,CLIENTNAME         --参与主体名称
                ,RWACUSTYPE         --RWA参与主体大类
                ,RWACUSTYPE_NAME    --RWA参与主体大类名称
                ,RWACUSSUBTYPE      --RWA参与主体小类
                ,RWACUSSUBTYPE_NAME --RWA参与主体小类名称
                ,DATADATE           --数据日期
                ,DATANO             --数据流水号
    )
    SELECT  REC.CLIENTID            AS CLIENTID        --参与主体ID
           ,REC.CLIENTNAME          AS CLIENTNAME      --参与主体名称
           ,'03'                    AS CUSTYPE         --参与主体大类
           ,'公司'                  AS CUSTYPE_NAME    --参与主体大类名称
           ,CASE WHEN REC.ANNUALSALE is not null AND REC.ANNUALSALE <=300000000 AND REC.ANNUALSALE > 0
                 THEN '0302'
                 ELSE '0301' END    AS CUSSUBTYPE--参与主体小类 中小企业-0302 一般公司-0301   --add by qhjiang 2017/05/02 添加中小企业的客户分类
           ,CASE WHEN REC.ANNUALSALE is not null AND REC.ANNUALSALE <=300000000 AND REC.ANNUALSALE > 0
                 THEN '中小企业'
                 ELSE '一般公司' END AS CUSSUBTYPE_NAME --参与主体小类名称
           ,TO_DATE(p_data_dt_str,'YYYYMMDD')        AS DATADATE --数据日期
           ,p_data_dt_str                            AS DATANO   --数据流水号
      FROM RWA_DEV.RWA_EI_CLIENT REC
      WHERE REC.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE RCCT WHERE RCCT.CLIENTID=REC.CLIENTID AND RCCT.DATANO=p_data_dt_str)
      ;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_CD_CUSTOMER_TYPE',cascade => true);

----------------------------------------------更新参与主体表的参与主体大小类----------------------------------------------------------
    MERGE INTO (SELECT CLIENTID,CUSTYPE,CUSTYPE_NAME,CUSSUBTYPE,CUSSUBTYPE_NAME FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')) T
    USING (SELECT T1.CLIENTID,T1.CLIENTNAME,T1.CLIENTTYPE,T2.ITEMNAME AS CUSTYPE_NAME,T1.CLIENTSUBTYPE,T3.ITEMNAME AS CUSSUBTYPE_NAME
             FROM RWA_DEV.RWA_EI_CLIENT T1
        LEFT JOIN RWA.CODE_LIBRARY T2
               ON T1.CLIENTTYPE = T2.ITEMNO
              AND T2.CODENO = 'ClientCategory'
        LEFT JOIN RWA.CODE_LIBRARY T3
               ON T1.CLIENTSUBTYPE = T3.ITEMNO
              AND T3.CODENO = 'ClientCategory'
            WHERE T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
              AND T1.CLIENTTYPE IS NOT NULL
              AND T1.CLIENTSUBTYPE IS NOT NULL) TC
       ON (T.CLIENTID = TC.CLIENTID)
     WHEN MATCHED THEN
   UPDATE SET T.CUSTYPE = TC.CLIENTTYPE,T.CUSSUBTYPE=TC.CLIENTSUBTYPE,T.CUSTYPE_NAME=TC.CUSTYPE_NAME,T.CUSSUBTYPE_NAME=TC.CUSSUBTYPE_NAME;
    COMMIT;

    MERGE INTO (SELECT CLIENTID,CLIENTTYPE,CLIENTSUBTYPE FROM RWA_DEV.RWA_EI_CLIENT WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD') AND CLIENTTYPE IS NULL AND CLIENTSUBTYPE IS NULL) T
    USING (SELECT T1.CLIENTID,T1.CLIENTNAME,T1.RWACUSTYPE,T1.RWACUSSUBTYPE
             FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE T1
            WHERE T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
              AND T1.RWACUSTYPE IS NOT NULL
              AND T1.RWACUSSUBTYPE IS NOT NULL) T2
       ON (T.CLIENTID = T2.CLIENTID)
     WHEN MATCHED THEN
   UPDATE SET T.CLIENTTYPE = T2.RWACUSTYPE,T.CLIENTSUBTYPE=T2.RWACUSSUBTYPE;
    COMMIT;

    --去除金融机构的内部评级，重庆银行内评系统不覆盖金融机构 参与主体
    UPDATE RWA_DEV.RWA_EI_CLIENT
		   SET DEFAULTFLAG    = '0',
		       MODELIRATING   = NULL,
		       MODELPD        = NULL,
		       IRATING        = NULL,
		       PD             = NULL,
		       MODELID        = NULL,
		       NEWDEFAULTFLAG = '0',
		       DEFAULTDATE    = NULL
		 WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		   AND CLIENTTYPE = '02'
    ;

    COMMIT;

    --去除金融机构的内部评级，重庆银行内评系统不覆盖金融机构 风险暴露
    UPDATE RWA_DEV.RWA_EI_EXPOSURE T
		   SET T.IRATING            = NULL,
		       T.PD                 = NULL,
		       T.DEFAULTFLAG        = '0',
		       T.BEEL               = NULL,
		       T.DEFAULTLGD         = 0,
		       T.NEWDEFAULTDEBTFLAG = '0',
		       T.PDPOOLMODELID      = NULL,
		       T.LGDPOOLMODELID     = NULL,
		       T.CCFPOOLMODELID     = NULL,
		       T.PDPOOLID           = NULL,
		       T.LGDPOOLID          = NULL,
		       T.CCFPOOLID          = NULL,
		       T.DefaultDate        = NULL
		 WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		   AND EXISTS (SELECT 1
		          FROM RWA_DEV.RWA_EI_CLIENT T1
		         WHERE T.CLIENTID = T1.CLIENTID
		           AND T.DATADATE = T1.DATADATE
		           AND T1.CLIENTTYPE = '02')
		;

		COMMIT;

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_CD_CUSTOMER_TYPE;
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_CLIENT表更新客户类型大小类分类数据记录为: ' || v_count || ' 条');
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功'||'-'||v_count;

    --定义异常
    EXCEPTION WHEN OTHERS THEN
      --DBMS_OUTPUT.PUT_LINE('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
      ROLLBACK;
      p_po_rtncode := sqlcode;
      p_po_rtnmsg  := '计算客户类型分类('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;

    RETURN;

END PRO_RWA_CD_CUSTOMER_TYPE0910;
/

