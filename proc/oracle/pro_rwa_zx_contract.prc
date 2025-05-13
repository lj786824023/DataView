CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ZX_CONTRACT(
                            p_data_dt_str IN  VARCHAR2,   --数据日期
                            p_po_rtncode  OUT VARCHAR2,   --返回编号 1 成功,0 失败
                            p_po_rtnmsg   OUT VARCHAR2    --返回描述
        )
  /*
    存储过程名称:RWA_DEV.PRO_RWA_ZX_CONTRACT
    实现功能:核心系统-直销银行垫款-信用风险暴露
    数据口径:全量
    跑批频率:月末运行
    版  本  :V1.0.0
    编写人  :YUSJ
    编写时间:2016-10-18
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.CBS_IAC|通用分户帐
    源  表2 :RWA.ORG_INFO|机构表
    源  表3 :RWA.RWA_WS_DSBANK_ADV|直销银行垫款补录表
    源  表4 :RWA.RWA_WP_DATASUPPLEMENT|补录任务明细信息表
    目标表1 :RWA_DEV.RWA_ZX_COMTRACT|直销银行合同表
    辅助表 :无
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ZX_CONTRACT';
  --定义判断值变量
  v_count1 INTEGER;
  --定义判断值变量
  --v_count2 INTEGER;
  --定义异常变量
  v_raise EXCEPTION;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_ZX_CONTRACT';
    --2.将满足条件的数据从源表插入到目标表中

    /*插入核心系统合同-拆出合同信息*/
    INSERT INTO RWA_DEV.RWA_ZX_CONTRACT(
               DATADATE                              --数据日期
              ,DATANO                                --数据流水号
              ,CONTRACTID                            --合同ID
              ,SCONTRACTID                           --源合同ID
              ,SSYSID                                --源系统ID
              ,CLIENTID                              --参与主体ID
              ,SORGID                                --源机构ID
              ,SORGNAME                              --源机构名称
              ,ORGSORTNO                             --所属机构排序号
              ,ORGID                                 --所属机构ID
              ,ORGNAME                               --所属机构名称
              ,INDUSTRYID                            --所属行业代码
              ,INDUSTRYNAME                          --所属行业名称
              ,BUSINESSLINE                          --条线
              ,ASSETTYPE                             --资产大类
              ,ASSETSUBTYPE                          --资产小类
              ,BUSINESSTYPEID                        --业务品种代码
              ,BUSINESSTYPENAME                      --业务品种名称
              ,CREDITRISKDATATYPE                    --信用风险数据类型
              ,STARTDATE                             --起始日期
              ,DUEDATE                               --到期日期
              ,ORIGINALMATURITY                      --原始期限
              ,RESIDUALM                             --剩余期限
              ,SETTLEMENTCURRENCY                    --结算币种
              ,CONTRACTAMOUNT                        --合同总金额
              ,NOTEXTRACTPART                        --合同未提取部分
              ,UNCONDCANCELFLAG                      --是否可随时无条件撤销
              ,ABSUAFLAG                             --资产证券化基础资产标识
              ,ABSPOOLID                             --证券化资产池ID
              ,GROUPID                               --分组编号
              ,GUARANTEETYPE                         --主要担保方式
              ,ABSPROPORTION                         --资产证券化比重
    )
       SELECT
                 TO_DATE(T1.DATANO,'YYYYMMDD')                                     AS DATADATE            --数据日期
                ,T1.DATANO                                                         AS DATANO              --数据流水号
                ,T1.IACAC_NO                                                       AS CONTRACTID          --合同ID
                ,T1.IACAC_NO                                                       AS SCONTRACTID         --源合同ID
                ,'ZX'                                                              AS SSYSID              --源系统ID
                ,T2.CUSTID1                                                        AS CLIENTID            --参与主体ID      取参与主体表的主体ID
                ,T1.IACGACBR                                                       AS SORGID              --源机构ID
                ,T3.ORGNAME                                                        AS SORGNAME            --源机构名称
                ,T3.SORTNO                                                         AS ORGSORTNO           --所属机构排序号
                ,T1.IACGACBR                                                       AS ORGID               --所属机构ID
                ,T3.ORGNAME                                                        AS ORGNAME             --所属机构名称
                ,T2.INDUSTRYID                                                     AS INDUSTRYID          --所属行业代码
                ,T4.ITEMNAME                                                       AS INDUSTRYNAME        --所属行业名称
                ,'0101'                                                            AS BUSINESSLINE        --条线            默认：01-大中
                ,''                                                                AS ASSETTYPE           --资产大类
                ,''                                                                AS ASSETSUBTYPE        --资产小类
                ,'109010'                                                          AS BUSINESSTYPEID      --业务品种代码
                ,'直销银行垫款'                                                     AS BUSINESSTYPENAME     --业务品种名称
                ,'01'                                                              AS CREDITRISKDATATYPE  --信用风险数据类型   默认‘一般非零售’01
                ,NVL(T2.IACCRTDAT,P_DATA_DT_STR)                                   AS STARTDATE           -- 起始日期
                ,NVL(T2.IACDLTDAT,TO_CHAR(TO_DATE(P_DATA_DT_STR,'YYYYMMDD')+30,'YYYYMMDD'))           AS DUEDATE             -- 到期日期
                ,NVL((TO_DATE(T2.IACDLTDAT,'YYYYMMDD')-TO_DATE(T2.IACCRTDAT,'YYYYMMDD'))/365,30/365)  AS ORIGINALMATURITY    --原始期限
                ,NVL((TO_DATE(T2.IACDLTDAT,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD'))/365,30/365) AS RESIDUALM           --剩余期限
                ,'CNY'                                                             AS SETTLEMENTCURRENCY  --结算币种
                ,ABS(T1.IACCURBAL)                                                 AS CONTRACTAMOUNT      --合同总金额
                ,0                                                                 AS NOTEXTRACTPART      --合同未提取部分    默认0
                ,'0'                                                               AS UNCONDCANCELFLAG    --是否可随时无条件撤销    默认否 0
                ,'0'                                                               AS ABSUAFLAG           --资产证券化基础资产标识  默认否 0
                ,''                                                                AS ABSPOOLID           --证券化资产池ID        不能为空
                ,''                                                                AS GROUPID             --分组编号
                ,''                                                                AS GUARANTEETYPE       --主要担保方式
                ,NULL                                                              AS ABSPROPORTION       --资产证券化比重
    FROM        RWA_DEV.CBS_IAC T1                                        --通用分户帐
    LEFT JOIN   (SELECT WDA.IACAC_NO
                       ,WDA.CUSTID1
                       ,WDA.INDUSTRYID
                       ,TO_CHAR(TO_DATE(WDA.IACCRTDAT,'YYYY-MM-DD'),'YYYYMMDD') AS IACCRTDAT
                       ,TO_CHAR(TO_DATE(WDA.IACDLTDAT,'YYYY-MM-DD'),'YYYYMMDD') AS IACDLTDAT
                   FROM RWA.RWA_WS_DSBANK_ADV WDA                         --直销银行垫款数据补录表
             INNER JOIN RWA.RWA_WP_DATASUPPLEMENT T6                      --数据补录表
                     ON WDA.SUPPORGID = T6.ORGID
                    AND T6.DATADATE = TO_DATE(P_DATA_DT_STR,'YYYYMMDD')
                    AND T6.SUPPTMPLID = 'M-0190'
                    AND T6.SUBMITFLAG = '1'
                  WHERE WDA.DATADATE = TO_DATE(P_DATA_DT_STR,'YYYYMMDD')
                ) T2
    ON          T1.IACAC_NO = T2.IACAC_NO
    LEFT JOIN   RWA.ORG_INFO T3
    ON          T1.IACGACBR = T3.ORGID
    LEFT JOIN   RWA.CODE_LIBRARY  T4                                      --码表，获取行业
    ON          T2.INDUSTRYID = T4.ITEMNO
    AND         T4.CODENO = 'IndustryType'
    WHERE       T1.IACITMNO = '13070800'                                  --直销银行垫款
    AND         T1.IACCURBAL <> 0                                         --账户余额不等于0
    AND         T1.DATANO = P_DATA_DT_STR
    ;
    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_ZX_CONTRACT',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_ZX_CONTRACT;

    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功' || '-' || v_count1;
    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
         ROLLBACK;
         p_po_rtncode := sqlcode;
         p_po_rtnmsg  := '合同信息('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_ZX_CONTRACT;
/

