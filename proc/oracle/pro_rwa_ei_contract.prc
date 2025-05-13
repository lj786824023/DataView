CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_CONTRACT(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_EI_CONTRACT
    实现功能:汇总合同表,插入所有合同表信息
    数据口径:全量
    跑批频率:月初
    版  本  :V1.0.0
    编写人  :SHUXD
    编写时间:2016-06-01
    单  位	:上海安硕信息技术股份有限公司
    源  表1	:RWA_DEV.RWA_XD_CONTRACT|信贷合同表
    源  表2	:RWA_DEV.RWA_HG_CONTRACT|回购合同表
    源  表3	:RWA_DEV.RWA_LC_CONTRACT|理财合同表
    源  表4	:RWA_DEV.RWA_PJ_CONTRACT|票据合同表
    源  表5	:RWA_DEV.RWA_TY_CONTRACT|同业合同表
    源  表6	:RWA_DEV.RWA_TZ_CONTRACT|投资合同表
    源  表7	:RWA_DEV.RWA_XYK_CONTRACT|信用卡合同表
    源  表8	:RWA_DEV.RWA_GQ_CONTRACT|股权合同表
    源  表9	:RWA_DEV.RWA_ABS_ISSURE_CONTRACT|资产证券化发行机构合同表
    源  表10:RWA_DEV.RWA_DZ_CONTRACT|抵债资产合同表
    源  表11:RWA_DEV.RWA_ZX_CONTRACT|直销银行合同表
    源  表12:RWA_DEV.RWA_YSP_CONTRACT|衍生品业务合同表
    
    目标表  :RWA_DEV.RWA_EI_CONTRACT|合同汇总表
    辅助表  :无
    变更记录(修改人|修改时间|修改内容)：
    pxl  2019/05/08  增加衍生品业务信息到目标表
  	*/
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_CONTRACT';
  --定义判断值变量
  v_count INTEGER;
  --定义异常变量
  v_raise EXCEPTION;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'YYYY-MM-DD hh24:mi:ss'));

    BEGIN
    --删除不存在的分区，如果此存储过程是第一次跑抛出异常，可以忽略
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_CONTRACT DROP PARTITION CONTRACT' || p_data_dt_str;

    COMMIT;

    EXCEPTION
      WHEN OTHERS THEN
        IF (SQLCODE <> '-2149') THEN
          --首次分区truncate会出现2149异常
         p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '汇总风险合同信息表('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
        END IF;
    END;

    --新增一个当前日期下的分区
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_CONTRACT ADD PARTITION CONTRACT' || p_data_dt_str || ' VALUES(TO_DATE('|| p_data_dt_str || ',''YYYYMMDD''))';

    COMMIT;

    /*插入信贷的合同信息*/
    INSERT /*+ APPEND */ INTO RWA_DEV.RWA_EI_CONTRACT(
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
                 DATADATE                                               AS DATADATE             --数据日期
                ,DATANO                                                 AS DATANO               --数据流水号
                ,CONTRACTID                                             AS CONTRACTID           --合同ID
                ,SCONTRACTID                                            AS SCONTRACTID          --源合同ID
                ,SSYSID                                                 AS SSYSID               --源系统ID
                ,CLIENTID                                               AS CLIENTID             --参与主体ID
                ,SORGID                                                 AS SORGID               --源机构ID
                ,SORGNAME                                               AS SORGNAME             --源机构名称
                ,ORGSORTNO                                              AS ORGSORTNO            --所属机构排序号
                ,ORGID                                                  AS ORGID                --所属机构ID
                ,ORGNAME                                                AS ORGNAME              --所属机构名称
                ,NVL(INDUSTRYID,'999999')                               AS INDUSTRYID           --所属行业代码
                ,NVL(INDUSTRYNAME,'未知'）                              AS INDUSTRYNAME         --所属行业名称
                ,BUSINESSLINE                                           AS BUSINESSLINE         --条线
                ,ASSETTYPE                                              AS ASSETTYPE            --资产大类
                ,ASSETSUBTYPE                                           AS ASSETSUBTYPE         --资产小类
                ,BUSINESSTYPEID                                         AS BUSINESSTYPEID       --业务品种代码
                ,BUSINESSTYPENAME                                       AS BUSINESSTYPENAME     --业务品种名称
                ,CREDITRISKDATATYPE                                     AS CREDITRISKDATATYPE   --信用风险数据类型
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')  AS STARTDATE            --起始日期
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')    AS DUEDATE              --到期日期
                ,ORIGINALMATURITY                                       AS ORIGINALMATURITY     --原始期限
                ,RESIDUALM                                              AS RESIDUALM            --剩余期限
                ,SETTLEMENTCURRENCY                                     AS SETTLEMENTCURRENCY   --结算币种
                ,CONTRACTAMOUNT                                         AS CONTRACTAMOUNT       --合同总金额
                ,NOTEXTRACTPART                                         AS NOTEXTRACTPART       --合同未提取部分
                ,UNCONDCANCELFLAG                                       AS UNCONDCANCELFLAG     --是否可随时无条件撤销
                ,ABSUAFLAG                                              AS ABSUAFLAG            --资产证券化基础资产标识
                ,ABSPOOLID                                              AS ABSPOOLID            --资产证券化池ID
                ,GROUPID                                                AS GROUPID              --分组编号
                ,GUARANTEETYPE												 									AS GUARANTEETYPE				--主要担保方式
                ,ABSPROPORTION                                          AS ABSPROPORTION        --资产证券化比重
    FROM 				RWA_DEV.RWA_XD_CONTRACT
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


    /*插入回购的合同信息*/
    INSERT /*+ APPEND */ INTO RWA_DEV.RWA_EI_CONTRACT(
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
                 DATADATE                                               AS DATADATE             --数据日期
                ,DATANO                                                 AS DATANO               --数据流水号
                ,CONTRACTID                                             AS CONTRACTID           --合同ID
                ,SCONTRACTID                                            AS SCONTRACTID          --源合同ID
                ,SSYSID                                                 AS SSYSID               --源系统ID
                ,CLIENTID                                               AS CLIENTID             --参与主体ID
                ,SORGID                                                 AS SORGID               --源机构ID
                ,SORGNAME                                               AS SORGNAME             --源机构名称
                ,ORGSORTNO                                              AS ORGSORTNO            --所属机构排序号
                ,ORGID                                                  AS ORGID                --所属机构ID
                ,ORGNAME                                                AS ORGNAME              --所属机构名称
                ,NVL(INDUSTRYID,'999999')                               AS INDUSTRYID           --所属行业代码
                ,NVL(INDUSTRYNAME,'未知'）                              AS INDUSTRYNAME         --所属行业名称
                ,BUSINESSLINE                                           AS BUSINESSLINE         --条线
                ,ASSETTYPE                                              AS ASSETTYPE            --资产大类
                ,ASSETSUBTYPE                                           AS ASSETSUBTYPE         --资产小类
                ,BUSINESSTYPEID                                         AS BUSINESSTYPEID       --业务品种代码
                ,BUSINESSTYPENAME                                       AS BUSINESSTYPENAME     --业务品种名称
                ,CREDITRISKDATATYPE                                     AS CREDITRISKDATATYPE   --信用风险数据类型
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')  AS STARTDATE            --起始日期
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')    AS DUEDATE              --到期日期
                ,ORIGINALMATURITY                                       AS ORIGINALMATURITY     --原始期限
                ,RESIDUALM                                              AS RESIDUALM            --剩余期限
                ,SETTLEMENTCURRENCY                                     AS SETTLEMENTCURRENCY   --结算币种
                ,CONTRACTAMOUNT                                         AS CONTRACTAMOUNT       --合同总金额
                ,NOTEXTRACTPART                                         AS NOTEXTRACTPART       --合同未提取部分
                ,UNCONDCANCELFLAG                                       AS UNCONDCANCELFLAG     --是否可随时无条件撤销
                ,ABSUAFLAG                                              AS ABSUAFLAG            --资产证券化基础资产标识
                ,ABSPOOLID                                              AS ABSPOOLID            --资产证券化池ID
                ,GROUPID                                                AS GROUPID              --分组编号
                ,GUARANTEETYPE												 									AS GUARANTEETYPE				--主要担保方式
                ,ABSPROPORTION                                          AS ABSPROPORTION        --资产证券化比重
    FROM 				RWA_DEV.RWA_HG_CONTRACT
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


    /*插入理财的合同信息*/
    INSERT /*+ APPEND */ INTO RWA_DEV.RWA_EI_CONTRACT(
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
                 DATADATE                                               AS DATADATE             --数据日期
                ,DATANO                                                 AS DATANO               --数据流水号
                ,CONTRACTID                                             AS CONTRACTID           --合同ID
                ,SCONTRACTID                                            AS SCONTRACTID          --源合同ID
                ,SSYSID                                                 AS SSYSID               --源系统ID
                ,CLIENTID                                               AS CLIENTID             --参与主体ID
                ,SORGID                                                 AS SORGID               --源机构ID
                ,SORGNAME                                               AS SORGNAME             --源机构名称
                ,ORGSORTNO                                              AS ORGSORTNO            --所属机构排序号
                ,ORGID                                                  AS ORGID                --所属机构ID
                ,ORGNAME                                                AS ORGNAME              --所属机构名称
                ,NVL(INDUSTRYID,'999999')                               AS INDUSTRYID           --所属行业代码
                ,NVL(INDUSTRYNAME,'未知'）                              AS INDUSTRYNAME         --所属行业名称
                ,BUSINESSLINE                                           AS BUSINESSLINE         --条线
                ,ASSETTYPE                                              AS ASSETTYPE            --资产大类
                ,ASSETSUBTYPE                                           AS ASSETSUBTYPE         --资产小类
                ,BUSINESSTYPEID                                         AS BUSINESSTYPEID       --业务品种代码
                ,BUSINESSTYPENAME                                       AS BUSINESSTYPENAME     --业务品种名称
                ,CREDITRISKDATATYPE                                     AS CREDITRISKDATATYPE   --信用风险数据类型
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')  AS STARTDATE            --起始日期
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')    AS DUEDATE              --到期日期
                ,ORIGINALMATURITY                                       AS ORIGINALMATURITY     --原始期限
                ,RESIDUALM                                              AS RESIDUALM            --剩余期限
                ,SETTLEMENTCURRENCY                                     AS SETTLEMENTCURRENCY   --结算币种
                ,CONTRACTAMOUNT                                         AS CONTRACTAMOUNT       --合同总金额
                ,NOTEXTRACTPART                                         AS NOTEXTRACTPART       --合同未提取部分
                ,UNCONDCANCELFLAG                                       AS UNCONDCANCELFLAG     --是否可随时无条件撤销
                ,ABSUAFLAG                                              AS ABSUAFLAG            --资产证券化基础资产标识
                ,ABSPOOLID                                              AS ABSPOOLID            --资产证券化池ID
                ,GROUPID                                                AS GROUPID              --分组编号
                ,GUARANTEETYPE												 									AS GUARANTEETYPE				--主要担保方式
                ,ABSPROPORTION                                          AS ABSPROPORTION        --资产证券化比重
    FROM 				RWA_DEV.RWA_LC_CONTRACT
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


    /*插入票据的合同信息*/
    INSERT /*+ APPEND */ INTO RWA_DEV.RWA_EI_CONTRACT(
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
                 DATADATE                                               AS DATADATE             --数据日期
                ,DATANO                                                 AS DATANO               --数据流水号
                ,CONTRACTID                                             AS CONTRACTID           --合同ID
                ,SCONTRACTID                                            AS SCONTRACTID          --源合同ID
                ,SSYSID                                                 AS SSYSID               --源系统ID
                ,CLIENTID                                               AS CLIENTID             --参与主体ID
                ,SORGID                                                 AS SORGID               --源机构ID
                ,SORGNAME                                               AS SORGNAME             --源机构名称
                ,ORGSORTNO                                              AS ORGSORTNO            --所属机构排序号
                ,ORGID                                                  AS ORGID                --所属机构ID
                ,ORGNAME                                                AS ORGNAME              --所属机构名称
                ,NVL(INDUSTRYID,'999999')                               AS INDUSTRYID           --所属行业代码
                ,NVL(INDUSTRYNAME,'未知'）                              AS INDUSTRYNAME         --所属行业名称
                ,BUSINESSLINE                                           AS BUSINESSLINE         --条线
                ,ASSETTYPE                                              AS ASSETTYPE            --资产大类
                ,ASSETSUBTYPE                                           AS ASSETSUBTYPE         --资产小类
                ,BUSINESSTYPEID                                         AS BUSINESSTYPEID       --业务品种代码
                ,BUSINESSTYPENAME                                       AS BUSINESSTYPENAME     --业务品种名称
                ,CREDITRISKDATATYPE                                     AS CREDITRISKDATATYPE   --信用风险数据类型
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')  AS STARTDATE            --起始日期
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')    AS DUEDATE              --到期日期
                ,ORIGINALMATURITY                                       AS ORIGINALMATURITY     --原始期限
                ,RESIDUALM                                              AS RESIDUALM            --剩余期限
                ,SETTLEMENTCURRENCY                                     AS SETTLEMENTCURRENCY   --结算币种
                ,CONTRACTAMOUNT                                         AS CONTRACTAMOUNT       --合同总金额
                ,NOTEXTRACTPART                                         AS NOTEXTRACTPART       --合同未提取部分
                ,UNCONDCANCELFLAG                                       AS UNCONDCANCELFLAG     --是否可随时无条件撤销
                ,ABSUAFLAG                                              AS ABSUAFLAG            --资产证券化基础资产标识
                ,ABSPOOLID                                              AS ABSPOOLID            --资产证券化池ID
                ,GROUPID                                                AS GROUPID              --分组编号
                ,GUARANTEETYPE												 									AS GUARANTEETYPE				--主要担保方式
                ,ABSPROPORTION                                          AS ABSPROPORTION        --资产证券化比重
    FROM 				RWA_DEV.RWA_PJ_CONTRACT
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


    /*插入同业的和同信息*/
    INSERT /*+ APPEND */ INTO RWA_DEV.RWA_EI_CONTRACT(
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
                 DATADATE                                               AS DATADATE             --数据日期
                ,DATANO                                                 AS DATANO               --数据流水号
                ,CONTRACTID                                             AS CONTRACTID           --合同ID
                ,SCONTRACTID                                            AS SCONTRACTID          --源合同ID
                ,SSYSID                                                 AS SSYSID               --源系统ID
                ,CLIENTID                                               AS CLIENTID             --参与主体ID
                ,SORGID                                                 AS SORGID               --源机构ID
                ,SORGNAME                                               AS SORGNAME             --源机构名称
                ,ORGSORTNO                                              AS ORGSORTNO            --所属机构排序号
                ,ORGID                                                  AS ORGID                --所属机构ID
                ,ORGNAME                                                AS ORGNAME              --所属机构名称
                ,NVL(INDUSTRYID,'999999')                               AS INDUSTRYID           --所属行业代码
                ,NVL(INDUSTRYNAME,'未知'）                              AS INDUSTRYNAME         --所属行业名称
                ,BUSINESSLINE                                           AS BUSINESSLINE         --条线
                ,ASSETTYPE                                              AS ASSETTYPE            --资产大类
                ,ASSETSUBTYPE                                           AS ASSETSUBTYPE         --资产小类
                ,BUSINESSTYPEID                                         AS BUSINESSTYPEID       --业务品种代码
                ,BUSINESSTYPENAME                                       AS BUSINESSTYPENAME     --业务品种名称
                ,CREDITRISKDATATYPE                                     AS CREDITRISKDATATYPE   --信用风险数据类型
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')  AS STARTDATE            --起始日期
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')    AS DUEDATE              --到期日期
                ,ORIGINALMATURITY                                       AS ORIGINALMATURITY     --原始期限
                ,RESIDUALM                                              AS RESIDUALM            --剩余期限
                ,SETTLEMENTCURRENCY                                     AS SETTLEMENTCURRENCY   --结算币种
                ,CONTRACTAMOUNT                                         AS CONTRACTAMOUNT       --合同总金额
                ,NOTEXTRACTPART                                         AS NOTEXTRACTPART       --合同未提取部分
                ,UNCONDCANCELFLAG                                       AS UNCONDCANCELFLAG     --是否可随时无条件撤销
                ,ABSUAFLAG                                              AS ABSUAFLAG            --资产证券化基础资产标识
                ,ABSPOOLID                                              AS ABSPOOLID            --资产证券化池ID
                ,GROUPID                                                AS GROUPID              --分组编号
                ,GUARANTEETYPE												 									AS GUARANTEETYPE				--主要担保方式
                ,ABSPROPORTION                                          AS ABSPROPORTION        --资产证券化比重
    FROM 				RWA_DEV.RWA_TY_CONTRACT
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


    /*插入投资的合同信息*/
    INSERT /*+ APPEND */ INTO RWA_DEV.RWA_EI_CONTRACT(
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
                 DATADATE                                               AS DATADATE             --数据日期
                ,DATANO                                                 AS DATANO               --数据流水号
                ,CONTRACTID                                             AS CONTRACTID           --合同ID
                ,SCONTRACTID                                            AS SCONTRACTID          --源合同ID
                ,SSYSID                                                 AS SSYSID               --源系统ID
                ,CLIENTID                                               AS CLIENTID             --参与主体ID
                ,SORGID                                                 AS SORGID               --源机构ID
                ,SORGNAME                                               AS SORGNAME             --源机构名称
                ,ORGSORTNO                                              AS ORGSORTNO            --所属机构排序号
                ,ORGID                                                  AS ORGID                --所属机构ID
                ,ORGNAME                                                AS ORGNAME              --所属机构名称
                ,NVL(INDUSTRYID,'999999')                               AS INDUSTRYID           --所属行业代码
                ,NVL(INDUSTRYNAME,'未知'）                              AS INDUSTRYNAME         --所属行业名称
                ,BUSINESSLINE                                           AS BUSINESSLINE         --条线
                ,ASSETTYPE                                              AS ASSETTYPE            --资产大类
                ,ASSETSUBTYPE                                           AS ASSETSUBTYPE         --资产小类
                ,BUSINESSTYPEID                                         AS BUSINESSTYPEID       --业务品种代码
                ,BUSINESSTYPENAME                                       AS BUSINESSTYPENAME     --业务品种名称
                ,CREDITRISKDATATYPE                                     AS CREDITRISKDATATYPE   --信用风险数据类型
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')  AS STARTDATE            --起始日期
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')    AS DUEDATE              --到期日期
                ,ORIGINALMATURITY                                       AS ORIGINALMATURITY     --原始期限
                ,RESIDUALM                                              AS RESIDUALM            --剩余期限
                ,SETTLEMENTCURRENCY                                     AS SETTLEMENTCURRENCY   --结算币种
                ,CONTRACTAMOUNT                                         AS CONTRACTAMOUNT       --合同总金额
                ,NOTEXTRACTPART                                         AS NOTEXTRACTPART       --合同未提取部分
                ,UNCONDCANCELFLAG                                       AS UNCONDCANCELFLAG     --是否可随时无条件撤销
                ,ABSUAFLAG                                              AS ABSUAFLAG            --资产证券化基础资产标识
                ,ABSPOOLID                                              AS ABSPOOLID            --资产证券化池ID
                ,GROUPID                                                AS GROUPID              --分组编号
                ,GUARANTEETYPE												 									AS GUARANTEETYPE				--主要担保方式
                ,ABSPROPORTION                                          AS ABSPROPORTION        --资产证券化比重
    FROM 				RWA_DEV.RWA_TZ_CONTRACT
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


    /*插入信用卡的合同信息*/
    INSERT /*+ APPEND */ INTO RWA_DEV.RWA_EI_CONTRACT(
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
                 DATADATE                                               AS DATADATE             --数据日期
                ,DATANO                                                 AS DATANO               --数据流水号
                ,CONTRACTID                                             AS CONTRACTID           --合同ID
                ,SCONTRACTID                                            AS SCONTRACTID          --源合同ID
                ,SSYSID                                                 AS SSYSID               --源系统ID
                ,CLIENTID                                               AS CLIENTID             --参与主体ID
                ,SORGID                                                 AS SORGID               --源机构ID
                ,SORGNAME                                               AS SORGNAME             --源机构名称
                ,ORGSORTNO                                              AS ORGSORTNO            --所属机构排序号
                ,ORGID                                                  AS ORGID                --所属机构ID
                ,ORGNAME                                                AS ORGNAME              --所属机构名称
                ,NVL(INDUSTRYID,'999999')                               AS INDUSTRYID           --所属行业代码
                ,NVL(INDUSTRYNAME,'未知'）                              AS INDUSTRYNAME         --所属行业名称
                ,BUSINESSLINE                                           AS BUSINESSLINE         --条线
                ,ASSETTYPE                                              AS ASSETTYPE            --资产大类
                ,ASSETSUBTYPE                                           AS ASSETSUBTYPE         --资产小类
                ,BUSINESSTYPEID                                         AS BUSINESSTYPEID       --业务品种代码
                ,BUSINESSTYPENAME                                       AS BUSINESSTYPENAME     --业务品种名称
                ,CREDITRISKDATATYPE                                     AS CREDITRISKDATATYPE   --信用风险数据类型
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')  AS STARTDATE            --起始日期
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')    AS DUEDATE              --到期日期
                ,ORIGINALMATURITY                                       AS ORIGINALMATURITY     --原始期限
                ,RESIDUALM                                              AS RESIDUALM            --剩余期限
                ,SETTLEMENTCURRENCY                                     AS SETTLEMENTCURRENCY   --结算币种
                ,CONTRACTAMOUNT                                         AS CONTRACTAMOUNT       --合同总金额
                ,NOTEXTRACTPART                                         AS NOTEXTRACTPART       --合同未提取部分
                ,UNCONDCANCELFLAG                                       AS UNCONDCANCELFLAG     --是否可随时无条件撤销
                ,ABSUAFLAG                                              AS ABSUAFLAG            --资产证券化基础资产标识
                ,ABSPOOLID                                              AS ABSPOOLID            --资产证券化池ID
                ,GROUPID                                                AS GROUPID              --分组编号
                ,GUARANTEETYPE												 									AS GUARANTEETYPE				--主要担保方式
                ,ABSPROPORTION                                          AS ABSPROPORTION        --资产证券化比重
    FROM 				RWA_DEV.RWA_XYK_CONTRACT A
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND NOT EXISTS(SELECT 1 FROM RWA_TZ_CONTRACT T WHERE A.CONTRACTID=T.CONTRACTID 
        AND T.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD') )
    ;
    COMMIT;


    /*插入股权的合同信息*/
    INSERT /*+ APPEND */ INTO RWA_DEV.RWA_EI_CONTRACT(
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
                 DATADATE                                               AS DATADATE             --数据日期
                ,DATANO                                                 AS DATANO               --数据流水号
                ,CONTRACTID                                             AS CONTRACTID           --合同ID
                ,SCONTRACTID                                            AS SCONTRACTID          --源合同ID
                ,SSYSID                                                 AS SSYSID               --源系统ID
                ,CLIENTID                                               AS CLIENTID             --参与主体ID
                ,SORGID                                                 AS SORGID               --源机构ID
                ,SORGNAME                                               AS SORGNAME             --源机构名称
                ,ORGSORTNO                                              AS ORGSORTNO            --所属机构排序号
                ,ORGID                                                  AS ORGID                --所属机构ID
                ,ORGNAME                                                AS ORGNAME              --所属机构名称
                ,NVL(INDUSTRYID,'999999')                               AS INDUSTRYID           --所属行业代码
                ,NVL(INDUSTRYNAME,'未知'）                              AS INDUSTRYNAME         --所属行业名称
                ,BUSINESSLINE                                           AS BUSINESSLINE         --条线
                ,ASSETTYPE                                              AS ASSETTYPE            --资产大类
                ,ASSETSUBTYPE                                           AS ASSETSUBTYPE         --资产小类
                ,BUSINESSTYPEID                                         AS BUSINESSTYPEID       --业务品种代码
                ,BUSINESSTYPENAME                                       AS BUSINESSTYPENAME     --业务品种名称
                ,CREDITRISKDATATYPE                                     AS CREDITRISKDATATYPE   --信用风险数据类型
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')  AS STARTDATE            --起始日期
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')    AS DUEDATE              --到期日期
                ,ORIGINALMATURITY                                       AS ORIGINALMATURITY     --原始期限
                ,RESIDUALM                                              AS RESIDUALM            --剩余期限
                ,SETTLEMENTCURRENCY                                     AS SETTLEMENTCURRENCY   --结算币种
                ,CONTRACTAMOUNT                                         AS CONTRACTAMOUNT       --合同总金额
                ,NOTEXTRACTPART                                         AS NOTEXTRACTPART       --合同未提取部分
                ,UNCONDCANCELFLAG                                       AS UNCONDCANCELFLAG     --是否可随时无条件撤销
                ,ABSUAFLAG                                              AS ABSUAFLAG            --资产证券化基础资产标识
                ,ABSPOOLID                                              AS ABSPOOLID            --资产证券化池ID
                ,GROUPID                                                AS GROUPID              --分组编号
                ,GUARANTEETYPE												 									AS GUARANTEETYPE				--主要担保方式
                ,ABSPROPORTION                                          AS ABSPROPORTION        --资产证券化比重
    FROM 				RWA_DEV.RWA_GQ_CONTRACT
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


    /*插入资产证券化发行机构的合同信息*/
    INSERT /*+ APPEND */ INTO RWA_DEV.RWA_EI_CONTRACT(
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
                 DATADATE                                               AS DATADATE             --数据日期
                ,DATANO                                                 AS DATANO               --数据流水号
                ,CONTRACTID                                             AS CONTRACTID           --合同ID
                ,SCONTRACTID                                            AS SCONTRACTID          --源合同ID
                ,SSYSID                                                 AS SSYSID               --源系统ID
                ,CLIENTID                                               AS CLIENTID             --参与主体ID
                ,SORGID                                                 AS SORGID               --源机构ID
                ,SORGNAME                                               AS SORGNAME             --源机构名称
                ,ORGSORTNO                                              AS ORGSORTNO            --所属机构排序号
                ,ORGID                                                  AS ORGID                --所属机构ID
                ,ORGNAME                                                AS ORGNAME              --所属机构名称
                ,NVL(INDUSTRYID,'999999')                               AS INDUSTRYID           --所属行业代码
                ,NVL(INDUSTRYNAME,'未知'）                              AS INDUSTRYNAME         --所属行业名称
                ,BUSINESSLINE                                           AS BUSINESSLINE         --条线
                ,ASSETTYPE                                              AS ASSETTYPE            --资产大类
                ,ASSETSUBTYPE                                           AS ASSETSUBTYPE         --资产小类
                ,BUSINESSTYPEID                                         AS BUSINESSTYPEID       --业务品种代码
                ,BUSINESSTYPENAME                                       AS BUSINESSTYPENAME     --业务品种名称
                ,CREDITRISKDATATYPE                                     AS CREDITRISKDATATYPE   --信用风险数据类型
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')  AS STARTDATE            --起始日期
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')    AS DUEDATE              --到期日期
                ,ORIGINALMATURITY                                       AS ORIGINALMATURITY     --原始期限
                ,RESIDUALM                                              AS RESIDUALM            --剩余期限
                ,SETTLEMENTCURRENCY                                     AS SETTLEMENTCURRENCY   --结算币种
                ,CONTRACTAMOUNT                                         AS CONTRACTAMOUNT       --合同总金额
                ,NOTEXTRACTPART                                         AS NOTEXTRACTPART       --合同未提取部分
                ,UNCONDCANCELFLAG                                       AS UNCONDCANCELFLAG     --是否可随时无条件撤销
                ,ABSUAFLAG                                              AS ABSUAFLAG            --资产证券化基础资产标识
                ,ABSPOOLID                                              AS ABSPOOLID            --资产证券化池ID
                ,GROUPID                                                AS GROUPID              --分组编号
                ,GUARANTEETYPE												 									AS GUARANTEETYPE				--主要担保方式
                ,ABSPROPORTION                                          AS ABSPROPORTION        --资产证券化比重
    FROM 				RWA_DEV.RWA_ABS_ISSURE_CONTRACT
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;

    /*插入抵债资产的合同信息*/
    INSERT /*+ APPEND */ INTO RWA_DEV.RWA_EI_CONTRACT(
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
                 DATADATE                                               AS DATADATE             --数据日期
                ,DATANO                                                 AS DATANO               --数据流水号
                ,CONTRACTID                                             AS CONTRACTID           --合同ID
                ,SCONTRACTID                                            AS SCONTRACTID          --源合同ID
                ,SSYSID                                                 AS SSYSID               --源系统ID
                ,CLIENTID                                               AS CLIENTID             --参与主体ID
                ,SORGID                                                 AS SORGID               --源机构ID
                ,SORGNAME                                               AS SORGNAME             --源机构名称
                ,ORGSORTNO                                              AS ORGSORTNO            --所属机构排序号
                ,ORGID                                                  AS ORGID                --所属机构ID
                ,ORGNAME                                                AS ORGNAME              --所属机构名称
                ,NVL(INDUSTRYID,'999999')                               AS INDUSTRYID           --所属行业代码
                ,NVL(INDUSTRYNAME,'未知'）                              AS INDUSTRYNAME         --所属行业名称
                ,BUSINESSLINE                                           AS BUSINESSLINE         --条线
                ,ASSETTYPE                                              AS ASSETTYPE            --资产大类
                ,ASSETSUBTYPE                                           AS ASSETSUBTYPE         --资产小类
                ,BUSINESSTYPEID                                         AS BUSINESSTYPEID       --业务品种代码
                ,BUSINESSTYPENAME                                       AS BUSINESSTYPENAME     --业务品种名称
                ,CREDITRISKDATATYPE                                     AS CREDITRISKDATATYPE   --信用风险数据类型
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')  AS STARTDATE            --起始日期
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')    AS DUEDATE              --到期日期
                ,ORIGINALMATURITY                                       AS ORIGINALMATURITY     --原始期限
                ,RESIDUALM                                              AS RESIDUALM            --剩余期限
                ,SETTLEMENTCURRENCY                                     AS SETTLEMENTCURRENCY   --结算币种
                ,CONTRACTAMOUNT                                         AS CONTRACTAMOUNT       --合同总金额
                ,NOTEXTRACTPART                                         AS NOTEXTRACTPART       --合同未提取部分
                ,UNCONDCANCELFLAG                                       AS UNCONDCANCELFLAG     --是否可随时无条件撤销
                ,ABSUAFLAG                                              AS ABSUAFLAG            --资产证券化基础资产标识
                ,ABSPOOLID                                              AS ABSPOOLID            --资产证券化池ID
                ,GROUPID                                                AS GROUPID              --分组编号
                ,GUARANTEETYPE												 									AS GUARANTEETYPE				--主要担保方式
                ,ABSPROPORTION                                          AS ABSPROPORTION        --资产证券化比重
    FROM 				RWA_DEV.RWA_DZ_CONTRACT
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;

     /*插入直销银行的合同信息*/
    INSERT /*+ APPEND */ INTO RWA_DEV.RWA_EI_CONTRACT(
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
                 DATADATE                                               AS DATADATE             --数据日期
                ,DATANO                                                 AS DATANO               --数据流水号
                ,CONTRACTID                                             AS CONTRACTID           --合同ID
                ,SCONTRACTID                                            AS SCONTRACTID          --源合同ID
                ,SSYSID                                                 AS SSYSID               --源系统ID
                ,CLIENTID                                               AS CLIENTID             --参与主体ID
                ,SORGID                                                 AS SORGID               --源机构ID
                ,SORGNAME                                               AS SORGNAME             --源机构名称
                ,ORGSORTNO                                              AS ORGSORTNO            --所属机构排序号
                ,ORGID                                                  AS ORGID                --所属机构ID
                ,ORGNAME                                                AS ORGNAME              --所属机构名称
                ,NVL(INDUSTRYID,'999999')                               AS INDUSTRYID           --所属行业代码
                ,NVL(INDUSTRYNAME,'未知'）                              AS INDUSTRYNAME         --所属行业名称
                ,BUSINESSLINE                                           AS BUSINESSLINE         --条线
                ,ASSETTYPE                                              AS ASSETTYPE            --资产大类
                ,ASSETSUBTYPE                                           AS ASSETSUBTYPE         --资产小类
                ,BUSINESSTYPEID                                         AS BUSINESSTYPEID       --业务品种代码
                ,BUSINESSTYPENAME                                       AS BUSINESSTYPENAME     --业务品种名称
                ,CREDITRISKDATATYPE                                     AS CREDITRISKDATATYPE   --信用风险数据类型
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')  AS STARTDATE            --起始日期
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')    AS DUEDATE              --到期日期
                ,ORIGINALMATURITY                                       AS ORIGINALMATURITY     --原始期限
                ,RESIDUALM                                              AS RESIDUALM            --剩余期限
                ,SETTLEMENTCURRENCY                                     AS SETTLEMENTCURRENCY   --结算币种
                ,CONTRACTAMOUNT                                         AS CONTRACTAMOUNT       --合同总金额
                ,NOTEXTRACTPART                                         AS NOTEXTRACTPART       --合同未提取部分
                ,UNCONDCANCELFLAG                                       AS UNCONDCANCELFLAG     --是否可随时无条件撤销
                ,ABSUAFLAG                                              AS ABSUAFLAG            --资产证券化基础资产标识
                ,ABSPOOLID                                              AS ABSPOOLID            --资产证券化池ID
                ,GROUPID                                                AS GROUPID              --分组编号
                ,GUARANTEETYPE												 									AS GUARANTEETYPE				--主要担保方式
                ,ABSPROPORTION                                          AS ABSPROPORTION        --资产证券化比重
    FROM 				RWA_DEV.RWA_ZX_CONTRACT
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;
    
    
     /*插入衍生品业务的合同信息*/
    INSERT /*+ APPEND */ INTO RWA_DEV.RWA_EI_CONTRACT(
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
                 DATADATE                                               AS DATADATE             --数据日期
                ,DATANO                                                 AS DATANO               --数据流水号
                ,CONTRACTID                                             AS CONTRACTID           --合同ID
                ,SCONTRACTID                                            AS SCONTRACTID          --源合同ID
                ,SSYSID                                                 AS SSYSID               --源系统ID
                ,CLIENTID                                               AS CLIENTID             --参与主体ID
                ,SORGID                                                 AS SORGID               --源机构ID
                ,SORGNAME                                               AS SORGNAME             --源机构名称
                ,ORGSORTNO                                              AS ORGSORTNO            --所属机构排序号
                ,ORGID                                                  AS ORGID                --所属机构ID
                ,ORGNAME                                                AS ORGNAME              --所属机构名称
                ,NVL(INDUSTRYID,'999999')                               AS INDUSTRYID           --所属行业代码
                ,NVL(INDUSTRYNAME,'未知'）                              AS INDUSTRYNAME         --所属行业名称
                ,BUSINESSLINE                                           AS BUSINESSLINE         --条线
                ,ASSETTYPE                                              AS ASSETTYPE            --资产大类
                ,ASSETSUBTYPE                                           AS ASSETSUBTYPE         --资产小类
                ,BUSINESSTYPEID                                         AS BUSINESSTYPEID       --业务品种代码
                ,BUSINESSTYPENAME                                       AS BUSINESSTYPENAME     --业务品种名称
                ,CREDITRISKDATATYPE                                     AS CREDITRISKDATATYPE   --信用风险数据类型
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')  AS STARTDATE            --起始日期
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')    AS DUEDATE              --到期日期
                ,ORIGINALMATURITY                                       AS ORIGINALMATURITY     --原始期限
                ,RESIDUALM                                              AS RESIDUALM            --剩余期限
                ,SETTLEMENTCURRENCY                                     AS SETTLEMENTCURRENCY   --结算币种
                ,CONTRACTAMOUNT                                         AS CONTRACTAMOUNT       --合同总金额
                ,NOTEXTRACTPART                                         AS NOTEXTRACTPART       --合同未提取部分
                ,UNCONDCANCELFLAG                                       AS UNCONDCANCELFLAG     --是否可随时无条件撤销
                ,ABSUAFLAG                                              AS ABSUAFLAG            --资产证券化基础资产标识
                ,ABSPOOLID                                              AS ABSPOOLID            --资产证券化池ID
                ,GROUPID                                                AS GROUPID              --分组编号
                ,GUARANTEETYPE                                          AS GUARANTEETYPE        --主要担保方式
                ,ABSPROPORTION                                          AS ABSPROPORTION        --资产证券化比重
    FROM        RWA_DEV.RWA_YSP_CONTRACT
    WHERE       DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;
    
    
    /*插入消费金融业务的合同信息*/
    INSERT /*+ APPEND */ INTO RWA_DEV.RWA_EI_CONTRACT(
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
                 DATADATE                                               AS DATADATE             --数据日期
                ,DATANO                                                 AS DATANO               --数据流水号
                ,CONTRACTID                                             AS CONTRACTID           --合同ID
                ,SCONTRACTID                                            AS SCONTRACTID          --源合同ID
                ,SSYSID                                                 AS SSYSID               --源系统ID
                ,CLIENTID                                               AS CLIENTID             --参与主体ID
                ,SORGID                                                 AS SORGID               --源机构ID
                ,SORGNAME                                               AS SORGNAME             --源机构名称
                ,ORGSORTNO                                              AS ORGSORTNO            --所属机构排序号
                ,ORGID                                                  AS ORGID                --所属机构ID
                ,ORGNAME                                                AS ORGNAME              --所属机构名称
                ,NVL(INDUSTRYID,'999999')                               AS INDUSTRYID           --所属行业代码
                ,NVL(INDUSTRYNAME,'未知'）                              AS INDUSTRYNAME         --所属行业名称
                ,BUSINESSLINE                                           AS BUSINESSLINE         --条线
                ,ASSETTYPE                                              AS ASSETTYPE            --资产大类
                ,ASSETSUBTYPE                                           AS ASSETSUBTYPE         --资产小类
                ,BUSINESSTYPEID                                         AS BUSINESSTYPEID       --业务品种代码
                ,BUSINESSTYPENAME                                       AS BUSINESSTYPENAME     --业务品种名称
                ,CREDITRISKDATATYPE                                     AS CREDITRISKDATATYPE   --信用风险数据类型
                ,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')  AS STARTDATE            --起始日期
                ,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')    AS DUEDATE              --到期日期
                ,ORIGINALMATURITY                                       AS ORIGINALMATURITY     --原始期限
                ,RESIDUALM                                              AS RESIDUALM            --剩余期限
                ,SETTLEMENTCURRENCY                                     AS SETTLEMENTCURRENCY   --结算币种
                ,CONTRACTAMOUNT                                         AS CONTRACTAMOUNT       --合同总金额
                ,NOTEXTRACTPART                                         AS NOTEXTRACTPART       --合同未提取部分
                ,UNCONDCANCELFLAG                                       AS UNCONDCANCELFLAG     --是否可随时无条件撤销
                ,ABSUAFLAG                                              AS ABSUAFLAG            --资产证券化基础资产标识
                ,ABSPOOLID                                              AS ABSPOOLID            --资产证券化池ID
                ,GROUPID                                                AS GROUPID              --分组编号
                ,GUARANTEETYPE                                          AS GUARANTEETYPE        --主要担保方式
                ,ABSPROPORTION                                          AS ABSPROPORTION        --资产证券化比重
    FROM        RWA_DEV.RWA_XF_CONTRACT
    WHERE       DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;
    
    -----------20191120 wzb---
       update rwa_ei_contract
   set ORGID='9998',SORGID='9998',ORGNAME='特殊处理'
   where  ORGID IS NULL AND  datano=P_DATA_DT_STR;
  COMMIT;
    
    --整理表信息
    --dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_CONTRACT',cascade => true);
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_CONTRACT',partname => 'CONTRACT'||p_data_dt_str,granularity => 'PARTITION',cascade => true);


    /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_CONTRACT WHERE DATANO = p_data_dt_str;
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_CONTRACT表当前插入的数据记录为:' || v_count1 || '条');

    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'YYYY-MM-DD hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功'||'-'||v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '信用风险合同表(RWA_DEV.PRO_RWA_EI_CONTRACT)ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_EI_CONTRACT;
/

