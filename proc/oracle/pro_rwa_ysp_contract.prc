CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_YSP_CONTRACT(P_DATA_DT_STR IN VARCHAR2, --数据日期 yyyyMMdd
                                                      P_PO_RTNCODE  OUT VARCHAR2, --返回编号 1 成功,0 失败
                                                      P_PO_RTNMSG   OUT VARCHAR2 --返回描述
                                                      )
/*
  存储过程名称:RWA_DEV.PRO_RWA_YSP_CONTRACT
  实现功能:财务系统-衍生品业务-合同信息(从数据源财务系统将合同相关信息导入RWA衍生品接口表合同信息表中)
  数据口径:全量
  跑批频率:月初运行
  版  本  :V1.0.0
  编写人  :
  编写时间:2019-04-17
  单  位  :上海安硕信息技术股份有限公司
  源  表1 :RWA_DEV.BRD_SWAP|互换表
  源  表2 :RWA.ORG_INFO|机构信息表
  源  表3 :RWA_DEV.NCM_CUSTOMER_INFO|客户信息表
  源  表4 :RWA.CODE_LIBRARY|代码库表
  变更记录(修改人|修改时间|修改内容):
  */
 AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  V_PRO_NAME VARCHAR2(200) := 'RWA_DEV.PRO_RWA_YSP_CONTRACT';
  --定义异常变量
  V_RAISE EXCEPTION;
  --定义当前插入的记录数
  V_COUNT INTEGER;

BEGIN
  --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

  /*如果是全量数据加载需清空目标表*/
  --1.清除目标表中的原有记录
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_YSP_CONTRACT';

  --2.将满足条件的数据从源表插入到目标表中
  INSERT INTO RWA_DEV.RWA_YSP_CONTRACT
    (DATADATE, --01数据日期
     DATANO, --02数据流水号
     CONTRACTID, --03合同ID
     SCONTRACTID, --04源合同ID
     SSYSID, --05源系统ID
     CLIENTID, --06参与主体ID
     SORGID, --07源机构ID
     SORGNAME, --08源机构名称
     ORGSORTNO, --09所属机构排序号
     ORGID, --10所属机构ID
     ORGNAME, --11所属机构名称
     INDUSTRYID, --12所属行业代码
     INDUSTRYNAME, --13所属行业名称
     BUSINESSLINE, --14业务条线
     ASSETTYPE, --15资产大类
     ASSETSUBTYPE, --16资产小类
     BUSINESSTYPEID, --17业务品种代码
     BUSINESSTYPENAME, --18业务品种名称
     CREDITRISKDATATYPE, --19信用风险数据类型
     STARTDATE, --20起始日期
     DUEDATE, --21到期日期
     ORIGINALMATURITY, --22原始期限
     RESIDUALM, --23剩余期限
     SETTLEMENTCURRENCY, --24结算币种
     CONTRACTAMOUNT, --25合同总金额
     NOTEXTRACTPART, --26合同未提取部分
     UNCONDCANCELFLAG, --27是否可随时无条件撤销
     ABSUAFLAG, --28资产证券化基础资产标识
     ABSPOOLID, --29证券化资产池ID
     GROUPID, --30分组编号
     GUARANTEETYPE, --31主要担保方式
     ABSPROPORTION --32资产证券化比重
     )
    SELECT TO_DATE(P_DATA_DT_STR, 'YYYYMMDD'), --01数据日期
           P_DATA_DT_STR, --02数据流水号
           T1.CONTRACTID, --03合同ID
           T1.CONTRACTID, --04源合同ID
           'YSP', --05源系统ID
           T1.CLIENTID, --06参与主体ID
           T1.SORGID, --07源机构ID
           T1.SORGNAME, --08源机构名称
           T1.ORGSORTNO, --09所属机构排序号
           T1.ORGID, --10所属机构ID
           T1.ORGNAME, --11所属机构名称
           T1.INDUSTRYID, --12所属行业代码
           T1.INDUSTRYNAME, --13所属行业名称
           T1.BUSINESSLINE, --14业务条线  0401:同业-金融市场部
           T1.ASSETTYPE, --15资产大类
           T1.ASSETSUBTYPE, --16资产小类
           T1.BUSINESSTYPEID, --17业务品种代码
           T1.BUSINESSTYPENAME, --18业务品种名称
           '07', --19信用风险数据类型 07:中央交易对手
           T1.STARTDATE, --20起始日期
           T1.DUEDATE, --21到期日期
           T1.ORIGINALMATURITY, --22原始期限
           T1.RESIDUALM, --23剩余期限
           T1.CURRENCY, --24结算币种
           T1.NORMALPRINCIPAL, --25合同总金额
           0, --26合同未提取部分
           '0', --27是否可随时无条件撤销
           '0', --28资产证券化基础资产标识
           '', --29证券化资产池ID
           '', --30分组编号
           '', --31主要担保方式
           0 --32资产证券化比重
      FROM RWA_YSP_EXPOSURE T1
     WHERE T1.DATANO = P_DATA_DT_STR;

  COMMIT;

  DBMS_STATS.GATHER_TABLE_STATS(OWNNAME => 'RWA_DEV',
                                TABNAME => 'RWA_YSP_CONTRACT',
                                CASCADE => TRUE);

  /*目标表数据统计*/
  --统计插入的记录数
  SELECT COUNT(1) INTO V_COUNT FROM RWA_DEV.RWA_YSP_CONTRACT;
  --Dbms_output.Put_line('RWA_DEV.RWA_TZ_CONTRACT表当前插入的财务系统-应收款投资数据记录为: ' || (v_count1 - v_count) || ' 条');
  --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

  P_PO_RTNCODE := '1';
  P_PO_RTNMSG  := '成功' || '-' || V_COUNT;
  --定义异常
EXCEPTION
  WHEN OTHERS THEN
    --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
    ROLLBACK;
    P_PO_RTNCODE := SQLCODE;
    P_PO_RTNMSG  := '合同信息(' || V_PRO_NAME || ')ETL转换失败！' || SQLERRM ||
                    ';错误行数为:' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    RETURN;
END PRO_RWA_YSP_CONTRACT;
/

