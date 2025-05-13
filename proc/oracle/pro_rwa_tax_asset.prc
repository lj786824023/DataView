CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_TAX_ASSET (
                            p_data_dt_str  IN   VARCHAR2,    --数据日期
                            p_po_rtncode   OUT  VARCHAR2,    --返回编号
                            p_po_rtnmsg    OUT  VARCHAR2     --返回描述
)
  /*
    存储过程名称:RWA_DEV.pro_rwa_tax_asset
    实现功能:净递延税资产虚拟、金融机构股权投资（未扣除部分）虚拟
    数据口径:全量
    跑批频率:月末运行
    版  本  :V1.0.0
    编写人  :qpzhong
    编写时间:2015-11-06
    单  位  :上海安硕信息技术股份有限公司
    源  表  :RWA_DEV.rwa_ei_failedttc          |不合格二级资本工具表
             RWA_DEV.rwa_ei_unconsfiinvest     |未并表金融机构投资表
             RWA_DEV.rwa_ei_accsubjectdata     |科目取数表
             RWA_DEV.RWA_EI_TAXASSET           |净递延税资产表
             RWA_DEV.RWA_EI_PROFITDIST         |利润分配方案表
             RWA_DEV.rwa_tmp_taxasset          |净递延税资产表
             RWA_DEV.rwa_ei_otocpremium        |其他一级资本工具及其溢价表
             RWA_DEV.FNS_BND_BOOK_B            |债券账面活动表
             RWA_DEV.FNS_BND_INFO_B            |债券信息表
    目标表  :RWA_DEV.rwa_ei_client                 |参与主体表
             RWA_DEV.rwa_ei_exposure               |暴露表
             RWA_DEV.rwa_ei_contract               |合同表

    辅助表  :dual
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.pro_rwa_tax_asset';
  v_datadate DATE := to_date(p_data_dt_str,'yyyy/mm/dd');       --数据日期
  v_datano VARCHAR2(8) := to_char(v_datadate, 'yyyymmdd');      --数据流水号
  v_startdate VARCHAR2(10) := to_char(v_datadate,'yyyy-mm-dd'); --起始日期
  dys NUMBER(24,6);  ---------递延税

  --定义业务变量
  p01    NUMBER(24,6);         --加回系数
  t16    NUMBER(24,6) := 0;    --少数股东资本中可计入并表集团核心一级资本的部分（考虑过渡期）
  t32    NUMBER(24,6) := 0;    --少数股东资本中可计入并表集团其他一级资本的部分（考虑过渡期）
  t53    NUMBER(24,6) := 0;    --少数股东资本中可计入并表集团二级资本的部分（考虑过渡期）
  d51    NUMBER(24,6) := 0;    --二级资本工具及其溢价可计入金额
  gpSTD  NUMBER(24,6) := 0;    --贷款损失准备缺口（采用权重法计算信用风险加权资产的银行）
  d1     NUMBER(24,6) := 0;    --核心一级资本
  d21    NUMBER(24,6) := 0;    --全额扣除项目合计
  d2110  NUMBER(24,6) := 0;    --商业银行间通过协议相互持有的核心一级资本
  d2111  NUMBER(24,6) := 0;    --对有控制权但不并表的金融机构的核心一级资本投资
  d2112  NUMBER(24,6) := 0;    --有控制权但不并表的金融机构的核心一级资本缺口
  d213   NUMBER(24,6) := 0;    --依赖于银行未来盈利亏损的净递延税资产
  d221   NUMBER(24,6) := 0;    --对未并表金融机构小额少数资本投资中的核心一级资本
  d2211  NUMBER(24,6) := 0;    --其中应扣除金额
  d222   NUMBER(24,6) := 0;    --对未并表金融机构大额少数资本投资中的核心一级资本
  d2221  NUMBER(24,6) := 0;    --其中应扣除金额
  --hushiwei 20171016 递延税取值修改
  d223   NUMBER(24,6) := 0;    --其他依赖于银行未来盈利的净递延税资产_权重250%部分
  d223_o NUMBER(24,6) := 0;    --其他依赖于银行未来盈利的净递延税资产_权重100%部分
  d2231  NUMBER(24,6) := 0;    --其中应扣除金额
  d224   number(24,6) := 0;    --对未并表金融机构大额少数资本投资中的核心一级资本和其他依赖于银行未来盈利的净递延税资产的未扣除部分
  d2241  NUMBER(24,6) := 0;    --超过核心一级资本15%的应扣除金额
  d22411 NUMBER(24,6) := 0;    --应在对金融机构大额少数资本投资中扣除的金额
  d22412 number(24,6) := 0;    --应在其他依赖于银行未来盈利的净递延税资产中扣除的金额
  d3     NUMBER(24,6) := 0;    --其他一级资本
  d4     NUMBER(24,6) := 0;    --其他一级资本监管扣除项目
  d412   NUMBER(24,6) := 0;    --商业银行间通过协议相互持有的其他一级资本
  d413   NUMBER(24,6) := 0;    --对未并表金融机构大额少数资本投资中的其他一级资本
  d414   NUMBER(24,6) := 0;    --对有控制权但不并表的金融机构的其他一级资本投资
  d415   NUMBER(24,6) := 0;    --有控制权但不并表的金融机构的其他一级资本缺口
  d421   NUMBER(24,6) := 0;    --对未并表金融机构小额少数资本投资中的其他一级资本
  d4211  NUMBER(24,6) := 0;    --其中应扣除部分
  d5     NUMBER(24,6) := 0;    --二级资本
  d6     NUMBER(24,6) := 0;    --二级资本监管扣除项目
  d612   NUMBER(24,6) := 0;    --商业银行间通过协议相互持有的二级资本
  d613   NUMBER(24,6) := 0;    --对未并表金融机构大额少数资本投资中的二级资本
  d614   NUMBER(24,6) := 0;    --对有控制权但不并表的金融机构的二级资本投资
  d615   NUMBER(24,6) := 0;    --有控制权但不并表的金融机构的二级资本缺口
  d621   NUMBER(24,6) := 0;    --对未并表金融机构小额少数资本投资中的二级资本
  d6211  NUMBER(24,6) := 0;    --其中应扣除部分
  d71    NUMBER(24,6)  := 0;   --核心一级资本净额1（仅扣除全额扣除项目）
  d72    NUMBER(24,6)  := 0;   --核心一级资本净额2（扣除全额扣除项目和小额少数投资应扣除部分后）
  d73    NUMBER(24,6)  := 0;   --核心一级资本净额3（扣除除2.2.4.1以外的所有扣除项后的净额）

  tax_asset NUMBER(24,6) := 0; --d223 - d2231 - d22412 依赖于银行未来盈利的净递延税资产（未扣除部分）
  block_asset NUMBER(24,6) := 0; --d221-d2211+d222-d2221-d22411+d421-d4211 对金融机构的股权投资（未扣除部分）

  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count1 INTEGER;
  v_count2 INTEGER;

  BEGIN
    Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录

    DELETE FROM RWA_DEV.rwa_ei_client WHERE datadate = v_datadate AND ssysid = 'JDYS' AND clientid = 'JDYS-' || v_datano AND clientname = '净递延税虚拟客户';
    DELETE FROM RWA_DEV.rwa_ei_exposure WHERE datadate = v_datadate AND ssysid = 'JDYS' AND exposureid = 'JDYS-011202-' || v_datano;
    DELETE FROM RWA_DEV.rwa_ei_exposure WHERE datadate = v_datadate AND ssysid = 'JDYS' AND exposureid = 'JDYS-011216-' || v_datano;
    DELETE FROM RWA_DEV.rwa_ei_contract WHERE datadate = v_datadate AND ssysid = 'JDYS' AND contractid = 'JDYS-011202-' || v_datano;
    DELETE FROM RWA_DEV.rwa_ei_contract WHERE datadate = v_datadate AND ssysid = 'JDYS' AND contractid = 'JDYS-011216-' || v_datano;

    DELETE FROM RWA_DEV.rwa_ei_client WHERE datadate = v_datadate AND ssysid = 'GQ' AND clientid = 'GQ' || v_datano AND CLIENTNAME=  '股权投资虚拟客户';
    DELETE FROM RWA_DEV.rwa_ei_exposure WHERE datadate = v_datadate AND ssysid = 'GQ' AND exposureid = 'GQ' || v_datano;
    DELETE FROM RWA_DEV.rwa_ei_contract WHERE datadate = v_datadate AND ssysid = 'GQ' AND contractid = 'GQ' || v_datano;
    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.rwa_tmp_taxasset';

    --d223 其他依赖于银行未来盈利的净递延税资产
    --d213 依赖未来盈利的由经营亏损引起的净递延税资产
    SELECT SUM(ASSET) AS ASSET into d213 FROM RWA_DEV.RWA_EI_TAXASSET WHERE DATADATE = v_datadate and taxtype = '1';

    --d223 其他依赖于银行未来盈利的净递延税资产_权重250%部分
    --hushiwei 20171016修改递延税取值   权重250%
    --chengang  20191121修改递延税取值   权重250% 
      /* 应收利息资产 
      其他应收款及应收款项投资资产 
      抵债资产资产 
      应收理财手续费收入暂收款资产 
      其他应收款-其他资产 
      预计负债-应付内退职工薪酬资产 
      应付工资及奖金资产 
      应付住房公积金资产
      应付工会经费资产
      应付基本社会保险资产 */

    SELECT SUM(YSLX_D + QTYSK_D + DZZC_D + YJFZ_D + YFGZJJJ_D + YFZFGJJ_D +
               YFNJ_D + YFGHJF_D + YFJBSHBX_D + YSLCSXFSRZSK_D + QTYSKQT_D) AS ASSET
      into d223
      FROM RWA_DEV.RWA_EI_TAXASSET
     WHERE DATADATE = v_datadate
       and taxtype = '2';

    --d223 其他依赖于银行未来盈利的净递延税资产_权重100%部分
    --hushiwei 20171016修改递延税取值   权重100%
    SELECT SUM(DKZCJZZB_D + CQGQTZ_D + CFTYKX_D + QTYSK_D +
            JYXJRZCGYJZBD_D + JYXJRZC_D +
            ZQTZGYJZBD_D + ZQTZ_D + QTGQGYJZBD_D + QTGQ_D + CYZDQJRZC_D +
            MRFSJRZCLXTZ_D + TXZCLXTZ_D + MCHGJRZCLXTZ_D) AS ASSET into d223_o
    FROM RWA_DEV.RWA_EI_TAXASSET
   WHERE DATADATE = v_datadate and taxtype = '2';

    --p01 加回系数
    SELECT DECODE(SUBSTR(v_datano,1,4),2013,0.8,2014,0.6,2015,0.4,2016,0.2,0) INTO p01 FROM DUAL;

    --d51 二级资本工具及其溢价可计入金额
    SELECT SUM(bookbalance) INTO d51
      FROM (SELECT NVL(SUM(bookbalance),0) AS bookbalance
              FROM (SELECT rmtype
                          ,DECODE(rmtype,5,bookbalance,4,bookbalance*0.8,3,bookbalance*0.6,2,bookbalance*0.4,1,bookbalance*0.2) bookbalance
                     FROM ( SELECT rmtype ,NVL(SUM(bookbalance),0) AS bookbalance
                            FROM ( SELECT RWA.getresidualmrange(datadate, to_date(honourdate, 'yyyy/mm/dd')) AS rmtype,bookbalance
                                     FROM RWA_DEV.rwa_ei_failedttc
                                    WHERE datadate = v_datadate
                                      AND qualflag = '1'
                          )
                          GROUP BY rmtype
                     )
              )
      UNION ALL
      SELECT least(nvl(SUM(bookbalance),0),800000000.0*GREATEST(1-(SUBSTR(p_data_dt_str,1,4)-2012)*0.1,0)) AS bookbalance
        FROM (SELECT rmtype ,DECODE(rmtype,5,bookbalance,4,bookbalance*0.8,3,bookbalance*0.6,2,bookbalance*0.4,1,bookbalance*0.2) bookbalance
              FROM (SELECT rmtype ,NVL(SUM(bookbalance),0) AS bookbalance
                     FROM (SELECT RWA.getresidualmrange(datadate,to_date(honourdate,'yyyy/mm/dd')) AS rmtype ,bookbalance
                             FROM RWA_DEV.rwa_ei_failedttc
                            WHERE datadate = v_datadate
                              AND qualflag = '0'
                    )
                    GROUP BY rmtype
               )
         )
   )
   ;

  --gpSTD 贷款损失准备缺口（采用权重法计算信用风险加权资产的银行）
  SELECT greatest(nvl(SUM(balance), 0), 0)  INTO gpSTD
    FROM (SELECT greatest(SUM(balance * rate1), SUM(balance * rate2)) balance
            FROM (SELECT T1.riskclassify, T1.balance, T2.rate1, T2.rate2
                    FROM (SELECT riskclassify, NVL(SUM(assetbalance), 0) AS balance
                            FROM RWA_DEV.rwa_ei_exposure
                           WHERE datadate = v_datadate
                             AND riskclassify IS NOT NULL
                             AND substr(accsubject1, 1, 4) IN ('1303', '1305', '1307', '1310')
                           GROUP BY riskclassify) T1
                   INNER JOIN (SELECT '01' riskclassify, 0 rate1, 0 rate2 FROM dual
                              UNION ALL
                              SELECT '02', 0, 0.02 FROM dual
                              UNION ALL
                              SELECT '03', 1, 0.25 FROM dual
                              UNION ALL
                              SELECT '04', 1, 0.5 FROM dual
                              UNION ALL
                              SELECT '05', 1, 1 FROM dual) T2
                      ON T1.riskclassify = T2.Riskclassify)
          UNION ALL
          SELECT -nvl(SUM(subjectbalance), 0) AS balance
            FROM RWA_DEV.rwa_ei_accsubjectdata
           WHERE datadate = v_datadate
             AND subjectcode = '1304' --贷款减值准备
          );

  --d1 核心一级资本 4001+4201+4002+4101+4102+4103+4104+6011+6021+6051+6061+6101+6111+6301-6402-6403-6411-6421-6602-6701-6711-6801-6901
  SELECT SUM(balance)
    INTO d1
    FROM (SELECT nvl(SUM(subjectbalance), 0) AS Balance
            FROM RWA_DEV.rwa_ei_accsubjectdata
           WHERE datadate = v_datadate
             AND subjectcode IN ('4001','4201','4002','4101','4102','4103','4104','6011','6021','6051','6061','6101','6111','6301')
          UNION ALL
          SELECT -nvl(SUM(subjectbalance), 0) AS Balance
            FROM RWA_DEV.rwa_ei_accsubjectdata
           WHERE datadate = v_datadate
             AND subjectcode IN ('6402','6403','6411','6421','6602','6701','6711','6801','6901'));

 /*
 * d2110 商业银行间通过协议相互持有的核心一级资本
 * d412  商业银行间通过协议相互持有的其他一级资本
 * d612 商业银行间通过协议相互持有的二级资本
*/
 SELECT nvl(SUM(ctocinvestamount), 0),
        nvl(SUM(otocinvestamount), 0),
        nvl(SUM(ttcinvestamount), 0)
   INTO d2110, d412, d612
   FROM RWA_DEV.rwa_ei_unconsfiinvest  --未并表金融机构投资表
  WHERE datadate = v_datadate
    AND equitynature = '04'
    AND equityinvesttype LIKE '02%' --金融机构
    AND consolidateflag = '0'
  ;

  /*
 * d2111 对有控制权但不并表的金融机构的核心一级资本投资
 * d414  对有控制权但不并表的金融机构的其他一级资本投资
 * d614 对有控制权但不并表的金融机构的二级资本投资
*/
 SELECT nvl(SUM(ctocinvestamount), 0),
        nvl(SUM(otocinvestamount), 0),
        nvl(SUM(ttcinvestamount), 0)
   INTO d2111, d414, d614
   FROM RWA_DEV.rwa_ei_unconsfiinvest
  WHERE datadate = v_datadate
    AND equityinvesttype like '02%'
    AND (( consolidateflag = '1')
        OR (equitynature = '01' AND consolidateflag = '0' AND notconsolidatecause = '01')
        OR (equitynature = '01' AND consolidateflag = '0' AND notconsolidatecause = '02')
        )
   ;

  /*
 * d2112 有控制权但不并表的金融机构的核心一级资本缺口
 * d415  有控制权但不并表的金融机构的其他一级资本缺口
 * d615 有控制权但不并表的金融机构的二级资本缺口
*/
SELECT nvl(SUM(ctocgap), 0), nvl(SUM(otocgap), 0), nvl(SUM(ttcgap), 0)
  INTO d2112, d415, d615
  FROM RWA_DEV.rwa_ei_unconsfiinvest
 WHERE datadate = v_datadate
   AND equityinvesttype like '02%'
   AND (
      ( consolidateflag = '1')
       OR (equitynature = '01' AND consolidateflag = '0' AND notconsolidatecause = '01')
       OR (equitynature = '01' AND consolidateflag = '0' AND notconsolidatecause = '02')
      )
  ;

  --d21 全额扣除项目合计

  SELECT SUM(balance) + nvl(d213,0) + nvl(gpSTD,0) + nvl(d2110,0) + nvl(d2111,0) + nvl(d2112,0)
    INTO d21
    FROM (SELECT nvl(SUM(subjectbalance), 0) AS Balance
            FROM RWA_DEV.rwa_ei_accsubjectdata
           WHERE datadate = v_datadate
             AND subjectcode IN ('1701')
          UNION ALL
          SELECT -nvl(SUM(subjectbalance), 0) AS Balance
            FROM RWA_DEV.rwa_ei_accsubjectdata
           WHERE datadate = v_datadate
             AND subjectcode IN ('1702')
          UNION ALL
          SELECT -nvl(SUM(ILDDEBT), 0) AS Balance
            FROM RWA_DEV.RWA_EI_PROFITDIST
           WHERE datadate = v_datadate)
   ;

/*
 * d221 对未并表金融机构小额少数资本投资中的核心一级资本
 * d421 对未并表金融机构小额少数资本投资中的其他一级资本
 * d621 对未并表金融机构小额少数资本投资中的二级资本
*/
  SELECT nvl(SUM(ctocinvestamount), 0),
         nvl(SUM(otocinvestamount), 0)/*,
         nvl(SUM(ttcinvestamount), 0)*/
    INTO d221, d421/*, d621*/
    FROM RWA_DEV.rwa_ei_unconsfiinvest t --未并表金融机构投资表
   WHERE datadate = v_datadate
     AND equitynature = '03'
     AND equityinvesttype LIKE '02%' --金融机构
     AND consolidateflag = '0'
  ;

	WITH TEMP_BND_BOOK AS (
    										SELECT BOND_ID,
												       INITIAL_COST,
												       INT_ADJUST,
												       MKT_VALUE_CHANGE,
												       RECEIVABLE_INT,
												       ACCOUNTABLE_INT
												  FROM (SELECT BOND_ID,
												               INITIAL_COST,
												               INT_ADJUST,
												               MKT_VALUE_CHANGE,
												               RECEIVABLE_INT,
												               ACCOUNTABLE_INT,
												               ROW_NUMBER() OVER(PARTITION BY BOND_ID ORDER BY SORT_SEQ DESC) RM
												          FROM FNS_BND_BOOK_B
												         WHERE AS_OF_DATE <= V_DATANO
												           AND DATANO = V_DATANO)
												 WHERE RM = 1
												 	 AND INITIAL_COST <> 0
		)
  SELECT NVL(SUM(B1.INITIAL_COST),0) INTO d621
    FROM TEMP_BND_BOOK B1
   INNER JOIN RWA_DEV.FNS_BND_INFO_B B2
      ON B1.BOND_ID = B2.BOND_ID
     AND B2.DATANO = V_DATANO
     AND B2.ASSET_CLASS IN ('10', '20', '40')
     AND B2.BOND_TYPE2 = '20'
   WHERE B1.BOND_ID IN (SELECT B3.EXPOSUREID
                          FROM RWA_DEV.RWA_EI_EXPOSURE B3
                         INNER JOIN RWA_DEV.RWA_EI_CLIENT B4
                            ON B4.CLIENTID = B3.CLIENTID
                           AND B4.DATADATE = B3.DATADATE
                         WHERE B3.CLAIMSLEVEL = '02'
                           AND B3.SSYSID = 'TZ'
                           AND B3.DATADATE = v_datadate
                           AND B4.CLIENTTYPE <> '02'
                           AND B4.CONSOLIDATEDSCFLAG = '0');

/* d222 对未并表金融机构大额少数资本投资中的核心一级资本
 * d413 对未并表金融机构大额少数资本投资中的其他一级资本
 * d613 对未并表金融机构大额少数资本投资中的二级资本
*/
  SELECT nvl(SUM(ctocinvestamount), 0),
         nvl(SUM(otocinvestamount), 0),
         nvl(SUM(ttcinvestamount), 0)
    INTO d222, d413, d613
    FROM RWA_DEV.rwa_ei_unconsfiinvest t --未并表金融机构投资表
   WHERE datadate = v_datadate
     AND equitynature = '02'
     AND equityinvesttype LIKE '02%' --金融机构
     and consolidateflag = '0'
  ;

--d731 核心一级资本净额1（仅扣除全额扣除项目）
  d71 := d1 - d21;

/*
 * d2211 其中应扣除金额
 * d4211 其中应扣除部分
 * d6211 其中应扣除部分
*/
  IF (d221 + d421 + nvl(d621,0)) <> 0 THEN
    SELECT greatest((d221 + d421 + nvl(d621,0) - d71 * 0.1) * d221 / (d221 + d421 + nvl(d621,0)), 0),
           greatest((d221 + d421 + nvl(d621,0) - d71 * 0.1) * d421 / (d221 + d421 + nvl(d621,0)), 0),
           greatest((d221 + d421 + nvl(d621,0) - d71 * 0.1) * d621 / (d221 + d421 + nvl(d621,0)), 0)
      INTO d2211, d4211, d6211
      FROM dual;
  END IF;

--d732 核心一级资本净额2（扣除全额扣除项目和小额少数投资应扣除部分后）
  d72 := d71 - d2211;

--d2221 其中应扣除金额
  SELECT greatest(d222 - d72*0.1,0) INTO d2221 FROM dual;

--d2231 其中应扣除金额
  SELECT greatest(d223 - d72*0.1,0) INTO d2231 FROM  dual ;

--d224 对未并表金融机构大额少数资本投资中的核心一级资本和其他依赖于银行未来盈利的净递延税资产的未扣除部分
  d224 := d222 - d2221 + d223 - d2231;

--d3 其他一级资本
 SELECT nvl(SUM(balance), 0) + t32
   INTO d3
   FROM (SELECT preferredpremium + othertoolspremium AS balance
           FROM RWA_DEV.rwa_ei_otocpremium
          WHERE datadate = v_datadate);

--d5 二级资本
  d5 := d51 + t53;

--d6 二级资本监管扣除项目
  d6 := d613 + d6211;

--d4 其他一级资本监管扣除项目
  SELECT -least(d5 - d6,0) + d413 + d4211 INTO d4 FROM dual ;

--d733 核心一级资本净额3（扣除除2.2.4.1以外的所有扣除项后的净额）
  SELECT d72 - d2221 - d2231 - least(d3 - d4,0) INTO d73 FROM dual ;

--d2241 其中,超过核心一级资本15%的应扣除金额
  SELECT greatest((d224 - d73 * 0.15)/0.85,0) INTO d2241 FROM  dual  ;

--d22411 应在对金融机构大额少数资本投资中扣除的金额
 IF
   d224 <> 0
 THEN
   d22411 := nvl(d2241,0) * (d222 - d2221) / d224;
 END IF;

--d22412 应在其他依赖于银行未来盈利的净递延税资产中扣除的金额
 IF
   d224 <> 0
 THEN
   d22412 := d2241 * (d223 - d2231) / d224;
 END IF;

dbms_output.put_line('p01   :' || p01    ); --加回系数
dbms_output.put_line('t16   :' || t16    ); --少数股东资本中可计入并表集团核心一级资本的部分（考虑过渡期）
dbms_output.put_line('t32   :' || t32    ); --少数股东资本中可计入并表集团其他一级资本的部分（考虑过渡期）
dbms_output.put_line('t53   :' || t53    ); --少数股东资本中可计入并表集团二级资本的部分（考虑过渡期）
dbms_output.put_line('d51   :' || d51    ); --二级资本工具及其溢价可计入金额
dbms_output.put_line('gpSTD :' || gpSTD  ); --贷款损失准备缺口（采用权重法计算信用风险加权资产的银行）
dbms_output.put_line('d1    :' || d1     ); --核心一级资本
dbms_output.put_line('d21   :' || d21    ); --全额扣除项目合计
dbms_output.put_line('d2110 :' || d2110  ); --商业银行间通过协议相互持有的核心一级资本
dbms_output.put_line('d2111 :' || d2111  ); --对有控制权但不并表的金融机构的核心一级资本投资
dbms_output.put_line('d2112 :' || d2112  ); --有控制权但不并表的金融机构的核心一级资本缺口
dbms_output.put_line('d221  :' || d221   ); --对未并表金融机构小额少数资本投资中的核心一级资本
dbms_output.put_line('d2211 :' || d2211  ); --其中应扣除金额
dbms_output.put_line('d222  :' || d222   ); --对未并表金融机构大额少数资本投资中的核心一级资本
dbms_output.put_line('d2221 :' || d2221  ); --其中应扣除金额
dbms_output.put_line('d223  :' || d223   ); --其他依赖于银行未来盈利的净递延税资产_权重250%部分
dbms_output.put_line('d223_o  :' || d223_o   ); --其他依赖于银行未来盈利的净递延税资产_权重100%部分
dbms_output.put_line('d2231 :' || d2231  ); --其中应扣除金额
dbms_output.put_line('d224  :' || d224   ); --对未并表金融机构大额少数资本投资中的核心一级资本和其他依赖于银行未来盈利的净递延税资产的未扣除部分
dbms_output.put_line('d2241 :' || d2241  );--超过核心一级资本15%的应扣除金额
dbms_output.put_line('d22411:' || d22411 ); --应在对金融机构大额少数资本投资中扣除的金额
dbms_output.put_line('d22412:' || d22412 ); --应在其他依赖于银行未来盈利的净递延税资产中扣除的金额
dbms_output.put_line('d3    :' || d3     ); --其他一级资本
dbms_output.put_line('d4    :' || d4     ); --其他一级资本监管扣除项目
dbms_output.put_line('d412  :' || d412   ); --商业银行间通过协议相互持有的其他一级资本
dbms_output.put_line('d413  :' || d413   ); --对未并表金融机构大额少数资本投资中的其他一级资本
dbms_output.put_line('d414  :' || d414   ); --对有控制权但不并表的金融机构的其他一级资本投资
dbms_output.put_line('d415  :' || d415   ); --有控制权但不并表的金融机构的其他一级资本缺口
dbms_output.put_line('d421  :' || d421   ); --对未并表金融机构小额少数资本投资中的其他一级资本
dbms_output.put_line('d4211 :' || d4211  ); --其中应扣除部分
dbms_output.put_line('d5    :' || d5     ); --二级资本
dbms_output.put_line('d6    :' || d6     ); --二级资本监管扣除项目
dbms_output.put_line('d612  :' || d612   ); --商业银行间通过协议相互持有的二级资本
dbms_output.put_line('d613  :' || d613   ); --对未并表金融机构大额少数资本投资中的二级资本
dbms_output.put_line('d614  :' || d614   ); --对有控制权但不并表的金融机构的二级资本投资
dbms_output.put_line('d615  :' || d615   ); --有控制权但不并表的金融机构的二级资本缺口
dbms_output.put_line('d621  :' || d621   ); --对未并表金融机构小额少数资本投资中的二级资本
dbms_output.put_line('d6211 :' || d6211  ); --其中应扣除部分
dbms_output.put_line('d71  :' || d71   ); --核心一级资本净额1（仅扣除全额扣除项目）
dbms_output.put_line('d72  :' || d72   ); --核心一级资本净额2（扣除全额扣除项目和小额少数投资应扣除部分后）
dbms_output.put_line('d73  :' || d73   ); --核心一级资本净额3（扣除除2.2.4.1以外的所有扣除项后的净额）


tax_asset := d223 - d2231 - nvl(d22412,0);
block_asset := d221 - d2211 + d222 - d2221 - nvl(d22411,0) + d421 - d4211;

dbms_output.put_line('依赖于银行未来盈利的净递延税资产（未扣除部分）:' || tax_asset);
dbms_output.put_line('对金融机构的股权投资（未扣除部分）:' || block_asset);

insert into RWA_DEV.rwa_tmp_taxasset (DATADATE, DATANO, TAX_ASSET, BLOCK_ASSET, P01, T16, T32, T53, D51, GPSTD, D1, D21, D2110, D2111, D2112, D221, D2211, D222, D2221, D223, D2231, D224, D2241, D22411, D22412, D3, D4, D412, D413, D414, D415, D421, D4211, D5, D6, D612, D613, D614, D615, D621, D6211, D71, D72, D73,TAX_ASSET_O)
values (v_datadate, v_datano, TAX_ASSET, BLOCK_ASSET, P01, T16, T32, T53, D51, GPSTD, D1, D21, D2110, D2111, D2112, D221, D2211, D222, D2221, D223, D2231, D224, D2241, D22411, D22412, D3, D4, D412, D413, D414, D415, D421, D4211, D5, D6, D612, D613, D614, D615, D621, D6211, D71, D72, D73,d223_o);
commit;

--2.将满足条件的数据从源表插入到目标表中
  /*插入目标表RWA_DEV.rwa_ei_client*/
  INSERT INTO RWA_DEV.rwa_ei_client(
         DataDate                   --数据日期
        ,DataNo                     --数据流水号
        ,ClientID                   --参与主体ID
        ,SourceClientID             --源参与主体ID
        ,SSysID                     --源系统ID
        ,ClientName                 --参与主体名称
        ,SOrgID                     --源机构ID
        ,SOrgName                   --源机构名称
        ,OrgSortNo                  --所属机构排序号
        ,OrgID                      --所属机构ID
        ,OrgName                    --所属机构名称
        ,IndustryID                 --所属行业代码
        ,IndustryName               --所属行业名称
        ,ClientType                 --参与主体大类
        ,ClientSubType              --参与主体小类
        ,RegistState                --注册国家或地区
        ,RCERating                  --境外注册地外部评级
        ,RCERAgency                 --境外注册地外部评级机构
        ,OrganizationCode           --组织机构代码
        ,ConsolidatedSCFlag         --是否并表子公司
        ,SLClientFlag               --专业贷款客户标识
        ,SLClientType               --专业贷款客户类型
        ,ExpoCategoryIRB            --内评法暴露类别
        ,ModelID                    --模型ID
        ,ModelIRating               --模型内部评级
        ,ModelPD                    --模型违约概率
        ,IRating                    --内部评级
        ,PD                         --违约概率
        ,DefaultFlag                --违约标识
        ,NewDefaultFlag             --新增违约标识
        ,DefaultDate                --违约时点
        ,ClientERating              --参与主体外部评级
        ,CCPFlag                    --中央交易对手标识
        ,QualCCPFlag                --是否合格中央交易对手
        ,ClearMemberFlag            --清算会员标识
        ,CompanySize                --企业规模
        ,SSMBFlag                   --标准小微企业标识
        ,AnnualSale                 --公司客户年销售额
        ,CountryCode                --注册国家代码
        ,MSMBFlag                   --工信部微小企业标识
    )
    SELECT
                v_datadate                AS datadate             --数据日期
                ,v_datano                 AS datano               --数据流水号
                ,'JDYS-' || v_datano      AS clientid             --参与主体代号
                ,'JDYS-' || v_datano      AS sourceclientid       --源参与主体代号
                ,'JDYS'                   AS ssysid               --源系统代号
                ,'净递延税虚拟客户'       AS clientname           --参与主体名称
                ,'9998'               AS sorgid               --源机构代码
                ,'重庆银行'               AS sorgname             --源机构名称
                ,'1'                      AS orgsortno            --机构排序号
                ,'9998'               AS orgid                --所属机构代码
                ,'重庆银行'               AS orgname              --所属机构名称
                ,'999999'                 AS industryid           --所属行业代码
                ,'未知'                   AS industryname         --所属行业名称
                ,'03'                     AS clienttype           --参与主体大类 (03公司)
                ,'0301'                   AS clientsubtype        --参与主体小类 (0301一般企业)
                ,'01'                     AS registstate          --注册国家或地区
                ,NULL                     AS rcerating            --境外注册地外部评级
                ,NULL                     AS rceragency           --境外注册地外部评级机构
                ,NULL                     AS organizationcode     --组织机构代码
                ,'0'                      AS consolidatedscflag   --是否并表子公司
                ,'0'                      AS slclientflag         --专业贷款客户标识
                ,NULL                     AS slclienttype         --专业贷款客户类型
                ,'020701'                 AS expocategoryirb      --内评法暴露类别 (020701其他风险暴露)
                ,NULL                     AS ModelID              --模型ID
                ,NULL                     AS modelirating         --模型内部评级
                ,NULL                     AS modelpd              --模型违约概率
                ,NULL                     AS irating              --内部评级
                ,NULL                     AS pd                   --违约概率
                ,'0'                      AS DefaultFlag          --违约标识
                ,'0'                      AS NewDefaultFlag       --新增违约标识
                ,NULL                     AS DefaultDate          --违约时点
                ,''                       AS clienterating        --参与主体外部评级
                ,'0'                      AS ccpflag              --中央交易对手标识
                ,'0'                      AS qualccpflag          --是否合格中央交易对手
                ,'0'                      AS clearmemberflag      --清算会员标识
                ,'01'                     AS CompanySize          --企业规模
                ,'0'                      AS ssmbflag             --标准小微企业标识
                ,NULL                     AS annualsale           --公司客户年销售额
                ,'CHN'                    AS countrycode          --注册国家代码
                ,'0'                      AS MSMBFlag             --工信部微小企业标识

    FROM        RWA_DEV.rwa_tmp_taxasset
    where       datadate = v_datadate
    and         TAX_ASSET<>0

    UNION ALL

    SELECT
                v_datadate                AS datadate             --数据日期
                ,v_datano                 AS datano               --数据流水号
                ,'GQ' || v_datano         AS clientid             --参与主体代号
                ,'GQ' || v_datano         AS sourceclientid       --源参与主体代号
                ,'GQ'                     AS ssysid               --源系统代号
                ,'股权投资虚拟客户'       AS clientname           --参与主体名称
                ,'9998'               AS sorgid               --源机构代码
                ,'重庆银行'               AS sorgname             --源机构名称
                ,'1'                      AS orgsortno            --机构排序号
                ,'9998'               AS orgid                --所属机构代码
                ,'重庆银行'               AS orgname              --所属机构名称
                ,'999999'                 AS industryid           --所属行业代码
                ,'未知'                   AS industryname         --所属行业名称
                ,'02'                     AS clienttype           --参与主体大类 (02金融机构)
                ,'0205'                   AS clientsubtype        --参与主体小类 (0205其他银行)
                ,'01'                     AS registstate          --注册国家或地区
                ,''		                    AS rcerating            --境外注册地外部评级
                ,NULL                     AS rceragency           --境外注册地外部评级机构
                ,NULL                     AS organizationcode     --组织机构代码
                ,'0'                      AS consolidatedscflag   --是否并表子公司
                ,'0'                      AS slclientflag         --专业贷款客户标识
                ,NULL                     AS slclienttype         --专业贷款客户类型
                ,'020501'                 AS expocategoryirb      --内评法暴露类别 (020501金融机构)
                ,NULL                     AS ModelID              --模型ID
                ,NULL                     AS modelirating         --模型内部评级
                ,NULL                     AS modelpd              --模型违约概率
                ,NULL                     AS irating              --内部评级
                ,NULL                     AS pd                   --违约概率
                ,'0'                      AS DefaultFlag          --违约标识
                ,'0'                      AS NewDefaultFlag       --新增违约标识
                ,NULL                     AS DefaultDate          --违约时点
                ,''                   		AS clienterating        --参与主体外部评级
                ,'0'                      AS ccpflag              --中央交易对手标识
                ,'0'                      AS qualccpflag          --是否合格中央交易对手
                ,'0'                      AS clearmemberflag      --清算会员标识
                ,'01'                     AS CompanySize          --企业规模
                ,'0'                      AS ssmbflag             --标准小微企业标识
                ,NULL                     AS annualsale           --公司客户年销售额
                ,'CHN'                    AS countrycode          --注册国家代码
                ,'0'                      AS MSMBFlag             --工信部微小企业标识
    FROM        RWA_DEV.RWA_TMP_TAXASSET
    WHERE       DATADATE = V_DATADATE
    AND         BLOCK_ASSET<>0
    ;

    COMMIT;
  /*插入目标表RWA_DEV.rwa_ei_exposure*/
    INSERT INTO RWA_DEV.rwa_ei_exposure(
                DataDate                                          --数据日期
               ,DataNo                                            --数据流水号
               ,ExposureID                                        --风险暴露ID
               ,DueID                                             --债项ID
               ,SSysID                                            --源系统ID
               ,ContractID                                        --合同ID
               ,ClientID                                          --参与主体ID
               ,SOrgID                                            --源机构ID
               ,SOrgName                                          --源机构名称
               ,OrgSortNo                                         --所属机构排序号
               ,OrgID                                             --所属机构ID
               ,OrgName                                           --所属机构名称
               ,AccOrgID                                          --账务机构ID
               ,AccOrgName                                        --账务机构名称
               ,IndustryID                                        --所属行业代码
               ,IndustryName                                      --所属行业名称
               ,BusinessLine                                      --业务条线
               ,AssetType                                         --资产大类
               ,AssetSubType                                      --资产小类
               ,BusinessTypeID                                    --业务品种代码
               ,BusinessTypeName                                  --业务品种名称
               ,CreditRiskDataType                                --信用风险数据类型
               ,AssetTypeOfHaircuts                               --折扣系数对应资产类别
               ,BusinessTypeSTD                                   --权重法业务类型
               ,ExpoClassSTD                                      --权重法暴露大类
               ,ExpoSubClassSTD                                   --权重法暴露小类
               ,ExpoClassIRB                                      --内评法暴露大类
               ,ExpoSubClassIRB                                   --内评法暴露小类
               ,ExpoBelong                                        --暴露所属标识
               ,BookType                                          --账户类别
               ,ReguTranType                                      --监管交易类型
               ,RepoTranFlag                                      --回购交易标识
               ,RevaFrequency                                     --重估频率
               ,Currency                                          --币种
               ,NormalPrincipal                                   --正常本金余额
               ,OverdueBalance                                    --逾期余额
               ,NonAccrualBalance                                 --非应计余额
               ,OnSheetBalance                                    --表内余额
               ,NormalInterest                                    --正常利息
               ,OnDebitInterest                                   --表内欠息
               ,OffDebitInterest                                  --表外欠息
               ,ExpenseReceivable                                 --应收费用
               ,AssetBalance                                      --资产余额
               ,AccSubject1                                       --科目一
               ,AccSubject2                                       --科目二
               ,AccSubject3                                       --科目三
               ,StartDate                                         --起始日期
               ,DueDate                                           --到期日期
               ,OriginalMaturity                                  --原始期限
               ,ResidualM                                         --剩余期限
               ,RiskClassify                                      --风险分类
               ,ExposureStatus                                    --风险暴露状态
               ,OverdueDays                                       --逾期天数
               ,SpecialProvision                                  --专项准备金
               ,GeneralProvision                                  --一般准备金
               ,EspecialProvision                                 --特别准备金
               ,WrittenOffAmount                                  --已核销金额
               ,OffExpoSource                                     --表外暴露来源
               ,OffBusinessType                                   --表外业务类型
               ,OffBusinessSdvsSTD                                --权重法表外业务类型细分
               ,UncondCancelFlag                                  --是否可随时无条件撤销
               ,CCFLevel                                          --信用转换系数级别
               ,CCFAIRB                                           --高级法信用转换系数
               ,ClaimsLevel                                       --债权级别
               ,BondFlag                                          --是否为债券
               ,BondIssueIntent                                   --债券发行目的
               ,NSURealPropertyFlag                               --是否非自用不动产
               ,RepAssetTermType                                  --抵债资产期限类型
               ,DependOnFPOBFlag                                  --是否依赖于银行未来盈利
               ,IRating                                           --内部评级
               ,PD                                                --违约概率
               ,LGDLevel                                          --违约损失率级别
               ,LGDAIRB                                           --高级法违约损失率
               ,MAIRB                                             --高级法有效期限
               ,EADAIRB                                           --高级法违约风险暴露
               ,DefaultFlag                                       --违约标识
               ,BEEL                                              --已违约暴露预期损失比率
               ,DefaultLGD                                        --已违约暴露违约损失率
               ,EquityExpoFlag                                    --股权暴露标识
               ,EquityInvestType                                  --股权投资对象类型
               ,EquityInvestCause                                 --股权投资形成原因
               ,SLFlag                                            --专业贷款标识
               ,SLType                                            --专业贷款类型
               ,PFPhase                                           --项目融资阶段
               ,ReguRating                                        --监管评级
               ,CBRCMPRatingFlag                                  --银监会认定评级是否更为审慎
               ,LargeFlucFlag                                     --是否波动性较大
               ,LiquExpoFlag                                      --是否清算过程中风险暴露
               ,PaymentDealFlag                                   --是否货款对付模式
               ,DelayTradingDays                                  --延迟交易天数
               ,SecuritiesFlag                                    --有价证券标识
               ,SecuIssuerID                                      --证券发行人ID
               ,RatingDurationType                                --评级期限类型
               ,SecuIssueRating                                   --证券发行等级
               ,SecuResidualM                                     --证券剩余期限
               ,SecuRevaFrequency                                 --证券重估频率
               ,CCPTranFlag                                       --是否中央交易对手相关交易
               ,CCPID                                             --中央交易对手ID
               ,QualCCPFlag                                       --是否合格中央交易对手
               ,BankRole                                          --银行角色
               ,ClearingMethod                                    --清算方式
               ,BankAssetFlag                                     --是否银行提交资产
               ,MatchConditions                                   --符合条件情况
               ,SFTFlag                                           --证券融资交易标识
               ,MasterNetAgreeFlag                                --净额结算主协议标识
               ,MasterNetAgreeID                                  --净额结算主协议ID
               ,SFTType                                           --证券融资交易类型
               ,SecuOwnerTransFlag                                --证券所有权是否转移
               ,OTCFlag                                           --场外衍生工具标识
               ,ValidNettingFlag                                  --有效净额结算协议标识
               ,ValidNetAgreementID                               --有效净额结算协议ID
               ,OTCType                                           --场外衍生工具类型
               ,DepositRiskPeriod                                 --保证金风险期间
               ,MTM                                               --重置成本
               ,MTMCurrency                                       --重置成本币种
               ,BuyerOrSeller                                     --买方卖方
               ,QualROFlag                                        --合格参照资产标识
               ,ROIssuerPerformFlag                               --参照资产发行人是否能履约
               ,BuyerInsolvencyFlag                               --信用保护买方是否破产
               ,NonpaymentFees                                    --尚未支付费用
               ,RetailExpoFlag                                    --零售暴露标识
               ,RetailClaimType                                   --零售债权类型
               ,MortgageType                                      --住房抵押贷款类型
               ,ExpoNumber                                        --风险暴露个数
               ,LTV                                               --贷款价值比
               ,Aging                                             --账龄
               ,NewDefaultDebtFlag                                --新增违约债项标识
               ,PDPoolModelID                                     --PD分池模型ID
               ,LGDPoolModelID                                    --LGD分池模型ID
               ,CCFPoolModelID                                    --CCF分池模型ID
               ,PDPoolID                                          --所属PD池ID
               ,LGDPoolID                                         --所属LGD池ID
               ,CCFPoolID                                         --所属CCF池ID
               ,ABSUAFlag                                         --资产证券化基础资产标识
               ,ABSPoolID                                         --证券化资产池ID
               ,GroupID                                           --分组编号
               ,DefaultDate                                       --违约时点
               ,ABSPROPORTION                                     --资产证券化比重
               ,DEBTORNUMBER                                      --借款人个数
    )
  SELECT
          v_datadate                                                  AS datadate                     --数据日期
          ,v_datano                                                   AS datano                       --数据流水号
          ,'JDYS-011202-' || v_datano                                        AS exposureid                   --风险暴露ID
          ,'JDYS-011202-' || v_datano                                        AS dueid                        --债项ID
          ,'JDYS'                                                     AS ssysid                       --源系统ID
          ,'JDYS-011202-' || v_datano                                        AS contractid                   --合同ID
          ,'JDYS-' || v_datano                                        AS clientid                     --参与主体ID
          ,'9998'                                                 AS sorgid                       --源机构ID
          ,'重庆银行'                                                 AS sorgname                     --源机构名称
          ,'1'                                                        AS orgsortno                    --机构排序号
          ,'9998'                                                 AS orgid                        --所属机构ID
          ,'重庆银行'                                                 AS orgname                      --所属机构名称
          ,'9998'                                                 AS accorgid                     --账务机构ID
          ,'重庆银行'                                                 AS accorgname                   --账务机构名称
          ,'999999'                                                   AS industryid                   --所属行业代码
          ,'未知'                                                     AS industryname                 --所属行业名称
          ,'0501'                                                     AS businessline                 --条线   (总行0501)
          ,'130'                                                      AS assettype                    --资产大类(130 其他表内资产)
          ,'13001'                                                    AS assetsubtype                 --资产小类(13001 递延所得税资产)
          ,'109070'                                                   AS businesstypeid               --业务品种代码
          ,'净递延税资产'                                             AS businesstypename             --业务品种名称
          ,'01'                                                       AS creditriskdatatype           --信用风险数据类型(01:一般非零售,02:一般零售)
          ,'01'                                                       AS assettypeofhaircuts          --折扣系数对应资产类别
          ,'07'                                                       AS businesstypestd              --权重法业务类型(对公 一般资产07 零售 个人06)
          ,'0112'                                                     AS expoclassstd                 --权重法暴露大类(对公 0106 零售 0108)
          ,'011202'                                                   AS exposubclassstd              --权重法暴露小类(对公 010601 零售 010803)
          ,'0203'                                                     AS expoclassirb                 --内评法暴露大类(对公 0203 零售 0204)
          ,'020301'                                                   AS exposubclassirb              --内评法暴露小类(对公 020301 零售 020403)
          ,'01'                                                       AS expobelong                   --暴露所属标识((01:表内;02:一般表外;03:交易对手;))
          ,'01'                                                       AS booktype                     --账户类别(固定值"银行账户",01:银行账户,02:交易账户)
          ,'03'                                                       AS regutrantype                 --监管交易类型(固定值"抵押贷款",01:回购交易;02:其他资本市场交易;03:抵押贷款;)
          ,'0'                                                        AS repotranflag                 --回购交易标识(固定值为"否" 0)
          ,1                                                          AS revafrequency                --重估频率
          ,'CNY'                                                      AS currency                     --币种('CNY'人民币)
          ,nvl(d223,0)                                           AS normalprincipal              --正常本金余额
          ,0                                                          AS overduebalance               --逾期余额
          ,0                                                          AS nonaccrualbalance            --非应计余额
          ,nvl(d223,0)                                           AS onsheetbalance               --表内余额(正常本金余额+逾期余额+非应计余额)
          ,0                                                          AS normalinterest               --正常利息
          ,0                                                          AS ondebitinterest              --表内欠息
          ,0                                                          AS offdebitinterest             --表外欠息
          ,0                                                          AS expensereceivable            --应收费用
          ,nvl(d223,0)                                           AS assetbalance                 --资产余额
          ,'18110000'                                                 AS accsubject1                  --科目一
          ,NULL                                                       AS accsubject2                  --科目二
          ,NULL                                                       AS accsubject3                  --科目三
          ,v_startdate                                                AS startdate                    --起始日期
          ,TO_CHAR(ADD_MONTHS(v_datadate,1),'YYYY-MM-DD')             AS duedate                      --到期日期
          ,(ADD_MONTHS(v_datadate,1) - v_datadate)/365                AS originalmaturity             --原始期限
          ,(ADD_MONTHS(v_datadate,1) - v_datadate)/365                AS residualm                    --剩余期限
          ,'01'                                                       AS riskclassify                 --风险分类(默认为01正常)
          ,''                                                         AS exposurestatus               --风险暴露状态(默认为空)
          ,0                                                          AS overduedays                  --逾期天数
          ,0                                                          AS specialprovision             --专项准备金
          ,0                                                          AS generalprovision             --一般准备金
          ,0                                                          AS especialprovision            --特别准备金
          ,0                                                          AS writtenoffamount             --已核销金额
          ,''                                                         AS offexposource                --表外暴露来源
          ,''                                                         AS offbusinesstype              --表外业务类型
          ,''                                                         AS offbusinesssdvsstd           --权重法表外业务类型细分
          ,'0'                                                        AS uncondcancelflag             --是否可随时无条件撤销(默认为否,1是0否)
          ,''                                                         AS ccflevel                     --信用转换系数级别
          ,NULL                                                       AS ccfairb                      --高级法信用转换系数
          ,'01'                                                       AS claimslevel                  --债权级别 (默认为高级债权01)
          ,'0'                                                        AS bondflag                     --是否为债券                  (默认为否,1是0否)
          ,''                                                         AS bondissueintent              --债券发行目的
          ,'0'                                                        AS nsurealpropertyflag          --是否非自用不动产              (默认为否,1是0否)
          ,''                                                         AS repassettermtype             --抵债资产期限类型
          ,'1'                                                        AS dependonfpobflag             --是否依赖于银行未来盈利     (默认为否,1是0否)
          ,''                                                         AS irating                      --内部评级
          ,NULL                                                       AS pd                           --违约概率
          ,''                                                         AS lgdlevel                     --违约损失率级别
          ,NULL                                                       AS lgdairb                      --高级法违约损失率
          ,NULL                                                       AS mairb                        --高级法有效期限
          ,NULL                                                       AS eadairb                      --高级法违约风险暴露
          ,'0'                                                        AS defaultflag                  --违约标识                    (默认为否,1是0否)
          ,NULL                                                       AS beel                         --已违约暴露预期损失比率
          ,NULL                                                       AS defaultlgd                   --已违约暴露违约损失率
          ,'0'                                                        AS equityexpoflag               --股权暴露标识                 (默认为否,1是0否)
          ,''                                                         AS equityinvesttype             --股权投资对象类型
          ,''                                                         AS equityinvestcause            --股权投资形成原因
          ,'0'                                                        AS slflag                       --专业贷款标识                (默认为否,1是0否)
          ,''                                                         AS sltype                       --专业贷款类型
          ,''                                                         AS pfphase                      --项目融资阶段
          ,''                                                         AS regurating                   --监管评级
          ,'0'                                                        AS cbrcmpratingflag             --银监会认定评级是否更为审慎   (默认为否,1是0否)
          ,'0'                                                        AS largeflucflag                --是否波动性较大              (默认为否,1是0否)
          ,'0'                                                        AS liquexpoflag                 --是否清算过程中风险暴露      (默认为否,1是0否)
          ,'0'                                                        AS paymentdealflag              --是否货款对付模式            (默认为否,1是0否)
          ,0                                                          AS delaytradingdays             --延迟交易天数
          ,'0'                                                        AS securitiesflag               --有价证券标识                 (默认为否,1是0否)
          ,''                                                         AS secuissuerid                 --证券发行人ID
          ,''                                                         AS ratingdurationtype           --评级期限类型
          ,''                                                         AS secuissuerating              --证券发行等级
          ,0                                                          AS securesidualm                --证券剩余期限
          ,0                                                          AS securevafrequency            --证券重估频率
          ,'0'                                                        AS ccptranflag                  --是否中央交易对手相关交易    (默认为否,1是0否)
          ,''                                                         AS ccpid                        --中央交易对手ID
          ,'0'                                                        AS qualccpflag                  --是否合格中央交易对手        (默认为否,1是0否)
          ,''                                                         AS bankrole                     --银行角色
          ,''                                                         AS clearingmethod               --清算方式
          ,'0'                                                        AS bankassetflag                --是否银行提交资产            (默认为否,1是0否)
          ,''                                                         AS matchconditions              --符合条件情况
          ,'0'                                                        AS sftflag                      --证券融资交易标识            (默认为否,1是0否)
          ,'0'                                                        AS masternetagreeflag           --净额结算主协议标识          (默认为否,1是0否)
          ,''                                                         AS masternetagreeid             --净额结算主协议ID
          ,''                                                         AS sfttype                      --证券融资交易类型
          ,'0'                                                        AS secuownertransflag           --证券所有权是否转移           (默认为否,1是0否)
          ,'0'                                                        AS otcflag                      --场外衍生工具标识            (默认为否,1是0否)
          ,'0'                                                        AS validnettingflag             --有效净额结算协议标识        (默认为否,1是0否)
          ,''                                                         AS validnetagreementid          --有效净额结算协议ID
          ,''                                                         AS otctype                      --场外衍生工具类型
          ,NULL                                                       AS depositriskperiod            --保证金风险期间
          ,0                                                          AS mtm                          --重置成本
          ,''                                                         AS mtmcurrency                  --重置成本币种
          ,''                                                         AS buyerorseller                --买方卖方
          ,'0'                                                        AS qualroflag                   --合格参照资产标识             (默认为否,1是0否)
          ,'0'                                                        AS roissuerperformflag          --参照资产发行人是否能履约    (默认为否,1是0否)
          ,'0'                                                        AS buyerinsolvencyflag          --信用保护买方是否破产        (默认为否,1是0否)
          ,0                                                          AS nonpaymentfees               --尚未支付费用
          ,'0'                                                        AS retailexpoflag               --零售暴露标识                 (默认为否,1是0否)
          ,''                                                         AS retailclaimtype              --零售债权类型
          ,''                                                         AS mortgagetype                 --住房抵押贷款类型
          ,1                                                          AS exponumber                   --风险暴露个数                (默认为1)
          ,0.8                                                        AS LTV                          --贷款价值比                            默认 0.8
          ,NULL                                                       AS Aging                        --账龄                                  默认 NULL
          ,''                                                         AS NewDefaultDebtFlag           --新增违约债项标识                             默认 NULL
          ,''                                                         AS pdpoolmodelid                --PD分池模型ID
          ,''                                                         AS lgdpoolmodelid               --LGD分池模型ID
          ,''                                                         AS ccfpoolmodelid               --CCF分池模型ID
          ,''                                                         AS pdpoolid                     --所属PD池ID
          ,''                                                         AS lgdpoolid                    --所属LGD池ID
          ,''                                                         AS ccfpoolid                    --所属CCF池ID
          ,'0'                                                        AS absuaflag                    --资产证券化基础资产标识     (默认为否,1是0否)
          ,''                                                         AS abspoolid                    --证券化资产池ID
          ,''                                                         AS groupid                      --分组编号
          ,NULL                                                       AS DefaultDate                  --违约时点
          ,NULL                                                       AS ABSPROPORTION                --资产证券化比重
          ,NULL                                                       AS DEBTORNUMBER                 --借款人个数
    FROM    RWA_DEV.RWA_TMP_TAXASSET
    WHERE   DATADATE = V_DATADATE
    AND     tax_asset<>0

  UNION ALL
  --hushiwei 20171016 递延税取值修改
  SELECT
          v_datadate                                                  AS datadate                     --数据日期
          ,v_datano                                                   AS datano                       --数据流水号
          ,'JDYS-011216-' || v_datano                                        AS exposureid                   --风险暴露ID
          ,'JDYS-011216-' || v_datano                                        AS dueid                        --债项ID
          ,'JDYS'                                                     AS ssysid                       --源系统ID
          ,'JDYS-011216-' || v_datano                                        AS contractid                   --合同ID
          ,'JDYS-' || v_datano                                        AS clientid                     --参与主体ID
          ,'9998'                                                 AS sorgid                       --源机构ID
          ,'重庆银行'                                                 AS sorgname                     --源机构名称
          ,'1'                                                        AS orgsortno                    --机构排序号
          ,'9998'                                                 AS orgid                        --所属机构ID
          ,'重庆银行'                                                 AS orgname                      --所属机构名称
          ,'9998'                                                 AS accorgid                     --账务机构ID
          ,'重庆银行'                                                 AS accorgname                   --账务机构名称
          ,'999999'                                                   AS industryid                   --所属行业代码
          ,'未知'                                                     AS industryname                 --所属行业名称
          ,'0501'                                                     AS businessline                 --条线   (总行0501)
          ,'130'                                                      AS assettype                    --资产大类(130 其他表内资产)
          ,'13001'                                                    AS assetsubtype                 --资产小类(13001 递延所得税资产)
          ,'109070'                                                   AS businesstypeid               --业务品种代码
          ,'净递延税资产'                                             AS businesstypename             --业务品种名称
          ,'01'                                                       AS creditriskdatatype           --信用风险数据类型(01:一般非零售,02:一般零售)
          ,'01'                                                       AS assettypeofhaircuts          --折扣系数对应资产类别
          ,'07'                                                       AS businesstypestd              --权重法业务类型(对公 一般资产07 零售 个人06)
          ,'0112'                                                     AS expoclassstd                 --权重法暴露大类(对公 0106 零售 0108)
          ,'011216'                                                   AS exposubclassstd              --权重法暴露小类(对公 010601 零售 010803)
          ,'0203'                                                     AS expoclassirb                 --内评法暴露大类(对公 0203 零售 0204)
          ,'020301'                                                   AS exposubclassirb              --内评法暴露小类(对公 020301 零售 020403)
          ,'01'                                                       AS expobelong                   --暴露所属标识((01:表内;02:一般表外;03:交易对手;))
          ,'01'                                                       AS booktype                     --账户类别(固定值"银行账户",01:银行账户,02:交易账户)
          ,'03'                                                       AS regutrantype                 --监管交易类型(固定值"抵押贷款",01:回购交易;02:其他资本市场交易;03:抵押贷款;)
          ,'0'                                                        AS repotranflag                 --回购交易标识(固定值为"否" 0)
          ,1                                                          AS revafrequency                --重估频率
          ,'CNY'                                                      AS currency                     --币种('CNY'人民币)
          ,nvl(tax_asset_o,0)                                           AS normalprincipal              --正常本金余额
          ,0                                                          AS overduebalance               --逾期余额
          ,0                                                          AS nonaccrualbalance            --非应计余额
          ,nvl(tax_asset_o,0)                                           AS onsheetbalance               --表内余额(正常本金余额+逾期余额+非应计余额)
          ,0                                                          AS normalinterest               --正常利息
          ,0                                                          AS ondebitinterest              --表内欠息
          ,0                                                          AS offdebitinterest             --表外欠息
          ,0                                                          AS expensereceivable            --应收费用
          ,nvl(tax_asset_o,0)                                           AS assetbalance                 --资产余额
          ,'18110000'                                                 AS accsubject1                  --科目一
          ,NULL                                                       AS accsubject2                  --科目二
          ,NULL                                                       AS accsubject3                  --科目三
          ,v_startdate                                                AS startdate                    --起始日期
          ,TO_CHAR(ADD_MONTHS(v_datadate,1),'YYYY-MM-DD')             AS duedate                      --到期日期
          ,(ADD_MONTHS(v_datadate,1) - v_datadate)/365                AS originalmaturity             --原始期限
          ,(ADD_MONTHS(v_datadate,1) - v_datadate)/365                AS residualm                    --剩余期限
          ,'01'                                                       AS riskclassify                 --风险分类(默认为01正常)
          ,''                                                         AS exposurestatus               --风险暴露状态(默认为空)
          ,0                                                          AS overduedays                  --逾期天数
          ,0                                                          AS specialprovision             --专项准备金
          ,0                                                          AS generalprovision             --一般准备金
          ,0                                                          AS especialprovision            --特别准备金
          ,0                                                          AS writtenoffamount             --已核销金额
          ,''                                                         AS offexposource                --表外暴露来源
          ,''                                                         AS offbusinesstype              --表外业务类型
          ,''                                                         AS offbusinesssdvsstd           --权重法表外业务类型细分
          ,'0'                                                        AS uncondcancelflag             --是否可随时无条件撤销(默认为否,1是0否)
          ,''                                                         AS ccflevel                     --信用转换系数级别
          ,NULL                                                       AS ccfairb                      --高级法信用转换系数
          ,'01'                                                       AS claimslevel                  --债权级别 (默认为高级债权01)
          ,'0'                                                        AS bondflag                     --是否为债券                  (默认为否,1是0否)
          ,''                                                         AS bondissueintent              --债券发行目的
          ,'0'                                                        AS nsurealpropertyflag          --是否非自用不动产              (默认为否,1是0否)
          ,''                                                         AS repassettermtype             --抵债资产期限类型
          ,'1'                                                        AS dependonfpobflag             --是否依赖于银行未来盈利     (默认为否,1是0否)
          ,''                                                         AS irating                      --内部评级
          ,NULL                                                       AS pd                           --违约概率
          ,''                                                         AS lgdlevel                     --违约损失率级别
          ,NULL                                                       AS lgdairb                      --高级法违约损失率
          ,NULL                                                       AS mairb                        --高级法有效期限
          ,NULL                                                       AS eadairb                      --高级法违约风险暴露
          ,'0'                                                        AS defaultflag                  --违约标识                    (默认为否,1是0否)
          ,NULL                                                       AS beel                         --已违约暴露预期损失比率
          ,NULL                                                       AS defaultlgd                   --已违约暴露违约损失率
          ,'0'                                                        AS equityexpoflag               --股权暴露标识                 (默认为否,1是0否)
          ,''                                                         AS equityinvesttype             --股权投资对象类型
          ,''                                                         AS equityinvestcause            --股权投资形成原因
          ,'0'                                                        AS slflag                       --专业贷款标识                (默认为否,1是0否)
          ,''                                                         AS sltype                       --专业贷款类型
          ,''                                                         AS pfphase                      --项目融资阶段
          ,''                                                         AS regurating                   --监管评级
          ,'0'                                                        AS cbrcmpratingflag             --银监会认定评级是否更为审慎   (默认为否,1是0否)
          ,'0'                                                        AS largeflucflag                --是否波动性较大              (默认为否,1是0否)
          ,'0'                                                        AS liquexpoflag                 --是否清算过程中风险暴露      (默认为否,1是0否)
          ,'0'                                                        AS paymentdealflag              --是否货款对付模式            (默认为否,1是0否)
          ,0                                                          AS delaytradingdays             --延迟交易天数
          ,'0'                                                        AS securitiesflag               --有价证券标识                 (默认为否,1是0否)
          ,''                                                         AS secuissuerid                 --证券发行人ID
          ,''                                                         AS ratingdurationtype           --评级期限类型
          ,''                                                         AS secuissuerating              --证券发行等级
          ,0                                                          AS securesidualm                --证券剩余期限
          ,0                                                          AS securevafrequency            --证券重估频率
          ,'0'                                                        AS ccptranflag                  --是否中央交易对手相关交易    (默认为否,1是0否)
          ,''                                                         AS ccpid                        --中央交易对手ID
          ,'0'                                                        AS qualccpflag                  --是否合格中央交易对手        (默认为否,1是0否)
          ,''                                                         AS bankrole                     --银行角色
          ,''                                                         AS clearingmethod               --清算方式
          ,'0'                                                        AS bankassetflag                --是否银行提交资产            (默认为否,1是0否)
          ,''                                                         AS matchconditions              --符合条件情况
          ,'0'                                                        AS sftflag                      --证券融资交易标识            (默认为否,1是0否)
          ,'0'                                                        AS masternetagreeflag           --净额结算主协议标识          (默认为否,1是0否)
          ,''                                                         AS masternetagreeid             --净额结算主协议ID
          ,''                                                         AS sfttype                      --证券融资交易类型
          ,'0'                                                        AS secuownertransflag           --证券所有权是否转移           (默认为否,1是0否)
          ,'0'                                                        AS otcflag                      --场外衍生工具标识            (默认为否,1是0否)
          ,'0'                                                        AS validnettingflag             --有效净额结算协议标识        (默认为否,1是0否)
          ,''                                                         AS validnetagreementid          --有效净额结算协议ID
          ,''                                                         AS otctype                      --场外衍生工具类型
          ,NULL                                                       AS depositriskperiod            --保证金风险期间
          ,0                                                          AS mtm                          --重置成本
          ,''                                                         AS mtmcurrency                  --重置成本币种
          ,''                                                         AS buyerorseller                --买方卖方
          ,'0'                                                        AS qualroflag                   --合格参照资产标识             (默认为否,1是0否)
          ,'0'                                                        AS roissuerperformflag          --参照资产发行人是否能履约    (默认为否,1是0否)
          ,'0'                                                        AS buyerinsolvencyflag          --信用保护买方是否破产        (默认为否,1是0否)
          ,0                                                          AS nonpaymentfees               --尚未支付费用
          ,'0'                                                        AS retailexpoflag               --零售暴露标识                 (默认为否,1是0否)
          ,''                                                         AS retailclaimtype              --零售债权类型
          ,''                                                         AS mortgagetype                 --住房抵押贷款类型
          ,1                                                          AS exponumber                   --风险暴露个数                (默认为1)
          ,0.8                                                        AS LTV                          --贷款价值比                            默认 0.8
          ,NULL                                                       AS Aging                        --账龄                                  默认 NULL
          ,''                                                         AS NewDefaultDebtFlag           --新增违约债项标识                             默认 NULL
          ,''                                                         AS pdpoolmodelid                --PD分池模型ID
          ,''                                                         AS lgdpoolmodelid               --LGD分池模型ID
          ,''                                                         AS ccfpoolmodelid               --CCF分池模型ID
          ,''                                                         AS pdpoolid                     --所属PD池ID
          ,''                                                         AS lgdpoolid                    --所属LGD池ID
          ,''                                                         AS ccfpoolid                    --所属CCF池ID
          ,'0'                                                        AS absuaflag                    --资产证券化基础资产标识     (默认为否,1是0否)
          ,''                                                         AS abspoolid                    --证券化资产池ID
          ,''                                                         AS groupid                      --分组编号
          ,NULL                                                       AS DefaultDate                  --违约时点
          ,NULL                                                       AS ABSPROPORTION                --资产证券化比重
          ,NULL                                                       AS DEBTORNUMBER                 --借款人个数
    FROM    RWA_DEV.RWA_TMP_TAXASSET
    WHERE   DATADATE = V_DATADATE
    AND     tax_asset_o<>0

  UNION ALL

      SELECT
          v_datadate                                                  AS datadate                     --数据日期
          ,v_datano                                                   AS datano                       --数据流水号
          ,'GQ' || v_datano                                           AS exposureid                   --风险暴露ID
          ,'GQ' || v_datano                                           AS dueid                        --债项ID
          ,'GQ'                                                       AS ssysid                       --源系统ID
          ,'GQ' || v_datano                                           AS contractid                   --合同ID
          ,'GQ' || v_datano                                           AS clientid                     --参与主体ID
          ,'9998'                                                 AS sorgid                       --源机构ID
          ,'重庆银行'                                                 AS sorgname                     --源机构名称
          ,'1'                                                        AS orgsortno                    --机构排序号
          ,'9998'                                                 AS orgid                        --所属机构ID
          ,'重庆银行'                                                 AS orgname                      --所属机构名称
          ,'9998'                                                 AS accorgid                     --账务机构ID
          ,'重庆银行'                                                 AS accorgname                   --账务机构名称
          ,'999999'                                                   AS industryid                   --所属行业代码
          ,'未知'                                                     AS industryname                 --所属行业名称
          ,'0501'                                                     AS businessline                 --条线          (总行0501)
          ,'121'                                                      AS assettype                    --资产大类(121 其他表内资产)
          ,'12103'                                                    AS assetsubtype                 --资产小类(12103 长期股权投资)
          ,'109060'                                                   AS businesstypeid               --业务品种代码
          ,'股权投资'                                                 AS businesstypename             --业务品种名称
          ,'03'                                                       AS creditriskdatatype           --信用风险数据类型(01:一般非零售,02:一般零售,03:股权)
          ,'02'                                                       AS assettypeofhaircuts          --折扣系数对应资产类别(02 具有现金价值的人寿保险单及类似理财产品)
          ,'05'                                                       AS businesstypestd              --权重法业务类型(对公 一般资产07 零售 个人06 股权05)
          ,'0110'                                                     AS expoclassstd                 --权重法暴露大类
          ,'011001'                                                   AS exposubclassstd              --权重法暴露小类
          ,'0205'                                                     AS expoclassirb                 --内评法暴露大类
          ,'020501'                                                   AS exposubclassirb              --内评法暴露小类
          ,'01'                                                       AS expobelong                   --暴露所属标识((01:表内;02:一般表外;03:交易对手;))
          ,'01'                                                       AS booktype                     --账户类别(固定值"银行账户",01:银行账户,02:交易账户)
          ,'02'                                                       AS regutrantype                 --监管交易类型(固定值"抵押贷款",01:回购交易;02:其他资本市场交易;03:抵押贷款;)
          ,'0'                                                        AS repotranflag                 --回购交易标识(固定值为"否" 0)
          ,1                                                          AS revafrequency                --重估频率
          ,'CNY'                                                      AS currency                     --币种(CNY人民币)
          ,BLOCK_ASSET                                                AS normalprincipal              --正常本金余额
          ,0                                                          AS overduebalance               --逾期余额
          ,0                                                          AS nonaccrualbalance            --非应计余额
          ,BLOCK_ASSET                                                AS onsheetbalance               --表内余额(正常本金余额+逾期余额+非应计余额)
          ,0                                                          AS normalinterest               --正常利息
          ,0                                                          AS ondebitinterest              --表内欠息
          ,0                                                          AS offdebitinterest             --表外欠息
          ,0                                                          AS expensereceivable            --应收费用
          ,BLOCK_ASSET                                                AS assetbalance                 --资产余额
          ,'15110100'                                                 AS accsubject1                  --科目一
          ,NULL                                                       AS accsubject2                  --科目二
          ,NULL                                                       AS accsubject3                  --科目三
          ,v_startdate                                                AS startdate                    --起始日期
          ,TO_CHAR(ADD_MONTHS(v_datadate,1),'YYYY-MM-DD')             AS duedate                      --到期日期
          ,(ADD_MONTHS(v_datadate,1) - v_datadate)/365                AS originalmaturity             --原始期限
          ,(ADD_MONTHS(v_datadate,1) - v_datadate)/365                AS residualm                    --剩余期限
          ,'01'                                                       AS riskclassify                 --风险分类
          ,'01'                                                       AS exposurestatus               --风险暴露状态(默认为空)
          ,0                                                          AS overduedays                  --逾期天数
          ,0                                                          AS specialprovision             --专项准备金
          ,0                                                          AS generalprovision             --一般准备金
          ,0                                                          AS especialprovision            --特别准备金
          ,0                                                          AS writtenoffamount             --已核销金额
          ,''                                                         AS offexposource                --表外暴露来源
          ,''                                                         AS offbusinesstype              --表外业务类型
          ,''                                                         AS offbusinesssdvsstd           --权重法表外业务类型细分
          ,'0'                                                        AS uncondcancelflag             --是否可随时无条件撤销(默认为否,1是0否)
          ,''                                                         AS ccflevel                     --信用转换系数级别
          ,NULL                                                       AS ccfairb                      --高级法信用转换系数
          ,'01'                                                       AS claimslevel                  --债权级别 (默认为高级债权01)
          ,'0'                                                        AS bondflag                     --是否为债券                  (默认为否,1是0否)
          ,''                                                         AS bondissueintent              --债券发行目的
          ,'0'                                                        AS nsurealpropertyflag          --是否非自用不动产              (默认为否,1是0否)
          ,''                                                         AS repassettermtype             --抵债资产期限类型
          ,'0'                                                        AS dependonfpobflag             --是否依赖于银行未来盈利     (默认为否,1是0否)
          ,''                                                         AS irating                      --内部评级
          ,NULL                                                       AS pd                           --违约概率
          ,''                                                         AS lgdlevel                     --违约损失率级别
          ,NULL                                                       AS lgdairb                      --高级法违约损失率
          ,NULL                                                       AS mairb                        --高级法有效期限
          ,NULL                                                       AS eadairb                      --高级法违约风险暴露
          ,'0'                                                        AS defaultflag                  --违约标识                    (默认为否,1是0否)
          ,0.45                                                       AS beel                         --已违约暴露预期损失比率
          ,0.45                                                       AS defaultlgd                   --已违约暴露违约损失率
          ,'1'                                                        AS equityexpoflag               --股权暴露标识                 (默认为否,1是0否)
          ,'01'                                                       AS equityinvesttype             --股权投资对象类型(01金融机构 02工商企业)
          ,'03'                                                       AS equityinvestcause            --股权投资形成原因
          ,'0'                                                        AS slflag                       --专业贷款标识                (默认为否,1是0否)
          ,''                                                         AS sltype                       --专业贷款类型
          ,''                                                         AS pfphase                      --项目融资阶段
          ,''                                                         AS regurating                   --监管评级
          ,'0'                                                        AS cbrcmpratingflag             --银监会认定评级是否更为审慎   (默认为否,1是0否)
          ,'0'                                                        AS largeflucflag                --是否波动性较大              (默认为否,1是0否)
          ,'0'                                                        AS liquexpoflag                 --是否清算过程中风险暴露      (默认为否,1是0否)
          ,'0'                                                        AS paymentdealflag              --是否货款对付模式            (默认为否,1是0否)
          ,0                                                          AS delaytradingdays             --延迟交易天数
          ,'0'                                                        AS securitiesflag               --有价证券标识                 (默认为否,1是0否)
          ,''                                                         AS secuissuerid                 --证券发行人ID
          ,''                                                         AS ratingdurationtype           --评级期限类型
          ,''                                                         AS secuissuerating              --证券发行等级
          ,0                                                          AS securesidualm                --证券剩余期限
          ,1                                                          AS securevafrequency            --证券重估频率
          ,'0'                                                        AS ccptranflag                  --是否中央交易对手相关交易    (默认为否,1是0否)
          ,''                                                         AS ccpid                        --中央交易对手ID
          ,'0'                                                        AS qualccpflag                  --是否合格中央交易对手        (默认为否,1是0否)
          ,''                                                         AS bankrole                     --银行角色
          ,''                                                         AS clearingmethod               --清算方式
          ,'0'                                                        AS bankassetflag                --是否银行提交资产            (默认为否,1是0否)
          ,''                                                         AS matchconditions              --符合条件情况
          ,'0'                                                        AS sftflag                      --证券融资交易标识            (默认为否,1是0否)
          ,'0'                                                        AS masternetagreeflag           --净额结算主协议标识          (默认为否,1是0否)
          ,''                                                         AS masternetagreeid             --净额结算主协议ID
          ,''                                                         AS sfttype                      --证券融资交易类型
          ,'0'                                                        AS secuownertransflag           --证券所有权是否转移           (默认为否,1是0否)
          ,'0'                                                        AS otcflag                      --场外衍生工具标识            (默认为否,1是0否)
          ,'0'                                                        AS validnettingflag             --有效净额结算协议标识        (默认为否,1是0否)
          ,''                                                         AS validnetagreementid          --有效净额结算协议ID
          ,''                                                         AS otctype                      --场外衍生工具类型
          ,NULL                                                       AS depositriskperiod            --保证金风险期间
          ,0                                                          AS mtm                          --重置成本
          ,''                                                         AS mtmcurrency                  --重置成本币种
          ,''                                                         AS buyerorseller                --买方卖方
          ,'0'                                                        AS qualroflag                   --合格参照资产标识             (默认为否,1是0否)
          ,'0'                                                        AS roissuerperformflag          --参照资产发行人是否能履约    (默认为否,1是0否)
          ,'0'                                                        AS buyerinsolvencyflag          --信用保护买方是否破产        (默认为否,1是0否)
          ,0                                                          AS nonpaymentfees               --尚未支付费用
          ,'0'                                                        AS retailexpoflag               --零售暴露标识                 (默认为否,1是0否)
          ,''                                                         AS retailclaimtype              --零售债权类型
          ,''                                                         AS mortgagetype                 --住房抵押贷款类型
          ,1                                                          AS exponumber                   --风险暴露个数                (默认为1)
          ,0.8                                                        AS LTV                          --贷款价值比                        默认 0.8
          ,NULL                                                       AS Aging                        --账龄                              默认 NULL
          ,''                                                         AS NewDefaultDebtFlag           --新增违约债项标识                         默认 NULL
          ,''                                                         AS PDPoolModelID                --PD分池模型ID                      默认 NULL
          ,''                                                         AS LGDPoolModelID               --LGD分池模型ID                     默认 NULL
          ,''                                                         AS CCFPoolModelID               --CCF分池模型ID                     默认 NULL
          ,''                                                         AS PDPoolID                     --所属PD池ID                        默认 NULL
          ,''                                                         AS LGDPoolID                    --所属LGD池ID                       默认 NULL
          ,''                                                         AS CCFPoolID                    --所属CCF池ID                       默认 NULL
          ,'0'                                                        AS ABSUAFlag                    --资产证券化基础资产标识            默认 否(0)
          ,''                                                         AS ABSPoolID                    --证券化资产池ID                    默认 NULL
          ,''                                                         AS GroupID                      --分组编号                          默认 NULL
          ,NULL                                                       AS DefaultDate                  --违约时点
          ,NULL                                                       AS ABSPROPORTION                --资产证券化比重
          ,NULL                                                       AS DEBTORNUMBER                 --借款人个数

    FROM        RWA_DEV.RWA_TMP_TAXASSET
    WHERE       DATADATE = V_DATADATE
    AND         BLOCK_ASSET<>0;

    COMMIT;

    /*插入目标表RWA_DEV.rwa_ei_contract*/
    INSERT INTO RWA_DEV.rwa_ei_contract(
                DataDate                             --数据日期
              ,DataNo                               --数据流水号
              ,ContractID                           --合同ID
              ,SContractID                          --源合同ID
              ,SSysID                               --源系统ID
              ,ClientID                             --参与主体ID
              ,SOrgID                               --源机构ID
              ,SOrgName                             --源机构名称
              ,OrgSortNo                            --所属机构排序号
              ,OrgID                                --所属机构ID
              ,OrgName                              --所属机构名称
              ,IndustryID                           --所属行业代码
              ,IndustryName                         --所属行业名称
              ,BusinessLine                         --业务条线
              ,AssetType                            --资产大类
              ,AssetSubType                         --资产小类
              ,BusinessTypeID                       --业务品种代码
              ,BusinessTypeName                     --业务品种名称
              ,CreditRiskDataType                   --信用风险数据类型
              ,StartDate                            --起始日期
              ,DueDate                              --到期日期
              ,OriginalMaturity                     --原始期限
              ,ResidualM                            --剩余期限
              ,SettlementCurrency                   --结算币种
              ,ContractAmount                       --合同总金额
              ,NotExtractPart                       --合同未提取部分
              ,UncondCancelFlag                     --是否可随时无条件撤销
              ,ABSUAFlag                            --资产证券化基础资产标识
              ,ABSPoolID                            --证券化资产池ID
              ,GroupID                              --分组编号
              ,GUARANTEETYPE                        --主要担保方式
              ,ABSPROPORTION                        --资产证券化比重
    )
    SELECT      DISTINCT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DataDate                             --数据日期
                ,p_data_dt_str                               AS DataNo                               --数据流水号
                ,T1.CONTRACTID                               AS ContractID                           --合同ID
                ,T1.CONTRACTID                               AS SContractID                          --源合同ID
                ,T1.SSYSID                                   AS SSysID                               --源系统ID
                ,T1.CLIENTID                                 AS ClientID                             --参与主体ID
                ,T1.SORGID                                   AS SOrgID                               --源机构ID
                ,T1.SORGNAME                                 AS SOrgName                             --源机构名称
                ,T1.ORGSORTNO                                AS OrgSortNo                            --所属机构排序号
                ,T1.ORGID                                    AS OrgID                                --所属机构ID
                ,T1.ORGNAME                                  AS OrgName                              --所属机构名称
                ,T1.INDUSTRYID                               AS IndustryID                           --所属行业代码
                ,T1.INDUSTRYNAME                             AS IndustryName                         --所属行业名称
                ,'0501'                                      AS BusinessLine                         --业务条线
                ,T1.ASSETTYPE                                AS AssetType                            --资产大类
                ,T1.ASSETSUBTYPE                             AS AssetSubType                         --资产小类
                ,T1.BUSINESSTYPEID                           AS BusinessTypeID                       --业务品种代码
                ,T1.BUSINESSTYPENAME                         AS BusinessTypeName                     --业务品种名称
                ,T1.CREDITRISKDATATYPE                       AS CreditRiskDataType                   --信用风险数据类型
                ,T1.STARTDATE                                AS StartDate                            --起始日期
                ,T1.DUEDATE                                  AS DueDate                              --到期日期
                ,T1.ORIGINALMATURITY                         AS OriginalMaturity                     --原始期限
                ,T1.RESIDUALM                                AS ResidualM                            --剩余期限
                ,T1.CURRENCY                                 AS SettlementCurrency                   --结算币种
                ,T1.NORMALPRINCIPAL                          AS ContractAmount                       --合同总金额
                ,0                                           AS NotExtractPart                       --合同未提取部分                        默认 0
                ,'0'                                         AS UncondCancelFlag                     --是否可随时无条件撤销                  默认 否(0)
                ,'0'                                         AS ABSUAFlag                            --资产证券化基础资产标识                默认 否(0)
                ,''                                          AS ABSPoolID                            --证券化资产池ID                        默认 空
                ,''                                          AS GroupID                              --分组编号                              默认 空
                ,''                                          AS GUARANTEETYPE                        --主要担保方式                          默认 空
                ,NULL                                        AS ABSPROPORTION                        --资产证券化比重

    FROM    RWA_DEV.rwa_ei_exposure T1
    WHERE   T1.datadate = v_datadate
    AND     T1.exposureid in ( 'JDYS-011202-' || v_datano,'JDYS-011216-' || v_datano,'GQ' || v_datano)
    AND     T1.ssysid in ('JDYS','GQ')

    ;

    COMMIT;
    
    
    

----------用18110000科目将将宋科补录的递延税金额减去，放在011216

select assetbalance INTO dys from rwa_ei_exposure where SSYSID='JDYS'AND EXPOSUBCLASSSTD='011202'AND DATANO=p_data_dt_str;

UPDATE RWA_EI_EXPOSURE A
SET ASSETBALANCE =
( select SUM(BALANCE_D*NVL(B.MIDDLEPRICE/100, 1)-BALANCE_C*NVL(B.MIDDLEPRICE/100, 1))-dys
 from fns_gl_balance a
 LEFT JOIN TMP_CURRENCY_CHANGE B
 ON A.currency_code=B.CURRENCYCODE
 AND B.DATANO=p_data_dt_str
 where 
        subject_no ='18110000'
        AND A.CURRENCY_CODE<>'RMB'
        AND A.DATANO=p_data_dt_str
        )
        WHERE A.DATANO=p_data_dt_str AND SSYSID='JDYS'AND EXPOSUBCLASSSTD='011216';
        
commit;
    
    


    /*目标表数据统计*/
    --统计插入的记录
    v_count1:=0;
    SELECT COUNT(1) INTO v_count2 FROM RWA_DEV.rwa_ei_client WHERE datadate = v_datadate AND ssysid = 'JDYS' AND clientid = 'JDYS-' || v_datano AND clientname = '净递延税虚拟客户';
    v_count1:=v_count1+v_count2;
    Dbms_output.Put_line('RWA_DEV.rwa_ei_client表当前插入的数据记录为:' || v_count2 || '条');

    SELECT COUNT(1) INTO v_count2 FROM RWA_DEV.rwa_ei_contract WHERE datadate = v_datadate AND ssysid = 'JDYS' AND contractid in ('JDYS-011202-' || v_datano,'JDYS-011216-' || v_datano);
    v_count1:=v_count1+v_count2;
    Dbms_output.Put_line('RWA_DEV.rwa_ei_contract表当前插入的数据记录为:' || v_count2 || '条');

    SELECT COUNT(1) INTO v_count2 FROM RWA_DEV.rwa_ei_exposure WHERE datadate = v_datadate AND ssysid = 'JDYS' AND exposureid in ('JDYS-011202-' || v_datano,'JDYS-011216-' || v_datano);
    v_count1:=v_count1+v_count2;
    Dbms_output.Put_line('RWA_DEV.rwa_ei_exposure表当前插入的数据记录为:' || v_count2 || '条');

    SELECT COUNT(1) INTO v_count2 FROM RWA_DEV.rwa_ei_client WHERE datadate = v_datadate AND ssysid = 'GQ' AND clientid = 'GQ' || v_datano AND clientname = '股权投资虚拟客户';
    v_count1:=v_count1+v_count2;
    Dbms_output.Put_line('RWA_DEV.rwa_ei_client表当前插入的数据记录为:' || v_count2 || '条');

    SELECT COUNT(1) INTO v_count2 FROM RWA_DEV.rwa_ei_contract WHERE datadate = v_datadate AND ssysid = 'GQ' AND contractid = 'GQ' || v_datano;
    v_count1:=v_count1+v_count2;
    Dbms_output.Put_line('RWA_DEV.rwa_ei_contract表当前插入的数据记录为:' || v_count2 || '条');

    SELECT COUNT(1) INTO v_count2 FROM RWA_DEV.rwa_ei_exposure WHERE datadate = v_datadate AND ssysid = 'GQ' AND exposureid = 'GQ' || v_datano;
    v_count1:=v_count1+v_count2;
    Dbms_output.Put_line('RWA_DEV.rwa_ei_exposure表当前插入的数据记录为:' || v_count2 || '条');

    Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功-'||v_count1;
    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
         Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '净递延税资产(RWA_DEV.pro_rwa_tax_asset)ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;

END PRO_RWA_TAX_ASSET;
/

