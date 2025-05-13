CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_TAX_ASSET (
                            p_data_dt_str  IN   VARCHAR2,    --��������
                            p_po_rtncode   OUT  VARCHAR2,    --���ر��
                            p_po_rtnmsg    OUT  VARCHAR2     --��������
)
  /*
    �洢��������:RWA_DEV.pro_rwa_tax_asset
    ʵ�ֹ���:������˰�ʲ����⡢���ڻ�����ȨͶ�ʣ�δ�۳����֣�����
    ���ݿھ�:ȫ��
    ����Ƶ��:��ĩ����
    ��  ��  :V1.0.0
    ��д��  :qpzhong
    ��дʱ��:2015-11-06
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��  :RWA_DEV.rwa_ei_failedttc          |���ϸ�����ʱ����߱�
             RWA_DEV.rwa_ei_unconsfiinvest     |δ������ڻ���Ͷ�ʱ�
             RWA_DEV.rwa_ei_accsubjectdata     |��Ŀȡ����
             RWA_DEV.RWA_EI_TAXASSET           |������˰�ʲ���
             RWA_DEV.RWA_EI_PROFITDIST         |������䷽����
             RWA_DEV.rwa_tmp_taxasset          |������˰�ʲ���
             RWA_DEV.rwa_ei_otocpremium        |����һ���ʱ����߼�����۱�
             RWA_DEV.FNS_BND_BOOK_B            |ծȯ������
             RWA_DEV.FNS_BND_INFO_B            |ծȯ��Ϣ��
    Ŀ���  :RWA_DEV.rwa_ei_client                 |���������
             RWA_DEV.rwa_ei_exposure               |��¶��
             RWA_DEV.rwa_ei_contract               |��ͬ��

    ������  :dual
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.pro_rwa_tax_asset';
  v_datadate DATE := to_date(p_data_dt_str,'yyyy/mm/dd');       --��������
  v_datano VARCHAR2(8) := to_char(v_datadate, 'yyyymmdd');      --������ˮ��
  v_startdate VARCHAR2(10) := to_char(v_datadate,'yyyy-mm-dd'); --��ʼ����
  dys NUMBER(24,6);  ---------����˰

  --����ҵ�����
  p01    NUMBER(24,6);         --�ӻ�ϵ��
  t16    NUMBER(24,6) := 0;    --�����ɶ��ʱ��пɼ��벢���ź���һ���ʱ��Ĳ��֣����ǹ����ڣ�
  t32    NUMBER(24,6) := 0;    --�����ɶ��ʱ��пɼ��벢��������һ���ʱ��Ĳ��֣����ǹ����ڣ�
  t53    NUMBER(24,6) := 0;    --�����ɶ��ʱ��пɼ��벢���Ŷ����ʱ��Ĳ��֣����ǹ����ڣ�
  d51    NUMBER(24,6) := 0;    --�����ʱ����߼�����ۿɼ�����
  gpSTD  NUMBER(24,6) := 0;    --������ʧ׼��ȱ�ڣ�����Ȩ�ط��������÷��ռ�Ȩ�ʲ������У�
  d1     NUMBER(24,6) := 0;    --����һ���ʱ�
  d21    NUMBER(24,6) := 0;    --ȫ��۳���Ŀ�ϼ�
  d2110  NUMBER(24,6) := 0;    --��ҵ���м�ͨ��Э���໥���еĺ���һ���ʱ�
  d2111  NUMBER(24,6) := 0;    --���п���Ȩ��������Ľ��ڻ����ĺ���һ���ʱ�Ͷ��
  d2112  NUMBER(24,6) := 0;    --�п���Ȩ��������Ľ��ڻ����ĺ���һ���ʱ�ȱ��
  d213   NUMBER(24,6) := 0;    --����������δ��ӯ������ľ�����˰�ʲ�
  d221   NUMBER(24,6) := 0;    --��δ������ڻ���С�������ʱ�Ͷ���еĺ���һ���ʱ�
  d2211  NUMBER(24,6) := 0;    --����Ӧ�۳����
  d222   NUMBER(24,6) := 0;    --��δ������ڻ�����������ʱ�Ͷ���еĺ���һ���ʱ�
  d2221  NUMBER(24,6) := 0;    --����Ӧ�۳����
  --hushiwei 20171016 ����˰ȡֵ�޸�
  d223   NUMBER(24,6) := 0;    --��������������δ��ӯ���ľ�����˰�ʲ�_Ȩ��250%����
  d223_o NUMBER(24,6) := 0;    --��������������δ��ӯ���ľ�����˰�ʲ�_Ȩ��100%����
  d2231  NUMBER(24,6) := 0;    --����Ӧ�۳����
  d224   number(24,6) := 0;    --��δ������ڻ�����������ʱ�Ͷ���еĺ���һ���ʱ�����������������δ��ӯ���ľ�����˰�ʲ���δ�۳�����
  d2241  NUMBER(24,6) := 0;    --��������һ���ʱ�15%��Ӧ�۳����
  d22411 NUMBER(24,6) := 0;    --Ӧ�ڶԽ��ڻ�����������ʱ�Ͷ���п۳��Ľ��
  d22412 number(24,6) := 0;    --Ӧ����������������δ��ӯ���ľ�����˰�ʲ��п۳��Ľ��
  d3     NUMBER(24,6) := 0;    --����һ���ʱ�
  d4     NUMBER(24,6) := 0;    --����һ���ʱ���ܿ۳���Ŀ
  d412   NUMBER(24,6) := 0;    --��ҵ���м�ͨ��Э���໥���е�����һ���ʱ�
  d413   NUMBER(24,6) := 0;    --��δ������ڻ�����������ʱ�Ͷ���е�����һ���ʱ�
  d414   NUMBER(24,6) := 0;    --���п���Ȩ��������Ľ��ڻ���������һ���ʱ�Ͷ��
  d415   NUMBER(24,6) := 0;    --�п���Ȩ��������Ľ��ڻ���������һ���ʱ�ȱ��
  d421   NUMBER(24,6) := 0;    --��δ������ڻ���С�������ʱ�Ͷ���е�����һ���ʱ�
  d4211  NUMBER(24,6) := 0;    --����Ӧ�۳�����
  d5     NUMBER(24,6) := 0;    --�����ʱ�
  d6     NUMBER(24,6) := 0;    --�����ʱ���ܿ۳���Ŀ
  d612   NUMBER(24,6) := 0;    --��ҵ���м�ͨ��Э���໥���еĶ����ʱ�
  d613   NUMBER(24,6) := 0;    --��δ������ڻ�����������ʱ�Ͷ���еĶ����ʱ�
  d614   NUMBER(24,6) := 0;    --���п���Ȩ��������Ľ��ڻ����Ķ����ʱ�Ͷ��
  d615   NUMBER(24,6) := 0;    --�п���Ȩ��������Ľ��ڻ����Ķ����ʱ�ȱ��
  d621   NUMBER(24,6) := 0;    --��δ������ڻ���С�������ʱ�Ͷ���еĶ����ʱ�
  d6211  NUMBER(24,6) := 0;    --����Ӧ�۳�����
  d71    NUMBER(24,6)  := 0;   --����һ���ʱ�����1�����۳�ȫ��۳���Ŀ��
  d72    NUMBER(24,6)  := 0;   --����һ���ʱ�����2���۳�ȫ��۳���Ŀ��С������Ͷ��Ӧ�۳����ֺ�
  d73    NUMBER(24,6)  := 0;   --����һ���ʱ�����3���۳���2.2.4.1��������п۳����ľ��

  tax_asset NUMBER(24,6) := 0; --d223 - d2231 - d22412 ����������δ��ӯ���ľ�����˰�ʲ���δ�۳����֣�
  block_asset NUMBER(24,6) := 0; --d221-d2211+d222-d2221-d22411+d421-d4211 �Խ��ڻ����Ĺ�ȨͶ�ʣ�δ�۳����֣�

  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count1 INTEGER;
  v_count2 INTEGER;

  BEGIN
    Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼

    DELETE FROM RWA_DEV.rwa_ei_client WHERE datadate = v_datadate AND ssysid = 'JDYS' AND clientid = 'JDYS-' || v_datano AND clientname = '������˰����ͻ�';
    DELETE FROM RWA_DEV.rwa_ei_exposure WHERE datadate = v_datadate AND ssysid = 'JDYS' AND exposureid = 'JDYS-011202-' || v_datano;
    DELETE FROM RWA_DEV.rwa_ei_exposure WHERE datadate = v_datadate AND ssysid = 'JDYS' AND exposureid = 'JDYS-011216-' || v_datano;
    DELETE FROM RWA_DEV.rwa_ei_contract WHERE datadate = v_datadate AND ssysid = 'JDYS' AND contractid = 'JDYS-011202-' || v_datano;
    DELETE FROM RWA_DEV.rwa_ei_contract WHERE datadate = v_datadate AND ssysid = 'JDYS' AND contractid = 'JDYS-011216-' || v_datano;

    DELETE FROM RWA_DEV.rwa_ei_client WHERE datadate = v_datadate AND ssysid = 'GQ' AND clientid = 'GQ' || v_datano AND CLIENTNAME=  '��ȨͶ������ͻ�';
    DELETE FROM RWA_DEV.rwa_ei_exposure WHERE datadate = v_datadate AND ssysid = 'GQ' AND exposureid = 'GQ' || v_datano;
    DELETE FROM RWA_DEV.rwa_ei_contract WHERE datadate = v_datadate AND ssysid = 'GQ' AND contractid = 'GQ' || v_datano;
    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.rwa_tmp_taxasset';

    --d223 ��������������δ��ӯ���ľ�����˰�ʲ�
    --d213 ����δ��ӯ�����ɾ�Ӫ��������ľ�����˰�ʲ�
    SELECT SUM(ASSET) AS ASSET into d213 FROM RWA_DEV.RWA_EI_TAXASSET WHERE DATADATE = v_datadate and taxtype = '1';

    --d223 ��������������δ��ӯ���ľ�����˰�ʲ�_Ȩ��250%����
    --hushiwei 20171016�޸ĵ���˰ȡֵ   Ȩ��250%
    --chengang  20191121�޸ĵ���˰ȡֵ   Ȩ��250% 
      /* Ӧ����Ϣ�ʲ� 
      ����Ӧ�տӦ�տ���Ͷ���ʲ� 
      ��ծ�ʲ��ʲ� 
      Ӧ������������������տ��ʲ� 
      ����Ӧ�տ�-�����ʲ� 
      Ԥ�Ƹ�ծ-Ӧ������ְ��н���ʲ� 
      Ӧ�����ʼ������ʲ� 
      Ӧ��ס���������ʲ�
      Ӧ�����ᾭ���ʲ�
      Ӧ��������ᱣ���ʲ� */

    SELECT SUM(YSLX_D + QTYSK_D + DZZC_D + YJFZ_D + YFGZJJJ_D + YFZFGJJ_D +
               YFNJ_D + YFGHJF_D + YFJBSHBX_D + YSLCSXFSRZSK_D + QTYSKQT_D) AS ASSET
      into d223
      FROM RWA_DEV.RWA_EI_TAXASSET
     WHERE DATADATE = v_datadate
       and taxtype = '2';

    --d223 ��������������δ��ӯ���ľ�����˰�ʲ�_Ȩ��100%����
    --hushiwei 20171016�޸ĵ���˰ȡֵ   Ȩ��100%
    SELECT SUM(DKZCJZZB_D + CQGQTZ_D + CFTYKX_D + QTYSK_D +
            JYXJRZCGYJZBD_D + JYXJRZC_D +
            ZQTZGYJZBD_D + ZQTZ_D + QTGQGYJZBD_D + QTGQ_D + CYZDQJRZC_D +
            MRFSJRZCLXTZ_D + TXZCLXTZ_D + MCHGJRZCLXTZ_D) AS ASSET into d223_o
    FROM RWA_DEV.RWA_EI_TAXASSET
   WHERE DATADATE = v_datadate and taxtype = '2';

    --p01 �ӻ�ϵ��
    SELECT DECODE(SUBSTR(v_datano,1,4),2013,0.8,2014,0.6,2015,0.4,2016,0.2,0) INTO p01 FROM DUAL;

    --d51 �����ʱ����߼�����ۿɼ�����
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

  --gpSTD ������ʧ׼��ȱ�ڣ�����Ȩ�ط��������÷��ռ�Ȩ�ʲ������У�
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
             AND subjectcode = '1304' --�����ֵ׼��
          );

  --d1 ����һ���ʱ� 4001+4201+4002+4101+4102+4103+4104+6011+6021+6051+6061+6101+6111+6301-6402-6403-6411-6421-6602-6701-6711-6801-6901
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
 * d2110 ��ҵ���м�ͨ��Э���໥���еĺ���һ���ʱ�
 * d412  ��ҵ���м�ͨ��Э���໥���е�����һ���ʱ�
 * d612 ��ҵ���м�ͨ��Э���໥���еĶ����ʱ�
*/
 SELECT nvl(SUM(ctocinvestamount), 0),
        nvl(SUM(otocinvestamount), 0),
        nvl(SUM(ttcinvestamount), 0)
   INTO d2110, d412, d612
   FROM RWA_DEV.rwa_ei_unconsfiinvest  --δ������ڻ���Ͷ�ʱ�
  WHERE datadate = v_datadate
    AND equitynature = '04'
    AND equityinvesttype LIKE '02%' --���ڻ���
    AND consolidateflag = '0'
  ;

  /*
 * d2111 ���п���Ȩ��������Ľ��ڻ����ĺ���һ���ʱ�Ͷ��
 * d414  ���п���Ȩ��������Ľ��ڻ���������һ���ʱ�Ͷ��
 * d614 ���п���Ȩ��������Ľ��ڻ����Ķ����ʱ�Ͷ��
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
 * d2112 �п���Ȩ��������Ľ��ڻ����ĺ���һ���ʱ�ȱ��
 * d415  �п���Ȩ��������Ľ��ڻ���������һ���ʱ�ȱ��
 * d615 �п���Ȩ��������Ľ��ڻ����Ķ����ʱ�ȱ��
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

  --d21 ȫ��۳���Ŀ�ϼ�

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
 * d221 ��δ������ڻ���С�������ʱ�Ͷ���еĺ���һ���ʱ�
 * d421 ��δ������ڻ���С�������ʱ�Ͷ���е�����һ���ʱ�
 * d621 ��δ������ڻ���С�������ʱ�Ͷ���еĶ����ʱ�
*/
  SELECT nvl(SUM(ctocinvestamount), 0),
         nvl(SUM(otocinvestamount), 0)/*,
         nvl(SUM(ttcinvestamount), 0)*/
    INTO d221, d421/*, d621*/
    FROM RWA_DEV.rwa_ei_unconsfiinvest t --δ������ڻ���Ͷ�ʱ�
   WHERE datadate = v_datadate
     AND equitynature = '03'
     AND equityinvesttype LIKE '02%' --���ڻ���
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

/* d222 ��δ������ڻ�����������ʱ�Ͷ���еĺ���һ���ʱ�
 * d413 ��δ������ڻ�����������ʱ�Ͷ���е�����һ���ʱ�
 * d613 ��δ������ڻ�����������ʱ�Ͷ���еĶ����ʱ�
*/
  SELECT nvl(SUM(ctocinvestamount), 0),
         nvl(SUM(otocinvestamount), 0),
         nvl(SUM(ttcinvestamount), 0)
    INTO d222, d413, d613
    FROM RWA_DEV.rwa_ei_unconsfiinvest t --δ������ڻ���Ͷ�ʱ�
   WHERE datadate = v_datadate
     AND equitynature = '02'
     AND equityinvesttype LIKE '02%' --���ڻ���
     and consolidateflag = '0'
  ;

--d731 ����һ���ʱ�����1�����۳�ȫ��۳���Ŀ��
  d71 := d1 - d21;

/*
 * d2211 ����Ӧ�۳����
 * d4211 ����Ӧ�۳�����
 * d6211 ����Ӧ�۳�����
*/
  IF (d221 + d421 + nvl(d621,0)) <> 0 THEN
    SELECT greatest((d221 + d421 + nvl(d621,0) - d71 * 0.1) * d221 / (d221 + d421 + nvl(d621,0)), 0),
           greatest((d221 + d421 + nvl(d621,0) - d71 * 0.1) * d421 / (d221 + d421 + nvl(d621,0)), 0),
           greatest((d221 + d421 + nvl(d621,0) - d71 * 0.1) * d621 / (d221 + d421 + nvl(d621,0)), 0)
      INTO d2211, d4211, d6211
      FROM dual;
  END IF;

--d732 ����һ���ʱ�����2���۳�ȫ��۳���Ŀ��С������Ͷ��Ӧ�۳����ֺ�
  d72 := d71 - d2211;

--d2221 ����Ӧ�۳����
  SELECT greatest(d222 - d72*0.1,0) INTO d2221 FROM dual;

--d2231 ����Ӧ�۳����
  SELECT greatest(d223 - d72*0.1,0) INTO d2231 FROM  dual ;

--d224 ��δ������ڻ�����������ʱ�Ͷ���еĺ���һ���ʱ�����������������δ��ӯ���ľ�����˰�ʲ���δ�۳�����
  d224 := d222 - d2221 + d223 - d2231;

--d3 ����һ���ʱ�
 SELECT nvl(SUM(balance), 0) + t32
   INTO d3
   FROM (SELECT preferredpremium + othertoolspremium AS balance
           FROM RWA_DEV.rwa_ei_otocpremium
          WHERE datadate = v_datadate);

--d5 �����ʱ�
  d5 := d51 + t53;

--d6 �����ʱ���ܿ۳���Ŀ
  d6 := d613 + d6211;

--d4 ����һ���ʱ���ܿ۳���Ŀ
  SELECT -least(d5 - d6,0) + d413 + d4211 INTO d4 FROM dual ;

--d733 ����һ���ʱ�����3���۳���2.2.4.1��������п۳����ľ��
  SELECT d72 - d2221 - d2231 - least(d3 - d4,0) INTO d73 FROM dual ;

--d2241 ����,��������һ���ʱ�15%��Ӧ�۳����
  SELECT greatest((d224 - d73 * 0.15)/0.85,0) INTO d2241 FROM  dual  ;

--d22411 Ӧ�ڶԽ��ڻ�����������ʱ�Ͷ���п۳��Ľ��
 IF
   d224 <> 0
 THEN
   d22411 := nvl(d2241,0) * (d222 - d2221) / d224;
 END IF;

--d22412 Ӧ����������������δ��ӯ���ľ�����˰�ʲ��п۳��Ľ��
 IF
   d224 <> 0
 THEN
   d22412 := d2241 * (d223 - d2231) / d224;
 END IF;

dbms_output.put_line('p01   :' || p01    ); --�ӻ�ϵ��
dbms_output.put_line('t16   :' || t16    ); --�����ɶ��ʱ��пɼ��벢���ź���һ���ʱ��Ĳ��֣����ǹ����ڣ�
dbms_output.put_line('t32   :' || t32    ); --�����ɶ��ʱ��пɼ��벢��������һ���ʱ��Ĳ��֣����ǹ����ڣ�
dbms_output.put_line('t53   :' || t53    ); --�����ɶ��ʱ��пɼ��벢���Ŷ����ʱ��Ĳ��֣����ǹ����ڣ�
dbms_output.put_line('d51   :' || d51    ); --�����ʱ����߼�����ۿɼ�����
dbms_output.put_line('gpSTD :' || gpSTD  ); --������ʧ׼��ȱ�ڣ�����Ȩ�ط��������÷��ռ�Ȩ�ʲ������У�
dbms_output.put_line('d1    :' || d1     ); --����һ���ʱ�
dbms_output.put_line('d21   :' || d21    ); --ȫ��۳���Ŀ�ϼ�
dbms_output.put_line('d2110 :' || d2110  ); --��ҵ���м�ͨ��Э���໥���еĺ���һ���ʱ�
dbms_output.put_line('d2111 :' || d2111  ); --���п���Ȩ��������Ľ��ڻ����ĺ���һ���ʱ�Ͷ��
dbms_output.put_line('d2112 :' || d2112  ); --�п���Ȩ��������Ľ��ڻ����ĺ���һ���ʱ�ȱ��
dbms_output.put_line('d221  :' || d221   ); --��δ������ڻ���С�������ʱ�Ͷ���еĺ���һ���ʱ�
dbms_output.put_line('d2211 :' || d2211  ); --����Ӧ�۳����
dbms_output.put_line('d222  :' || d222   ); --��δ������ڻ�����������ʱ�Ͷ���еĺ���һ���ʱ�
dbms_output.put_line('d2221 :' || d2221  ); --����Ӧ�۳����
dbms_output.put_line('d223  :' || d223   ); --��������������δ��ӯ���ľ�����˰�ʲ�_Ȩ��250%����
dbms_output.put_line('d223_o  :' || d223_o   ); --��������������δ��ӯ���ľ�����˰�ʲ�_Ȩ��100%����
dbms_output.put_line('d2231 :' || d2231  ); --����Ӧ�۳����
dbms_output.put_line('d224  :' || d224   ); --��δ������ڻ�����������ʱ�Ͷ���еĺ���һ���ʱ�����������������δ��ӯ���ľ�����˰�ʲ���δ�۳�����
dbms_output.put_line('d2241 :' || d2241  );--��������һ���ʱ�15%��Ӧ�۳����
dbms_output.put_line('d22411:' || d22411 ); --Ӧ�ڶԽ��ڻ�����������ʱ�Ͷ���п۳��Ľ��
dbms_output.put_line('d22412:' || d22412 ); --Ӧ����������������δ��ӯ���ľ�����˰�ʲ��п۳��Ľ��
dbms_output.put_line('d3    :' || d3     ); --����һ���ʱ�
dbms_output.put_line('d4    :' || d4     ); --����һ���ʱ���ܿ۳���Ŀ
dbms_output.put_line('d412  :' || d412   ); --��ҵ���м�ͨ��Э���໥���е�����һ���ʱ�
dbms_output.put_line('d413  :' || d413   ); --��δ������ڻ�����������ʱ�Ͷ���е�����һ���ʱ�
dbms_output.put_line('d414  :' || d414   ); --���п���Ȩ��������Ľ��ڻ���������һ���ʱ�Ͷ��
dbms_output.put_line('d415  :' || d415   ); --�п���Ȩ��������Ľ��ڻ���������һ���ʱ�ȱ��
dbms_output.put_line('d421  :' || d421   ); --��δ������ڻ���С�������ʱ�Ͷ���е�����һ���ʱ�
dbms_output.put_line('d4211 :' || d4211  ); --����Ӧ�۳�����
dbms_output.put_line('d5    :' || d5     ); --�����ʱ�
dbms_output.put_line('d6    :' || d6     ); --�����ʱ���ܿ۳���Ŀ
dbms_output.put_line('d612  :' || d612   ); --��ҵ���м�ͨ��Э���໥���еĶ����ʱ�
dbms_output.put_line('d613  :' || d613   ); --��δ������ڻ�����������ʱ�Ͷ���еĶ����ʱ�
dbms_output.put_line('d614  :' || d614   ); --���п���Ȩ��������Ľ��ڻ����Ķ����ʱ�Ͷ��
dbms_output.put_line('d615  :' || d615   ); --�п���Ȩ��������Ľ��ڻ����Ķ����ʱ�ȱ��
dbms_output.put_line('d621  :' || d621   ); --��δ������ڻ���С�������ʱ�Ͷ���еĶ����ʱ�
dbms_output.put_line('d6211 :' || d6211  ); --����Ӧ�۳�����
dbms_output.put_line('d71  :' || d71   ); --����һ���ʱ�����1�����۳�ȫ��۳���Ŀ��
dbms_output.put_line('d72  :' || d72   ); --����һ���ʱ�����2���۳�ȫ��۳���Ŀ��С������Ͷ��Ӧ�۳����ֺ�
dbms_output.put_line('d73  :' || d73   ); --����һ���ʱ�����3���۳���2.2.4.1��������п۳����ľ��


tax_asset := d223 - d2231 - nvl(d22412,0);
block_asset := d221 - d2211 + d222 - d2221 - nvl(d22411,0) + d421 - d4211;

dbms_output.put_line('����������δ��ӯ���ľ�����˰�ʲ���δ�۳����֣�:' || tax_asset);
dbms_output.put_line('�Խ��ڻ����Ĺ�ȨͶ�ʣ�δ�۳����֣�:' || block_asset);

insert into RWA_DEV.rwa_tmp_taxasset (DATADATE, DATANO, TAX_ASSET, BLOCK_ASSET, P01, T16, T32, T53, D51, GPSTD, D1, D21, D2110, D2111, D2112, D221, D2211, D222, D2221, D223, D2231, D224, D2241, D22411, D22412, D3, D4, D412, D413, D414, D415, D421, D4211, D5, D6, D612, D613, D614, D615, D621, D6211, D71, D72, D73,TAX_ASSET_O)
values (v_datadate, v_datano, TAX_ASSET, BLOCK_ASSET, P01, T16, T32, T53, D51, GPSTD, D1, D21, D2110, D2111, D2112, D221, D2211, D222, D2221, D223, D2231, D224, D2241, D22411, D22412, D3, D4, D412, D413, D414, D415, D421, D4211, D5, D6, D612, D613, D614, D615, D621, D6211, D71, D72, D73,d223_o);
commit;

--2.���������������ݴ�Դ����뵽Ŀ�����
  /*����Ŀ���RWA_DEV.rwa_ei_client*/
  INSERT INTO RWA_DEV.rwa_ei_client(
         DataDate                   --��������
        ,DataNo                     --������ˮ��
        ,ClientID                   --��������ID
        ,SourceClientID             --Դ��������ID
        ,SSysID                     --ԴϵͳID
        ,ClientName                 --������������
        ,SOrgID                     --Դ����ID
        ,SOrgName                   --Դ��������
        ,OrgSortNo                  --�������������
        ,OrgID                      --��������ID
        ,OrgName                    --������������
        ,IndustryID                 --������ҵ����
        ,IndustryName               --������ҵ����
        ,ClientType                 --�����������
        ,ClientSubType              --��������С��
        ,RegistState                --ע����һ����
        ,RCERating                  --����ע����ⲿ����
        ,RCERAgency                 --����ע����ⲿ��������
        ,OrganizationCode           --��֯��������
        ,ConsolidatedSCFlag         --�Ƿ񲢱��ӹ�˾
        ,SLClientFlag               --רҵ����ͻ���ʶ
        ,SLClientType               --רҵ����ͻ�����
        ,ExpoCategoryIRB            --��������¶���
        ,ModelID                    --ģ��ID
        ,ModelIRating               --ģ���ڲ�����
        ,ModelPD                    --ģ��ΥԼ����
        ,IRating                    --�ڲ�����
        ,PD                         --ΥԼ����
        ,DefaultFlag                --ΥԼ��ʶ
        ,NewDefaultFlag             --����ΥԼ��ʶ
        ,DefaultDate                --ΥԼʱ��
        ,ClientERating              --���������ⲿ����
        ,CCPFlag                    --���뽻�׶��ֱ�ʶ
        ,QualCCPFlag                --�Ƿ�ϸ����뽻�׶���
        ,ClearMemberFlag            --�����Ա��ʶ
        ,CompanySize                --��ҵ��ģ
        ,SSMBFlag                   --��׼С΢��ҵ��ʶ
        ,AnnualSale                 --��˾�ͻ������۶�
        ,CountryCode                --ע����Ҵ���
        ,MSMBFlag                   --���Ų�΢С��ҵ��ʶ
    )
    SELECT
                v_datadate                AS datadate             --��������
                ,v_datano                 AS datano               --������ˮ��
                ,'JDYS-' || v_datano      AS clientid             --�����������
                ,'JDYS-' || v_datano      AS sourceclientid       --Դ�����������
                ,'JDYS'                   AS ssysid               --Դϵͳ����
                ,'������˰����ͻ�'       AS clientname           --������������
                ,'9998'               AS sorgid               --Դ��������
                ,'��������'               AS sorgname             --Դ��������
                ,'1'                      AS orgsortno            --���������
                ,'9998'               AS orgid                --������������
                ,'��������'               AS orgname              --������������
                ,'999999'                 AS industryid           --������ҵ����
                ,'δ֪'                   AS industryname         --������ҵ����
                ,'03'                     AS clienttype           --����������� (03��˾)
                ,'0301'                   AS clientsubtype        --��������С�� (0301һ����ҵ)
                ,'01'                     AS registstate          --ע����һ����
                ,NULL                     AS rcerating            --����ע����ⲿ����
                ,NULL                     AS rceragency           --����ע����ⲿ��������
                ,NULL                     AS organizationcode     --��֯��������
                ,'0'                      AS consolidatedscflag   --�Ƿ񲢱��ӹ�˾
                ,'0'                      AS slclientflag         --רҵ����ͻ���ʶ
                ,NULL                     AS slclienttype         --רҵ����ͻ�����
                ,'020701'                 AS expocategoryirb      --��������¶��� (020701�������ձ�¶)
                ,NULL                     AS ModelID              --ģ��ID
                ,NULL                     AS modelirating         --ģ���ڲ�����
                ,NULL                     AS modelpd              --ģ��ΥԼ����
                ,NULL                     AS irating              --�ڲ�����
                ,NULL                     AS pd                   --ΥԼ����
                ,'0'                      AS DefaultFlag          --ΥԼ��ʶ
                ,'0'                      AS NewDefaultFlag       --����ΥԼ��ʶ
                ,NULL                     AS DefaultDate          --ΥԼʱ��
                ,''                       AS clienterating        --���������ⲿ����
                ,'0'                      AS ccpflag              --���뽻�׶��ֱ�ʶ
                ,'0'                      AS qualccpflag          --�Ƿ�ϸ����뽻�׶���
                ,'0'                      AS clearmemberflag      --�����Ա��ʶ
                ,'01'                     AS CompanySize          --��ҵ��ģ
                ,'0'                      AS ssmbflag             --��׼С΢��ҵ��ʶ
                ,NULL                     AS annualsale           --��˾�ͻ������۶�
                ,'CHN'                    AS countrycode          --ע����Ҵ���
                ,'0'                      AS MSMBFlag             --���Ų�΢С��ҵ��ʶ

    FROM        RWA_DEV.rwa_tmp_taxasset
    where       datadate = v_datadate
    and         TAX_ASSET<>0

    UNION ALL

    SELECT
                v_datadate                AS datadate             --��������
                ,v_datano                 AS datano               --������ˮ��
                ,'GQ' || v_datano         AS clientid             --�����������
                ,'GQ' || v_datano         AS sourceclientid       --Դ�����������
                ,'GQ'                     AS ssysid               --Դϵͳ����
                ,'��ȨͶ������ͻ�'       AS clientname           --������������
                ,'9998'               AS sorgid               --Դ��������
                ,'��������'               AS sorgname             --Դ��������
                ,'1'                      AS orgsortno            --���������
                ,'9998'               AS orgid                --������������
                ,'��������'               AS orgname              --������������
                ,'999999'                 AS industryid           --������ҵ����
                ,'δ֪'                   AS industryname         --������ҵ����
                ,'02'                     AS clienttype           --����������� (02���ڻ���)
                ,'0205'                   AS clientsubtype        --��������С�� (0205��������)
                ,'01'                     AS registstate          --ע����һ����
                ,''		                    AS rcerating            --����ע����ⲿ����
                ,NULL                     AS rceragency           --����ע����ⲿ��������
                ,NULL                     AS organizationcode     --��֯��������
                ,'0'                      AS consolidatedscflag   --�Ƿ񲢱��ӹ�˾
                ,'0'                      AS slclientflag         --רҵ����ͻ���ʶ
                ,NULL                     AS slclienttype         --רҵ����ͻ�����
                ,'020501'                 AS expocategoryirb      --��������¶��� (020501���ڻ���)
                ,NULL                     AS ModelID              --ģ��ID
                ,NULL                     AS modelirating         --ģ���ڲ�����
                ,NULL                     AS modelpd              --ģ��ΥԼ����
                ,NULL                     AS irating              --�ڲ�����
                ,NULL                     AS pd                   --ΥԼ����
                ,'0'                      AS DefaultFlag          --ΥԼ��ʶ
                ,'0'                      AS NewDefaultFlag       --����ΥԼ��ʶ
                ,NULL                     AS DefaultDate          --ΥԼʱ��
                ,''                   		AS clienterating        --���������ⲿ����
                ,'0'                      AS ccpflag              --���뽻�׶��ֱ�ʶ
                ,'0'                      AS qualccpflag          --�Ƿ�ϸ����뽻�׶���
                ,'0'                      AS clearmemberflag      --�����Ա��ʶ
                ,'01'                     AS CompanySize          --��ҵ��ģ
                ,'0'                      AS ssmbflag             --��׼С΢��ҵ��ʶ
                ,NULL                     AS annualsale           --��˾�ͻ������۶�
                ,'CHN'                    AS countrycode          --ע����Ҵ���
                ,'0'                      AS MSMBFlag             --���Ų�΢С��ҵ��ʶ
    FROM        RWA_DEV.RWA_TMP_TAXASSET
    WHERE       DATADATE = V_DATADATE
    AND         BLOCK_ASSET<>0
    ;

    COMMIT;
  /*����Ŀ���RWA_DEV.rwa_ei_exposure*/
    INSERT INTO RWA_DEV.rwa_ei_exposure(
                DataDate                                          --��������
               ,DataNo                                            --������ˮ��
               ,ExposureID                                        --���ձ�¶ID
               ,DueID                                             --ծ��ID
               ,SSysID                                            --ԴϵͳID
               ,ContractID                                        --��ͬID
               ,ClientID                                          --��������ID
               ,SOrgID                                            --Դ����ID
               ,SOrgName                                          --Դ��������
               ,OrgSortNo                                         --�������������
               ,OrgID                                             --��������ID
               ,OrgName                                           --������������
               ,AccOrgID                                          --�������ID
               ,AccOrgName                                        --�����������
               ,IndustryID                                        --������ҵ����
               ,IndustryName                                      --������ҵ����
               ,BusinessLine                                      --ҵ������
               ,AssetType                                         --�ʲ�����
               ,AssetSubType                                      --�ʲ�С��
               ,BusinessTypeID                                    --ҵ��Ʒ�ִ���
               ,BusinessTypeName                                  --ҵ��Ʒ������
               ,CreditRiskDataType                                --���÷�����������
               ,AssetTypeOfHaircuts                               --�ۿ�ϵ����Ӧ�ʲ����
               ,BusinessTypeSTD                                   --Ȩ�ط�ҵ������
               ,ExpoClassSTD                                      --Ȩ�ط���¶����
               ,ExpoSubClassSTD                                   --Ȩ�ط���¶С��
               ,ExpoClassIRB                                      --��������¶����
               ,ExpoSubClassIRB                                   --��������¶С��
               ,ExpoBelong                                        --��¶������ʶ
               ,BookType                                          --�˻����
               ,ReguTranType                                      --��ܽ�������
               ,RepoTranFlag                                      --�ع����ױ�ʶ
               ,RevaFrequency                                     --�ع�Ƶ��
               ,Currency                                          --����
               ,NormalPrincipal                                   --�����������
               ,OverdueBalance                                    --�������
               ,NonAccrualBalance                                 --��Ӧ�����
               ,OnSheetBalance                                    --�������
               ,NormalInterest                                    --������Ϣ
               ,OnDebitInterest                                   --����ǷϢ
               ,OffDebitInterest                                  --����ǷϢ
               ,ExpenseReceivable                                 --Ӧ�շ���
               ,AssetBalance                                      --�ʲ����
               ,AccSubject1                                       --��Ŀһ
               ,AccSubject2                                       --��Ŀ��
               ,AccSubject3                                       --��Ŀ��
               ,StartDate                                         --��ʼ����
               ,DueDate                                           --��������
               ,OriginalMaturity                                  --ԭʼ����
               ,ResidualM                                         --ʣ������
               ,RiskClassify                                      --���շ���
               ,ExposureStatus                                    --���ձ�¶״̬
               ,OverdueDays                                       --��������
               ,SpecialProvision                                  --ר��׼����
               ,GeneralProvision                                  --һ��׼����
               ,EspecialProvision                                 --�ر�׼����
               ,WrittenOffAmount                                  --�Ѻ������
               ,OffExpoSource                                     --���Ⱪ¶��Դ
               ,OffBusinessType                                   --����ҵ������
               ,OffBusinessSdvsSTD                                --Ȩ�ط�����ҵ������ϸ��
               ,UncondCancelFlag                                  --�Ƿ����ʱ����������
               ,CCFLevel                                          --����ת��ϵ������
               ,CCFAIRB                                           --�߼�������ת��ϵ��
               ,ClaimsLevel                                       --ծȨ����
               ,BondFlag                                          --�Ƿ�Ϊծȯ
               ,BondIssueIntent                                   --ծȯ����Ŀ��
               ,NSURealPropertyFlag                               --�Ƿ�����ò�����
               ,RepAssetTermType                                  --��ծ�ʲ���������
               ,DependOnFPOBFlag                                  --�Ƿ�����������δ��ӯ��
               ,IRating                                           --�ڲ�����
               ,PD                                                --ΥԼ����
               ,LGDLevel                                          --ΥԼ��ʧ�ʼ���
               ,LGDAIRB                                           --�߼���ΥԼ��ʧ��
               ,MAIRB                                             --�߼�����Ч����
               ,EADAIRB                                           --�߼���ΥԼ���ձ�¶
               ,DefaultFlag                                       --ΥԼ��ʶ
               ,BEEL                                              --��ΥԼ��¶Ԥ����ʧ����
               ,DefaultLGD                                        --��ΥԼ��¶ΥԼ��ʧ��
               ,EquityExpoFlag                                    --��Ȩ��¶��ʶ
               ,EquityInvestType                                  --��ȨͶ�ʶ�������
               ,EquityInvestCause                                 --��ȨͶ���γ�ԭ��
               ,SLFlag                                            --רҵ�����ʶ
               ,SLType                                            --רҵ��������
               ,PFPhase                                           --��Ŀ���ʽ׶�
               ,ReguRating                                        --�������
               ,CBRCMPRatingFlag                                  --������϶������Ƿ��Ϊ����
               ,LargeFlucFlag                                     --�Ƿ񲨶��Խϴ�
               ,LiquExpoFlag                                      --�Ƿ���������з��ձ�¶
               ,PaymentDealFlag                                   --�Ƿ����Ը�ģʽ
               ,DelayTradingDays                                  --�ӳٽ�������
               ,SecuritiesFlag                                    --�м�֤ȯ��ʶ
               ,SecuIssuerID                                      --֤ȯ������ID
               ,RatingDurationType                                --������������
               ,SecuIssueRating                                   --֤ȯ���еȼ�
               ,SecuResidualM                                     --֤ȯʣ������
               ,SecuRevaFrequency                                 --֤ȯ�ع�Ƶ��
               ,CCPTranFlag                                       --�Ƿ����뽻�׶�����ؽ���
               ,CCPID                                             --���뽻�׶���ID
               ,QualCCPFlag                                       --�Ƿ�ϸ����뽻�׶���
               ,BankRole                                          --���н�ɫ
               ,ClearingMethod                                    --���㷽ʽ
               ,BankAssetFlag                                     --�Ƿ������ύ�ʲ�
               ,MatchConditions                                   --�����������
               ,SFTFlag                                           --֤ȯ���ʽ��ױ�ʶ
               ,MasterNetAgreeFlag                                --���������Э���ʶ
               ,MasterNetAgreeID                                  --���������Э��ID
               ,SFTType                                           --֤ȯ���ʽ�������
               ,SecuOwnerTransFlag                                --֤ȯ����Ȩ�Ƿ�ת��
               ,OTCFlag                                           --�����������߱�ʶ
               ,ValidNettingFlag                                  --��Ч�������Э���ʶ
               ,ValidNetAgreementID                               --��Ч�������Э��ID
               ,OTCType                                           --����������������
               ,DepositRiskPeriod                                 --��֤������ڼ�
               ,MTM                                               --���óɱ�
               ,MTMCurrency                                       --���óɱ�����
               ,BuyerOrSeller                                     --������
               ,QualROFlag                                        --�ϸ�����ʲ���ʶ
               ,ROIssuerPerformFlag                               --�����ʲ��������Ƿ�����Լ
               ,BuyerInsolvencyFlag                               --���ñ������Ƿ��Ʋ�
               ,NonpaymentFees                                    --��δ֧������
               ,RetailExpoFlag                                    --���۱�¶��ʶ
               ,RetailClaimType                                   --����ծȨ����
               ,MortgageType                                      --ס����Ѻ��������
               ,ExpoNumber                                        --���ձ�¶����
               ,LTV                                               --�����ֵ��
               ,Aging                                             --����
               ,NewDefaultDebtFlag                                --����ΥԼծ���ʶ
               ,PDPoolModelID                                     --PD�ֳ�ģ��ID
               ,LGDPoolModelID                                    --LGD�ֳ�ģ��ID
               ,CCFPoolModelID                                    --CCF�ֳ�ģ��ID
               ,PDPoolID                                          --����PD��ID
               ,LGDPoolID                                         --����LGD��ID
               ,CCFPoolID                                         --����CCF��ID
               ,ABSUAFlag                                         --�ʲ�֤ȯ�������ʲ���ʶ
               ,ABSPoolID                                         --֤ȯ���ʲ���ID
               ,GroupID                                           --������
               ,DefaultDate                                       --ΥԼʱ��
               ,ABSPROPORTION                                     --�ʲ�֤ȯ������
               ,DEBTORNUMBER                                      --����˸���
    )
  SELECT
          v_datadate                                                  AS datadate                     --��������
          ,v_datano                                                   AS datano                       --������ˮ��
          ,'JDYS-011202-' || v_datano                                        AS exposureid                   --���ձ�¶ID
          ,'JDYS-011202-' || v_datano                                        AS dueid                        --ծ��ID
          ,'JDYS'                                                     AS ssysid                       --ԴϵͳID
          ,'JDYS-011202-' || v_datano                                        AS contractid                   --��ͬID
          ,'JDYS-' || v_datano                                        AS clientid                     --��������ID
          ,'9998'                                                 AS sorgid                       --Դ����ID
          ,'��������'                                                 AS sorgname                     --Դ��������
          ,'1'                                                        AS orgsortno                    --���������
          ,'9998'                                                 AS orgid                        --��������ID
          ,'��������'                                                 AS orgname                      --������������
          ,'9998'                                                 AS accorgid                     --�������ID
          ,'��������'                                                 AS accorgname                   --�����������
          ,'999999'                                                   AS industryid                   --������ҵ����
          ,'δ֪'                                                     AS industryname                 --������ҵ����
          ,'0501'                                                     AS businessline                 --����   (����0501)
          ,'130'                                                      AS assettype                    --�ʲ�����(130 ���������ʲ�)
          ,'13001'                                                    AS assetsubtype                 --�ʲ�С��(13001 ��������˰�ʲ�)
          ,'109070'                                                   AS businesstypeid               --ҵ��Ʒ�ִ���
          ,'������˰�ʲ�'                                             AS businesstypename             --ҵ��Ʒ������
          ,'01'                                                       AS creditriskdatatype           --���÷�����������(01:һ�������,02:һ������)
          ,'01'                                                       AS assettypeofhaircuts          --�ۿ�ϵ����Ӧ�ʲ����
          ,'07'                                                       AS businesstypestd              --Ȩ�ط�ҵ������(�Թ� һ���ʲ�07 ���� ����06)
          ,'0112'                                                     AS expoclassstd                 --Ȩ�ط���¶����(�Թ� 0106 ���� 0108)
          ,'011202'                                                   AS exposubclassstd              --Ȩ�ط���¶С��(�Թ� 010601 ���� 010803)
          ,'0203'                                                     AS expoclassirb                 --��������¶����(�Թ� 0203 ���� 0204)
          ,'020301'                                                   AS exposubclassirb              --��������¶С��(�Թ� 020301 ���� 020403)
          ,'01'                                                       AS expobelong                   --��¶������ʶ((01:����;02:һ�����;03:���׶���;))
          ,'01'                                                       AS booktype                     --�˻����(�̶�ֵ"�����˻�",01:�����˻�,02:�����˻�)
          ,'03'                                                       AS regutrantype                 --��ܽ�������(�̶�ֵ"��Ѻ����",01:�ع�����;02:�����ʱ��г�����;03:��Ѻ����;)
          ,'0'                                                        AS repotranflag                 --�ع����ױ�ʶ(�̶�ֵΪ"��" 0)
          ,1                                                          AS revafrequency                --�ع�Ƶ��
          ,'CNY'                                                      AS currency                     --����('CNY'�����)
          ,nvl(d223,0)                                           AS normalprincipal              --�����������
          ,0                                                          AS overduebalance               --�������
          ,0                                                          AS nonaccrualbalance            --��Ӧ�����
          ,nvl(d223,0)                                           AS onsheetbalance               --�������(�����������+�������+��Ӧ�����)
          ,0                                                          AS normalinterest               --������Ϣ
          ,0                                                          AS ondebitinterest              --����ǷϢ
          ,0                                                          AS offdebitinterest             --����ǷϢ
          ,0                                                          AS expensereceivable            --Ӧ�շ���
          ,nvl(d223,0)                                           AS assetbalance                 --�ʲ����
          ,'18110000'                                                 AS accsubject1                  --��Ŀһ
          ,NULL                                                       AS accsubject2                  --��Ŀ��
          ,NULL                                                       AS accsubject3                  --��Ŀ��
          ,v_startdate                                                AS startdate                    --��ʼ����
          ,TO_CHAR(ADD_MONTHS(v_datadate,1),'YYYY-MM-DD')             AS duedate                      --��������
          ,(ADD_MONTHS(v_datadate,1) - v_datadate)/365                AS originalmaturity             --ԭʼ����
          ,(ADD_MONTHS(v_datadate,1) - v_datadate)/365                AS residualm                    --ʣ������
          ,'01'                                                       AS riskclassify                 --���շ���(Ĭ��Ϊ01����)
          ,''                                                         AS exposurestatus               --���ձ�¶״̬(Ĭ��Ϊ��)
          ,0                                                          AS overduedays                  --��������
          ,0                                                          AS specialprovision             --ר��׼����
          ,0                                                          AS generalprovision             --һ��׼����
          ,0                                                          AS especialprovision            --�ر�׼����
          ,0                                                          AS writtenoffamount             --�Ѻ������
          ,''                                                         AS offexposource                --���Ⱪ¶��Դ
          ,''                                                         AS offbusinesstype              --����ҵ������
          ,''                                                         AS offbusinesssdvsstd           --Ȩ�ط�����ҵ������ϸ��
          ,'0'                                                        AS uncondcancelflag             --�Ƿ����ʱ����������(Ĭ��Ϊ��,1��0��)
          ,''                                                         AS ccflevel                     --����ת��ϵ������
          ,NULL                                                       AS ccfairb                      --�߼�������ת��ϵ��
          ,'01'                                                       AS claimslevel                  --ծȨ���� (Ĭ��Ϊ�߼�ծȨ01)
          ,'0'                                                        AS bondflag                     --�Ƿ�Ϊծȯ                  (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS bondissueintent              --ծȯ����Ŀ��
          ,'0'                                                        AS nsurealpropertyflag          --�Ƿ�����ò�����              (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS repassettermtype             --��ծ�ʲ���������
          ,'1'                                                        AS dependonfpobflag             --�Ƿ�����������δ��ӯ��     (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS irating                      --�ڲ�����
          ,NULL                                                       AS pd                           --ΥԼ����
          ,''                                                         AS lgdlevel                     --ΥԼ��ʧ�ʼ���
          ,NULL                                                       AS lgdairb                      --�߼���ΥԼ��ʧ��
          ,NULL                                                       AS mairb                        --�߼�����Ч����
          ,NULL                                                       AS eadairb                      --�߼���ΥԼ���ձ�¶
          ,'0'                                                        AS defaultflag                  --ΥԼ��ʶ                    (Ĭ��Ϊ��,1��0��)
          ,NULL                                                       AS beel                         --��ΥԼ��¶Ԥ����ʧ����
          ,NULL                                                       AS defaultlgd                   --��ΥԼ��¶ΥԼ��ʧ��
          ,'0'                                                        AS equityexpoflag               --��Ȩ��¶��ʶ                 (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS equityinvesttype             --��ȨͶ�ʶ�������
          ,''                                                         AS equityinvestcause            --��ȨͶ���γ�ԭ��
          ,'0'                                                        AS slflag                       --רҵ�����ʶ                (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS sltype                       --רҵ��������
          ,''                                                         AS pfphase                      --��Ŀ���ʽ׶�
          ,''                                                         AS regurating                   --�������
          ,'0'                                                        AS cbrcmpratingflag             --������϶������Ƿ��Ϊ����   (Ĭ��Ϊ��,1��0��)
          ,'0'                                                        AS largeflucflag                --�Ƿ񲨶��Խϴ�              (Ĭ��Ϊ��,1��0��)
          ,'0'                                                        AS liquexpoflag                 --�Ƿ���������з��ձ�¶      (Ĭ��Ϊ��,1��0��)
          ,'0'                                                        AS paymentdealflag              --�Ƿ����Ը�ģʽ            (Ĭ��Ϊ��,1��0��)
          ,0                                                          AS delaytradingdays             --�ӳٽ�������
          ,'0'                                                        AS securitiesflag               --�м�֤ȯ��ʶ                 (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS secuissuerid                 --֤ȯ������ID
          ,''                                                         AS ratingdurationtype           --������������
          ,''                                                         AS secuissuerating              --֤ȯ���еȼ�
          ,0                                                          AS securesidualm                --֤ȯʣ������
          ,0                                                          AS securevafrequency            --֤ȯ�ع�Ƶ��
          ,'0'                                                        AS ccptranflag                  --�Ƿ����뽻�׶�����ؽ���    (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS ccpid                        --���뽻�׶���ID
          ,'0'                                                        AS qualccpflag                  --�Ƿ�ϸ����뽻�׶���        (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS bankrole                     --���н�ɫ
          ,''                                                         AS clearingmethod               --���㷽ʽ
          ,'0'                                                        AS bankassetflag                --�Ƿ������ύ�ʲ�            (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS matchconditions              --�����������
          ,'0'                                                        AS sftflag                      --֤ȯ���ʽ��ױ�ʶ            (Ĭ��Ϊ��,1��0��)
          ,'0'                                                        AS masternetagreeflag           --���������Э���ʶ          (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS masternetagreeid             --���������Э��ID
          ,''                                                         AS sfttype                      --֤ȯ���ʽ�������
          ,'0'                                                        AS secuownertransflag           --֤ȯ����Ȩ�Ƿ�ת��           (Ĭ��Ϊ��,1��0��)
          ,'0'                                                        AS otcflag                      --�����������߱�ʶ            (Ĭ��Ϊ��,1��0��)
          ,'0'                                                        AS validnettingflag             --��Ч�������Э���ʶ        (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS validnetagreementid          --��Ч�������Э��ID
          ,''                                                         AS otctype                      --����������������
          ,NULL                                                       AS depositriskperiod            --��֤������ڼ�
          ,0                                                          AS mtm                          --���óɱ�
          ,''                                                         AS mtmcurrency                  --���óɱ�����
          ,''                                                         AS buyerorseller                --������
          ,'0'                                                        AS qualroflag                   --�ϸ�����ʲ���ʶ             (Ĭ��Ϊ��,1��0��)
          ,'0'                                                        AS roissuerperformflag          --�����ʲ��������Ƿ�����Լ    (Ĭ��Ϊ��,1��0��)
          ,'0'                                                        AS buyerinsolvencyflag          --���ñ������Ƿ��Ʋ�        (Ĭ��Ϊ��,1��0��)
          ,0                                                          AS nonpaymentfees               --��δ֧������
          ,'0'                                                        AS retailexpoflag               --���۱�¶��ʶ                 (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS retailclaimtype              --����ծȨ����
          ,''                                                         AS mortgagetype                 --ס����Ѻ��������
          ,1                                                          AS exponumber                   --���ձ�¶����                (Ĭ��Ϊ1)
          ,0.8                                                        AS LTV                          --�����ֵ��                            Ĭ�� 0.8
          ,NULL                                                       AS Aging                        --����                                  Ĭ�� NULL
          ,''                                                         AS NewDefaultDebtFlag           --����ΥԼծ���ʶ                             Ĭ�� NULL
          ,''                                                         AS pdpoolmodelid                --PD�ֳ�ģ��ID
          ,''                                                         AS lgdpoolmodelid               --LGD�ֳ�ģ��ID
          ,''                                                         AS ccfpoolmodelid               --CCF�ֳ�ģ��ID
          ,''                                                         AS pdpoolid                     --����PD��ID
          ,''                                                         AS lgdpoolid                    --����LGD��ID
          ,''                                                         AS ccfpoolid                    --����CCF��ID
          ,'0'                                                        AS absuaflag                    --�ʲ�֤ȯ�������ʲ���ʶ     (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS abspoolid                    --֤ȯ���ʲ���ID
          ,''                                                         AS groupid                      --������
          ,NULL                                                       AS DefaultDate                  --ΥԼʱ��
          ,NULL                                                       AS ABSPROPORTION                --�ʲ�֤ȯ������
          ,NULL                                                       AS DEBTORNUMBER                 --����˸���
    FROM    RWA_DEV.RWA_TMP_TAXASSET
    WHERE   DATADATE = V_DATADATE
    AND     tax_asset<>0

  UNION ALL
  --hushiwei 20171016 ����˰ȡֵ�޸�
  SELECT
          v_datadate                                                  AS datadate                     --��������
          ,v_datano                                                   AS datano                       --������ˮ��
          ,'JDYS-011216-' || v_datano                                        AS exposureid                   --���ձ�¶ID
          ,'JDYS-011216-' || v_datano                                        AS dueid                        --ծ��ID
          ,'JDYS'                                                     AS ssysid                       --ԴϵͳID
          ,'JDYS-011216-' || v_datano                                        AS contractid                   --��ͬID
          ,'JDYS-' || v_datano                                        AS clientid                     --��������ID
          ,'9998'                                                 AS sorgid                       --Դ����ID
          ,'��������'                                                 AS sorgname                     --Դ��������
          ,'1'                                                        AS orgsortno                    --���������
          ,'9998'                                                 AS orgid                        --��������ID
          ,'��������'                                                 AS orgname                      --������������
          ,'9998'                                                 AS accorgid                     --�������ID
          ,'��������'                                                 AS accorgname                   --�����������
          ,'999999'                                                   AS industryid                   --������ҵ����
          ,'δ֪'                                                     AS industryname                 --������ҵ����
          ,'0501'                                                     AS businessline                 --����   (����0501)
          ,'130'                                                      AS assettype                    --�ʲ�����(130 ���������ʲ�)
          ,'13001'                                                    AS assetsubtype                 --�ʲ�С��(13001 ��������˰�ʲ�)
          ,'109070'                                                   AS businesstypeid               --ҵ��Ʒ�ִ���
          ,'������˰�ʲ�'                                             AS businesstypename             --ҵ��Ʒ������
          ,'01'                                                       AS creditriskdatatype           --���÷�����������(01:һ�������,02:һ������)
          ,'01'                                                       AS assettypeofhaircuts          --�ۿ�ϵ����Ӧ�ʲ����
          ,'07'                                                       AS businesstypestd              --Ȩ�ط�ҵ������(�Թ� һ���ʲ�07 ���� ����06)
          ,'0112'                                                     AS expoclassstd                 --Ȩ�ط���¶����(�Թ� 0106 ���� 0108)
          ,'011216'                                                   AS exposubclassstd              --Ȩ�ط���¶С��(�Թ� 010601 ���� 010803)
          ,'0203'                                                     AS expoclassirb                 --��������¶����(�Թ� 0203 ���� 0204)
          ,'020301'                                                   AS exposubclassirb              --��������¶С��(�Թ� 020301 ���� 020403)
          ,'01'                                                       AS expobelong                   --��¶������ʶ((01:����;02:һ�����;03:���׶���;))
          ,'01'                                                       AS booktype                     --�˻����(�̶�ֵ"�����˻�",01:�����˻�,02:�����˻�)
          ,'03'                                                       AS regutrantype                 --��ܽ�������(�̶�ֵ"��Ѻ����",01:�ع�����;02:�����ʱ��г�����;03:��Ѻ����;)
          ,'0'                                                        AS repotranflag                 --�ع����ױ�ʶ(�̶�ֵΪ"��" 0)
          ,1                                                          AS revafrequency                --�ع�Ƶ��
          ,'CNY'                                                      AS currency                     --����('CNY'�����)
          ,nvl(tax_asset_o,0)                                           AS normalprincipal              --�����������
          ,0                                                          AS overduebalance               --�������
          ,0                                                          AS nonaccrualbalance            --��Ӧ�����
          ,nvl(tax_asset_o,0)                                           AS onsheetbalance               --�������(�����������+�������+��Ӧ�����)
          ,0                                                          AS normalinterest               --������Ϣ
          ,0                                                          AS ondebitinterest              --����ǷϢ
          ,0                                                          AS offdebitinterest             --����ǷϢ
          ,0                                                          AS expensereceivable            --Ӧ�շ���
          ,nvl(tax_asset_o,0)                                           AS assetbalance                 --�ʲ����
          ,'18110000'                                                 AS accsubject1                  --��Ŀһ
          ,NULL                                                       AS accsubject2                  --��Ŀ��
          ,NULL                                                       AS accsubject3                  --��Ŀ��
          ,v_startdate                                                AS startdate                    --��ʼ����
          ,TO_CHAR(ADD_MONTHS(v_datadate,1),'YYYY-MM-DD')             AS duedate                      --��������
          ,(ADD_MONTHS(v_datadate,1) - v_datadate)/365                AS originalmaturity             --ԭʼ����
          ,(ADD_MONTHS(v_datadate,1) - v_datadate)/365                AS residualm                    --ʣ������
          ,'01'                                                       AS riskclassify                 --���շ���(Ĭ��Ϊ01����)
          ,''                                                         AS exposurestatus               --���ձ�¶״̬(Ĭ��Ϊ��)
          ,0                                                          AS overduedays                  --��������
          ,0                                                          AS specialprovision             --ר��׼����
          ,0                                                          AS generalprovision             --һ��׼����
          ,0                                                          AS especialprovision            --�ر�׼����
          ,0                                                          AS writtenoffamount             --�Ѻ������
          ,''                                                         AS offexposource                --���Ⱪ¶��Դ
          ,''                                                         AS offbusinesstype              --����ҵ������
          ,''                                                         AS offbusinesssdvsstd           --Ȩ�ط�����ҵ������ϸ��
          ,'0'                                                        AS uncondcancelflag             --�Ƿ����ʱ����������(Ĭ��Ϊ��,1��0��)
          ,''                                                         AS ccflevel                     --����ת��ϵ������
          ,NULL                                                       AS ccfairb                      --�߼�������ת��ϵ��
          ,'01'                                                       AS claimslevel                  --ծȨ���� (Ĭ��Ϊ�߼�ծȨ01)
          ,'0'                                                        AS bondflag                     --�Ƿ�Ϊծȯ                  (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS bondissueintent              --ծȯ����Ŀ��
          ,'0'                                                        AS nsurealpropertyflag          --�Ƿ�����ò�����              (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS repassettermtype             --��ծ�ʲ���������
          ,'1'                                                        AS dependonfpobflag             --�Ƿ�����������δ��ӯ��     (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS irating                      --�ڲ�����
          ,NULL                                                       AS pd                           --ΥԼ����
          ,''                                                         AS lgdlevel                     --ΥԼ��ʧ�ʼ���
          ,NULL                                                       AS lgdairb                      --�߼���ΥԼ��ʧ��
          ,NULL                                                       AS mairb                        --�߼�����Ч����
          ,NULL                                                       AS eadairb                      --�߼���ΥԼ���ձ�¶
          ,'0'                                                        AS defaultflag                  --ΥԼ��ʶ                    (Ĭ��Ϊ��,1��0��)
          ,NULL                                                       AS beel                         --��ΥԼ��¶Ԥ����ʧ����
          ,NULL                                                       AS defaultlgd                   --��ΥԼ��¶ΥԼ��ʧ��
          ,'0'                                                        AS equityexpoflag               --��Ȩ��¶��ʶ                 (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS equityinvesttype             --��ȨͶ�ʶ�������
          ,''                                                         AS equityinvestcause            --��ȨͶ���γ�ԭ��
          ,'0'                                                        AS slflag                       --רҵ�����ʶ                (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS sltype                       --רҵ��������
          ,''                                                         AS pfphase                      --��Ŀ���ʽ׶�
          ,''                                                         AS regurating                   --�������
          ,'0'                                                        AS cbrcmpratingflag             --������϶������Ƿ��Ϊ����   (Ĭ��Ϊ��,1��0��)
          ,'0'                                                        AS largeflucflag                --�Ƿ񲨶��Խϴ�              (Ĭ��Ϊ��,1��0��)
          ,'0'                                                        AS liquexpoflag                 --�Ƿ���������з��ձ�¶      (Ĭ��Ϊ��,1��0��)
          ,'0'                                                        AS paymentdealflag              --�Ƿ����Ը�ģʽ            (Ĭ��Ϊ��,1��0��)
          ,0                                                          AS delaytradingdays             --�ӳٽ�������
          ,'0'                                                        AS securitiesflag               --�м�֤ȯ��ʶ                 (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS secuissuerid                 --֤ȯ������ID
          ,''                                                         AS ratingdurationtype           --������������
          ,''                                                         AS secuissuerating              --֤ȯ���еȼ�
          ,0                                                          AS securesidualm                --֤ȯʣ������
          ,0                                                          AS securevafrequency            --֤ȯ�ع�Ƶ��
          ,'0'                                                        AS ccptranflag                  --�Ƿ����뽻�׶�����ؽ���    (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS ccpid                        --���뽻�׶���ID
          ,'0'                                                        AS qualccpflag                  --�Ƿ�ϸ����뽻�׶���        (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS bankrole                     --���н�ɫ
          ,''                                                         AS clearingmethod               --���㷽ʽ
          ,'0'                                                        AS bankassetflag                --�Ƿ������ύ�ʲ�            (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS matchconditions              --�����������
          ,'0'                                                        AS sftflag                      --֤ȯ���ʽ��ױ�ʶ            (Ĭ��Ϊ��,1��0��)
          ,'0'                                                        AS masternetagreeflag           --���������Э���ʶ          (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS masternetagreeid             --���������Э��ID
          ,''                                                         AS sfttype                      --֤ȯ���ʽ�������
          ,'0'                                                        AS secuownertransflag           --֤ȯ����Ȩ�Ƿ�ת��           (Ĭ��Ϊ��,1��0��)
          ,'0'                                                        AS otcflag                      --�����������߱�ʶ            (Ĭ��Ϊ��,1��0��)
          ,'0'                                                        AS validnettingflag             --��Ч�������Э���ʶ        (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS validnetagreementid          --��Ч�������Э��ID
          ,''                                                         AS otctype                      --����������������
          ,NULL                                                       AS depositriskperiod            --��֤������ڼ�
          ,0                                                          AS mtm                          --���óɱ�
          ,''                                                         AS mtmcurrency                  --���óɱ�����
          ,''                                                         AS buyerorseller                --������
          ,'0'                                                        AS qualroflag                   --�ϸ�����ʲ���ʶ             (Ĭ��Ϊ��,1��0��)
          ,'0'                                                        AS roissuerperformflag          --�����ʲ��������Ƿ�����Լ    (Ĭ��Ϊ��,1��0��)
          ,'0'                                                        AS buyerinsolvencyflag          --���ñ������Ƿ��Ʋ�        (Ĭ��Ϊ��,1��0��)
          ,0                                                          AS nonpaymentfees               --��δ֧������
          ,'0'                                                        AS retailexpoflag               --���۱�¶��ʶ                 (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS retailclaimtype              --����ծȨ����
          ,''                                                         AS mortgagetype                 --ס����Ѻ��������
          ,1                                                          AS exponumber                   --���ձ�¶����                (Ĭ��Ϊ1)
          ,0.8                                                        AS LTV                          --�����ֵ��                            Ĭ�� 0.8
          ,NULL                                                       AS Aging                        --����                                  Ĭ�� NULL
          ,''                                                         AS NewDefaultDebtFlag           --����ΥԼծ���ʶ                             Ĭ�� NULL
          ,''                                                         AS pdpoolmodelid                --PD�ֳ�ģ��ID
          ,''                                                         AS lgdpoolmodelid               --LGD�ֳ�ģ��ID
          ,''                                                         AS ccfpoolmodelid               --CCF�ֳ�ģ��ID
          ,''                                                         AS pdpoolid                     --����PD��ID
          ,''                                                         AS lgdpoolid                    --����LGD��ID
          ,''                                                         AS ccfpoolid                    --����CCF��ID
          ,'0'                                                        AS absuaflag                    --�ʲ�֤ȯ�������ʲ���ʶ     (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS abspoolid                    --֤ȯ���ʲ���ID
          ,''                                                         AS groupid                      --������
          ,NULL                                                       AS DefaultDate                  --ΥԼʱ��
          ,NULL                                                       AS ABSPROPORTION                --�ʲ�֤ȯ������
          ,NULL                                                       AS DEBTORNUMBER                 --����˸���
    FROM    RWA_DEV.RWA_TMP_TAXASSET
    WHERE   DATADATE = V_DATADATE
    AND     tax_asset_o<>0

  UNION ALL

      SELECT
          v_datadate                                                  AS datadate                     --��������
          ,v_datano                                                   AS datano                       --������ˮ��
          ,'GQ' || v_datano                                           AS exposureid                   --���ձ�¶ID
          ,'GQ' || v_datano                                           AS dueid                        --ծ��ID
          ,'GQ'                                                       AS ssysid                       --ԴϵͳID
          ,'GQ' || v_datano                                           AS contractid                   --��ͬID
          ,'GQ' || v_datano                                           AS clientid                     --��������ID
          ,'9998'                                                 AS sorgid                       --Դ����ID
          ,'��������'                                                 AS sorgname                     --Դ��������
          ,'1'                                                        AS orgsortno                    --���������
          ,'9998'                                                 AS orgid                        --��������ID
          ,'��������'                                                 AS orgname                      --������������
          ,'9998'                                                 AS accorgid                     --�������ID
          ,'��������'                                                 AS accorgname                   --�����������
          ,'999999'                                                   AS industryid                   --������ҵ����
          ,'δ֪'                                                     AS industryname                 --������ҵ����
          ,'0501'                                                     AS businessline                 --����          (����0501)
          ,'121'                                                      AS assettype                    --�ʲ�����(121 ���������ʲ�)
          ,'12103'                                                    AS assetsubtype                 --�ʲ�С��(12103 ���ڹ�ȨͶ��)
          ,'109060'                                                   AS businesstypeid               --ҵ��Ʒ�ִ���
          ,'��ȨͶ��'                                                 AS businesstypename             --ҵ��Ʒ������
          ,'03'                                                       AS creditriskdatatype           --���÷�����������(01:һ�������,02:һ������,03:��Ȩ)
          ,'02'                                                       AS assettypeofhaircuts          --�ۿ�ϵ����Ӧ�ʲ����(02 �����ֽ��ֵ�����ٱ��յ���������Ʋ�Ʒ)
          ,'05'                                                       AS businesstypestd              --Ȩ�ط�ҵ������(�Թ� һ���ʲ�07 ���� ����06 ��Ȩ05)
          ,'0110'                                                     AS expoclassstd                 --Ȩ�ط���¶����
          ,'011001'                                                   AS exposubclassstd              --Ȩ�ط���¶С��
          ,'0205'                                                     AS expoclassirb                 --��������¶����
          ,'020501'                                                   AS exposubclassirb              --��������¶С��
          ,'01'                                                       AS expobelong                   --��¶������ʶ((01:����;02:һ�����;03:���׶���;))
          ,'01'                                                       AS booktype                     --�˻����(�̶�ֵ"�����˻�",01:�����˻�,02:�����˻�)
          ,'02'                                                       AS regutrantype                 --��ܽ�������(�̶�ֵ"��Ѻ����",01:�ع�����;02:�����ʱ��г�����;03:��Ѻ����;)
          ,'0'                                                        AS repotranflag                 --�ع����ױ�ʶ(�̶�ֵΪ"��" 0)
          ,1                                                          AS revafrequency                --�ع�Ƶ��
          ,'CNY'                                                      AS currency                     --����(CNY�����)
          ,BLOCK_ASSET                                                AS normalprincipal              --�����������
          ,0                                                          AS overduebalance               --�������
          ,0                                                          AS nonaccrualbalance            --��Ӧ�����
          ,BLOCK_ASSET                                                AS onsheetbalance               --�������(�����������+�������+��Ӧ�����)
          ,0                                                          AS normalinterest               --������Ϣ
          ,0                                                          AS ondebitinterest              --����ǷϢ
          ,0                                                          AS offdebitinterest             --����ǷϢ
          ,0                                                          AS expensereceivable            --Ӧ�շ���
          ,BLOCK_ASSET                                                AS assetbalance                 --�ʲ����
          ,'15110100'                                                 AS accsubject1                  --��Ŀһ
          ,NULL                                                       AS accsubject2                  --��Ŀ��
          ,NULL                                                       AS accsubject3                  --��Ŀ��
          ,v_startdate                                                AS startdate                    --��ʼ����
          ,TO_CHAR(ADD_MONTHS(v_datadate,1),'YYYY-MM-DD')             AS duedate                      --��������
          ,(ADD_MONTHS(v_datadate,1) - v_datadate)/365                AS originalmaturity             --ԭʼ����
          ,(ADD_MONTHS(v_datadate,1) - v_datadate)/365                AS residualm                    --ʣ������
          ,'01'                                                       AS riskclassify                 --���շ���
          ,'01'                                                       AS exposurestatus               --���ձ�¶״̬(Ĭ��Ϊ��)
          ,0                                                          AS overduedays                  --��������
          ,0                                                          AS specialprovision             --ר��׼����
          ,0                                                          AS generalprovision             --һ��׼����
          ,0                                                          AS especialprovision            --�ر�׼����
          ,0                                                          AS writtenoffamount             --�Ѻ������
          ,''                                                         AS offexposource                --���Ⱪ¶��Դ
          ,''                                                         AS offbusinesstype              --����ҵ������
          ,''                                                         AS offbusinesssdvsstd           --Ȩ�ط�����ҵ������ϸ��
          ,'0'                                                        AS uncondcancelflag             --�Ƿ����ʱ����������(Ĭ��Ϊ��,1��0��)
          ,''                                                         AS ccflevel                     --����ת��ϵ������
          ,NULL                                                       AS ccfairb                      --�߼�������ת��ϵ��
          ,'01'                                                       AS claimslevel                  --ծȨ���� (Ĭ��Ϊ�߼�ծȨ01)
          ,'0'                                                        AS bondflag                     --�Ƿ�Ϊծȯ                  (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS bondissueintent              --ծȯ����Ŀ��
          ,'0'                                                        AS nsurealpropertyflag          --�Ƿ�����ò�����              (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS repassettermtype             --��ծ�ʲ���������
          ,'0'                                                        AS dependonfpobflag             --�Ƿ�����������δ��ӯ��     (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS irating                      --�ڲ�����
          ,NULL                                                       AS pd                           --ΥԼ����
          ,''                                                         AS lgdlevel                     --ΥԼ��ʧ�ʼ���
          ,NULL                                                       AS lgdairb                      --�߼���ΥԼ��ʧ��
          ,NULL                                                       AS mairb                        --�߼�����Ч����
          ,NULL                                                       AS eadairb                      --�߼���ΥԼ���ձ�¶
          ,'0'                                                        AS defaultflag                  --ΥԼ��ʶ                    (Ĭ��Ϊ��,1��0��)
          ,0.45                                                       AS beel                         --��ΥԼ��¶Ԥ����ʧ����
          ,0.45                                                       AS defaultlgd                   --��ΥԼ��¶ΥԼ��ʧ��
          ,'1'                                                        AS equityexpoflag               --��Ȩ��¶��ʶ                 (Ĭ��Ϊ��,1��0��)
          ,'01'                                                       AS equityinvesttype             --��ȨͶ�ʶ�������(01���ڻ��� 02������ҵ)
          ,'03'                                                       AS equityinvestcause            --��ȨͶ���γ�ԭ��
          ,'0'                                                        AS slflag                       --רҵ�����ʶ                (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS sltype                       --רҵ��������
          ,''                                                         AS pfphase                      --��Ŀ���ʽ׶�
          ,''                                                         AS regurating                   --�������
          ,'0'                                                        AS cbrcmpratingflag             --������϶������Ƿ��Ϊ����   (Ĭ��Ϊ��,1��0��)
          ,'0'                                                        AS largeflucflag                --�Ƿ񲨶��Խϴ�              (Ĭ��Ϊ��,1��0��)
          ,'0'                                                        AS liquexpoflag                 --�Ƿ���������з��ձ�¶      (Ĭ��Ϊ��,1��0��)
          ,'0'                                                        AS paymentdealflag              --�Ƿ����Ը�ģʽ            (Ĭ��Ϊ��,1��0��)
          ,0                                                          AS delaytradingdays             --�ӳٽ�������
          ,'0'                                                        AS securitiesflag               --�м�֤ȯ��ʶ                 (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS secuissuerid                 --֤ȯ������ID
          ,''                                                         AS ratingdurationtype           --������������
          ,''                                                         AS secuissuerating              --֤ȯ���еȼ�
          ,0                                                          AS securesidualm                --֤ȯʣ������
          ,1                                                          AS securevafrequency            --֤ȯ�ع�Ƶ��
          ,'0'                                                        AS ccptranflag                  --�Ƿ����뽻�׶�����ؽ���    (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS ccpid                        --���뽻�׶���ID
          ,'0'                                                        AS qualccpflag                  --�Ƿ�ϸ����뽻�׶���        (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS bankrole                     --���н�ɫ
          ,''                                                         AS clearingmethod               --���㷽ʽ
          ,'0'                                                        AS bankassetflag                --�Ƿ������ύ�ʲ�            (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS matchconditions              --�����������
          ,'0'                                                        AS sftflag                      --֤ȯ���ʽ��ױ�ʶ            (Ĭ��Ϊ��,1��0��)
          ,'0'                                                        AS masternetagreeflag           --���������Э���ʶ          (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS masternetagreeid             --���������Э��ID
          ,''                                                         AS sfttype                      --֤ȯ���ʽ�������
          ,'0'                                                        AS secuownertransflag           --֤ȯ����Ȩ�Ƿ�ת��           (Ĭ��Ϊ��,1��0��)
          ,'0'                                                        AS otcflag                      --�����������߱�ʶ            (Ĭ��Ϊ��,1��0��)
          ,'0'                                                        AS validnettingflag             --��Ч�������Э���ʶ        (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS validnetagreementid          --��Ч�������Э��ID
          ,''                                                         AS otctype                      --����������������
          ,NULL                                                       AS depositriskperiod            --��֤������ڼ�
          ,0                                                          AS mtm                          --���óɱ�
          ,''                                                         AS mtmcurrency                  --���óɱ�����
          ,''                                                         AS buyerorseller                --������
          ,'0'                                                        AS qualroflag                   --�ϸ�����ʲ���ʶ             (Ĭ��Ϊ��,1��0��)
          ,'0'                                                        AS roissuerperformflag          --�����ʲ��������Ƿ�����Լ    (Ĭ��Ϊ��,1��0��)
          ,'0'                                                        AS buyerinsolvencyflag          --���ñ������Ƿ��Ʋ�        (Ĭ��Ϊ��,1��0��)
          ,0                                                          AS nonpaymentfees               --��δ֧������
          ,'0'                                                        AS retailexpoflag               --���۱�¶��ʶ                 (Ĭ��Ϊ��,1��0��)
          ,''                                                         AS retailclaimtype              --����ծȨ����
          ,''                                                         AS mortgagetype                 --ס����Ѻ��������
          ,1                                                          AS exponumber                   --���ձ�¶����                (Ĭ��Ϊ1)
          ,0.8                                                        AS LTV                          --�����ֵ��                        Ĭ�� 0.8
          ,NULL                                                       AS Aging                        --����                              Ĭ�� NULL
          ,''                                                         AS NewDefaultDebtFlag           --����ΥԼծ���ʶ                         Ĭ�� NULL
          ,''                                                         AS PDPoolModelID                --PD�ֳ�ģ��ID                      Ĭ�� NULL
          ,''                                                         AS LGDPoolModelID               --LGD�ֳ�ģ��ID                     Ĭ�� NULL
          ,''                                                         AS CCFPoolModelID               --CCF�ֳ�ģ��ID                     Ĭ�� NULL
          ,''                                                         AS PDPoolID                     --����PD��ID                        Ĭ�� NULL
          ,''                                                         AS LGDPoolID                    --����LGD��ID                       Ĭ�� NULL
          ,''                                                         AS CCFPoolID                    --����CCF��ID                       Ĭ�� NULL
          ,'0'                                                        AS ABSUAFlag                    --�ʲ�֤ȯ�������ʲ���ʶ            Ĭ�� ��(0)
          ,''                                                         AS ABSPoolID                    --֤ȯ���ʲ���ID                    Ĭ�� NULL
          ,''                                                         AS GroupID                      --������                          Ĭ�� NULL
          ,NULL                                                       AS DefaultDate                  --ΥԼʱ��
          ,NULL                                                       AS ABSPROPORTION                --�ʲ�֤ȯ������
          ,NULL                                                       AS DEBTORNUMBER                 --����˸���

    FROM        RWA_DEV.RWA_TMP_TAXASSET
    WHERE       DATADATE = V_DATADATE
    AND         BLOCK_ASSET<>0;

    COMMIT;

    /*����Ŀ���RWA_DEV.rwa_ei_contract*/
    INSERT INTO RWA_DEV.rwa_ei_contract(
                DataDate                             --��������
              ,DataNo                               --������ˮ��
              ,ContractID                           --��ͬID
              ,SContractID                          --Դ��ͬID
              ,SSysID                               --ԴϵͳID
              ,ClientID                             --��������ID
              ,SOrgID                               --Դ����ID
              ,SOrgName                             --Դ��������
              ,OrgSortNo                            --�������������
              ,OrgID                                --��������ID
              ,OrgName                              --������������
              ,IndustryID                           --������ҵ����
              ,IndustryName                         --������ҵ����
              ,BusinessLine                         --ҵ������
              ,AssetType                            --�ʲ�����
              ,AssetSubType                         --�ʲ�С��
              ,BusinessTypeID                       --ҵ��Ʒ�ִ���
              ,BusinessTypeName                     --ҵ��Ʒ������
              ,CreditRiskDataType                   --���÷�����������
              ,StartDate                            --��ʼ����
              ,DueDate                              --��������
              ,OriginalMaturity                     --ԭʼ����
              ,ResidualM                            --ʣ������
              ,SettlementCurrency                   --�������
              ,ContractAmount                       --��ͬ�ܽ��
              ,NotExtractPart                       --��ͬδ��ȡ����
              ,UncondCancelFlag                     --�Ƿ����ʱ����������
              ,ABSUAFlag                            --�ʲ�֤ȯ�������ʲ���ʶ
              ,ABSPoolID                            --֤ȯ���ʲ���ID
              ,GroupID                              --������
              ,GUARANTEETYPE                        --��Ҫ������ʽ
              ,ABSPROPORTION                        --�ʲ�֤ȯ������
    )
    SELECT      DISTINCT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DataDate                             --��������
                ,p_data_dt_str                               AS DataNo                               --������ˮ��
                ,T1.CONTRACTID                               AS ContractID                           --��ͬID
                ,T1.CONTRACTID                               AS SContractID                          --Դ��ͬID
                ,T1.SSYSID                                   AS SSysID                               --ԴϵͳID
                ,T1.CLIENTID                                 AS ClientID                             --��������ID
                ,T1.SORGID                                   AS SOrgID                               --Դ����ID
                ,T1.SORGNAME                                 AS SOrgName                             --Դ��������
                ,T1.ORGSORTNO                                AS OrgSortNo                            --�������������
                ,T1.ORGID                                    AS OrgID                                --��������ID
                ,T1.ORGNAME                                  AS OrgName                              --������������
                ,T1.INDUSTRYID                               AS IndustryID                           --������ҵ����
                ,T1.INDUSTRYNAME                             AS IndustryName                         --������ҵ����
                ,'0501'                                      AS BusinessLine                         --ҵ������
                ,T1.ASSETTYPE                                AS AssetType                            --�ʲ�����
                ,T1.ASSETSUBTYPE                             AS AssetSubType                         --�ʲ�С��
                ,T1.BUSINESSTYPEID                           AS BusinessTypeID                       --ҵ��Ʒ�ִ���
                ,T1.BUSINESSTYPENAME                         AS BusinessTypeName                     --ҵ��Ʒ������
                ,T1.CREDITRISKDATATYPE                       AS CreditRiskDataType                   --���÷�����������
                ,T1.STARTDATE                                AS StartDate                            --��ʼ����
                ,T1.DUEDATE                                  AS DueDate                              --��������
                ,T1.ORIGINALMATURITY                         AS OriginalMaturity                     --ԭʼ����
                ,T1.RESIDUALM                                AS ResidualM                            --ʣ������
                ,T1.CURRENCY                                 AS SettlementCurrency                   --�������
                ,T1.NORMALPRINCIPAL                          AS ContractAmount                       --��ͬ�ܽ��
                ,0                                           AS NotExtractPart                       --��ͬδ��ȡ����                        Ĭ�� 0
                ,'0'                                         AS UncondCancelFlag                     --�Ƿ����ʱ����������                  Ĭ�� ��(0)
                ,'0'                                         AS ABSUAFlag                            --�ʲ�֤ȯ�������ʲ���ʶ                Ĭ�� ��(0)
                ,''                                          AS ABSPoolID                            --֤ȯ���ʲ���ID                        Ĭ�� ��
                ,''                                          AS GroupID                              --������                              Ĭ�� ��
                ,''                                          AS GUARANTEETYPE                        --��Ҫ������ʽ                          Ĭ�� ��
                ,NULL                                        AS ABSPROPORTION                        --�ʲ�֤ȯ������

    FROM    RWA_DEV.rwa_ei_exposure T1
    WHERE   T1.datadate = v_datadate
    AND     T1.exposureid in ( 'JDYS-011202-' || v_datano,'JDYS-011216-' || v_datano,'GQ' || v_datano)
    AND     T1.ssysid in ('JDYS','GQ')

    ;

    COMMIT;
    
    
    

----------��18110000��Ŀ�����οƲ�¼�ĵ���˰����ȥ������011216

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
    
    


    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    v_count1:=0;
    SELECT COUNT(1) INTO v_count2 FROM RWA_DEV.rwa_ei_client WHERE datadate = v_datadate AND ssysid = 'JDYS' AND clientid = 'JDYS-' || v_datano AND clientname = '������˰����ͻ�';
    v_count1:=v_count1+v_count2;
    Dbms_output.Put_line('RWA_DEV.rwa_ei_client��ǰ��������ݼ�¼Ϊ:' || v_count2 || '��');

    SELECT COUNT(1) INTO v_count2 FROM RWA_DEV.rwa_ei_contract WHERE datadate = v_datadate AND ssysid = 'JDYS' AND contractid in ('JDYS-011202-' || v_datano,'JDYS-011216-' || v_datano);
    v_count1:=v_count1+v_count2;
    Dbms_output.Put_line('RWA_DEV.rwa_ei_contract��ǰ��������ݼ�¼Ϊ:' || v_count2 || '��');

    SELECT COUNT(1) INTO v_count2 FROM RWA_DEV.rwa_ei_exposure WHERE datadate = v_datadate AND ssysid = 'JDYS' AND exposureid in ('JDYS-011202-' || v_datano,'JDYS-011216-' || v_datano);
    v_count1:=v_count1+v_count2;
    Dbms_output.Put_line('RWA_DEV.rwa_ei_exposure��ǰ��������ݼ�¼Ϊ:' || v_count2 || '��');

    SELECT COUNT(1) INTO v_count2 FROM RWA_DEV.rwa_ei_client WHERE datadate = v_datadate AND ssysid = 'GQ' AND clientid = 'GQ' || v_datano AND clientname = '��ȨͶ������ͻ�';
    v_count1:=v_count1+v_count2;
    Dbms_output.Put_line('RWA_DEV.rwa_ei_client��ǰ��������ݼ�¼Ϊ:' || v_count2 || '��');

    SELECT COUNT(1) INTO v_count2 FROM RWA_DEV.rwa_ei_contract WHERE datadate = v_datadate AND ssysid = 'GQ' AND contractid = 'GQ' || v_datano;
    v_count1:=v_count1+v_count2;
    Dbms_output.Put_line('RWA_DEV.rwa_ei_contract��ǰ��������ݼ�¼Ϊ:' || v_count2 || '��');

    SELECT COUNT(1) INTO v_count2 FROM RWA_DEV.rwa_ei_exposure WHERE datadate = v_datadate AND ssysid = 'GQ' AND exposureid = 'GQ' || v_datano;
    v_count1:=v_count1+v_count2;
    Dbms_output.Put_line('RWA_DEV.rwa_ei_exposure��ǰ��������ݼ�¼Ϊ:' || v_count2 || '��');

    Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�-'||v_count1;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '������˰�ʲ�(RWA_DEV.pro_rwa_tax_asset)ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;

END PRO_RWA_TAX_ASSET;
/

