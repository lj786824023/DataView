CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_KHPJ_WSIB(
                            p_data_dt_str IN  VARCHAR2,   --�������� yyyyMMdd
                            p_po_rtncode  OUT VARCHAR2,   --���ر�� 1 �ɹ�,0 ʧ��
                            p_po_rtnmsg   OUT VARCHAR2    --��������
        )
  /*
    �洢��������:RWA_DEV.PRO_RWA_WSIB_KHPJ
    ʵ�ֹ���:
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :
    ��дʱ��:2020-01-06
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_WSIB_KHPJ';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    --EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA.RWA_WSIB_KHPJ';
    DELETE FROM RWA.RWA_WSIB_KHPJ WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    COMMIT;

    --2.���������������ݴ�Դ����뵽Ŀ�����
    
    INSERT INTO RWA.RWA_WSIB_KHPJ 
    (
    DATADATE                               --��������
   ,ORGID                                  --����ID
   ,CUSTOMERID                             --�ͻ����
   ,CUSTOMERNAME                           --�ͻ�����
   ,CREDITLEVEL                            --������Ϣ
    )
  select 
       TO_DATE(p_data_dt_str, 'YYYYMMDD') AS DATADATE --��������
       ,'9998'    AS ORGID                              --����ID
       ,K1.CUSTOMERID  AS CUSTOMERID                    --�ͻ����
       ,K1.CUSTOMERNAME  AS    CUSTOMERNAME             --�ͻ�����
       ,k2.creditlevel    AS   CREDITLEVEL              --������Ϣ
  from (select distinct nvl(T3.TAXID, 'OPI' || TRIM(T2.CNO)) as CUSTOMERID,
          T3.Cfn1 as CUSTOMERNAME
          FROM RWA_DEV.OPI_SWDT T1 --��������
          LEFT JOIN RWA_DEV.OPI_SWDH T2 --������ͷ 
            ON T1.DEALNO = T2.DEALNO
           AND T2.DATANO = p_data_dt_str
          LEFT JOIN RWA_DEV.OPI_CUST T3 --�ͻ���Ϣ
            ON T2.CNO = T3.CNO
           AND T3.DATANO = p_data_dt_str
         WHERE T1.DATANO = p_data_dt_str
           AND T1.PAYRECIND = 'R'
           AND SUBSTR(T2.COST, 1, 1) = 3
           AND SUBSTR(T2.COST, 6, 1) in (1, 2)
           AND T2.VERIND = 1 
           AND T2.REVDATE IS NULL
        union
        select distinct NVL(T2.TAXID, 'OPI' || TRIM(T1.CUST)) as CUSTOMERID,
                        T2.Cfn1 as CUSTOMERNAME
          FROM OPI_FXDH T1
          LEFT JOIN OPI_CUST T2
            ON T1.DATANO = T2.DATANO
           AND T1.CUST = T2.CNO
         WHERE T1.PS = 'P' --���׷���  ��
           AND SUBSTR(T1.COST, 1, 1) = 2
           AND SUBSTR(T1.COST, 6, 1) IN (3, 2, 1)
           AND T1.VERIND = 1
           AND T1.REVDATE IS NULL
           AND T1.DATANO = p_data_dt_str) K1
           LEFT JOIN(SELECT CUSTOMERID, creditlevel
           FROM RWA.RWA_WS_KHPJ_BL
          WHERE DATADATE =
          (SELECT MAX(DATADATE)
          FROM RWA.RWA_WS_KHPJ_BL
          WHERE DATADATE < TO_DATE(p_data_dt_str, 'YYYYMMDD'))) K2---ȡ��һ�ڵĲ�¼������Ϣ�̵�
           on TRIM(K1.CUSTOMERID)=K2.CUSTOMERID;
           
           commit;
 
    dbms_stats.gather_table_stats(ownname => 'RWA',tabname => 'RWA_WSIB_KHPJ',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA.RWA_WSIB_KHPJ WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA.RWA_WSIB_KHPJ��ǰ����ĺ���ϵͳ-ֱ�����е���̵����ݼ�¼Ϊ: ' || v_count || ' ��');



    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
         ROLLBACK;
         p_po_rtncode := sqlcode;
         p_po_rtnmsg  := '�ͻ�������Ϣ��¼�����̵�('|| v_pro_name ||')����ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_KHPJ_WSIB;
/

