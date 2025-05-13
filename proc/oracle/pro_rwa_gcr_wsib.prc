CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_GCR_WSIB(p_data_dt_str IN VARCHAR2, --�������� yyyyMMdd
                                             p_po_rtncode  OUT VARCHAR2, --���ر�� 1 �ɹ�,0 ʧ��
                                             p_po_rtnmsg   OUT VARCHAR2 --��������
                                             )
/*
  �洢��������:RWA_DEV.PRO_RWA_WSIB_GCR
  ʵ�ֹ���:���ʻ����Ĺ۲��ղ�¼�̵�
  ���ݿھ�:ȫ��
  ����Ƶ��:�³�����
  ��  ��  :V1.0.0
  ��д��  :CHENGANG
  ��дʱ��:2020-04-26
  ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
  �����¼(�޸���|�޸�ʱ��|�޸�����):
  */
 AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_WSIB_GCR';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

BEGIN
  --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

  /*�����ȫ�����ݼ��������Ŀ���*/
  --1.���Ŀ����е�ԭ�м�¼
  --EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA.RWA_WSIB_GCR';
  DELETE FROM RWA.RWA_WSIB_GCR
   WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD');

  COMMIT;

  --2.���������������ݴ�Դ����뵽Ŀ�����

  INSERT INTO RWA.RWA_WSIB_GCR
    (DATADATE --��������
    ,
     ORGID --����ID
    ,
     DEALNO --��ˮ��
    ,
     CFN1 --�ͻ�����
    ,
     GCRQ --�۲�����
    ,
     GCRZ --�۲���ֹ
     )
    select TO_DATE(p_data_dt_str, 'YYYYMMDD') AS DATADATE --��������
          ,
           '9998' AS ORGID --����ID
          ,
           TT.DEALNO,--��ˮ��
           TT.CFN1,--�ͻ�����
           '',
           ''
      from (select DISTINCT T1.DEALNO, T3.CFN1
              FROM RWA_DEV.OPI_SWDT T1 --��������
             inner JOIN RWA_DEV.OPI_SWDH T2 --������ͷ 
                ON T1.DEALNO = T2.DEALNO
               AND T2.DATANO = p_data_dt_str
               AND T2.PORT <> 'SWDK' --�ų��ṹ�Դ��ҵ��
              LEFT JOIN RWA_DEV.OPI_CUST T3 --�ͻ���Ϣ
                ON trim(T2.CNO) = trim(T3.CNO)
               AND T3.DATANO = p_data_dt_str
             WHERE T1.DATANO = p_data_dt_str
               AND SUBSTR(T2.COST, 1, 1) = '3' --��һλ=3  --����Ϊ����/���ҵ���ҵ��
                  --AND SUBSTR(T2.COST, 4, 1) <> '3' --ȡ�����˻��£�̨���ǰ�����������ҵ��ģ������������ȥ����
               AND SUBSTR(T2.COST, 6, 1) IN ('1', '2', '3') --����λ=1  --���ʵ���
               AND T2.VERIND = 1
               AND TRIM(T2.REVDATE) IS NULL
            -- ORDER BY T1.DEALNO
            )   TT;

  commit;

  dbms_stats.gather_table_stats(ownname => 'RWA',
                                tabname => 'RWA_WSIB_GCR',
                                cascade => true);

  /*Ŀ�������ͳ��*/
  --ͳ�Ʋ���ļ�¼��
  SELECT COUNT(1)
    INTO v_count
    FROM RWA.RWA_WSIB_GCR
   WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD');
  --Dbms_output.Put_line('RWA.RWA_WSIB_GCR��ǰ����ĺ���ϵͳ-ֱ�����е���̵����ݼ�¼Ϊ: ' || v_count || ' ��');

  --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

  p_po_rtncode := '1';
  p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
  --�����쳣
EXCEPTION
  WHEN OTHERS THEN
    --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
    ROLLBACK;
    p_po_rtncode := sqlcode;
    p_po_rtnmsg  := '�۲�����Ϣ�����̵�(' || v_pro_name || ')����ʧ�ܣ�' || sqlerrm ||
                    ';��������Ϊ:' || dbms_utility.format_error_backtrace;
    RETURN;
END PRO_RWA_GCR_WSIB;
/

