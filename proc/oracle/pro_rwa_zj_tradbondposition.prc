CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ZJ_TRADBONDPOSITION(
                            p_data_dt_str IN  VARCHAR2,   --�������� yyyyMMdd
                            p_po_rtncode  OUT VARCHAR2,   --���ر�� 1 �ɹ�,0 ʧ��
                            p_po_rtnmsg   OUT VARCHAR2    --��������
        )
  /*
    �洢��������:RWA_DEV.PRO_RWA_ZJ_TRADBONDPOSITION
    ʵ�ֹ���:�г�����-�ʽ�ϵͳ-����ծȯͷ���
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :CHENGANG
    ��дʱ��:2019-04-18
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.BRD_BOND|ծ����Ϣ��
    Դ  ��2 :RWA_DEV.BRD_SECURITY_POSI|ծȯͷ����Ϣ��
     �����¼(�޸���|�޸�ʱ��|�޸�����):
     pxl 2019/09/05  ����Դ�߼�
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ZJ_TRADBONDPOSITION';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;
  --v_count1 INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_ZJ_TRADBONDPOSITION';

    --2.���������������ݴ�Դ����뵽Ŀ�����
    INSERT INTO RWA_DEV.RWA_ZJ_TRADBONDPOSITION
      (DATADATE, --��������
       POSITIONID, --ͷ��ID
       BONDID, --ծȯID
       TRANORGID, --���׻���ID
       ACCORGID, --�������ID
       INSTRUMENTSTYPE, --���ڹ�������
       ACCSUBJECTS, --��ƿ�Ŀ
       DENOMINATION, --���
       MARKETVALUE, --��ֵ
       DISCOUNTPREMIUM, --�����
       FAIRVALUECHANGE, --���ʼ�ֵ�䶯
       BOOKBALANCE, --�������
       CURRENCY --����
       )
      SELECT TO_DATE(p_data_dt_str, 'YYYYMMDD'), --��������
             T1.ACCT_NO, --ͷ��ID                                                                                     
             T1.ACCT_NO, --ծȯID
             T1.MAG_ORG_CD,  --���׻���ID
             T1.MAG_ORG_CD,  --�������ID
             '0101',     --���ڹ�������  Ĭ��ծȯ
             T1.SBJT_CD, --��ƿ�Ŀ
             T1.PAR_VALUE, --���
             NVL(T2.CLEAN_PRICE, 0), --��ֵ  ȡ��ծȯ����
             0,   --�����
             T1.FAIR_EXCH_VAL,  --���ʼ�ֵ�䶯
             NVL(T1.POSITION_INITIAL_VALUE, 0) +
             NVL(T1.FAIR_EXCH_VAL, 0), --NVL(T1.ACCRUAL, 0), --������� = �ɱ���Ŀ���˽��  + ���ʼ�ֵ�䶯���            
             DECODE(T2.CCY_CD,'156','CNY',T2.CCY_CD) --����
        FROM BRD_SECURITY_POSI T1 --ծȯͷ����Ϣ
       INNER JOIN BRD_BOND T2 --ծȯ
         ON T1.SECURITY_REFERENCE = T2.BOND_ID
        AND T2.DATANO = p_data_dt_str
        AND T2.BELONG_GROUP = '4' --�ʽ�ϵͳ                       
      WHERE T1.DATANO = p_data_dt_str
        AND T1.SBJT_CD = '11010101'  --�Թ��ʼ�ֵ��������䶯���뵱������Ľ����ʲ�         
        AND T2.BOND_TYPE NOT IN ('TTC')   --�ų��ǹ�ծ  TTC �����ʱ�����
    ;
    
    COMMIT;
    
    --- ����Ʒ����ͷ��
     /* INSERT INTO RWA_DEV.RWA_ZJ_TRADBONDPOSITION
      (DATADATE, --��������
       POSITIONID, --ͷ��ID
       BONDID, --ծȯID
       TRANORGID, --���׻���ID
       ACCORGID, --�������ID
       INSTRUMENTSTYPE, --���ڹ�������
       ACCSUBJECTS, --��ƿ�Ŀ
       DENOMINATION, --���
       MARKETVALUE, --��ֵ
       DISCOUNTPREMIUM, --�����
       FAIRVALUECHANGE, --���ʼ�ֵ�䶯
       BOOKBALANCE, --�������
       CURRENCY --����
       )
      SELECT TO_DATE(p_data_dt_str, 'YYYYMMDD'), --��������
             T.DEALNO || T.SEQ, --ͷ��ID                                                                                     
             T.DEALNO || T.SEQ, --ծȯID
             '6001', --���׻���ID
             '6001', --�������ID
             '0201', --���ڹ�������  Ĭ��ծȯ
             '70120000', --��ƿ�Ŀ
             T.NOTCCYAMT, --���
             ABS(T.NPVBAMT), --��ֵ  ȡ��ծȯ����
             0, --�����
             0, --���ʼ�ֵ�䶯
             ABS(T.NPVBAMT), --�������    
             T.INTCCY --����
        FROM OPI_SWDT T --��������
        LEFT JOIN OPI_SWDH H --������ͷ 
          ON T.DEALNO = H.DEALNO
         AND H.DATANO = p_data_dt_str
       WHERE T.DATANO = p_data_dt_str
    ;
    
    COMMIT;
    */

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_ZJ_TRADBONDPOSITION',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_ZJ_TRADBONDPOSITION;
    --Dbms_output.Put_line('RWA_DEV.RWA_TZ_CONTRACT��ǰ����Ĳ���ϵͳ-Ӧ�տ�Ͷ�����ݼ�¼Ϊ: ' || (v_count1 - v_count) || ' ��');



    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
         ROLLBACK;
         p_po_rtncode := sqlcode;
         p_po_rtnmsg  := '����ծȯͷ���('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_ZJ_TRADBONDPOSITION;
/

