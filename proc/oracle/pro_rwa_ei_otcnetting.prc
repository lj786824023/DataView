CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_OTCNETTING(P_DATA_DT_STR IN VARCHAR2, --�������� yyyyMMdd
                                                      P_PO_RTNCODE  OUT VARCHAR2, --���ر�� 1 �ɹ�,0 ʧ��
                                                      P_PO_RTNMSG   OUT VARCHAR2 --��������
                                                      )
/*
  �洢��������:RWA_DEV.PRO_RWA_EI_OTCNETTING
  ʵ�ֹ���:����ϵͳ-����Ʒҵ��-�����������߾�������
  ���ݿھ�:ȫ��
  ����Ƶ��:�³�����
  ��  ��  :V1.0.0
  ��д��  :CHENGANG
  ��дʱ��:2019-04-17
  ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾

  �����¼(�޸���|�޸�ʱ��|�޸�����):

  */
 AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  V_PRO_NAME VARCHAR2(200) := 'RWA_DEV.RWA_EI_OTCNETTING';
  --�����쳣����
  V_RAISE EXCEPTION;
  --���嵱ǰ����ļ�¼��
  V_COUNT INTEGER;

BEGIN
  --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

  /*�����ȫ�����ݼ��������Ŀ���*/

 BEGIN
     --ɾ�������ڵķ���������˴洢�����ǵ�һ�����׳��쳣�����Ժ���
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_OTCNETTING DROP PARTITION EXPOSURE' ||
                      p_data_dt_str;

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      IF (SQLCODE <> '-2149') THEN
        --�״η���truncate�����2149�쳣
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '�������÷��ձ�¶��(' || v_pro_name || ')ETLת��ʧ�ܣ�' ||
                        sqlerrm || ';��������Ϊ:' ||
                        dbms_utility.format_error_backtrace;
        RETURN;
      END IF;
  END;


  --����һ����ǰ�����µķ���
  EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_OTCNETTING ADD PARTITION EXPOSURE' ||
                    p_data_dt_str || ' VALUES(TO_DATE(' || p_data_dt_str ||
                    ',''YYYYMMDD''))';

  COMMIT;
 /* --1.���Ŀ����е�ԭ�м�¼



  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_EI_OTCNETTING';*/

  --2.���������������ݴ�Դ����뵽Ŀ�����
  INSERT INTO RWA_DEV.RWA_EI_OTCNETTING
   (      DATADATE,          --��������
          DATANO,          --������ˮ��
          VALIDNETAGREEMENTID,          --��Ч�������Э��ID
          COUNTERPARTYID,          --���׶���ID
          ORGSORTNO,          --���������
          ORGID,          --��������ID
          ORGNAME,          --������������
          INDUSTRYID,          --������ҵ����
          INDUSTRYNAME,          --������ҵ����
          BUSINESSLINE,          --����
          ASSETTYPE,          --�ʲ�����
          ASSETSUBTYPE,          --�ʲ�С��
          BUSINESSTYPESTD,          --Ȩ�ط�ҵ������
          EXPOCLASSSTD,          --Ȩ�ط���¶����
          EXPOSUBCLASSSTD,          --Ȩ�ط���¶С��
          EXPOCLASSIRB,          --��������¶����
          EXPOSUBCLASSIRB,          --��������¶С��
          BOOKTYPE,          --�˻����
          REPOTRANFLAG,          --�ع����ױ�ʶ
          CLAIMSLEVEL,          --ծȨ����
          ORIGINALMATURITY,          --ԭʼ����
          PRINCIPAL,          --���屾��
          IRATING,          --�ڲ�����
          PD,          --ΥԼ����
          GROUPID         --������
     )
    SELECT  DATADATE,          --��������
          DATANO,          --������ˮ��
          VALIDNETAGREEMENTID,          --��Ч�������Э��ID
          COUNTERPARTYID,          --���׶���ID
          ORGSORTNO,          --���������
          ORGID,          --��������ID
          ORGNAME,          --������������
          INDUSTRYID,          --������ҵ����
          INDUSTRYNAME,          --������ҵ����
          BUSINESSLINE,          --����
          ASSETTYPE,          --�ʲ�����
          ASSETSUBTYPE,          --�ʲ�С��
          BUSINESSTYPESTD,          --Ȩ�ط�ҵ������
          EXPOCLASSSTD,          --Ȩ�ط���¶����
          EXPOSUBCLASSSTD,          --Ȩ�ط���¶С��
          EXPOCLASSIRB,          --��������¶����
          EXPOSUBCLASSIRB,          --��������¶С��
          BOOKTYPE,          --�˻����
          REPOTRANFLAG,          --�ع����ױ�ʶ
          CLAIMSLEVEL,          --ծȨ����
          ORIGINALMATURITY,          --ԭʼ����
          PRINCIPAL,          --���屾��
          IRATING,          --�ڲ�����
          PD,          --ΥԼ����
          GROUPID         --������
         FROM  RWA_YSP_OTCNETTING T
        WHERE T.DATANO = p_data_dt_str
      ;
  COMMIT;

  DBMS_STATS.GATHER_TABLE_STATS(OWNNAME => 'RWA_DEV',
                                TABNAME => 'RWA_EI_OTCNETTING',
                                CASCADE => TRUE);

  /*Ŀ�������ͳ��*/
  --ͳ�Ʋ���ļ�¼��
  SELECT COUNT(1) INTO V_COUNT FROM RWA_DEV.RWA_EI_OTCNETTING;
  --Dbms_output.Put_line('RWA_DEV.RWA_TZ_CONTRACT��ǰ����Ĳ���ϵͳ-Ӧ�տ�Ͷ�����ݼ�¼Ϊ: ' || (v_count1 - v_count) || ' ��');
  --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

  P_PO_RTNCODE := '1';
  P_PO_RTNMSG  := '�ɹ�' || '-' || V_COUNT;
  --�����쳣
EXCEPTION
  WHEN OTHERS THEN
    --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
    ROLLBACK;
    P_PO_RTNCODE := SQLCODE;
    P_PO_RTNMSG  := '��ͬ��Ϣ(' || V_PRO_NAME || ')ETLת��ʧ�ܣ�' || SQLERRM ||
                    ';��������Ϊ:' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    RETURN;
END PRO_RWA_EI_OTCNETTING;
/

