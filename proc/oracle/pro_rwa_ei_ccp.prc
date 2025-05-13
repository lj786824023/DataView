CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_CCP(
			 											p_data_dt_str	IN	VARCHAR2,		--��������
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_EI_CCP
    ʵ�ֹ���:���뽻�׶��ֱ�,�������뽻�׶��ֱ�
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�
    ��  ��  :V1.0.0
    ��д��  :CHENGANG
    ��дʱ��:2018-04-19
    ��  λ	:�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1	:RWA_DEV.RWA_YSP_CCP|���뽻�׶��ֱ�
    Ŀ���  :RWA_DEV.RWA_EI_CCP|���뽻�׶��ֱ�
    ������  :��
    �����¼(�޸���|�޸�ʱ��|�޸�����)��
    PXL  2019/05/08  �ñ��޷���  ������������sql
    
  	*/
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_CCP';
  --�����ж�ֵ����
  v_count INTEGER;
  --�����쳣����
  v_raise EXCEPTION;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --�����ʷ����
    DELETE FROM RWA_DEV.RWA_EI_CCP WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYY/MM/DD');
    
    COMMIT;


    /*�������뽻�׶��ֱ�*/
    INSERT INTO RWA_DEV.RWA_EI_CCP(
            DATADATE,          --��������
            DATANO,          --������ˮ��
            CCPID,          --���뽻�׶���ID
            CCPNAME,          --���뽻�׶�������
            QUALCCPFLAG,          --�Ƿ�ϸ����뽻�׶���
            ORGSORTNO,          --���������
            ORGID,          --��������ID
            ORGNAME,          --������������
            INDUSTRYID,          --������ҵ����
            INDUSTRYNAME,          --������ҵ����
            BUSINESSLINE,          --����
            ASSETTYPE,          --�ʲ�����
            ASSETSUBTYPE,          --�ʲ�С��
            DEFAULTFUND          --ΥԼ����
                          )
            SELECT DATADATE     AS DATADATE, --��������
                   DATANO       AS DATANO, --������ˮ��
                   CCPID        AS CCPID, --���뽻�׶���ID
                   CCPNAME      AS CCPNAME, --���뽻�׶�������
                   QUALCCPFLAG  AS QUALCCPFLAG, --�Ƿ�ϸ����뽻�׶���
                   ORGSORTNO    AS ORGSORTNO, --���������
                   ORGID        AS ORGID, --��������ID
                   ORGNAME      AS ORGNAME, --������������
                   INDUSTRYID   AS INDUSTRYID, --������ҵ����
                   INDUSTRYNAME AS INDUSTRYNAME, --������ҵ����
                   BUSINESSLINE AS BUSINESSLINE, --����
                   ASSETTYPE    AS ASSETTYPE, --�ʲ�����
                   ASSETSUBTYPE AS ASSETSUBTYPE, --�ʲ�С��
                   DEFAULTFUND  AS DEFAULTFUND --ΥԼ����
              FROM RWA_DEV.RWA_YSP_CCP
             WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD');
   
    COMMIT;


    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_CCP WHERE DATANO = p_data_dt_str;
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_CCP��ǰ��������ݼ�¼Ϊ:' || v_count1 || '��');

    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�'||'-'||v_count;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '���뽻�׶��ֱ�('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_EI_CCP;
/

