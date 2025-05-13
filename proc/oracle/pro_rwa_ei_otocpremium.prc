CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_OTOCPREMIUM(p_data_dt_str  IN  VARCHAR2, --��������
                                                      p_po_rtncode   OUT VARCHAR2, --���ر��
                                                      p_po_rtnmsg    OUT VARCHAR2  --��������
)
  /*
    �洢��������:RWA_DEV.PRO_RWA_EI_OTOCPREMIUM
    ʵ�ֹ���:����������(�ӹ���)��Ϣ��������һ���ʱ����߼�����۱���
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2018-01-25
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.FNS_GL_BALANCE|��������(�ӹ���)
    Դ  ��2 :RWA.CODE_LIBRARY|�����
    Ŀ���1 :RWA_DEV.RWA_EI_OTOCPREMIUM|����һ���ʱ����߼�����۱�
    ������  :��
    ע��    ��Ŀǰ��Ҫ���¿�Ŀ��
                          44010101 ����һ���ʱ�-���ȹɼ������
    �����¼(�޸���|�޸�ʱ��|�޸�����):

    */

  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_OTOCPREMIUM';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ�����ļ�¼��
  v_count INTEGER :=0;
  --���嵱ǰ����ļ�¼��
  v_count1 INTEGER;

  BEGIN

    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --���Ŀ����е�ԭ�м�¼
    --EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_EI_OTOCPREMIUM';
    DELETE FROM RWA_DEV.RWA_EI_OTOCPREMIUM WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');


    --DBMS_OUTPUT.PUT_LINE('��ʼ�����롾��Ŀȡ����' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));


	INSERT INTO RWA_DEV.RWA_EI_OTOCPREMIUM(
				DATADATE          		--��������
				,DATANO            		--�����ڴ�
				,SERIALNO          		--��ˮ��
				,PREFERREDPREMIUM  		--���ȹɼ������
				,OTHERTOOLSPREMIUM 		--�������߼������
				,INPUTUSERID       		--�Ǽ���ID
				,INPUTORGID        		--�Ǽǻ���ID
				,INPUTTIME         		--�Ǽ�ʱ��
				,UPDATEUSERID      		--������ID
				,UPDATEORGID       		--���»���ID
				,UPDATETIME        		--����ʱ��
	)
    WITH TMP_44010101 AS (
		SELECT SUM(BALANCE_C-BALANCE_D) AS BAL FROM RWA_DEV.FNS_GL_BALANCE WHERE SUBJECT_NO = '44010101' AND CURRENCY_CODE = 'RMB' AND DATANO = p_data_dt_str
	)
    SELECT
				TO_DATE(p_data_dt_str,'YYYYMMDD')				AS DATADATE          		--��������
				,p_data_dt_str									AS DATANO            		--�����ڴ�
				,p_data_dt_str || 'OTO01'						AS SERIALNO          		--��ˮ��
				,T1.BAL											AS PREFERREDPREMIUM  		--���ȹɼ������
				,0												AS OTHERTOOLSPREMIUM 		--�������߼������
				,'SYSTEM'										AS INPUTUSERID       		--�Ǽ���ID				Ĭ�� SYSTEM
				,'01000000'										AS INPUTORGID        		--�Ǽǻ���ID			Ĭ�� 01000000-��������/���в���
				,TO_CHAR(SYSDATE,'YYYY/MM/DD HH24:MI:SS')		AS INPUTTIME         		--�Ǽ�ʱ��
				,''												AS UPDATEUSERID      		--������ID
				,''												AS UPDATEORGID       		--���»���ID
				,''												AS UPDATETIME        		--����ʱ��
	FROM		TMP_44010101 T1
	;

    COMMIT;
    --DBMS_OUTPUT.PUT_LINE('���������롾��Ŀȡ����' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_EI_OTOCPREMIUM WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --DBMS_OUTPUT.PUT_LINE('RWA_DEV.RWA_EI_OTOCPREMIUM���в�������Ϊ��' || v_count1 || '��');

    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�'||'-'||v_count1;

    --�����쳣
    EXCEPTION WHEN OTHERS THEN
        --DBMS_OUTPUT.PUT_LINE('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '���롾����һ���ʱ����߼�����۱�('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;

    RETURN;

END PRO_RWA_EI_OTOCPREMIUM;
/

