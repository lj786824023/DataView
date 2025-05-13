CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_CD_MK_SECURITIESTYPE(
														p_data_dt_str IN  VARCHAR2, --��������
                            p_po_rtncode  OUT VARCHAR2, --���ر��
                            p_po_rtnmsg   OUT VARCHAR2  --��������
)
  /*
    �洢��������:RWA_DEV.PRO_RWA_CD_MK_SECURITIESTYPE
    ʵ�ֹ���:���ϵͳ-�г�����-���ܱ�׼����¶��-�ӹ�֤ȯ����ֶ�
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :QHJIANG
    ��дʱ��:2016-04-14
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1  :RWA_DEV.RWA_EI_MARKETEXPOSURESTD|���ܱ�׼����¶��
    Դ  ��2  :RWA_DEV.RWA_EI_ISSUERRATING|������������Ϣ���ܱ�
    Ŀ���1 :RWA_DEV.RWA_EI_MARKETEXPOSURESTD|���ܱ�׼����¶��
    ������  :��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    pxl 20190910 �����ϸ�ծȯ�ж��߼�
    */

  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_CD_MK_SECURITIESTYPE';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN

    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    --DBMS_OUTPUT.PUT_LINE('��ʼ���ӹ����г����ձ�׼�����ձ�¶��-֤ȯ����ֶΡ�' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    /**
     * �ӹ��߼���
     * 1��01 ����֤ȯ���������������������������з��еĸ���ծȯ�Ͷ������ʹ��ߡ�
     *    ��:RWA_EI_MARKETEXPOSURESTD.ISSUERSUBTYPE in  0101	�й���������
     *                                                  0102	������Ȩ���һ򾭼�ʵ���������������
     *                                                  0103	�й���������
     *                                                  0104	������������
     * 2��02 �ϸ�֤ȯ������
     *   ��1����߿������С������������к͹��ʻ��һ�����֯���е�ծȯ��
     *   ��:RWA_EI_MARKETEXPOSURESTD.ISSUERSUBTYPE in(0207	��߿������С������������к͹��ʻ��һ�����֯)
     *   ��2���ҹ���������ʵ�����ҵ���� ���е�ծȯ��
     *   ��:RWA_EI_MARKETEXPOSURESTD.ISSUERSUBTYPE in(0105 �й��������� 0202 �й���ҵ���� 0106 �й��ط�����)
     *   ��3�����������Һϸ��ⲿ����������ΪͶ�ʼ���BB+���ϣ��ķ������巢�е�ծȯ��
     *        ��ǰ���� ���� 7.31������ȡ������������Ϊ��ȥ��/01/01 - ��ǰ���ڡ���
     *        ��ǰ���� ���� 7.31������ȡ������������Ϊ������/01/01 - ��ǰ���ڡ�
     *   ��: RWA_EI_ISSUERRATING.RatingResult>BB+
     * 3 09 ����֤ȯ ʣ�µ�Ĭ��
     */
	UPDATE RWA_DEV.RWA_EI_MARKETEXPOSURESTD T
	SET    T.SECURITIESTYPE = (CASE WHEN T.ISSUERSUBTYPE IN('0101','0102','0103','0104')
	                                THEN '01'
	                                WHEN T.ISSUERSUBTYPE='0207'
	                                THEN '02'
	                                WHEN T.ISSUERSUBTYPE IN('0105','0106','0202')
	                                THEN '02'
	                                WHEN (SELECT COUNT(DISTINCT T1.RATINGORG)
	                                      FROM RWA_DEV.RWA_EI_ISSUERRATING T1
	                                      WHERE T1.ISSUERID=T.ISSUERID
	                                      AND T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
                                        --�����������߼�  ������ҵ�����۵��� pxl  20190910
	                                      /*AND (TO_DATE(p_data_dt_str,'YYYYMMDD')<TO_DATE(SUBSTR(p_data_dt_str,0,4)||'0731','YYYYMMDD')              --����7��31��
	                                           AND TO_DATE(RATINGDATE,'YYYY-MM-DD')<=TO_DATE(p_data_dt_str,'YYYYMMDD')              								--���������ڡ�ȥ��/01/01 - ��ǰ���ڡ�
	                                           AND (TO_DATE(RATINGDATE,'YYYY-MM-DD')>=add_months(TO_DATE(SUBSTR(p_data_dt_str,0,4)||'0101','YYYYMMDD'),-12)
	                                           OR
	                                           TO_DATE(p_data_dt_str,'YYYYMMDD')>=TO_DATE(SUBSTR(p_data_dt_str,0,4)||'0731','YYYYMMDD'))             --����7��31��
	                                           AND TO_DATE(RATINGDATE,'YYYY-MM-DD')<=TO_DATE(p_data_dt_str,'YYYYMMDD')                              --���������ڡ�����/01/01 - ��ǰ���ڡ�
	                                           AND TO_DATE(RATINGDATE,'YYYY-MM-DD')>=TO_DATE(SUBSTR(p_data_dt_str,0,4)||'0101','YYYYMMDD')
	                                          )
	                                      AND T1.RATINGRESULT<'0111'*/)>=2
	                                THEN '02'
	                                ELSE '09' END)
	WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
	;
	COMMIT;

    --DBMS_OUTPUT.PUT_LINE('�������ӹ����г����ձ�׼�����ձ�¶��-֤ȯ����ֶΡ�' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    SELECT COUNT(1)  INTO v_count FROM RWA_DEV.RWA_EI_MARKETEXPOSURESTD WHERE SECURITIESTYPE IS NOT NULL AND DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�' || '-' || v_count;

    --�����쳣
    EXCEPTION WHEN OTHERS THEN
        --DBMS_OUTPUT.PUT_LINE('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '�ӹ��г����ձ�׼�����ձ�¶��-֤ȯ����ֶ�('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;

    RETURN;

END PRO_RWA_CD_MK_SECURITIESTYPE;
/

