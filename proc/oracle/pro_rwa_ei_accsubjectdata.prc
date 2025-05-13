CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_ACCSUBJECTDATA(p_data_dt_str  IN  VARCHAR2, --��������
                                                      p_po_rtncode   OUT VARCHAR2, --���ر��
                                                      p_po_rtnmsg    OUT VARCHAR2  --��������
)
  /*
    �洢��������:RWA_DEV.PRO_RWA_EI_ACCSUBJECTDATA
    ʵ�ֹ���:����������(�ӹ���)��Ϣ�����Ŀȡ������)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :QHJIANG
    ��дʱ��:2016-06-28
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.FNS_GL_BALANCE|��������(�ӹ���)
    Դ  ��2 :RWA.CODE_LIBRARY|�����
    Ŀ���1 :RWA_DEV.RWA_EI_ACCSUBJECTDATA|��Ŀȡ����
    ������  :��
    ע��    ��Ŀǰ��Ҫ���¿�Ŀ��
                          4001 �ɱ� 4201 ���� 4002 �ʱ����� 4101 ӯ�๫��
                          4102 һ�����׼�� 4103 �������� 4104 ������� 6011 ��Ϣ����
                          6021 ���������� 6051 ����ҵ������ 6061 ������� 6101 ���ʼ�ֵ�䶯����
                          6111 Ͷ������ 6301 Ӫҵ������ 6402 ����ҵ��֧�� 6403 Ӫҵ˰�𼰸���
                          6411 ��Ϣ֧�� 6421 ������֧�� 6602 ������� 6701 �ʲ���ֵ��ʧ
                          6711 Ӫҵ��֧�� 6801 ����˰ 6901 ��ǰ���������� 1811 ��������˰�ʲ�
                          1304 ������ʧ׼��
                          ����6112 �ʲ���������-�̶��ʲ�-�����豸 6113��������-��������  
                          4003�����ۺ����棨�ų� 40030102--�����ۺ�����-�����ʲ���ֵ׼����
                         
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    
    */

  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_ACCSUBJECTDATA';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ�����ļ�¼��
  v_count INTEGER :=0;
  --���嵱ǰ����ļ�¼��
  v_count1 INTEGER;

  BEGIN

    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --���Ŀ����е�ԭ�м�¼
    --EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_EI_ACCSUBJECTDATA';
    DELETE FROM RWA_DEV.RWA_EI_ACCSUBJECTDATA WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');


    --DBMS_OUTPUT.PUT_LINE('��ʼ�����롾��Ŀȡ����' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    
    DECLARE
        v_insert_sql VARCHAR2(300);
    --ͬ���α��ȡ��Ҫ���ж�����
    CURSOR cur_cursor IS
        SELECT ITEMNO,ITEMNAME
          FROM RWA.CODE_LIBRARY 
         WHERE CODENO='AccSubjectData' AND ISINUSE='1';
        c_cursor cur_cursor%rowtype; 
    BEGIN
    --�����α�
    OPEN cur_cursor;
    --ͨ��ѭ�������������α�
    --DBMS_OUTPUT.PUT_LINE('>>>>>>Insert��俪ʼִ����>>>>>>>');
    LOOP
        FETCH cur_cursor INTO c_cursor;
        --���α������ɺ��˳��α�
        EXIT WHEN cur_cursor%notfound;
        v_count := v_count + 1;
        --����sql
        v_insert_sql:='INSERT INTO RWA_DEV.RWA_EI_ACCSUBJECTDATA(DATADATE,DATANO,SUBJECTCODE,SUBJECTNAME) VALUES(TO_DATE('||p_data_dt_str||',''yyyyMMdd''),'||p_data_dt_str||','''||c_cursor.ITEMNO||''','''||c_cursor.ITEMNAME||''')';
        --DBMS_OUTPUT.PUT_LINE(v_insert_sql);
        --ִ��sql
        EXECUTE IMMEDIATE v_insert_sql;
        --COMMIT;
        --����ѭ��
    END LOOP;
    --DBMS_OUTPUT.PUT_LINE('�������ܼ�¼Ϊ��'|| v_count);
    --DBMS_OUTPUT.PUT_LINE('Insert����Ѿ�ִ�н���������');
    --�ر��α�
    CLOSE cur_cursor;
    END;
    
    --G4A �е�1.7���⴦��
    UPDATE RWA_DEV.RWA_EI_ACCSUBJECTDATA REA
       SET REA.SUBJECTBALANCE = NVL((SELECT SUM(CASE WHEN CL.ATTRIBUTE8='D-C' THEN FGB.BALANCE_D-FGB.BALANCE_C
                                                     WHEN CL.ATTRIBUTE8='C-D' THEN FGB.BALANCE_C-FGB.BALANCE_D
                                                     ELSE FGB.BALANCE_D-FGB.BALANCE_C END) AS SUBJECTBALANCE --��Ŀ���
                                       FROM RWA_DEV.FNS_GL_BALANCE FGB
                                       LEFT JOIN RWA.CODE_LIBRARY CL 
                                         ON CL.CODENO='NewSubject'
                                        AND FGB.SUBJECT_NO=CL.ITEMNO
                                        AND CL.ISINUSE='1'
                                      WHERE FGB.DATANO = p_data_dt_str
                                        AND FGB.CURRENCY_CODE = 'RMB'
                                        AND FGB.Subject_No<>'40030102'--�����ۺ�����-�����ʲ���ֵ׼��
                                        AND FGB.SUBJECT_NO LIKE REA.SUBJECTCODE||'%'),0)
     WHERE REA.DATANO = p_data_dt_str;
    COMMIT;
    
    --G4A-1��1 ��Ҫ���������ų���
    UPDATE RWA_DEV.RWA_EI_ACCSUBJECTDATA REA
       SET REA.SUBJECTBALANCE=REA.SUBJECTBALANCE+(SELECT SUM(FGB.BALANCE_C-FGB.BALANCE_D)
                                                  FROM RWA_DEV.FNS_GL_BALANCE FGB
                                                  WHERE FGB.DATANO = p_data_dt_str
                                                  AND FGB.CURRENCY_CODE = 'RMB'
                                                  AND FGB.Subject_No='40030102'
                                                         )
    WHERE REA.DATANO=p_data_dt_str AND REA.subjectcode='1304';
    COMMIT;
    --DBMS_OUTPUT.PUT_LINE('���������롾��Ŀȡ����' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_EI_ACCSUBJECTDATA;
    --DBMS_OUTPUT.PUT_LINE('RWA_DEV.RWA_EI_ACCSUBJECTDATA-��Ŀȡ�����в�������Ϊ��' || v_count1 || '��');

    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�'||'-'||v_count1;

    --�����쳣
    EXCEPTION WHEN OTHERS THEN
        --DBMS_OUTPUT.PUT_LINE('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '���롾��Ŀȡ����('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;

    RETURN;

END PRO_RWA_EI_ACCSUBJECTDATA;
/

