CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_XD_ACCEPTOR_WSIB(
			 											p_data_dt_str	IN	VARCHAR2,		--�������� yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--���ر�� 1 �ɹ�,0 ʧ��
														p_po_rtnmsg		OUT	VARCHAR2		--��������
				)
  /*
    �洢��������:RWA_DEV.PRO_RWA_XD_ACCEPTOR_WSIB
    ʵ�ֹ���:�Ŵ�ϵͳ-�ж���-��¼�̵�(������Դ�Ŵ�ϵͳ��ҵ�������Ϣȫ������RWA�Ŵ��ж��˲�¼�̵ױ���)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-06-20
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.NCM_BUSINESS_DUEBILL|����ҵ������Ϣ��
    Դ  ��2 :RWA.ORG_INFO|������Ϣ��
    Ŀ���1 :RWA.RWA_WSIB_XD_ACCEPTOR|�Ŵ��ж��˲�¼�̵ױ�
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_XD_ACCEPTOR_WSIB';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    --����Ŵ��ж����̵ױ�
    DELETE FROM RWA.RWA_WSIB_XD_ACCEPTOR WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    COMMIT;

    --2.���������������ݴ�Դ����뵽Ŀ�����
    --2.1�Ŵ�ϵͳ-Ʊ������ҵ��
    INSERT INTO RWA.RWA_WSIB_XD_ACCEPTOR(
                DATADATE                               --��������
                ,ORGID             										 --����ID
                ,BDSERIALNO        										 --��ݱ��
                ,CONTRACTNO        										 --��ͬ���
                ,BUSINESSTYPE      										 --ҵ��Ʒ��
                ,BILLNO            										 --Ʊ�ݱ��
                ,ACCEPTOR          										 --�ж���/�ж���ҵ����
                ,ACCEPTORGCODE     										 --�ж���/�ж���ҵ��֯��������
                ,ACCEPTCOUNTRYCODE 										 --�ж���/�ж���ҵע����Ҵ���
                ,ACCEPTINDUSTRYID  										 --�ж���/�ж���ҵ������ҵ����
                ,ACCEPTSCOPE       										 --�ж���ҵ��ģ
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,CASE WHEN T2.SORTNO LIKE '1020010700%' THEN '02017000'									 --�ɶ�����/��˾ҵ��
                			WHEN T2.SORTNO LIKE '1020020700%' THEN '02027000'									 --��������/��˾���в�
                      WHEN T2.SORTNO LIKE '1020030700%' THEN '02037000'                   --��������/��˾ҵ��
                      WHEN T2.SORTNO LIKE '1030020700%' THEN '03027000'                   --������������/��˾���в�
                      WHEN T2.SORTNO LIKE '1030030700%' THEN '03037000'                   --���з���/��˾���в�
                      ELSE '01270000'                                                     --���й�˾���в�
                 END                                           AS ORGID                     --����ID              ���ղ�¼������������Ĭ��Ϊ���н���ͬҵ����(01370000)
                ,T1.SERIALNO                                  AS BDSERIALNO               --��ݱ��
                ,T1.RELATIVESERIALNO2                         AS CONTRACTNO               --��ͬ���
                ,T1.BUSINESSTYPE                              AS BUSINESSTYPE             --ҵ��Ʒ��
                ,T1.BILLNO                                     AS BILLNO                   --Ʊ�ݱ��
                ,''                                             AS ACCEPTOR                 --�ж���/�ж���ҵ����
                ,''                                            AS ACCEPTORGCODE            --�ж���/�ж���ҵ��֯��������
                ,''                                           AS ACCEPTCOUNTRYCODE        --�ж���/�ж���ҵע����Ҵ���
                ,''                                           AS ACCEPTINDUSTRYID         --�ж���/�ж���ҵ������ҵ����
                ,''                                           AS ACCEPTSCOPE              --�ж���ҵ��ģ

    FROM        RWA_DEV.NCM_BUSINESS_DUEBILL T1                              --����ҵ������Ϣ��
    LEFT JOIN    RWA.ORG_INFO T2
    ON          T1.OPERATEORGID = T2.ORGID
    WHERE       T1.BALANCE > 0                                            --������0
    AND         (T1.FINISHDATE IS NULL OR  T1.FINISHDATE = '')            --δ�������Ч���
    AND         T1.BUSINESSTYPE IN ('104010','104020')                    --104010=���гжһ�Ʊ���֣�104020=��ҵ�жһ�Ʊ����
    AND         T1.DATANO = p_data_dt_str
    ;

    COMMIT;

    --2.2�Ŵ�ϵͳ-����Ѻ��ҵ��
    INSERT INTO RWA.RWA_WSIB_XD_ACCEPTOR(
                DATADATE                               --��������
                ,ORGID                                  --����ID
                ,BDSERIALNO                             --��ݱ��
                ,CONTRACTNO                             --��ͬ���
                ,BUSINESSTYPE                           --ҵ��Ʒ��
                ,BILLNO                                 --Ʊ�ݱ��
                ,ACCEPTOR                               --�ж���/�ж���ҵ����
                ,ACCEPTORGCODE                          --�ж���/�ж���ҵ��֯��������
                ,ACCEPTCOUNTRYCODE                      --�ж���/�ж���ҵע����Ҵ���
                ,ACCEPTINDUSTRYID                       --�ж���/�ж���ҵ������ҵ����
                ,ACCEPTSCOPE                            --�ж���ҵ��ģ
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,CASE WHEN T2.SORTNO LIKE '1020010700%' THEN '02017000'                   --�ɶ�����/��˾ҵ��
                      WHEN T2.SORTNO LIKE '1020020700%' THEN '02027000'                   --��������/��˾���в�
                      WHEN T2.SORTNO LIKE '1020030700%' THEN '02037000'                   --��������/��˾ҵ��
                      WHEN T2.SORTNO LIKE '1030020700%' THEN '03027000'                   --������������/��˾���в�
                      WHEN T2.SORTNO LIKE '1030030700%' THEN '03037000'                   --���з���/��˾���в�
                      ELSE '01270000'                                                     --���й�˾���в�
                 END                                           AS ORGID                     --����ID              ���ղ�¼������������Ĭ��Ϊ���н���ͬҵ����(01370000)
                ,T1.SERIALNO                                  AS BDSERIALNO               --��ݱ��
                ,T1.RELATIVESERIALNO2                         AS CONTRACTNO               --��ͬ���
                ,T1.BUSINESSTYPE                              AS BUSINESSTYPE             --ҵ��Ʒ��
                ,''                                             AS BILLNO                   --Ʊ�ݱ��
                ,''                                             AS ACCEPTOR                 --�ж���/�ж���ҵ����
                ,''                                            AS ACCEPTORGCODE            --�ж���/�ж���ҵ��֯��������
                ,''                                           AS ACCEPTCOUNTRYCODE        --�ж���/�ж���ҵע����Ҵ���
                ,''                                           AS ACCEPTINDUSTRYID         --�ж���/�ж���ҵ������ҵ����
                ,''                                           AS ACCEPTSCOPE              --�ж���ҵ��ģ

    FROM        RWA_DEV.NCM_BUSINESS_DUEBILL T1                              --����ҵ������Ϣ��
    LEFT JOIN    RWA.ORG_INFO T2
    ON          T1.OPERATEORGID = T2.ORGID
    WHERE       T1.BALANCE > 0                                            --������0
    AND         (T1.FINISHDATE IS NULL OR  T1.FINISHDATE = '')            --δ�������Ч���
    AND         T1.BUSINESSTYPE = '105040'                                --105040=����Ѻ��
    AND         T1.DATANO = p_data_dt_str
    ;

    COMMIT;

    --2.3�Ŵ�ϵͳ-����ͥҵ��
    INSERT INTO RWA.RWA_WSIB_XD_ACCEPTOR(
                DATADATE                               --��������
                ,ORGID                                  --����ID
                ,BDSERIALNO                             --��ݱ��
                ,CONTRACTNO                             --��ͬ���
                ,BUSINESSTYPE                           --ҵ��Ʒ��
                ,BILLNO                                 --Ʊ�ݱ��
                ,ACCEPTOR                               --�ж���/�ж���ҵ����
                ,ACCEPTORGCODE                          --�ж���/�ж���ҵ��֯��������
                ,ACCEPTCOUNTRYCODE                      --�ж���/�ж���ҵע����Ҵ���
                ,ACCEPTINDUSTRYID                       --�ж���/�ж���ҵ������ҵ����
                ,ACCEPTSCOPE                            --�ж���ҵ��ģ
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,CASE WHEN T2.SORTNO LIKE '1020010700%' THEN '02017000'                   --�ɶ�����/��˾ҵ��
                      WHEN T2.SORTNO LIKE '1020020700%' THEN '02027000'                   --��������/��˾���в�
                      WHEN T2.SORTNO LIKE '1020030700%' THEN '02037000'                   --��������/��˾ҵ��
                      WHEN T2.SORTNO LIKE '1030020700%' THEN '03027000'                   --������������/��˾���в�
                      WHEN T2.SORTNO LIKE '1030030700%' THEN '03037000'                   --���з���/��˾���в�
                      ELSE '01270000'                                                     --���й�˾���в�
                 END                                           AS ORGID                     --����ID              ���ղ�¼������������Ĭ��Ϊ���н���ͬҵ����(01370000)
                ,T1.SERIALNO                                  AS BDSERIALNO               --��ݱ��
                ,T1.RELATIVESERIALNO2                         AS CONTRACTNO               --��ͬ���
                ,T1.BUSINESSTYPE                              AS BUSINESSTYPE             --ҵ��Ʒ��
                ,''                                             AS BILLNO                   --Ʊ�ݱ��
                ,''                                             AS ACCEPTOR                 --�ж���/�ж���ҵ����
                ,''                                            AS ACCEPTORGCODE            --�ж���/�ж���ҵ��֯��������
                ,''                                           AS ACCEPTCOUNTRYCODE        --�ж���/�ж���ҵע����Ҵ���
                ,''                                           AS ACCEPTINDUSTRYID         --�ж���/�ж���ҵ������ҵ����
                ,''                                           AS ACCEPTSCOPE              --�ж���ҵ��ģ

    FROM        RWA_DEV.NCM_BUSINESS_DUEBILL T1                              --����ҵ������Ϣ��
    LEFT JOIN    RWA.ORG_INFO T2
    ON          T1.OPERATEORGID = T2.ORGID
    WHERE       T1.BALANCE > 0                                            --������0
    AND         (T1.FINISHDATE IS NULL OR  T1.FINISHDATE = '')            --δ�������Ч���
    AND         T1.BUSINESSTYPE = '105100'                                --105100=����Ʊ�ݣ�����ͥ��
    AND         T1.DATANO = p_data_dt_str
    ;

    COMMIT;

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA.RWA_WSIB_XD_ACCEPTOR WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA.RWA_WSIB_XD_ACCEPTOR��ǰ������Ŵ�ϵͳ-�ж����̵����ݼ�¼Ϊ: ' || v_count || ' ��');



    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '�Ŵ��ж��˲�¼�����̵�('|| v_pro_name ||')����ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_XD_ACCEPTOR_WSIB;
/

