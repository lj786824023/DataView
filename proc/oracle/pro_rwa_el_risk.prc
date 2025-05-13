CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EL_RISK(
                                            p_data_dt_str   IN    VARCHAR2,   --��������
                                            p_po_rtncode    OUT   VARCHAR2,   --���ر��
                                            p_po_rtnmsg     OUT   VARCHAR2    --��������
                                           )
  /*
    �洢��������:rwa_dev.pro_rwa_el_risk
    ʵ�ֹ���:���ܱ�-�������ݼ�¼��,��ṹΪ�������ݼ�¼��
    ���ݿھ�:����
    ����Ƶ��:��ĩ
    ��  ��  :V1.0.0
    ��д��  :qpzhong
    ��дʱ��:2016-10-11
    ��  λ   :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��   :��
    Ŀ���   :rwa.rwa_el_risk|�������ݼ�¼��
    ������   :��
    �����¼(�޸���|�޸�ʱ��|�޸�����)��
  */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'rwa_dev.pro_rwa_el_risk';
  --�����ж�ֵ����
  v_count INTEGER;
  --�����쳣����
  v_raise EXCEPTION;

  v_cnt integer;

  BEGIN
    Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));


    SELECT COUNT(1) INTO v_cnt from RWA.rwa_el_risk where datadate = TO_DATE(p_data_dt_str,'YYYY-MM-DD');

    IF V_CNT = 0 THEN

    --2.���������������ݴ�Դ����뵽Ŀ�����
    /*����Ŀ���*/
    INSERT INTO rwa.rwa_el_risk(
                       datadate                                    --��������
                       ,datano                                     --������ˮ��
                       ,creditvalidatestate                        --���÷���У��״̬
                       ,creditexpodeterflag                        --���÷��ձ�¶�϶���ʶ
                       ,creditexpodeteruserid                      --���÷��ձ�¶�϶���ID
                       ,creditexpodeterorgid                       --���÷��ձ�¶�϶�����ID
                       ,creditexpodetertime                        --���÷��ձ�¶�϶�ʱ��
                       ,creditconfirmflag                          --���÷���ȷ�ϱ�ʶ
                       ,creditconfirmuserid                        --���÷���ȷ����ID
                       ,creditconfirmorgid                         --���÷���ȷ�ϻ���ID
                       ,creditconfirmtime                          --���÷���ȷ��ʱ��
                       ,creditgroupflag                            --���÷��շ����ʶ
                       ,marketvalidatestate                        --�г�����У��״̬
                       ,marketconfirmflag                          --�г�����ȷ�ϱ�ʶ
                       ,marketconfirmuserid                        --�г�����ȷ����ID
                       ,marketconfirmorgid                         --�г�����ȷ�ϻ���ID
                       ,marketconfirmtime                          --�г�����ȷ��ʱ��
                       ,operatevalidatestate                       --��������У��״̬
                       ,operateconfirmflag                         --��������ȷ�ϱ�ʶ
                       ,operateconfirmuserid                       --��������ȷ����ID
                       ,operateconfirmorgid                        --��������ȷ�ϻ���ID
                       ,operateconfirmtime                         --��������ȷ��ʱ��
                       ,capitalconfirmflag                         --�����ʱ�ȷ�ϱ�ʶ
                       ,capitalconfirmuserid                       --�����ʱ�ȷ����ID
                       ,capitalconfirmorgid                        --�����ʱ�ȷ�ϻ���ID
                       ,capitalconfirmtime                         --�����ʱ�ȷ��ʱ��
                       ,consolidateconfirmflag                     --�ӹ�˾�����ϴ�ȷ��
                       ,consolidateconfirmuserid                   --�ӹ�˾�����ϴ�ȷ����ID
                       ,consolidateconfirmorgid                    --�ӹ�˾�����ϴ�ȷ�ϻ���
                       ,consolidateconfirmtime                     --�ӹ�˾�����ϴ�ȷ��ʱ��
      )
      SELECT
                       TO_DATE(p_data_dt_str,'YYYYMMDD')                      AS datadate                                             --��������
                       ,p_data_dt_str                                         AS datano                                     --������ˮ��
                       ,''                                                    AS creditvalidatestate                        --���÷���У��״̬
                       ,'0'                                                   AS creditexpodeterflag                        --���÷��ձ�¶�϶���ʶ               (Ĭ��Ϊ��,1��0��)
                       ,''                                                    AS creditexpodeteruserid                      --���÷��ձ�¶�϶���ID
                       ,''                                                    AS creditexpodeterorgid                       --���÷��ձ�¶�϶�����ID
                       ,''                                                    AS creditexpodetertime                        --���÷��ձ�¶�϶�ʱ��
                       ,'0'                                                   AS creditconfirmflag                          --���÷���ȷ�ϱ�ʶ                     (Ĭ��Ϊ��,1��0��)
                       ,''                                                    AS creditconfirmuserid                        --���÷���ȷ����ID
                       ,''                                                    AS creditconfirmorgid                         --���÷���ȷ�ϻ���ID
                       ,''                                                    AS creditconfirmtime                          --���÷���ȷ��ʱ��
                       ,''                                                    AS creditgroupflag                            --���÷��շ����ʶ
                       ,''                                                    AS marketvalidatestate                        --�г�����У��״̬
                       ,'0'                                                   AS marketconfirmflag                          --�г�����ȷ�ϱ�ʶ                     (Ĭ��Ϊ��,1��0��)
                       ,''                                                    AS marketconfirmuserid                        --�г�����ȷ����ID
                       ,''                                                    AS marketconfirmorgid                         --�г�����ȷ�ϻ���ID
                       ,''                                                    AS marketconfirmtime                          --�г�����ȷ��ʱ��
                       ,''                                                    AS operatevalidatestate                       --��������У��״̬
                       ,'0'                                                   AS operateconfirmflag                         --��������ȷ�ϱ�ʶ                     (Ĭ��Ϊ��,1��0��)
                       ,''                                                    AS operateconfirmuserid                       --��������ȷ����ID
                       ,''                                                    AS operateconfirmorgid                        --��������ȷ�ϻ���ID
                       ,''                                                    AS operateconfirmtime                         --��������ȷ��ʱ��
                       ,'0'                                                   AS capitalconfirmflag                         --�����ʱ�ȷ�ϱ�ʶ                     (Ĭ��Ϊ��,1��0��)
                       ,''                                                    AS capitalconfirmuserid                       --�����ʱ�ȷ����ID
                       ,''                                                    AS capitalconfirmorgid                        --�����ʱ�ȷ�ϻ���ID
                       ,''                                                    AS capitalconfirmtime                         --�����ʱ�ȷ��ʱ��
                       ,'0'                                                   AS consolidateconfirmflag                     --�ӹ�˾�����ϴ�ȷ��
                       ,''                                                    AS consolidateconfirmuserid                   --�ӹ�˾�����ϴ�ȷ����ID
                       ,''                                                    AS consolidateconfirmorgid                    --�ӹ�˾�����ϴ�ȷ�ϻ���
                       ,''                                                    AS consolidateconfirmtime                     --�ӹ�˾�����ϴ�ȷ��ʱ��
      FROM DUAL
      ;
      COMMIT;

      END IF;
    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count FROM rwa.rwa_el_risk   WHERE   datadate = TO_DATE(p_data_dt_str,'YYYYMMDD');
    Dbms_output.Put_line('rwa.rwa_el_risk��ǰ��������ݼ�¼Ϊ:' || v_count || '��');
    Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

      p_po_rtncode := '1';
      p_po_rtnmsg  := '�ɹ�';
      --�����쳣
      EXCEPTION
      WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
      ROLLBACK;
      p_po_rtncode := sqlcode;
      p_po_rtnmsg  := '���ܱ�-�������ݼ�¼��(RWA_DEV.PRO_RWA_EL_RISK)ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
      RETURN;

END pro_rwa_el_risk;
/

